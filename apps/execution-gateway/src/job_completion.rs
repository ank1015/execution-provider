use crate::error::Result;
use sqlx::{PgPool, Postgres, Transaction, postgres::PgListener};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const CHANNEL: &str = "execution_gateway_job_terminal";
pub const MAX_WAITERS: usize = 1_024;
pub const MAX_WAITERS_PER_USER: usize = 64;

#[derive(Default)]
struct Waiters {
    by_job: HashMap<Uuid, HashMap<u64, Waiter>>,
    by_user: HashMap<Uuid, usize>,
    total: usize,
}

struct Waiter {
    user: Uuid,
    sender: oneshot::Sender<()>,
}

#[derive(Default)]
pub struct JobCompletions {
    next_id: AtomicU64,
    waiters: Mutex<Waiters>,
}

pub struct Subscription {
    job: Uuid,
    id: u64,
    receiver: oneshot::Receiver<()>,
    completions: Weak<JobCompletions>,
}

impl JobCompletions {
    pub fn subscribe(self: &Arc<Self>, user: Uuid, job: Uuid) -> Option<Subscription> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        let mut waiters = self.waiters.lock().expect("job completion waiters lock");
        if waiters.total >= MAX_WAITERS
            || waiters.by_user.get(&user).copied().unwrap_or_default() >= MAX_WAITERS_PER_USER
        {
            return None;
        }
        waiters
            .by_job
            .entry(job)
            .or_default()
            .insert(id, Waiter { user, sender });
        *waiters.by_user.entry(user).or_default() += 1;
        waiters.total += 1;
        Some(Subscription {
            job,
            id,
            receiver,
            completions: Arc::downgrade(self),
        })
    }

    fn notify(&self, job: Uuid) {
        let mut state = self.waiters.lock().expect("job completion waiters lock");
        let job_waiters = state.by_job.remove(&job);
        if let Some(job_waiters) = job_waiters {
            for waiter in job_waiters.values() {
                decrement_user(&mut state.by_user, waiter.user);
            }
            state.total -= job_waiters.len();
            drop(state);
            for (_, waiter) in job_waiters {
                let _ = waiter.sender.send(());
            }
        }
    }

    fn remove(&self, job: Uuid, id: u64) {
        let mut waiters = self.waiters.lock().expect("job completion waiters lock");
        if let Some(job_waiters) = waiters.by_job.get_mut(&job) {
            let removed = job_waiters.remove(&id);
            if job_waiters.is_empty() {
                waiters.by_job.remove(&job);
            }
            if let Some(removed) = removed {
                decrement_user(&mut waiters.by_user, removed.user);
                waiters.total -= 1;
            }
        }
    }
}

fn decrement_user(waiters: &mut HashMap<Uuid, usize>, user: Uuid) {
    if let Some(count) = waiters.get_mut(&user) {
        *count -= 1;
        if *count == 0 {
            waiters.remove(&user);
        }
    }
}

impl Subscription {
    pub async fn wait(&mut self, shutdown: &CancellationToken, timeout: Duration) {
        tokio::select! {
            _ = &mut self.receiver => {}
            _ = tokio::time::sleep(timeout) => {}
            _ = shutdown.cancelled() => {}
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(completions) = self.completions.upgrade() {
            completions.remove(self.job, self.id);
        }
    }
}

/// PostgreSQL delivers this wake-up hint only if the surrounding transaction commits.
pub async fn publish(tx: &mut Transaction<'_, Postgres>, job: Uuid) -> Result<()> {
    sqlx::query("SELECT pg_notify($1,$2)")
        .bind(CHANNEL)
        .bind(job.to_string())
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Establish LISTEN before HTTP admission starts, then fan out terminal job IDs locally.
pub async fn start(
    pool: PgPool,
    completions: Arc<JobCompletions>,
    shutdown: CancellationToken,
) -> Result<tokio::task::JoinHandle<()>> {
    let mut listener = PgListener::connect_with(&pool).await?;
    listener.listen(CHANNEL).await?;
    Ok(tokio::spawn(async move {
        listen(completions, shutdown, listener).await;
    }))
}

async fn listen(
    completions: Arc<JobCompletions>,
    shutdown: CancellationToken,
    mut listener: PgListener,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            notification = listener.recv() => match notification {
                Ok(notification) => {
                    if let Ok(job) = Uuid::parse_str(notification.payload()) {
                        completions.notify(job);
                    }
                }
                Err(_) => {
                    eprintln!("job completion listener disconnected; reconnecting");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{JobCompletions, MAX_WAITERS, MAX_WAITERS_PER_USER};
    use std::sync::Arc;
    use uuid::Uuid;

    #[tokio::test]
    async fn wakes_only_waiters_for_the_completed_job() {
        let completions = Arc::new(JobCompletions::default());
        let user = Uuid::new_v4();
        let first_job = Uuid::new_v4();
        let mut first = completions.subscribe(user, first_job).unwrap();
        let mut also_first = completions.subscribe(user, first_job).unwrap();
        let mut second = completions.subscribe(user, Uuid::new_v4()).unwrap();

        completions.notify(first_job);
        assert!((&mut first.receiver).await.is_ok());
        assert!((&mut also_first.receiver).await.is_ok());
        assert!(second.receiver.try_recv().is_err());
    }

    #[test]
    fn bounds_waiters_per_user_and_releases_capacity_on_drop() {
        let completions = Arc::new(JobCompletions::default());
        let user = Uuid::new_v4();
        let mut subscriptions = (0..MAX_WAITERS_PER_USER)
            .map(|_| completions.subscribe(user, Uuid::new_v4()).unwrap())
            .collect::<Vec<_>>();
        assert!(completions.subscribe(user, Uuid::new_v4()).is_none());
        subscriptions.pop();
        assert!(completions.subscribe(user, Uuid::new_v4()).is_some());
    }

    #[test]
    fn bounds_waiters_across_users_and_releases_capacity_on_completion() {
        let completions = Arc::new(JobCompletions::default());
        let job = Uuid::new_v4();
        let mut subscriptions = Vec::new();
        for index in 0..MAX_WAITERS {
            let user = Uuid::from_u128((index / MAX_WAITERS_PER_USER + 1) as u128);
            subscriptions.push(completions.subscribe(user, job).unwrap());
        }
        assert!(
            completions
                .subscribe(Uuid::new_v4(), Uuid::new_v4())
                .is_none()
        );
        completions.notify(job);
        assert!(
            completions
                .subscribe(Uuid::new_v4(), Uuid::new_v4())
                .is_some()
        );
    }
}
