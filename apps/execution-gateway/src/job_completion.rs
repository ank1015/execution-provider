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

#[derive(Default)]
pub struct JobCompletions {
    next_id: AtomicU64,
    waiters: Mutex<HashMap<Uuid, HashMap<u64, oneshot::Sender<()>>>>,
}

pub struct Subscription {
    job: Uuid,
    id: u64,
    receiver: oneshot::Receiver<()>,
    completions: Weak<JobCompletions>,
}

impl JobCompletions {
    pub fn subscribe(self: &Arc<Self>, job: Uuid) -> Subscription {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.waiters
            .lock()
            .expect("job completion waiters lock")
            .entry(job)
            .or_default()
            .insert(id, sender);
        Subscription {
            job,
            id,
            receiver,
            completions: Arc::downgrade(self),
        }
    }

    fn notify(&self, job: Uuid) {
        let waiters = self
            .waiters
            .lock()
            .expect("job completion waiters lock")
            .remove(&job);
        if let Some(waiters) = waiters {
            for (_, waiter) in waiters {
                let _ = waiter.send(());
            }
        }
    }

    fn remove(&self, job: Uuid, id: u64) {
        let mut waiters = self.waiters.lock().expect("job completion waiters lock");
        if let Some(job_waiters) = waiters.get_mut(&job) {
            job_waiters.remove(&id);
            if job_waiters.is_empty() {
                waiters.remove(&job);
            }
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
    use super::JobCompletions;
    use std::sync::Arc;
    use uuid::Uuid;

    #[tokio::test]
    async fn wakes_only_waiters_for_the_completed_job() {
        let completions = Arc::new(JobCompletions::default());
        let first_job = Uuid::new_v4();
        let mut first = completions.subscribe(first_job);
        let mut also_first = completions.subscribe(first_job);
        let mut second = completions.subscribe(Uuid::new_v4());

        completions.notify(first_job);
        assert!((&mut first.receiver).await.is_ok());
        assert!((&mut also_first.receiver).await.is_ok());
        assert!(second.receiver.try_recv().is_err());
    }
}
