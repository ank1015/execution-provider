use crate::error::{Error, Result};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{
    FromRow, PgPool, Postgres, QueryBuilder,
    postgres::{PgPoolOptions, PgRow},
};
use std::time::Duration;
use uuid::Uuid;

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

pub async fn pool(url: &str) -> Result<PgPool> {
    Ok(PgPoolOptions::new().max_connections(10).acquire_timeout(Duration::from_secs(5))
        .after_connect(|connection, _| Box::pin(async move {
            sqlx::query("SELECT set_config('statement_timeout','5s',false),set_config('lock_timeout','3s',false),set_config('idle_in_transaction_session_timeout','30s',false),set_config('timezone','UTC',false)")
                .execute(connection).await?;
            Ok(())
        })).connect(url).await?)
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Page {
    pub limit: Option<i64>,
    pub cursor: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Cursor {
    at: DateTime<Utc>,
    id: Uuid,
}

pub async fn page<T>(
    pool: &PgPool,
    mut query: QueryBuilder<'_, Postgres>,
    page: Page,
) -> Result<Value>
where
    T: for<'r> FromRow<'r, PgRow> + Send + Unpin + Serialize,
{
    let limit = page.limit.unwrap_or(50);
    if !(1..=100).contains(&limit) {
        return Err(Error::invalid("limit must be between 1 and 100"));
    }
    if let Some(cursor) = page.cursor {
        let cursor: Cursor = URL_SAFE_NO_PAD
            .decode(cursor)
            .ok()
            .and_then(|v| serde_json::from_slice(&v).ok())
            .ok_or_else(|| Error::invalid("invalid cursor"))?;
        query
            .push(" AND (created_at, id) < (")
            .push_bind(cursor.at)
            .push(", ")
            .push_bind(cursor.id)
            .push(")");
    }
    query
        .push(" ORDER BY created_at DESC, id DESC LIMIT ")
        .push_bind(limit + 1);
    let mut values: Vec<Value> = query
        .build_query_as::<T>()
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<_, _>>()?;
    let more = values.len() > limit as usize;
    values.truncate(limit as usize);
    let cursor = if more {
        let last = values.last().ok_or_else(Error::internal)?;
        let cursor = Cursor {
            at: serde_json::from_value(last["createdAt"].clone())?,
            id: serde_json::from_value(last["id"].clone())?,
        };
        Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&cursor)?))
    } else {
        None
    };
    Ok(json!({"data": values, "nextCursor": cursor}))
}
