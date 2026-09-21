//! The control-plane database, from the worker's side.
//!
//! The worker reads the config version a run is pinned to, and writes its own
//! job rows. It never writes anything else: users, the pipeline registry and
//! config versions belong to the web, which also owns migrations. If the schema
//! is missing, queries fail loudly rather than the worker inventing tables.
//!
//! Job rows are written directly rather than reported over HTTP, which is the
//! reason a server database was chosen over SQLite: the worker keeps owning the
//! job lifecycle end to end.

use sqlx::postgres::{PgPool, PgPoolOptions};

/// Connect with a small pool: concurrency is 1 by default and these are short
/// control-plane statements, not analytical queries.
pub async fn connect(url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(
            std::env::var("DATABASE_POOL_MAX")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(4),
        )
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(url)
        .await
}

/// The config a run should use, by version id.
///
/// Taken from the queue message rather than read from the head, so a save partway
/// through a run cannot change what the run does.
pub async fn config_for_version(
    pool: &PgPool,
    config_version_id: i64,
) -> Result<Option<serde_json::Value>, sqlx::Error> {
    let row: Option<(serde_json::Value,)> =
        sqlx::query_as("SELECT config FROM config_versions WHERE id = $1")
            .bind(config_version_id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(config,)| config))
}

/// The live config version for a pipeline, for runs enqueued without one (an S3
/// upload webhook, say, which knows a prefix and nothing else).
pub async fn current_version_for(
    pool: &PgPool,
    pipeline: &str,
) -> Result<Option<i64>, sqlx::Error> {
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT config_version_id FROM pipelines_current WHERE pipeline = $1",
    )
    .bind(pipeline)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(id,)| id))
}

/// Record a job as queued. Idempotent on job id so a retried enqueue does not
/// duplicate history.
#[allow(clippy::too_many_arguments)]
pub async fn insert_queued(
    pool: &PgPool,
    id: &str,
    pipeline: &str,
    config_version_id: Option<i64>,
    trigger: &str,
    enqueued_at_ms: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO jobs (id, pipeline, config_version_id, status, trigger, enqueued_at)
         VALUES ($1, $2, $3, 'queued', $4, to_timestamp($5::double precision / 1000))
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(id)
    .bind(pipeline)
    .bind(config_version_id)
    .bind(trigger)
    .bind(enqueued_at_ms)
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark a job as started by this worker, counting the attempt.
pub async fn mark_running(pool: &PgPool, id: &str, worker: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE jobs
            SET status = 'running',
                worker = $2,
                started_at = now(),
                attempts = attempts + 1
          WHERE id = $1",
    )
    .bind(id)
    .bind(worker)
    .execute(pool)
    .await?;
    Ok(())
}

/// Counters and outcome of a finished run.
#[derive(Debug, Default, Clone)]
pub struct JobOutcomeRow {
    pub status: String,
    pub error: Option<String>,
    pub files_processed: Option<i32>,
    pub partitions_written: Option<i32>,
    pub rows_deduped: Option<i32>,
}

pub async fn mark_finished(
    pool: &PgPool,
    id: &str,
    outcome: &JobOutcomeRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE jobs
            SET status = $2,
                completed_at = now(),
                error = $3,
                files_processed = $4,
                partitions_written = $5,
                rows_deduped = $6
          WHERE id = $1",
    )
    .bind(id)
    .bind(outcome.status.as_str())
    .bind(outcome.error.as_deref())
    .bind(outcome.files_processed)
    .bind(outcome.partitions_written)
    .bind(outcome.rows_deduped)
    .execute(pool)
    .await?;
    Ok(())
}
