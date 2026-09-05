//! Redis-backed job queue: stream consumer, per-pipeline locks, delayed
//! retries, crash reclaim, webhook debounce, and live job state.
//!
//! Schema (all keys prefixed `karet:`) — see karet-jobs-redis-design.html:
//!   - `jobs:stream` stream, consumer group `workers`; messages carry one
//!     `payload` field of JSON
//!   - `jobs:live:<id>` hash of live job state + progress
//!   - `jobs:index:<pipeline>` ZSET job_id scored by enqueued_at ms
//!   - `jobs:delayed` ZSET JSON payload scored by fire-at ms
//!   - `lock:pipeline:<slug>` run lock, value = `job_id:attempt` (fence), PX + heartbeat
//!   - `debounce` ZSET slug scored by fire-at ms
//!   - `debounce:first:<slug>` batch-start ms, drives the max-wait cap
//!
//! Redis holds coordination state; S3 holds history. The terminal job
//! record is written to S3 *before* the stream ack, so a crash in between
//! re-runs the job (at-least-once) rather than losing the record.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, RedisError};
use serde::{Deserialize, Serialize};

use crate::job::{self, JobContext, JobError, JobOutcome, Progress, ProgressSink};

pub const STREAM_KEY: &str = "karet:jobs:stream";
pub const GROUP: &str = "workers";
pub const DELAYED_KEY: &str = "karet:jobs:delayed";
pub const DEBOUNCE_KEY: &str = "karet:debounce";
const STREAM_MAXLEN: usize = 4096;

/// Debounce timing, mirrors the web app's in-memory debouncer.
pub const QUIET_MS: i64 = 5_000;
pub const MAX_WAIT_MS: i64 = 30_000;

fn live_key(job_id: &str) -> String {
    format!("karet:jobs:live:{job_id}")
}
fn index_key(pipeline: &str) -> String {
    format!("karet:jobs:index:{pipeline}")
}
fn lock_key(pipeline: &str) -> String {
    format!("karet:lock:pipeline:{pipeline}")
}
fn debounce_first_key(slug: &str) -> String {
    format!("karet:debounce:first:{slug}")
}
fn events_channel(pipeline: &str) -> String {
    format!("karet:jobs:events:{pipeline}")
}

/// Notify subscribers (the web app's SSE endpoint) that a job's live
/// state changed. Best-effort; the UI reconciles by polling anyway.
async fn publish_job_event(conn: &mut MultiplexedConnection, pipeline: &str, job_id: &str) {
    let _: Result<(), RedisError> = conn.publish(events_channel(pipeline), job_id).await;
}

/// Tunables, all env-overridable (see README).
#[derive(Clone, Debug)]
pub struct QueueSettings {
    pub max_attempts: u32,
    pub lock_ttl_ms: u64,
    pub heartbeat_ms: u64,
    /// PEL idle time after which another worker may reclaim a message.
    pub reclaim_idle_ms: u64,
    /// TTL for terminal live hashes.
    pub live_terminal_ttl_s: u64,
}

impl Default for QueueSettings {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            lock_ttl_ms: 90_000,
            heartbeat_ms: 30_000,
            reclaim_idle_ms: 120_000,
            live_terminal_ttl_s: 24 * 60 * 60,
        }
    }
}

/// Shared context for all queue loops.
pub struct QueueCtx {
    pub client: redis::Client,
    pub job_ctx: JobContext,
    pub consumer_name: String,
    pub settings: QueueSettings,
    pub in_flight: AtomicUsize,
    /// True while the consumer loop is successfully reading the stream;
    /// /health reports it so a dead consumer can't hide behind a fresh
    /// per-request connection.
    pub consumer_ok: std::sync::atomic::AtomicBool,
    /// Set to true by the shutdown signal; loops exit at the next check.
    pub shutdown: tokio::sync::watch::Receiver<bool>,
}

/// One queued job. Serialized as JSON into the stream's `payload` field
/// and into the delayed ZSET, so both carry identical information.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobMessage {
    pub job_id: String,
    pub pipeline: String,
    pub prefix: String,
    #[serde(default)]
    pub clean_run: bool,
    pub trigger: String,
    /// Unix ms; sorts the index ZSET.
    pub enqueued_at: i64,
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Connect with a response timeout above the longest blocking read (the
/// consumer's XREADGROUP BLOCK 5000). The client default is 500ms, which
/// spuriously fails long commands. All queue connections go through here.
pub async fn connect(client: &redis::Client) -> Result<MultiplexedConnection, RedisError> {
    let config = redis::AsyncConnectionConfig::new()
        .set_response_timeout(Some(std::time::Duration::from_secs(15)));
    client.get_multiplexed_async_connection_with_config(&config).await
}

/// Reuse the loop's connection or establish a fresh one. Loops set their
/// slot to `None` on any command error; multiplexed connections don't
/// recover from socket death on their own.
async fn ensure_conn<'a>(
    client: &redis::Client,
    slot: &'a mut Option<MultiplexedConnection>,
    label: &str,
) -> Option<&'a mut MultiplexedConnection> {
    if slot.is_none() {
        match connect(client).await {
            Ok(c) => *slot = Some(c),
            Err(e) => {
                tracing::warn!("redis connect failed ({label}): {e}");
                return None;
            }
        }
    }
    slot.as_mut()
}

fn now_iso() -> String {
    chrono::Utc::now()
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Mint a job id in the same shape the web app uses.
pub fn new_job_id() -> String {
    let suffix: String = uuid::Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .take(6)
        .collect();
    format!("job-{}-{}", now_ms(), suffix)
}

/// Retry backoff: 30s * 2^(attempts-1), capped at 10 minutes.
pub fn backoff_ms(attempts: u32) -> i64 {
    let base: i64 = 30_000;
    let capped = base.saturating_mul(1i64 << (attempts.saturating_sub(1)).min(6));
    capped.min(600_000)
}

// ---------------------------------------------------------------------------
// Enqueue
// ---------------------------------------------------------------------------

/// Enqueue a job: stream entry + live hash + index entry, atomically.
pub async fn enqueue(
    conn: &mut MultiplexedConnection,
    msg: &JobMessage,
) -> Result<(), RedisError> {
    let payload = serde_json::to_string(msg).expect("JobMessage serializes");
    let mut pipe = redis::pipe();
    pipe.atomic()
        .xadd_maxlen(
            STREAM_KEY,
            redis::streams::StreamMaxlen::Approx(STREAM_MAXLEN),
            "*",
            &[("payload", payload.as_str())],
        )
        .hset_multiple(
            live_key(&msg.job_id),
            &[
                ("status", "queued"),
                ("pipeline", msg.pipeline.as_str()),
                ("trigger", msg.trigger.as_str()),
            ],
        )
        .hset(live_key(&msg.job_id), "enqueued_at", msg.enqueued_at)
        .hset(live_key(&msg.job_id), "clean_run", msg.clean_run.to_string())
        .zadd(index_key(&msg.pipeline), &msg.job_id, msg.enqueued_at)
        // Safety net: never-claimed hashes expire eventually; terminal
        // transitions shorten this to live_terminal_ttl_s.
        .expire(live_key(&msg.job_id), 7 * 24 * 60 * 60)
        .ignore();
    pipe.query_async::<()>(conn).await?;
    publish_job_event(conn, &msg.pipeline, &msg.job_id).await;
    Ok(())
}

/// Idempotently create the consumer group (and the stream if missing).
/// The group starts at `0`, not `$`: jobs enqueued while no worker was
/// running (deploys, crashes) must still be delivered once one boots.
pub async fn ensure_group(conn: &mut MultiplexedConnection) -> Result<(), RedisError> {
    let result: Result<(), RedisError> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(STREAM_KEY)
        .arg(GROUP)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(conn)
        .await;
    match result {
        Ok(()) => Ok(()),
        Err(e) if e.to_string().contains("BUSYGROUP") => Ok(()),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// Locks (Lua for the guarded release)
// ---------------------------------------------------------------------------

const RELEASE_LOCK_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end"#;

const RENEW_LOCK_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
  return 0
end"#;

/// Lock value: `<job_id>:<attempt>`. The attempt suffix fences out stale
/// holders: a presumed-dead worker that is still running cannot renew or
/// release a lock now owned by a later attempt.
fn fence(job_id: &str, attempt: u32) -> String {
    format!("{job_id}:{attempt}")
}

async fn try_lock(
    conn: &mut MultiplexedConnection,
    pipeline: &str,
    fence: &str,
    ttl_ms: u64,
) -> Result<bool, RedisError> {
    let result: Option<String> = redis::cmd("SET")
        .arg(lock_key(pipeline))
        .arg(fence)
        .arg("NX")
        .arg("PX")
        .arg(ttl_ms)
        .query_async(conn)
        .await?;
    Ok(result.is_some())
}

async fn release_lock(
    conn: &mut MultiplexedConnection,
    pipeline: &str,
    fence: &str,
) -> Result<(), RedisError> {
    redis::Script::new(RELEASE_LOCK_LUA)
        .key(lock_key(pipeline))
        .arg(fence)
        .invoke_async::<()>(conn)
        .await
}

/// Renew the lock TTL iff we still hold it. Returns false when fenced out.
async fn renew_lock(
    conn: &mut MultiplexedConnection,
    pipeline: &str,
    fence: &str,
    ttl_ms: u64,
) -> Result<bool, RedisError> {
    let renewed: i64 = redis::Script::new(RENEW_LOCK_LUA)
        .key(lock_key(pipeline))
        .arg(fence)
        .arg(ttl_ms)
        .invoke_async(conn)
        .await?;
    Ok(renewed == 1)
}

// ---------------------------------------------------------------------------
// Live state + terminal records
// ---------------------------------------------------------------------------

/// Progress sink that forwards updates to the live hash via a channel; a
/// writer task drains it so the executor never awaits Redis mid-pipeline.
pub struct RedisProgress {
    tx: tokio::sync::mpsc::UnboundedSender<Progress>,
}

impl ProgressSink for RedisProgress {
    fn update(&self, progress: Progress) {
        let _ = self.tx.send(progress);
    }
}

impl RedisProgress {
    /// Returns the sink plus the drain task's join handle. Dropping the
    /// sink closes the channel, ending the drain task.
    pub fn start(
        mut conn: MultiplexedConnection,
        pipeline: String,
        job_id: String,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Progress>();
        let handle = tokio::spawn(async move {
            while let Some(p) = rx.recv().await {
                let key = live_key(&job_id);
                let result: Result<(), RedisError> = match p {
                    Progress::Downloading { done, total } => {
                        redis::pipe()
                            .hset(&key, "stage", "downloading")
                            .hset(&key, "files_done", done)
                            .hset(&key, "files_total", total)
                            .ignore()
                            .query_async(&mut conn)
                            .await
                    }
                    Progress::Ingesting {
                        mappings_done,
                        mappings_total,
                        partitions_written,
                    } => {
                        redis::pipe()
                            .hset(&key, "stage", "ingesting")
                            .hset(&key, "mappings_done", mappings_done)
                            .hset(&key, "mappings_total", mappings_total)
                            .hset(&key, "partitions_written", partitions_written)
                            .ignore()
                            .query_async(&mut conn)
                            .await
                    }
                };
                if let Err(e) = result {
                    tracing::warn!("progress write failed: {e}");
                } else {
                    publish_job_event(&mut conn, &pipeline, &job_id).await;
                }
            }
        });
        (Self { tx }, handle)
    }
}

/// Terminal fields for both the S3 record and the live hash.
struct Terminal<'a> {
    status: &'a str,
    error: Option<String>,
    errors: Vec<String>,
    partitions_written: Option<usize>,
    files_processed: Option<usize>,
}

/// Write the terminal S3 record (web `JobRecord` shape), update the live
/// hash, expire it, release the lock, and ack the stream entry — in that
/// order. S3-first means a crash re-runs the job instead of losing history.
#[allow(clippy::too_many_arguments)]
async fn finish_job(
    ctx: &QueueCtx,
    conn: &mut MultiplexedConnection,
    msg: &JobMessage,
    stream_id: &str,
    started_at_iso: &str,
    attempts: u32,
    terminal: Terminal<'_>,
) -> Result<(), String> {
    let completed_at = now_iso();
    let mut record = serde_json::json!({
        "id": msg.job_id,
        "pipeline": msg.pipeline,
        "status": terminal.status,
        "startedAt": started_at_iso,
        "completedAt": completed_at,
        "trigger": msg.trigger,
        "attempts": attempts,
        "worker": ctx.consumer_name,
    });
    if let Some(e) = &terminal.error {
        record["error"] = serde_json::json!(e);
    }
    if !terminal.errors.is_empty() {
        record["errors"] = serde_json::json!(terminal.errors);
    }
    if let Some(n) = terminal.partitions_written {
        record["partitions_written"] = serde_json::json!(n);
    }
    if let Some(n) = terminal.files_processed {
        record["files_processed"] = serde_json::json!(n);
    }

    let key = format!("{}jobs/{}.json", msg.prefix, msg.job_id);
    let record_write = ctx
        .job_ctx
        .s3_client
        .put_object()
        .bucket(&ctx.job_ctx.pipelines_bucket)
        .key(&key)
        .body(aws_sdk_s3::primitives::ByteStream::from(
            serde_json::to_vec(&record).expect("record serializes"),
        ))
        .content_type("application/json")
        .send()
        .await;
    // A failed record write must not wedge the job in `running`: keep the
    // terminal state visible in the live hash (its TTL is the availability
    // window) and surface the miss in `error`. Worst case per the design's
    // failure matrix: one terminal record lost while S3 is down.
    let mut error = terminal.error;
    if let Err(e) = record_write {
        let miss = format!(
            "terminal record write failed for {key}: {}",
            crate::s3::err_chain(&e)
        );
        tracing::error!("{miss}");
        error = Some(match error {
            Some(prior) => format!("{prior} ({miss})"),
            None => miss,
        });
    }

    let live = live_key(&msg.job_id);
    let mut pipe = redis::pipe();
    pipe.atomic()
        .hset(&live, "status", terminal.status)
        .hset(&live, "finished_at", completed_at.as_str())
        .expire(&live, ctx.settings.live_terminal_ttl_s as i64)
        .ignore();
    if let Some(e) = &error {
        pipe.hset(&live, "error", e.as_str()).ignore();
    }
    if let Some(n) = terminal.partitions_written {
        pipe.hset(&live, "partitions_written", n).ignore();
    }
    pipe.query_async::<()>(conn)
        .await
        .map_err(|e| format!("live terminal update failed: {e}"))?;
    publish_job_event(conn, &msg.pipeline, &msg.job_id).await;

    release_lock(conn, &msg.pipeline, &fence(&msg.job_id, attempts))
        .await
        .map_err(|e| format!("lock release failed: {e}"))?;
    let _: Result<(), RedisError> = conn.xack(STREAM_KEY, GROUP, &[stream_id]).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Claim + execute
// ---------------------------------------------------------------------------

/// Defer a message: schedule a re-enqueue on the delayed ZSET and ack the
/// current delivery.
async fn defer(
    conn: &mut MultiplexedConnection,
    msg: &JobMessage,
    stream_id: &str,
    fire_at_ms: i64,
) -> Result<(), RedisError> {
    let payload = serde_json::to_string(msg).expect("JobMessage serializes");
    let mut pipe = redis::pipe();
    pipe.atomic()
        .zadd(DELAYED_KEY, payload, fire_at_ms)
        .xack(STREAM_KEY, GROUP, &[stream_id])
        .ignore();
    pipe.query_async::<()>(conn).await
}

/// Handle one claimed stream entry end to end.
async fn handle_claimed(ctx: &Arc<QueueCtx>, stream_id: String, payload: String) {
    let mut conn = match connect(&ctx.client).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("redis connection failed in handler: {e}");
            return;
        }
    };

    let msg: JobMessage = match serde_json::from_str(&payload) {
        Ok(m) => m,
        Err(e) => {
            // Poison entry: ack it away and log. No record to write — we
            // don't even know the job id.
            tracing::error!("unparseable job payload ({e}); acking: {payload}");
            let _: Result<(), RedisError> = conn.xack(STREAM_KEY, GROUP, &[&stream_id]).await;
            return;
        }
    };

    // Prefix shape gate (same rule as the HTTP path).
    if let Err(reason) = crate::http::validate_pipeline_prefix(&msg.prefix) {
        let started = now_iso();
        let _ = finish_job(
            ctx, &mut conn, &msg, &stream_id, &started, 1,
            Terminal {
                status: "failed",
                error: Some(format!("invalid pipeline_prefix: {reason}")),
                errors: Vec::new(),
                partitions_written: None,
                files_processed: None,
            },
        )
        .await
        .map_err(|e| tracing::error!("{e}"));
        return;
    }

    // Attempt accounting before the lock so a poison job can't loop
    // forever between defer and claim.
    let attempts: u32 = match conn.hincr(live_key(&msg.job_id), "attempts", 1u32).await {
        Ok(n) => n,
        Err(e) => {
            tracing::error!("attempts incr failed: {e}");
            1
        }
    };
    if attempts > ctx.settings.max_attempts {
        let started = now_iso();
        let status = "failed";
        let _ = finish_job(
            ctx, &mut conn, &msg, &stream_id, &started, attempts,
            Terminal {
                status,
                error: Some(format!(
                    "gave up after {} attempts (worker crashes or repeated transient failures)",
                    attempts - 1
                )),
                errors: Vec::new(),
                partitions_written: None,
                files_processed: None,
            },
        )
        .await
        .map_err(|e| tracing::error!("{e}"));
        return;
    }

    // Per-pipeline serialization: busy → defer, don't block the consumer.
    match try_lock(&mut conn, &msg.pipeline, &fence(&msg.job_id, attempts), ctx.settings.lock_ttl_ms).await {
        Ok(true) => {}
        Ok(false) => {
            // Not this job's fault; don't burn an attempt on lock-busy.
            let _: Result<(), RedisError> = async {
                conn.hincr::<_, _, _, i64>(live_key(&msg.job_id), "attempts", -1).await?;
                Ok(())
            }
            .await;
            if let Err(e) = defer(&mut conn, &msg, &stream_id, now_ms() + 30_000).await {
                tracing::error!("defer failed for {}: {e}", msg.job_id);
            }
            return;
        }
        Err(e) => {
            tracing::error!("lock attempt failed: {e}");
            return; // stays in PEL; reclaimed later
        }
    }

    ctx.in_flight.fetch_add(1, Ordering::SeqCst);
    let started_at_iso = now_iso();
    let _: Result<(), RedisError> = redis::pipe()
        .hset(live_key(&msg.job_id), "status", "running")
        .hset(live_key(&msg.job_id), "worker", ctx.consumer_name.as_str())
        .hset(live_key(&msg.job_id), "started_at", started_at_iso.as_str())
        .ignore()
        .query_async(&mut conn)
        .await;
    publish_job_event(&mut conn, &msg.pipeline, &msg.job_id).await;

    // Heartbeat: renew the lock and reset PEL idle time so neither the
    // lock TTL nor the reclaimer fires while we're alive and working.
    let hb_ctx = ctx.clone();
    let hb_msg = msg.clone();
    let hb_stream_id = stream_id.clone();
    let heartbeat = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(
            hb_ctx.settings.heartbeat_ms,
        ));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let hb_fence = fence(&hb_msg.job_id, attempts);
        let mut conn: Option<MultiplexedConnection> = None;
        loop {
            interval.tick().await;
            let c = match &mut conn {
                Some(c) => c,
                None => match connect(&hb_ctx.client).await {
                    Ok(c) => conn.insert(c),
                    Err(e) => {
                        tracing::warn!("heartbeat connect failed for {}: {e}; retrying", hb_msg.job_id);
                        continue;
                    }
                },
            };
            match renew_lock(c, &hb_msg.pipeline, &hb_fence, hb_ctx.settings.lock_ttl_ms).await {
                Ok(true) => {}
                Ok(false) => {
                    // Fenced out: another attempt owns the pipeline now.
                    tracing::error!(
                        "lock for {} lost to a newer attempt; this run's uploads may overlap",
                        hb_msg.job_id
                    );
                    return;
                }
                Err(e) => {
                    tracing::warn!("heartbeat renew failed for {}: {e}", hb_msg.job_id);
                    conn = None;
                    continue;
                }
            }
            // XCLAIM to self with IDLE 0 resets the PEL idle clock.
            let claimed: Result<redis::Value, RedisError> = redis::cmd("XCLAIM")
                .arg(STREAM_KEY)
                .arg(GROUP)
                .arg(&hb_ctx.consumer_name)
                .arg(0)
                .arg(&hb_stream_id)
                .arg("IDLE")
                .arg(0)
                .arg("JUSTID")
                .query_async(c)
                .await;
            if let Err(e) = claimed {
                tracing::warn!("heartbeat XCLAIM failed for {}: {e}", hb_msg.job_id);
                conn = None;
            }
        }
    });

    let (progress, progress_task) =
        RedisProgress::start(conn.clone(), msg.pipeline.clone(), msg.job_id.clone());
    let outcome = job::execute_job(&ctx.job_ctx, &msg.prefix, msg.clean_run, &progress).await;
    drop(progress); // close channel so the drain task ends
    let _ = progress_task.await;
    heartbeat.abort();

    let result = match outcome {
        Ok(JobOutcome {
            partitions_written,
            files_processed,
            errors,
        }) => {
            let error = if errors.is_empty() {
                None
            } else {
                Some(format!(
                    "{partitions_written} partitions written, {} error(s): {}",
                    errors.len(),
                    errors[0]
                ))
            };
            finish_job(
                ctx, &mut conn, &msg, &stream_id, &started_at_iso, attempts,
                Terminal {
                    status: "completed",
                    error,
                    errors,
                    partitions_written: Some(partitions_written),
                    files_processed: Some(files_processed),
                },
            )
            .await
        }
        // Empty lake prefix: complete with zero counts, not an error.
        Err(JobError::NoFiles) => {
            finish_job(
                ctx, &mut conn, &msg, &stream_id, &started_at_iso, attempts,
                Terminal {
                    status: "completed",
                    error: Some("no CSV files found to process".into()),
                    errors: Vec::new(),
                    partitions_written: Some(0),
                    files_processed: Some(0),
                },
            )
            .await
        }
        // Possibly-transient: config object unreadable (network, S3 5xx).
        Err(JobError::ConfigRead(e)) => {
            tracing::warn!("config read failed for {} (attempt {attempts}): {e}", msg.job_id);
            release_lock(&mut conn, &msg.pipeline, &fence(&msg.job_id, attempts))
                .await
                .map_err(|err| format!("lock release failed: {err}"))
                .and(if attempts >= ctx.settings.max_attempts {
                    finish_job(
                        ctx, &mut conn, &msg, &stream_id, &started_at_iso, attempts,
                        Terminal {
                            status: "failed",
                            error: Some(e),
                            errors: Vec::new(),
                            partitions_written: None,
                            files_processed: None,
                        },
                    )
                    .await
                } else {
                    let _: Result<(), RedisError> = redis::pipe()
                        .hset(live_key(&msg.job_id), "status", "queued")
                        .ignore()
                        .query_async(&mut conn)
                        .await;
                    defer(&mut conn, &msg, &stream_id, now_ms() + backoff_ms(attempts))
                        .await
                        .map_err(|err| format!("defer failed: {err}"))
                })
        }
        // Permanent config problems.
        Err(e @ (JobError::ConfigParse(_) | JobError::ConfigInvalid(_))) => {
            finish_job(
                ctx, &mut conn, &msg, &stream_id, &started_at_iso, attempts,
                Terminal {
                    status: "failed",
                    error: Some(e.to_string()),
                    errors: Vec::new(),
                    partitions_written: None,
                    files_processed: None,
                },
            )
            .await
        }
    };
    if let Err(e) = result {
        tracing::error!("terminal handling failed for {}: {e}", msg.job_id);
    }
    ctx.in_flight.fetch_sub(1, Ordering::SeqCst);
}

// ---------------------------------------------------------------------------
// Loops
// ---------------------------------------------------------------------------

/// Main consumer loop: blocks on XREADGROUP, handles one message at a time
/// (WORKER_CONCURRENCY > 1 runs multiple loops).
pub async fn consumer_loop(ctx: Arc<QueueCtx>) {
    let mut shutdown = ctx.shutdown.clone();
    let mut conn: Option<MultiplexedConnection> = None;
    loop {
        if *shutdown.borrow() {
            tracing::info!("consumer loop exiting (shutdown)");
            return;
        }
        let c = match &mut conn {
            Some(c) => c,
            None => match connect(&ctx.client).await {
                Ok(mut c) => {
                    if let Err(e) = ensure_group(&mut c).await {
                        tracing::error!("XGROUP CREATE failed: {e}");
                    }
                    conn.insert(c)
                }
                Err(e) => {
                    ctx.consumer_ok.store(false, Ordering::SeqCst);
                    tracing::error!("redis connect failed (consumer): {e}; retrying in 5s");
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        shutdown.changed(),
                    )
                    .await;
                    continue;
                }
            },
        };
        let reply: Result<redis::streams::StreamReadReply, RedisError> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(GROUP)
            .arg(&ctx.consumer_name)
            .arg("BLOCK")
            .arg(5000)
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(STREAM_KEY)
            .arg(">")
            .query_async(c)
            .await;
        match reply {
            Ok(reply) => {
                ctx.consumer_ok.store(true, Ordering::SeqCst);
                for stream in reply.keys {
                    for entry in stream.ids {
                        let payload: String = entry
                            .map
                            .get("payload")
                            .and_then(|v| redis::from_redis_value(v.clone()).ok())
                            .unwrap_or_default();
                        handle_claimed(&ctx, entry.id.clone(), payload).await;
                    }
                }
            }
            Err(e) => {
                // Multiplexed connections don't recover from socket death;
                // drop and reconnect rather than retrying a dead handle.
                ctx.consumer_ok.store(false, Ordering::SeqCst);
                conn = None;
                tracing::warn!("XREADGROUP failed: {e}; reconnecting in 2s");
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    shutdown.changed(),
                )
                .await;
            }
        }
    }
}

/// Move due entries from the delayed ZSET back onto the stream. Runs on
/// every worker; the Lua script makes each entry move exactly once.
const MOVE_DUE_LUA: &str = r#"
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 10)
for _, payload in ipairs(due) do
  redis.call('ZREM', KEYS[1], payload)
  redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], '*', 'payload', payload)
end
return #due"#;

pub async fn delayed_mover_loop(ctx: Arc<QueueCtx>) {
    let shutdown = ctx.shutdown.clone();
    let script = redis::Script::new(MOVE_DUE_LUA);
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    let mut conn: Option<MultiplexedConnection> = None;
    loop {
        interval.tick().await;
        if *shutdown.borrow() {
            return;
        }
        let Some(c) = ensure_conn(&ctx.client, &mut conn, "mover").await else {
            continue;
        };
        let moved: Result<i64, RedisError> = script
            .key(DELAYED_KEY)
            .key(STREAM_KEY)
            .arg(now_ms())
            .arg(STREAM_MAXLEN)
            .invoke_async(c)
            .await;
        match moved {
            Ok(n) if n > 0 => tracing::info!("re-enqueued {n} delayed job(s)"),
            Ok(_) => {}
            Err(e) => {
                tracing::warn!("delayed mover failed: {e}");
                conn = None;
            }
        }
    }
}

/// Reclaim PEL entries whose consumer died (idle > reclaim_idle_ms) and
/// run them here.
pub async fn reclaimer_loop(ctx: Arc<QueueCtx>) {
    let shutdown = ctx.shutdown.clone();
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    let mut conn: Option<MultiplexedConnection> = None;
    let mut tick: u64 = 0;
    loop {
        interval.tick().await;
        if *shutdown.borrow() {
            return;
        }
        tick += 1;
        let mut failed = false;
        let Some(c) = ensure_conn(&ctx.client, &mut conn, "reclaimer").await else {
            continue;
        };
        // Claim one entry at a time: entries claimed in a batch but not
        // yet processed have no heartbeat, so they'd exceed the idle
        // threshold mid-queue and get double-claimed by another worker.
        loop {
            let reply: Result<redis::streams::StreamAutoClaimReply, RedisError> =
                redis::cmd("XAUTOCLAIM")
                    .arg(STREAM_KEY)
                    .arg(GROUP)
                    .arg(&ctx.consumer_name)
                    .arg(ctx.settings.reclaim_idle_ms)
                    .arg("0-0")
                    .arg("COUNT")
                    .arg(1)
                    .query_async(&mut *c)
                    .await;
            match reply {
                Ok(reply) if reply.claimed.is_empty() => break,
                Ok(reply) => {
                    for entry in reply.claimed {
                        let payload: String = entry
                            .map
                            .get("payload")
                            .and_then(|v| redis::from_redis_value(v.clone()).ok())
                            .unwrap_or_default();
                        tracing::warn!("reclaimed stale job entry {}", entry.id);
                        handle_claimed(&ctx, entry.id.clone(), payload).await;
                    }
                }
                Err(e) => {
                    tracing::warn!("XAUTOCLAIM failed: {e}");
                    failed = true;
                    break;
                }
            }
        }
        // Sweep every 10th pass; it scans the live keyspace and the
        // stream, which is too heavy for every minute on every worker.
        if !failed && tick % 10 == 1 {
            if let Err(e) = sweep_orphaned_live_hashes(&ctx, c).await {
                tracing::warn!("orphan sweep failed: {e}");
                failed = true;
            }
        }
        if failed {
            conn = None;
        }
    }
}

/// Grace before a non-terminal live hash with no stream entry counts as
/// orphaned; covers the enqueue window between HSET and XADD visibility.
const ORPHAN_GRACE_MS: i64 = 10 * 60 * 1000;

/// Mark non-terminal live hashes whose stream entry no longer exists as
/// `abandoned`. Happens when XTRIM drops an unprocessed entry under heavy
/// backlog; without the sweep the hash shows `queued` forever.
pub async fn sweep_orphaned_live_hashes(
    ctx: &Arc<QueueCtx>,
    conn: &mut MultiplexedConnection,
) -> Result<(), RedisError> {
    let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(STREAM_KEY)
        .arg("-")
        .arg("+")
        .query_async(&mut *conn)
        .await?;
    let mut stream_job_ids = std::collections::HashSet::new();
    for id in &reply.ids {
        if let Some(payload) = id
            .map
            .get("payload")
            .and_then(|v| redis::from_redis_value::<String>(v.clone()).ok())
        {
            if let Ok(msg) = serde_json::from_str::<JobMessage>(&payload) {
                stream_job_ids.insert(msg.job_id);
            }
        }
    }

    let mut live_keys: Vec<String> = Vec::new();
    let mut cursor: u64 = 0;
    loop {
        let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("karet:jobs:live:*")
            .arg("COUNT")
            .arg(100)
            .query_async(&mut *conn)
            .await?;
        live_keys.extend(batch);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    let cutoff = now_ms() - ORPHAN_GRACE_MS;
    for key in live_keys {
        let job_id = key.trim_start_matches("karet:jobs:live:").to_string();
        if stream_job_ids.contains(&job_id) {
            continue;
        }
        let (status, enqueued_at, pipeline): (Option<String>, Option<i64>, Option<String>) =
            redis::pipe()
                .hget(&key, "status")
                .hget(&key, "enqueued_at")
                .hget(&key, "pipeline")
                .query_async(&mut *conn)
                .await?;
        let non_terminal = matches!(status.as_deref(), Some("queued") | Some("running"));
        if !non_terminal || enqueued_at.unwrap_or(0) > cutoff {
            continue;
        }
        // Running jobs are covered by the PEL reclaim path; only sweep
        // jobs whose stream entry is truly gone.
        tracing::warn!("sweeping orphaned live hash for {job_id} (stream entry gone)");
        redis::pipe()
            .atomic()
            .hset(&key, "status", "abandoned")
            .hset(&key, "error", "queue entry lost (stream trimmed); re-run the job")
            .hset(&key, "finished_at", now_iso())
            .expire(&key, ctx.settings.live_terminal_ttl_s as i64)
            .ignore()
            .query_async::<()>(&mut *conn)
            .await?;
        if let Some(pipeline) = pipeline {
            publish_job_event(conn, &pipeline, &job_id).await;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Webhook debounce
// ---------------------------------------------------------------------------

/// Record an upload event for `slug`: extend the quiet window (5s) capped
/// at 30s past the first event of the batch. Called by the webhook handler.
pub async fn debounce_event(
    conn: &mut MultiplexedConnection,
    slug: &str,
) -> Result<i64, RedisError> {
    let now = now_ms();
    // Remember the batch start; NX so only the first event sets it. TTL
    // comfortably above max wait so stale keys can't wedge a slug.
    let first: Option<String> = redis::cmd("SET")
        .arg(debounce_first_key(slug))
        .arg(now)
        .arg("NX")
        .arg("GET")
        .arg("PX")
        .arg(MAX_WAIT_MS * 4)
        .query_async(conn)
        .await?;
    let batch_start: i64 = first.and_then(|s| s.parse().ok()).unwrap_or(now);
    let fire_at = (now + QUIET_MS).min(batch_start + MAX_WAIT_MS);
    let _: () = conn.zadd(DEBOUNCE_KEY, slug, fire_at).await?;
    Ok(fire_at - now)
}

/// Pop debounce entries that are due, atomically.
const POP_DUE_LUA: &str = r#"
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 10)
for _, slug in ipairs(due) do
  redis.call('ZREM', KEYS[1], slug)
  redis.call('DEL', 'karet:debounce:first:' .. slug)
end
return due"#;

/// Fire due debounce windows: enqueue a webhook-triggered job per slug.
pub async fn debounce_scheduler_loop(ctx: Arc<QueueCtx>) {
    let shutdown = ctx.shutdown.clone();
    let script = redis::Script::new(POP_DUE_LUA);
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    let mut conn: Option<MultiplexedConnection> = None;
    loop {
        interval.tick().await;
        if *shutdown.borrow() {
            return;
        }
        let Some(c) = ensure_conn(&ctx.client, &mut conn, "debounce").await else {
            continue;
        };
        let due: Result<Vec<String>, RedisError> = script
            .key(DEBOUNCE_KEY)
            .arg(now_ms())
            .invoke_async(&mut *c)
            .await;
        match due {
            Ok(slugs) => {
                for slug in slugs {
                    let msg = JobMessage {
                        job_id: new_job_id(),
                        pipeline: slug.clone(),
                        prefix: format!("pipelines/{slug}/"),
                        clean_run: false,
                        trigger: "webhook".into(),
                        enqueued_at: now_ms(),
                    };
                    match enqueue(c, &msg).await {
                        Ok(()) => tracing::info!("debounce fired: enqueued {} for {slug}", msg.job_id),
                        Err(e) => tracing::error!("debounce enqueue failed for {slug}: {e}"),
                    }
                }
            }
            Err(e) => {
                tracing::warn!("debounce pop failed: {e}");
                conn = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_message_round_trips_through_json() {
        let msg = JobMessage {
            job_id: "job-123-abc".into(),
            pipeline: "demo".into(),
            prefix: "pipelines/demo/".into(),
            clean_run: true,
            trigger: "manual".into(),
            enqueued_at: 1_700_000_000_000,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: JobMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn job_message_defaults_clean_run_false() {
        let json = r#"{"job_id":"j","pipeline":"p","prefix":"pipelines/p/","trigger":"webhook","enqueued_at":0}"#;
        let msg: JobMessage = serde_json::from_str(json).unwrap();
        assert!(!msg.clean_run);
    }

    #[test]
    fn backoff_doubles_and_caps() {
        assert_eq!(backoff_ms(1), 30_000);
        assert_eq!(backoff_ms(2), 60_000);
        assert_eq!(backoff_ms(3), 120_000);
        assert_eq!(backoff_ms(10), 600_000); // capped at 10 min
    }

    #[test]
    fn new_job_id_matches_web_shape() {
        let id = new_job_id();
        // job-<ms>-<6 chars>
        let parts: Vec<&str> = id.splitn(3, '-').collect();
        assert_eq!(parts[0], "job");
        assert!(parts[1].parse::<i64>().is_ok(), "{id}");
        assert_eq!(parts[2].len(), 6, "{id}");
    }

    #[test]
    fn live_and_lock_keys_are_scoped() {
        assert_eq!(live_key("j1"), "karet:jobs:live:j1");
        assert_eq!(lock_key("demo"), "karet:lock:pipeline:demo");
        assert_eq!(index_key("demo"), "karet:jobs:index:demo");
    }
}
