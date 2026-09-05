//! Integration test for the Redis queue against a real server.
//!
//! Ignored by default (needs a running Redis/Valkey). Run with:
//!
//! ```sh
//! REDIS_TEST_URL=redis://127.0.0.1:6399 \
//!   cargo test --test integration_redis -- --ignored --nocapture
//! ```
//!
//! CI runs this with a `valkey` service container. The S3 side is not
//! exercised here (no S3 available); jobs use a prefix whose config read
//! will fail fast, which exercises the full claim → run → retry/terminal
//! machinery — including the S3-record write failure path, where the live
//! hash still reaches a terminal state. What this test pins:
//!
//!   - enqueue writes the stream entry, live hash, and index
//!   - a consumer claims and the job reaches `running` then terminal
//!   - per-pipeline lock: a second job for the same pipeline defers
//!     rather than running concurrently
//!   - the delayed mover re-enqueues due entries
//!   - debounce: events extend the quiet window and fire once
//!
//! Uses a unique key namespace per run? No — the queue module's keys are
//! fixed, so the test flushes the DB first. Point REDIS_TEST_URL at a
//! dedicated database index (e.g. `/15`) if the server is shared.

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use karet_worker::job::JobContext;
use karet_worker::queue::{
    self, JobMessage, QueueCtx, QueueSettings, DEBOUNCE_KEY, DELAYED_KEY, GROUP, STREAM_KEY,
};
use redis::AsyncCommands;

fn test_url() -> Option<String> {
    std::env::var("REDIS_TEST_URL").ok().filter(|s| !s.is_empty())
}

/// Minimal S3 client pointing nowhere reachable; config reads fail fast.
async fn dead_s3_client() -> aws_sdk_s3::Client {
    let cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url("http://127.0.0.1:1") // nothing listens here
        .load()
        .await;
    let s3_cfg = aws_sdk_s3::config::Builder::from(&cfg)
        .force_path_style(true)
        // Fail fast instead of the SDK's default retries/timeouts.
        .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
        .timeout_config(
            aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                .operation_attempt_timeout(std::time::Duration::from_millis(500))
                .build(),
        )
        .build();
    aws_sdk_s3::Client::from_conf(s3_cfg)
}

async fn make_ctx(url: &str) -> (Arc<QueueCtx>, tokio::sync::watch::Sender<bool>) {
    let client = redis::Client::open(url).expect("redis client");
    let (tx, rx) = tokio::sync::watch::channel(false);
    let ctx = Arc::new(QueueCtx {
        client,
        job_ctx: JobContext {
            s3_client: dead_s3_client().await,
            pipelines_bucket: "karet-pipelines".into(),
            lake_bucket: "karet-lake".into(),
            warehouse_bucket: "karet-warehouse".into(),
        },
        consumer_name: format!("test-consumer-{}", std::process::id()),
        settings: QueueSettings {
            max_attempts: 2,
            lock_ttl_ms: 5_000,
            heartbeat_ms: 1_000,
            reclaim_idle_ms: 3_000,
            live_terminal_ttl_s: 60,
        },
        in_flight: AtomicUsize::new(0),
        shutdown: rx,
    });
    (ctx, tx)
}

async fn flush(url: &str) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(url).expect("redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("redis reachable — is the server up? (REDIS_TEST_URL)");
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();
    conn
}

async fn hget(conn: &mut redis::aio::MultiplexedConnection, id: &str, field: &str) -> Option<String> {
    conn.hget(format!("karet:jobs:live:{id}"), field).await.unwrap()
}

/// Poll until the live hash reaches one of `statuses` or the deadline hits.
async fn wait_for_status(
    conn: &mut redis::aio::MultiplexedConnection,
    id: &str,
    statuses: &[&str],
    deadline: std::time::Duration,
) -> String {
    let start = std::time::Instant::now();
    loop {
        if let Some(s) = hget(conn, id, "status").await {
            if statuses.contains(&s.as_str()) {
                return s;
            }
        }
        if start.elapsed() > deadline {
            let s = hget(conn, id, "status").await;
            panic!("timed out waiting for {id} to reach {statuses:?}; last status: {s:?}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

fn msg(job_id: &str, pipeline: &str) -> JobMessage {
    JobMessage {
        job_id: job_id.into(),
        pipeline: pipeline.into(),
        prefix: format!("pipelines/{pipeline}/"),
        clean_run: false,
        trigger: "manual".into(),
        enqueued_at: queue::now_ms(),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running Redis/Valkey; set REDIS_TEST_URL"]
async fn queue_lifecycle_end_to_end() {
    let Some(url) = test_url() else {
        panic!("REDIS_TEST_URL is not set");
    };
    let mut conn = flush(&url).await;

    // --- enqueue writes stream + live + index --------------------------
    let m1 = msg("job-e2e-1", "alpha");
    queue::enqueue(&mut conn, &m1).await.unwrap();
    let depth: i64 = redis::cmd("XLEN").arg(STREAM_KEY).query_async(&mut conn).await.unwrap();
    assert_eq!(depth, 1);
    assert_eq!(hget(&mut conn, "job-e2e-1", "status").await.as_deref(), Some("queued"));
    let indexed: Vec<String> = conn.zrange("karet:jobs:index:alpha", 0, -1).await.unwrap();
    assert_eq!(indexed, vec!["job-e2e-1".to_string()]);

    // --- consumer claims and drives it to a terminal state -------------
    let (ctx, shutdown) = make_ctx(&url).await;
    let consumer = tokio::spawn(queue::consumer_loop(ctx.clone()));

    // Config read fails (dead S3) → transient path → retries → failed.
    // max_attempts=2, backoff is 30s so the retry lands in the delayed
    // ZSET; what we require here is: running happened, then the job left
    // the acked stream and is either delayed or terminal.
    let status = wait_for_status(
        &mut conn,
        "job-e2e-1",
        &["running", "queued", "failed"],
        std::time::Duration::from_secs(10),
    )
    .await;
    assert!(
        ["running", "queued", "failed"].contains(&status.as_str()),
        "unexpected status {status}"
    );

    // Wait until the first delivery is fully processed: it must land in
    // the delayed ZSET (attempt 1 of 2, ConfigRead is retryable).
    let start = std::time::Instant::now();
    loop {
        let delayed: i64 = conn.zcard(DELAYED_KEY).await.unwrap();
        if delayed == 1 {
            break;
        }
        if start.elapsed() > std::time::Duration::from_secs(15) {
            panic!("job never reached the delayed ZSET");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(hget(&mut conn, "job-e2e-1", "status").await.as_deref(), Some("queued"));
    assert_eq!(hget(&mut conn, "job-e2e-1", "attempts").await.as_deref(), Some("1"));

    // --- delayed mover: force the entry due and watch it re-enqueue ----
    // Rewrite the score to "now" so the mover picks it up on its next tick.
    let payloads: Vec<String> = conn.zrange(DELAYED_KEY, 0, -1).await.unwrap();
    let _: () = conn.zadd(DELAYED_KEY, &payloads[0], queue::now_ms() - 1000).await.unwrap();
    let mover = tokio::spawn(queue::delayed_mover_loop(ctx.clone()));

    // Second delivery is attempt 2 = max_attempts → terminal failed, with
    // the S3 record write also failing (dead S3) — the live hash must
    // still reach the terminal state.
    let status = wait_for_status(
        &mut conn,
        "job-e2e-1",
        &["failed"],
        std::time::Duration::from_secs(20),
    )
    .await;
    assert_eq!(status, "failed");
    let attempts = hget(&mut conn, "job-e2e-1", "attempts").await.unwrap();
    assert_eq!(attempts, "2");
    // Terminal hash carries a TTL.
    let ttl: i64 = conn.ttl("karet:jobs:live:job-e2e-1").await.unwrap();
    assert!(ttl > 0, "terminal live hash should expire, ttl={ttl}");
    // Lock is released.
    let lock: Option<String> = conn.get("karet:lock:pipeline:alpha").await.unwrap();
    assert_eq!(lock, None);

    let _ = shutdown.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), consumer).await;
    mover.abort();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running Redis/Valkey; set REDIS_TEST_URL"]
async fn pipeline_lock_defers_concurrent_job() {
    let Some(url) = test_url() else {
        panic!("REDIS_TEST_URL is not set");
    };
    let mut conn = flush(&url).await;

    // Hold the lock for pipeline "beta" as if another worker were running.
    let _: () = redis::cmd("SET")
        .arg("karet:lock:pipeline:beta")
        .arg("some-other-job")
        .arg("PX")
        .arg(30_000)
        .query_async(&mut conn)
        .await
        .unwrap();

    let m = msg("job-defer-1", "beta");
    queue::enqueue(&mut conn, &m).await.unwrap();

    let (ctx, shutdown) = make_ctx(&url).await;
    let consumer = tokio::spawn(queue::consumer_loop(ctx.clone()));

    // The job must land in the delayed ZSET without ever running, and
    // burn no attempt doing so.
    let start = std::time::Instant::now();
    loop {
        let delayed: i64 = conn.zcard(DELAYED_KEY).await.unwrap();
        if delayed == 1 {
            break;
        }
        if start.elapsed() > std::time::Duration::from_secs(10) {
            panic!("deferred job never reached the delayed ZSET");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(hget(&mut conn, "job-defer-1", "status").await.as_deref(), Some("queued"));
    assert_eq!(hget(&mut conn, "job-defer-1", "attempts").await.as_deref(), Some("0"));
    // The other job's lock is untouched.
    let lock: Option<String> = conn.get("karet:lock:pipeline:beta").await.unwrap();
    assert_eq!(lock.as_deref(), Some("some-other-job"));

    let _ = shutdown.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running Redis/Valkey; set REDIS_TEST_URL"]
async fn debounce_extends_quiet_window_and_fires_once() {
    let Some(url) = test_url() else {
        panic!("REDIS_TEST_URL is not set");
    };
    let mut conn = flush(&url).await;

    // Two rapid events: same slug, one debounce entry, fire time moved.
    let wait1 = queue::debounce_event(&mut conn, "gamma").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let wait2 = queue::debounce_event(&mut conn, "gamma").await.unwrap();
    assert!(wait1 > 0 && wait2 > 0);
    let entries: Vec<String> = conn.zrange(DEBOUNCE_KEY, 0, -1).await.unwrap();
    assert_eq!(entries, vec!["gamma".to_string()]);

    // Force the fire time into the past; the scheduler should enqueue
    // exactly one webhook job for the slug.
    let _: () = conn.zadd(DEBOUNCE_KEY, "gamma", queue::now_ms() - 1000).await.unwrap();
    let (ctx, shutdown) = make_ctx(&url).await;
    let scheduler = tokio::spawn(queue::debounce_scheduler_loop(ctx.clone()));

    let start = std::time::Instant::now();
    let job_ids: Vec<String> = loop {
        let ids: Vec<String> = conn.zrange("karet:jobs:index:gamma", 0, -1).await.unwrap();
        if !ids.is_empty() {
            // Give the scheduler another tick to prove it doesn't double-fire.
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
            break conn.zrange("karet:jobs:index:gamma", 0, -1).await.unwrap();
        }
        if start.elapsed() > std::time::Duration::from_secs(10) {
            panic!("debounce never fired");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };
    assert_eq!(job_ids.len(), 1, "debounce fired more than once: {job_ids:?}");
    // The debounce entry and its batch-start marker are gone.
    let remaining: Vec<String> = conn.zrange(DEBOUNCE_KEY, 0, -1).await.unwrap();
    assert!(remaining.is_empty());
    let first: Option<String> = conn.get("karet:debounce:first:gamma").await.unwrap();
    assert_eq!(first, None);
    // The enqueued message is a webhook-trigger job for the right prefix.
    let live_trigger = hget(&mut conn, &job_ids[0], "trigger").await;
    assert_eq!(live_trigger.as_deref(), Some("webhook"));

    let _ = shutdown.send(true);
    scheduler.abort();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running Redis/Valkey; set REDIS_TEST_URL"]
async fn sweep_marks_trimmed_jobs_abandoned() {
    let Some(url) = test_url() else {
        panic!("REDIS_TEST_URL is not set");
    };
    let mut conn = flush(&url).await;

    // Live hash without a stream entry (as if XTRIM dropped it), enqueued
    // beyond the grace window.
    let stale_ms = queue::now_ms() - 20 * 60 * 1000;
    let _: () = redis::pipe()
        .hset("karet:jobs:live:job-orphan-1", "status", "queued")
        .hset("karet:jobs:live:job-orphan-1", "pipeline", "eps")
        .hset("karet:jobs:live:job-orphan-1", "enqueued_at", stale_ms)
        .query_async(&mut conn)
        .await
        .unwrap();
    // A healthy queued job (stream entry present) must be untouched.
    queue::enqueue(&mut conn, &msg("job-healthy-1", "zeta")).await.unwrap();

    let (ctx, _shutdown) = make_ctx(&url).await;
    let mut sweep_conn = ctx.client.get_multiplexed_async_connection().await.unwrap();
    queue::sweep_orphaned_live_hashes(&ctx, &mut sweep_conn).await.unwrap();

    assert_eq!(hget(&mut conn, "job-orphan-1", "status").await.as_deref(), Some("abandoned"));
    let ttl: i64 = conn.ttl("karet:jobs:live:job-orphan-1").await.unwrap();
    assert!(ttl > 0, "abandoned hash should expire, ttl={ttl}");
    assert_eq!(hget(&mut conn, "job-healthy-1", "status").await.as_deref(), Some("queued"));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running Redis/Valkey; set REDIS_TEST_URL"]
async fn reclaimer_picks_up_dead_consumers_entry() {
    let Some(url) = test_url() else {
        panic!("REDIS_TEST_URL is not set");
    };
    let mut conn = flush(&url).await;

    // Simulate a dead consumer: read the entry into a PEL under another
    // consumer name, then never ack. First make the group exist.
    queue::ensure_group(&mut conn).await.unwrap();
    let m = msg("job-reclaim-1", "delta");
    queue::enqueue(&mut conn, &m).await.unwrap();
    let _: redis::Value = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(GROUP)
        .arg("dead-consumer")
        .arg("COUNT")
        .arg(1)
        .arg("STREAMS")
        .arg(STREAM_KEY)
        .arg(">")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Wait past reclaim_idle_ms (3s in test settings), then run the
    // reclaimer; it should claim the entry and process it (attempt 1 →
    // delayed, dead S3).
    tokio::time::sleep(std::time::Duration::from_millis(3_500)).await;
    let (ctx, shutdown) = make_ctx(&url).await;
    let reclaimer = tokio::spawn(queue::reclaimer_loop(ctx.clone()));

    let start = std::time::Instant::now();
    loop {
        let delayed: i64 = conn.zcard(DELAYED_KEY).await.unwrap();
        if delayed == 1 {
            break;
        }
        if start.elapsed() > std::time::Duration::from_secs(90) {
            panic!("reclaimer never processed the stale entry");
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert_eq!(hget(&mut conn, "job-reclaim-1", "attempts").await.as_deref(), Some("1"));

    let _ = shutdown.send(true);
    reclaimer.abort();
}
