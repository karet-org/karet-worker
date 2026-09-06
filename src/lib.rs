//! `karet-worker`, data pipeline worker for the Karet analytics platform.
//!
//! Reads `Pipeline_Config` from S3, ingests CSVs with Polars, evaluates
//! AST-JSON mapping expressions, and writes partitioned Parquet to S3.

pub mod ast;
pub mod assertions;
pub mod config;
pub mod error;
pub mod evaluator;
pub mod http;
pub mod job;
pub mod lookup;
pub mod pipeline;
pub mod queue;
pub mod s3;

#[cfg(any(test, feature = "test-support"))]
pub mod testgen;

/// Env vars the worker cannot start without.
pub const REQUIRED_ENV_VARS: &[&str] = &[
    "S3_BUCKET_PIPELINES",
    "S3_BUCKET_LAKE",
    "S3_BUCKET_WAREHOUSE",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION",
    "AWS_ENDPOINT_URL",
    "KARET_WORKER_TOKEN",
    "REDIS_URL",
    "KARET_WEBHOOK_SECRET",
];

/// Assert every env var in `names` is set to a non-empty value.
pub fn require_env_vars(names: &[&str]) -> Result<(), String> {
    let missing: Vec<&str> = names
        .iter()
        .copied()
        .filter(|name| match std::env::var(name) {
            Ok(v) => v.is_empty(),
            Err(_) => true,
        })
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "missing required environment variable(s): {}. \
             set them before starting karet-worker (see docker-compose.yaml)",
            missing.join(", ")
        ))
    }
}

/// Binary entry point, builds the HTTP router and serves it on `PORT`.
/// When `REDIS_URL` is set, also starts the queue loops (consumer,
/// delayed mover, reclaimer, debounce scheduler).
pub async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!("starting karet-worker");

    if let Err(message) = require_env_vars(REQUIRED_ENV_VARS) {
        tracing::error!("{message}");
        return Err(message.into());
    }

    let pipelines_bucket = std::env::var("S3_BUCKET_PIPELINES").expect("checked above");
    let lake_bucket = std::env::var("S3_BUCKET_LAKE").expect("checked above");
    let warehouse_bucket = std::env::var("S3_BUCKET_WAREHOUSE").expect("checked above");
    let auth_token = std::env::var("KARET_WORKER_TOKEN").expect("checked above");

    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(std::env::var("AWS_ENDPOINT_URL").unwrap_or_default())
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
        .force_path_style(true)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

    // The Redis queue is the job transport (REDIS_URL is required). The
    // webhook secret must be non-empty so /events/s3 can never run open.
    let redis_url = std::env::var("REDIS_URL").expect("checked above");
    let webhook_secret = std::env::var("KARET_WEBHOOK_SECRET").expect("checked above");

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let client = redis::Client::open(redis_url.as_str())?;
    let consumer_name = format!(
        "worker-{}-{}",
        std::env::var("HOSTNAME").unwrap_or_else(|_| "local".into()),
        std::process::id()
    );
    let settings = queue::QueueSettings {
        max_attempts: env_parse("MAX_ATTEMPTS", 3),
        lock_ttl_ms: env_parse("JOB_LOCK_TTL_MS", 90_000),
        heartbeat_ms: env_parse("HEARTBEAT_MS", 30_000),
        ..queue::QueueSettings::default()
    };
    let queue_ctx = std::sync::Arc::new(queue::QueueCtx {
        client,
        job_ctx: job::JobContext {
            s3_client: s3_client.clone(),
            pipelines_bucket: pipelines_bucket.clone(),
            lake_bucket: lake_bucket.clone(),
            warehouse_bucket: warehouse_bucket.clone(),
        },
        consumer_name,
        settings,
        in_flight: std::sync::atomic::AtomicUsize::new(0),
        consumer_ok: std::sync::atomic::AtomicBool::new(true),
        shutdown: shutdown_rx.clone(),
    });

    let state = http::AppState {
        pipelines_bucket,
        lake_bucket,
        warehouse_bucket,
        s3_client: Some(s3_client),
        auth_token,
        queue: Some(queue_ctx.clone()),
        webhook_secret: Some(webhook_secret),
        routing: std::sync::Arc::new(tokio::sync::RwLock::new(http::RoutingCache::default())),
    };

    let concurrency: usize = env_parse("WORKER_CONCURRENCY", 1);
    tracing::info!(
        "queue enabled: consumer={} concurrency={concurrency}",
        queue_ctx.consumer_name
    );
    let mut loop_handles = Vec::new();
    for _ in 0..concurrency.max(1) {
        loop_handles.push(tokio::spawn(queue::consumer_loop(queue_ctx.clone())));
    }
    loop_handles.push(tokio::spawn(queue::delayed_mover_loop(queue_ctx.clone())));
    loop_handles.push(tokio::spawn(queue::reclaimer_loop(queue_ctx.clone())));
    loop_handles.push(tokio::spawn(queue::debounce_scheduler_loop(queue_ctx.clone())));

    let app = http::router(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("listening on {addr}");

    // Graceful shutdown: SIGTERM/SIGINT stops the HTTP server and flips
    // the watch flag; queue loops exit at their next check and in-flight
    // jobs run to completion before the process exits.
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    tracing::info!("http server stopped; draining queue loops");
    let _ = shutdown_tx.send(true);
    for handle in loop_handles {
        let _ = handle.await;
    }
    tracing::info!("shutdown complete");
    Ok(())
}

/// Resolves when SIGTERM or SIGINT arrives.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
}

/// Parse an env var with a default; invalid values fall back with a warning.
fn env_parse<T: std::str::FromStr + std::fmt::Display + Copy>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(raw) => raw.parse().unwrap_or_else(|_| {
            tracing::warn!("invalid {name}={raw}; using default {default}");
            default
        }),
        Err(_) => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes env-var mutation so concurrent tests don't stomp.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        use std::sync::{Mutex, OnceLock};
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn require_env_vars_succeeds_when_all_set() {
        let _guard = env_lock();
        // SAFETY: env mutation is serialized by `env_lock`.
        unsafe {
            std::env::set_var("KARET_TEST_A", "a");
            std::env::set_var("KARET_TEST_B", "b");
        }
        let result = require_env_vars(&["KARET_TEST_A", "KARET_TEST_B"]);
        // Clean up even if the assertion fails.
        unsafe {
            std::env::remove_var("KARET_TEST_A");
            std::env::remove_var("KARET_TEST_B");
        }
        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    #[test]
    fn require_env_vars_names_every_missing_var() {
        let _guard = env_lock();
        // SAFETY: env mutation is serialized by `env_lock`.
        unsafe {
            std::env::remove_var("KARET_TEST_MISSING_1");
            std::env::remove_var("KARET_TEST_MISSING_2");
            std::env::set_var("KARET_TEST_PRESENT", "x");
        }
        let result = require_env_vars(&[
            "KARET_TEST_MISSING_1",
            "KARET_TEST_PRESENT",
            "KARET_TEST_MISSING_2",
        ]);
        unsafe {
            std::env::remove_var("KARET_TEST_PRESENT");
        }
        let err = result.expect_err("expected Err when vars are missing");
        assert!(
            err.contains("KARET_TEST_MISSING_1"),
            "error should name KARET_TEST_MISSING_1: {err}"
        );
        assert!(
            err.contains("KARET_TEST_MISSING_2"),
            "error should name KARET_TEST_MISSING_2: {err}"
        );
        assert!(
            !err.contains("KARET_TEST_PRESENT"),
            "error should not name the present var: {err}"
        );
    }

    #[test]
    fn require_env_vars_treats_empty_string_as_missing() {
        let _guard = env_lock();
        unsafe {
            std::env::set_var("KARET_TEST_EMPTY", "");
        }
        let result = require_env_vars(&["KARET_TEST_EMPTY"]);
        unsafe {
            std::env::remove_var("KARET_TEST_EMPTY");
        }
        let err = result.expect_err("empty string should be treated as missing");
        assert!(err.contains("KARET_TEST_EMPTY"), "{err}");
    }
}
