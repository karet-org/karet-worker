//! `karet-worker` -- data pipeline worker for the Karet analytics platform.
//!
//! Reads `Pipeline_Config` from S3, ingests CSVs with Polars, evaluates
//! AST-JSON mapping expressions, and writes partitioned Parquet to S3.

pub mod ast;
pub mod assertions;
pub mod config;
pub mod error;
pub mod evaluator;
pub mod http;
pub mod lookup;
pub mod pipeline;
pub mod s3;

#[cfg(any(test, feature = "test-support"))]
pub mod testgen;

/// Env vars the worker cannot start without.
pub const REQUIRED_ENV_VARS: &[&str] = &[
    "S3_BUCKET",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION",
    "AWS_ENDPOINT_URL",
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

/// Binary entry point -- builds the HTTP router and serves it on `PORT`.
pub async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!("starting karet-worker");

    if let Err(message) = require_env_vars(REQUIRED_ENV_VARS) {
        tracing::error!("{message}");
        return Err(message.into());
    }

    let bucket = std::env::var("S3_BUCKET").expect("S3_BUCKET checked above");

    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(std::env::var("AWS_ENDPOINT_URL").unwrap_or_default())
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
        .force_path_style(true)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

    let state = http::AppState {
        s3_bucket: bucket,
        s3_client: Some(s3_client),
    };

    let app = http::router(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
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
