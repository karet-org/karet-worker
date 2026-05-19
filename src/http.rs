//! HTTP API (Axum).
//!
//! Routes: `GET /health`, `POST /config/validate`, `POST /jobs/run`.

use std::sync::Arc;

use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use uuid::Uuid;

use crate::assertions::validate_assertions;
use crate::config::{self, ConfigError, PipelineConfig};
use crate::lookup;
use crate::pipeline;
use crate::s3 as s3mod;

#[derive(Clone)]
pub struct AppState {
    pub s3_bucket: String,
    pub s3_client: Option<aws_sdk_s3::Client>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/config/validate", post(post_config_validate))
        .route("/jobs/run", post(run_pipeline))
        .with_state(Arc::new(state))
}

/// `GET /health` — liveness probe.
async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// `POST /config/validate` — deserialize the body as a `PipelineConfig`
/// and run [`config::validate`]. Always returns 200; the `ok` field
/// reflects the result and `errors` lists details.
async fn post_config_validate(body: String) -> impl IntoResponse {
    match serde_json::from_str::<PipelineConfig>(&body) {
        Ok(cfg) => match config::validate(&cfg) {
            Ok(()) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))),
            Err(errs) => (
                StatusCode::OK,
                Json(serde_json::json!({
                    "ok": false,
                    "errors": errs.iter().map(error_to_json).collect::<Vec<_>>(),
                })),
            ),
        },
        Err(e) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "ok": false,
                "errors": [{
                    "kind": "schema",
                    "message": e.to_string(),
                    "path": "/",
                }],
            })),
        ),
    }
}

#[derive(Debug, Deserialize)]
struct RunPipelineRequest {
    pipeline_prefix: String,
    #[serde(default)]
    clean_run: bool,
}

/// `POST /jobs/run` — execute a pipeline run for the given prefix.
/// Reads `<prefix>pipeline.json`, lists raw CSVs, runs the pipeline, writes
/// Parquet output.
async fn run_pipeline(
    State(state): State<Arc<AppState>>,
    Json(body): Json<RunPipelineRequest>,
) -> axum::response::Response {
    let s3_client = match &state.s3_client {
        Some(c) => c.clone(),
        None => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "no_s3_client",
                "S3 client not configured",
                Vec::new(),
            );
        }
    };

    let prefix = &body.pipeline_prefix;
    let config_key = format!("{prefix}pipeline.json");

    let config_bytes = match s3mod::get_bytes(&s3_client, &state.s3_bucket, &config_key).await {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "config_read_failed",
                &e,
                Vec::new(),
            );
        }
    };
    let cfg: PipelineConfig = match serde_json::from_slice(&config_bytes) {
        Ok(c) => c,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "config_parse_failed",
                &e.to_string(),
                Vec::new(),
            );
        }
    };

    let matchers = lookup::build_registry(&cfg.lookup_mappings);

    // clean_run: delete existing clean output under the tables the current
    // config declares (so stale tables from prior configs aren't wiped).
    if body.clean_run {
        for table in &cfg.analytic_tables {
            let table_prefix = format!("{prefix}clean/{}/", table.id);
            match s3mod::list_keys(&s3_client, &state.s3_bucket, &table_prefix).await {
                Ok(keys) => {
                    for key in keys {
                        let _ = s3_client
                            .delete_object()
                            .bucket(&state.s3_bucket)
                            .key(&key)
                            .send()
                            .await;
                    }
                    tracing::info!("clean_run: deleted existing clean output under {table_prefix}");
                }
                Err(e) => tracing::warn!(
                    "clean_run: failed to list clean keys under {table_prefix}: {e}"
                ),
            }
        }
    }

    // Download every raw CSV under each source container's path_prefix.
    let mut all_files: Vec<(String, Vec<u8>)> = Vec::new();
    for sc in &cfg.source_containers {
        let raw_prefix = format!("{prefix}{}", sc.path_prefix);
        let keys = match s3mod::list_keys(&s3_client, &state.s3_bucket, &raw_prefix).await {
            Ok(k) => k,
            Err(e) => {
                tracing::warn!("failed to list keys for {raw_prefix}: {e}");
                continue;
            }
        };
        for key in keys {
            if !key.ends_with(".csv") {
                continue;
            }
            match s3mod::get_bytes(&s3_client, &state.s3_bucket, &key).await {
                Ok(bytes) => {
                    // Strip pipeline prefix so the key matches path_prefix.
                    let rel_key = key.strip_prefix(prefix).unwrap_or(&key).to_string();
                    all_files.push((rel_key, bytes));
                }
                Err(e) => tracing::warn!("failed to download {key}: {e}"),
            }
        }
    }

    if all_files.is_empty() {
        return error_response(
            StatusCode::OK,
            "no_files",
            "No CSV files found to process",
            Vec::new(),
        );
    }

    let uploader = s3mod::S3PartitionUploader::new(
        s3_client.clone(),
        state.s3_bucket.clone(),
        prefix.to_string(),
    );

    let mut total_partitions = 0usize;
    let mut errors: Vec<String> = Vec::new();

    for mapping in &cfg.mappings {
        let sc = match cfg
            .source_containers
            .iter()
            .find(|s| s.id == mapping.source_container_id)
        {
            Some(s) => s,
            None => continue,
        };
        let mapping_files: Vec<(String, Vec<u8>)> = all_files
            .iter()
            .filter(|(k, _)| k.starts_with(&sc.path_prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        if mapping_files.is_empty() {
            continue;
        }

        let lf = match pipeline::ingest_many(&mapping_files, &cfg, &matchers) {
            Ok(lf) => lf,
            Err(e) => {
                errors.push(format!("ingest {}: {e}", mapping.id));
                continue;
            }
        };
        let df = match lf.collect() {
            Ok(df) => df,
            Err(e) => {
                errors.push(format!("collect {}: {e}", mapping.id));
                continue;
            }
        };

        let table = match cfg
            .analytic_tables
            .iter()
            .find(|t| t.id == mapping.analytic_table_id)
        {
            Some(t) => t,
            None => {
                errors.push(format!("table {} not found", mapping.analytic_table_id));
                continue;
            }
        };

        // Assertions: failure fails this mapping only; others still run.
        let violations = validate_assertions(&df, table);
        if !violations.is_empty() {
            for v in &violations {
                errors.push(format!("assertion {}: {v}", mapping.id));
            }
            tracing::warn!(
                mapping = %mapping.id,
                count = violations.len(),
                "assertion violations; skipping upload",
            );
            continue;
        }

        let partitions = match pipeline::produce_partitions(&df, mapping, table) {
            Ok(p) => p,
            Err(e) => {
                errors.push(format!("partition {}: {e}", mapping.id));
                continue;
            }
        };

        match pipeline::upload_partitions(&uploader, &partitions) {
            Ok(keys) => {
                total_partitions += keys.len();
            }
            Err(e) => {
                errors.push(format!("upload {}: {e}", mapping.id));
            }
        }
    }

    let job_id = Uuid::new_v4().to_string();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "job_id": job_id,
            "partitions_written": total_partitions,
            "files_processed": all_files.len(),
            "errors": errors,
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

/// Build the `{"error": {...}}` envelope used by every 4xx/5xx response.
fn error_response(
    status: StatusCode,
    kind: &str,
    message: &str,
    details: Vec<String>,
) -> axum::response::Response {
    (
        status,
        Json(serde_json::json!({
            "error": {
                "kind": kind,
                "message": message,
                "details": details,
            }
        })),
    )
        .into_response()
}

/// Convert a [`ConfigError`] into the `/config/validate` error shape.
fn error_to_json(err: &ConfigError) -> serde_json::Value {
    let (kind, path) = match err {
        ConfigError::DuplicateId { path, .. } => ("duplicate_id", path.as_str()),
        ConfigError::DanglingReference { path, .. } => ("dangling_reference", path.as_str()),
        ConfigError::MissingField { path, .. } => ("missing_field", path.as_str()),
        ConfigError::Schema { path, .. } => ("schema", path.as_str()),
    };
    serde_json::json!({
        "kind": kind,
        "message": err.to_string(),
        "path": path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Method, Request};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    const VALID_CONFIG: &str = r#"{
        "version": 1,
        "source_containers": [],
        "lookup_mappings": [],
        "mappings": [],
        "analytic_tables": [],
        "layout": {}
    }"#;

    fn test_state() -> AppState {
        AppState {
            s3_bucket: "karet-data".into(),
            s3_client: None,
        }
    }

    async fn read_json(response: axum::response::Response) -> serde_json::Value {
        let body = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&body).unwrap()
    }

    #[tokio::test]
    async fn health_returns_200() {
        let app = router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn config_validate_returns_ok_on_valid() {
        let app = router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/config/validate")
                    .header("content-type", "application/json")
                    .body(Body::from(VALID_CONFIG))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let v = read_json(response).await;
        assert_eq!(v["ok"], true);
    }

    #[tokio::test]
    async fn config_validate_returns_errors_on_invalid() {
        let bad = r#"{
            "version": 1,
            "source_containers": [
                {"id": "s", "name": "S", "path_prefix": "raw/s/", "schema": [{"name":"c","type":"string"}]},
                {"id": "s", "name": "S2", "path_prefix": "raw/s2/", "schema": [{"name":"c","type":"string"}]}
            ],
            "lookup_mappings": [],
            "mappings": [],
            "analytic_tables": [],
            "layout": {}
        }"#;
        let app = router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/config/validate")
                    .header("content-type", "application/json")
                    .body(Body::from(bad))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let v = read_json(response).await;
        assert_eq!(v["ok"], false);
        let errors = v["errors"].as_array().expect("errors is an array");
        assert!(!errors.is_empty());
        assert!(errors
            .iter()
            .any(|e| e["kind"] == "duplicate_id" && e["path"].as_str().is_some()));
    }

    #[tokio::test]
    async fn config_validate_reports_schema_on_unparseable_body() {
        let app = router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/config/validate")
                    .header("content-type", "application/json")
                    .body(Body::from("not json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let v = read_json(response).await;
        assert_eq!(v["ok"], false);
        assert_eq!(v["errors"][0]["kind"], "schema");
    }
}
