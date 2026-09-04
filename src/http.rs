//! HTTP API (Axum).
//!
//! Routes: `GET /health`, `POST /config/validate`, `POST /jobs/run`.

use std::sync::Arc;

use axum::{
    extract::{Json, Request, State},
    http::{header, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
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
    /// Bucket for ELT control-plane data (configs, dashboards, jobs).
    pub pipelines_bucket: String,
    /// Bucket for raw ingested CSV data.
    pub lake_bucket: String,
    /// Bucket for query-ready partitioned Parquet output.
    pub warehouse_bucket: String,
    pub s3_client: Option<aws_sdk_s3::Client>,
    /// Shared bearer token (`KARET_WORKER_TOKEN`) required on every
    /// mutating route. `/health` stays open for liveness probes.
    pub auth_token: String,
}

pub fn router(state: AppState) -> Router {
    let state = Arc::new(state);
    // Mutating routes sit behind the bearer-token check; `/health` stays
    // open so liveness probes need no credentials.
    let protected = Router::new()
        .route("/config/validate", post(post_config_validate))
        .route("/jobs/run", post(run_pipeline))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_bearer_token,
        ));
    Router::new()
        .route("/health", get(health))
        .merge(protected)
        .with_state(state)
}

/// Middleware: require `Authorization: Bearer <KARET_WORKER_TOKEN>`.
///
/// The worker has no user model; possession of the shared token is the
/// entire authorization signal, mirroring the web app's webhook secret.
async fn require_bearer_token(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let provided = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    match provided {
        Some(token) if constant_time_eq(token.as_bytes(), state.auth_token.as_bytes()) => {
            next.run(request).await
        }
        _ => error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "missing or invalid bearer token",
            Vec::new(),
        ),
    }
}

/// Constant-time byte comparison so token verification doesn't leak
/// match-prefix length through response timing.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Maximum accepted `pipeline_prefix` length. Generous for
/// `pipelines/<slug>/` shapes while still bounding pathological input.
const MAX_PREFIX_LEN: usize = 512;

/// Validate a caller-supplied pipeline prefix before it is interpolated
/// into S3 keys for reads, writes, and (with `clean_run`) deletes.
///
/// Accepts only `segment/segment/.../` shapes: a trailing slash, no
/// leading slash, non-empty segments of `[A-Za-z0-9._-]`, and no `.` /
/// `..` segments. The web app sends `pipelines/<slug>/` where the slug is
/// already `[a-z0-9-]`, so this is a superset of legitimate traffic.
pub fn validate_pipeline_prefix(prefix: &str) -> Result<(), String> {
    if prefix.is_empty() {
        return Err("pipeline_prefix must not be empty".into());
    }
    if prefix.len() > MAX_PREFIX_LEN {
        return Err(format!(
            "pipeline_prefix is too long ({} bytes, max {MAX_PREFIX_LEN})",
            prefix.len()
        ));
    }
    if prefix.starts_with('/') {
        return Err("pipeline_prefix must not start with '/'".into());
    }
    if !prefix.ends_with('/') {
        return Err("pipeline_prefix must end with '/'".into());
    }
    for segment in prefix[..prefix.len() - 1].split('/') {
        if segment.is_empty() {
            return Err("pipeline_prefix must not contain empty path segments".into());
        }
        if segment == "." || segment == ".." {
            return Err("pipeline_prefix must not contain '.' or '..' segments".into());
        }
        if !segment
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        {
            return Err(format!(
                "pipeline_prefix segment '{segment}' contains characters outside [A-Za-z0-9._-]"
            ));
        }
    }
    Ok(())
}

/// `GET /health`, liveness probe.
async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// `POST /config/validate`, deserialize the body as a `PipelineConfig`
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

/// `POST /jobs/run`, execute a pipeline run for the given prefix.
/// Reads `<prefix>pipeline.json`, lists raw CSVs, runs the pipeline, writes
/// Parquet output.
async fn run_pipeline(
    State(state): State<Arc<AppState>>,
    Json(body): Json<RunPipelineRequest>,
) -> axum::response::Response {
    // Reject malformed/traversal-shaped prefixes before any S3 operation;
    // this string is interpolated into read, write, and delete keys.
    if let Err(message) = validate_pipeline_prefix(&body.pipeline_prefix) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_prefix",
            &message,
            Vec::new(),
        );
    }

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

    let config_bytes = match s3mod::get_bytes(&s3_client, &state.pipelines_bucket, &config_key).await {
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

    // clean_run: delete existing warehouse output under the tables the
    // current config declares (so stale tables from prior configs aren't
    // wiped).
    if body.clean_run {
        for table in &cfg.analytic_tables {
            let table_prefix = format!("{prefix}{}/", table.id);
            match s3mod::list_keys(&s3_client, &state.warehouse_bucket, &table_prefix).await {
                Ok(keys) => {
                    for key in keys {
                        let _ = s3_client
                            .delete_object()
                            .bucket(&state.warehouse_bucket)
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

    // Download every raw CSV file under each source container's path_prefix
    // from the lake bucket.
    let mut all_files: Vec<(String, Vec<u8>)> = Vec::new();
    for sc in &cfg.source_containers {
        let raw_prefix = format!("{prefix}{}", sc.path_prefix);
        let ext = ".csv";
        let keys = match s3mod::list_keys(&s3_client, &state.lake_bucket, &raw_prefix).await {
            Ok(k) => k,
            Err(e) => {
                tracing::warn!("failed to list keys for {raw_prefix}: {e}");
                continue;
            }
        };
        for key in keys {
            if !key.ends_with(ext) {
                continue;
            }
            match s3mod::get_bytes(&s3_client, &state.lake_bucket, &key).await {
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
        state.warehouse_bucket.clone(),
        prefix.to_string(),
    );

    // Precompile the lookup registry once per job; shared by every mapping.
    let matchers = lookup::build_registry(&cfg.lookup_mappings);

    let mut total_partitions = 0usize;
    let mut errors: Vec<String> = Vec::new();
    let files_processed = all_files.len();

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
            "files_processed": files_processed,
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

    const TEST_TOKEN: &str = "test-token";

    fn test_state() -> AppState {
        AppState {
            pipelines_bucket: "karet-pipelines".into(),
            lake_bucket: "karet-lake".into(),
            warehouse_bucket: "karet-warehouse".into(),
            s3_client: None,
            auth_token: TEST_TOKEN.into(),
        }
    }

    /// POST `body` to `uri` with the given bearer token (None = no header).
    async fn post_with_auth(
        uri: &str,
        token: Option<&str>,
        body: &str,
    ) -> axum::response::Response {
        let app = router(test_state());
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("content-type", "application/json");
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        app.oneshot(builder.body(Body::from(body.to_owned())).unwrap())
            .await
            .unwrap()
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
        let response = post_with_auth("/config/validate", Some(TEST_TOKEN), VALID_CONFIG).await;
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
        let response = post_with_auth("/config/validate", Some(TEST_TOKEN), bad).await;
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
        let response = post_with_auth("/config/validate", Some(TEST_TOKEN), "not json").await;
        assert_eq!(response.status(), StatusCode::OK);
        let v = read_json(response).await;
        assert_eq!(v["ok"], false);
        assert_eq!(v["errors"][0]["kind"], "schema");
    }

    // ---- Bearer-token auth ------------------------------------------------

    #[tokio::test]
    async fn post_routes_reject_missing_token() {
        for uri in ["/config/validate", "/jobs/run"] {
            let response = post_with_auth(uri, None, "{}").await;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{uri}");
            let v = read_json(response).await;
            assert_eq!(v["error"]["kind"], "unauthorized", "{uri}");
        }
    }

    #[tokio::test]
    async fn post_routes_reject_wrong_token() {
        for uri in ["/config/validate", "/jobs/run"] {
            let response = post_with_auth(uri, Some("wrong-token"), "{}").await;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{uri}");
        }
    }

    #[tokio::test]
    async fn health_needs_no_token() {
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

    // ---- pipeline_prefix validation ----------------------------------------

    #[tokio::test]
    async fn jobs_run_rejects_traversal_prefix() {
        let response = post_with_auth(
            "/jobs/run",
            Some(TEST_TOKEN),
            r#"{"pipeline_prefix": "pipelines/../other/", "clean_run": true}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let v = read_json(response).await;
        assert_eq!(v["error"]["kind"], "invalid_prefix");
    }

    #[tokio::test]
    async fn jobs_run_with_valid_prefix_reaches_s3_client_check() {
        // Prefix validation passes, so the handler proceeds to the S3
        // client check, which fails in tests (s3_client: None). Proves
        // validation runs before, and independently of, S3 access.
        let response = post_with_auth(
            "/jobs/run",
            Some(TEST_TOKEN),
            r#"{"pipeline_prefix": "pipelines/demo/"}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let v = read_json(response).await;
        assert_eq!(v["error"]["kind"], "no_s3_client");
    }

    #[test]
    fn validate_pipeline_prefix_accepts_legitimate_shapes() {
        for prefix in [
            "pipelines/demo/",
            "pipelines/my-pipeline-2/",
            "a/b.c/d_e/",
            "single/",
        ] {
            assert!(
                validate_pipeline_prefix(prefix).is_ok(),
                "expected Ok for {prefix:?}"
            );
        }
    }

    #[test]
    fn validate_pipeline_prefix_rejects_malformed_shapes() {
        let cases = [
            "",
            "no-trailing-slash",
            "/leading/slash/",
            "double//slash/",
            "pipelines/../other/",
            "./relative/",
            "pipelines/sp ace/",
            "pipelines/semi;colon/",
            "pipelines/quo'te/",
        ];
        for prefix in cases {
            assert!(
                validate_pipeline_prefix(prefix).is_err(),
                "expected Err for {prefix:?}"
            );
        }
        let too_long = format!("{}/", "a".repeat(600));
        assert!(validate_pipeline_prefix(&too_long).is_err());
    }
}
