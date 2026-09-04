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

use crate::config::{self, ConfigError, PipelineConfig};

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
    /// Present when `REDIS_URL` is set: the queue context shared with the
    /// consumer loops. Enables `/events/s3` and enriches `/health`.
    pub queue: Option<Arc<crate::queue::QueueCtx>>,
    /// Shared secret for `/events/s3` (`KARET_WEBHOOK_SECRET`). Required
    /// non-empty when the queue is enabled.
    pub webhook_secret: Option<String>,
}

pub fn router(state: AppState) -> Router {
    let state = Arc::new(state);
    // Mutating routes sit behind the bearer-token check. `route_layer`
    // (not `layer`) so the middleware wraps only matched routes — with
    // plain `layer` the router's 404 fallback answers 401 for every
    // unknown path, which broke RustFS's HEAD health probe of `/`.
    let protected = Router::new()
        .route("/config/validate", post(post_config_validate))
        .route("/jobs/run", post(run_pipeline))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_bearer_token,
        ));
    Router::new()
        // RustFS HEAD-probes the webhook endpoint's origin root before
        // delivering events; axum serves HEAD via the GET handler.
        .route("/", get(root))
        .route("/health", get(health))
        .route("/events/s3", post(post_s3_events))
        .merge(protected)
        .with_state(state)
}

/// `GET|HEAD /`: identification + webhook-origin health probe target.
async fn root() -> impl IntoResponse {
    (StatusCode::OK, "karet-worker")
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

/// `GET /health`. Liveness plus, when the queue is enabled, a readiness
/// signal: Redis reachability, queue depth, and in-flight count. Returns
/// 503 when the queue is enabled but Redis is unreachable.
async fn health(State(state): State<Arc<AppState>>) -> axum::response::Response {
    let Some(queue) = &state.queue else {
        return (StatusCode::OK, "ok").into_response();
    };
    let in_flight = queue.in_flight.load(std::sync::atomic::Ordering::SeqCst);
    match queue.client.get_multiplexed_async_connection().await {
        Ok(mut conn) => {
            let depth: i64 = redis::cmd("XLEN")
                .arg(crate::queue::STREAM_KEY)
                .query_async(&mut conn)
                .await
                .unwrap_or(-1);
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "redis": "ok",
                    "queue_depth": depth,
                    "in_flight": in_flight,
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "redis": format!("error: {e}"),
                "in_flight": in_flight,
            })),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// RustFS object-event webhook (moved here from the web app)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct S3EventRecord {
    #[serde(rename = "eventName", default)]
    event_name: String,
    #[serde(default)]
    s3: Option<S3EventInner>,
}

#[derive(Debug, Deserialize)]
struct S3EventInner {
    bucket: Option<S3EventBucket>,
    object: Option<S3EventObject>,
}

#[derive(Debug, Deserialize)]
struct S3EventBucket {
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct S3EventObject {
    key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct S3EventPayload {
    #[serde(rename = "Records", default)]
    records: Vec<S3EventRecord>,
}

/// Pull `<slug>` out of a `pipelines/<slug>/...` key (URL-decoded first,
/// matching the S3 event spec). Slug rule mirrors the web app's
/// `sanitizeSlug`: `[a-z0-9-]` after lowercasing; anything else → None.
fn pipeline_slug_from_key(raw_key: &str) -> Option<String> {
    let key = urldecode(raw_key);
    let rest = key.strip_prefix("pipelines/")?;
    let slug_raw = rest.split('/').next()?;
    if slug_raw.is_empty() || rest.len() == slug_raw.len() {
        return None; // no second path segment
    }
    let slug: String = slug_raw
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '-' })
        .collect();
    let slug = slug.trim_matches('-').to_string();
    if slug.is_empty() {
        None
    } else {
        Some(slug)
    }
}

/// Minimal percent-decoding (S3 events encode keys like URL query args,
/// with `+` for space).
fn urldecode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(b) => {
                        out.push(b);
                        i += 3;
                    }
                    None => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `POST /events/s3`: RustFS object-created notifications. Auth is the
/// webhook secret (its own channel, not the worker bearer token, because
/// RustFS can only be configured with a static URL + headers). Accepts
/// `X-Karet-Webhook-Secret: <secret>` or `Authorization: Bearer <secret>`.
async fn post_s3_events(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> axum::response::Response {
    let Some(queue) = state.queue.clone() else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "queue_disabled",
            "REDIS_URL is not configured; webhook events have nowhere to go",
            Vec::new(),
        );
    };
    let Some(expected) = state.webhook_secret.as_deref().filter(|s| !s.is_empty()) else {
        // Fail closed: no secret configured, nothing is accepted.
        return error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "webhook secret is not configured",
            Vec::new(),
        );
    };

    let headers = request.headers();
    let provided = headers
        .get("x-karet-webhook-secret")
        .and_then(|v| v.to_str().ok())
        .or_else(|| {
            headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
        });
    match provided {
        Some(secret) if constant_time_eq(secret.as_bytes(), expected.as_bytes()) => {}
        _ => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "missing or invalid webhook secret",
                Vec::new(),
            );
        }
    }

    let body = match axum::body::to_bytes(request.into_body(), 1 << 20).await {
        Ok(b) => b,
        Err(_) => {
            return error_response(StatusCode::BAD_REQUEST, "invalid_body", "unreadable body", Vec::new());
        }
    };
    let payload: S3EventPayload = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(_) => {
            return error_response(StatusCode::BAD_REQUEST, "invalid_json", "body is not an S3 event payload", Vec::new());
        }
    };

    let mut conn = match queue.client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "redis_unavailable",
                &format!("cannot record event: {e}"),
                Vec::new(),
            );
        }
    };

    let mut scheduled: Vec<String> = Vec::new();
    let received = payload.records.len();
    for rec in payload.records {
        // Only object-create events; deletes/restores must not trigger runs.
        if !rec.event_name.starts_with("s3:ObjectCreated:") {
            continue;
        }
        // Only raw uploads (lake bucket). Warehouse writes would loop.
        let bucket = rec
            .s3
            .as_ref()
            .and_then(|s| s.bucket.as_ref())
            .and_then(|b| b.name.as_deref());
        if bucket != Some(state.lake_bucket.as_str()) {
            continue;
        }
        let Some(key) = rec.s3.as_ref().and_then(|s| s.object.as_ref()).and_then(|o| o.key.as_deref()) else {
            continue;
        };
        let Some(slug) = pipeline_slug_from_key(key) else {
            continue;
        };
        match crate::queue::debounce_event(&mut conn, &slug).await {
            Ok(fire_in_ms) => {
                tracing::info!("debounced upload event for {slug}; fires in {fire_in_ms}ms");
                if !scheduled.contains(&slug) {
                    scheduled.push(slug);
                }
            }
            Err(e) => tracing::error!("debounce_event failed for {slug}: {e}"),
        }
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({ "received": received, "scheduled": scheduled })),
    )
        .into_response()
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

/// `POST /jobs/run` (legacy synchronous path; the Redis consumer is the
/// preferred transport). Executes a pipeline run for the given prefix via
/// the shared executor and reports the outcome in the original response
/// shape.
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

    let ctx = crate::job::JobContext {
        s3_client,
        pipelines_bucket: state.pipelines_bucket.clone(),
        lake_bucket: state.lake_bucket.clone(),
        warehouse_bucket: state.warehouse_bucket.clone(),
    };

    match crate::job::execute_job(
        &ctx,
        &body.pipeline_prefix,
        body.clean_run,
        &crate::job::NoopProgress,
    )
    .await
    {
        Ok(outcome) => {
            let job_id = Uuid::new_v4().to_string();
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "job_id": job_id,
                    "partitions_written": outcome.partitions_written,
                    "files_processed": outcome.files_processed,
                    "errors": outcome.errors,
                })),
            )
                .into_response()
        }
        Err(crate::job::JobError::NoFiles) => error_response(
            StatusCode::OK,
            "no_files",
            "No CSV files found to process",
            Vec::new(),
        ),
        Err(crate::job::JobError::ConfigRead(e)) => {
            error_response(StatusCode::BAD_REQUEST, "config_read_failed", &e, Vec::new())
        }
        Err(crate::job::JobError::ConfigParse(e)) => {
            error_response(StatusCode::BAD_REQUEST, "config_parse_failed", &e, Vec::new())
        }
        Err(crate::job::JobError::ConfigInvalid(e)) => {
            error_response(StatusCode::BAD_REQUEST, "config_invalid", &e, Vec::new())
        }
    }
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
            queue: None,
            webhook_secret: Some("test-webhook-secret".into()),
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

    #[tokio::test]
    async fn root_answers_head_probe_without_auth() {
        // RustFS HEAD-probes the webhook origin's root before delivering.
        let app = router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::HEAD)
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn unknown_path_is_404_not_401() {
        // Regression: the auth middleware must wrap only matched routes.
        // With `.layer` it wrapped the fallback too, turning every
        // unknown path into a 401 and failing RustFS's health probe.
        let app = router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
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

    // ---- /events/s3 webhook ------------------------------------------------

    #[tokio::test]
    async fn events_returns_503_when_queue_disabled() {
        // queue: None (legacy mode) — events have nowhere to go.
        let app = router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/events/s3")
                    .header("x-karet-webhook-secret", "test-webhook-secret")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"Records":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let v = read_json(response).await;
        assert_eq!(v["error"]["kind"], "queue_disabled");
    }

    #[test]
    fn pipeline_slug_from_key_extracts_and_sanitizes() {
        assert_eq!(
            pipeline_slug_from_key("pipelines/demo/raw/tx/jan.csv"),
            Some("demo".into())
        );
        // URL-encoded key (S3 event spec)
        assert_eq!(
            pipeline_slug_from_key("pipelines/my-pipe/raw/a%20b.csv"),
            Some("my-pipe".into())
        );
        // uppercase + illegal chars sanitize like the web app
        assert_eq!(
            pipeline_slug_from_key("pipelines/My_Pipe/raw/x.csv"),
            Some("my-pipe".into())
        );
        // not under pipelines/ or no second segment
        assert_eq!(pipeline_slug_from_key("other/demo/x.csv"), None);
        assert_eq!(pipeline_slug_from_key("pipelines/demo"), None);
        assert_eq!(pipeline_slug_from_key("pipelines//x.csv"), None);
    }

    #[test]
    fn urldecode_handles_percent_and_plus() {
        assert_eq!(urldecode("a%2Fb+c"), "a/b c");
        assert_eq!(urldecode("plain"), "plain");
        assert_eq!(urldecode("bad%zz"), "bad%zz");
    }
}
