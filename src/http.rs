//! HTTP API (Axum).
//!
//! Routes: `GET /health`, `POST /config/validate`, `POST /events/s3`.
//! Jobs arrive via the Redis stream (see `queue.rs`), not HTTP.

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
    /// Upload-routing table: `(source path_prefix, pipeline slug)` pairs
    /// from every pipeline config, cached briefly (see [`ROUTING_TTL`]).
    pub routing: Arc<tokio::sync::RwLock<RoutingCache>>,
}

/// Cached `(prefix, slug)` pairs for webhook routing.
#[derive(Default)]
pub struct RoutingCache {
    built_at: Option<std::time::Instant>,
    entries: Vec<(String, String)>,
}

/// How long the routing table is trusted before a rebuild. A short TTL
/// keeps a just-published source from missing uploads for long, without
/// hitting S3 on every event.
const ROUTING_TTL: std::time::Duration = std::time::Duration::from_secs(30);

/// The subset of `pipeline.json` the router needs. Parsed leniently so a
/// config from any schema era still routes.
#[derive(serde::Deserialize)]
struct RoutingConfig {
    #[serde(default)]
    source_containers: Vec<RoutingSource>,
}

#[derive(serde::Deserialize)]
struct RoutingSource {
    #[serde(default)]
    path_prefix: String,
}

/// Return the slugs of every pipeline with a source prefix matching `key`,
/// rebuilding the routing table if stale. One upload may route to several
/// pipelines: shared lake folders fan out by design.
async fn route_key(state: &AppState, key: &str) -> Vec<String> {
    {
        let cache = state.routing.read().await;
        if cache.built_at.is_some_and(|t| t.elapsed() < ROUTING_TTL) {
            return match_key(&cache.entries, key);
        }
    }
    // Build outside the lock so a slow store doesn't serialize every
    // webhook event behind the rebuild. Concurrent rebuilds are
    // harmless: last swap wins.
    let entries = build_routing(state).await;
    let mut cache = state.routing.write().await;
    cache.entries = entries;
    cache.built_at = Some(std::time::Instant::now());
    match_key(&cache.entries, key)
}

fn match_key(entries: &[(String, String)], key: &str) -> Vec<String> {
    let mut slugs: Vec<String> = entries
        .iter()
        .filter(|(prefix, _)| !prefix.is_empty() && key.starts_with(prefix.as_str()))
        .map(|(_, slug)| slug.clone())
        .collect();
    slugs.sort();
    slugs.dedup();
    slugs
}

/// List `pipelines/<slug>/pipeline.json` objects and collect every source
/// prefix. Errors degrade to an empty table (logged); the next event or
/// TTL expiry retries.
async fn build_routing(state: &AppState) -> Vec<(String, String)> {
    let Some(client) = &state.s3_client else {
        return Vec::new();
    };
    let keys = match crate::s3::list_keys(client, &state.pipelines_bucket, "pipelines/").await {
        Ok(k) => k,
        Err(e) => {
            tracing::error!("routing: list pipelines failed: {e}");
            return Vec::new();
        }
    };
    let mut entries = Vec::new();
    for key in keys {
        let Some(slug) = key
            .strip_prefix("pipelines/")
            .and_then(|r| r.strip_suffix("/pipeline.json"))
        else {
            continue;
        };
        if slug.is_empty() || slug.contains('/') {
            continue;
        }
        match crate::s3::get_bytes(client, &state.pipelines_bucket, &key).await {
            Ok(bytes) => match serde_json::from_slice::<RoutingConfig>(&bytes) {
                Ok(cfg) => {
                    for sc in cfg.source_containers {
                        entries.push((sc.path_prefix, slug.to_string()));
                    }
                }
                Err(e) => tracing::warn!("routing: parse {key} failed: {e}"),
            },
            Err(e) => tracing::warn!("routing: read {key} failed: {e}"),
        }
    }
    entries
}

pub fn router(state: AppState) -> Router {
    let state = Arc::new(state);
    // The one remaining mutating route sits behind the bearer-token
    // check; `/health` stays open so liveness probes need no credentials,
    // and `/events/s3` enforces its own webhook secret. `route_layer`
    // (not `layer`) so the middleware wraps only matched routes — with
    // plain `layer` the router's 404 fallback answers 401 for every
    // unknown path, which broke RustFS's HEAD health probe of `/`.
    let protected = Router::new()
        .route("/config/validate", post(post_config_validate))
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
    let consumer_ok = queue.consumer_ok.load(std::sync::atomic::Ordering::SeqCst);
    match crate::queue::connect(&queue.client).await {
        Ok(mut conn) => {
            let depth: i64 = redis::cmd("XLEN")
                .arg(crate::queue::STREAM_KEY)
                .query_async(&mut conn)
                .await
                .unwrap_or(-1);
            let status = if consumer_ok && depth >= 0 {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            (
                status,
                Json(serde_json::json!({
                    "redis": if depth >= 0 { "ok" } else { "error" },
                    "consumer": if consumer_ok { "ok" } else { "reconnecting" },
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
    // Three accepted channels: our own header, `Authorization: Bearer x`,
    // and a raw `Authorization: x` — RustFS's `WEBHOOK_AUTH_TOKEN` sends
    // the configured value verbatim, whose exact shape is undocumented.
    let provided = headers
        .get("x-karet-webhook-secret")
        .and_then(|v| v.to_str().ok())
        .or_else(|| {
            headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.strip_prefix("Bearer ").unwrap_or(v))
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
        for slug in route_key(&state, &urldecode(key)).await {
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
async fn post_config_validate(
    State(state): State<Arc<AppState>>,
    body: String,
) -> impl IntoResponse {
    // A validate call precedes every config publish; drop the routing
    // cache so a source pointed at a new folder routes its first upload
    // instead of waiting out the TTL.
    state.routing.write().await.built_at = None;
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
        "dimensions": [],
        "mappings": [],
        "analytic_tables": [],
        "layout": {}
    }"#;

    const TEST_TOKEN: &str = "test-token";

    fn test_state() -> AppState {
        AppState {
            routing: Arc::new(tokio::sync::RwLock::new(RoutingCache::default())),
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
            "dimensions": [],
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
    async fn config_validate_rejects_missing_token() {
        let response = post_with_auth("/config/validate", None, "{}").await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let v = read_json(response).await;
        assert_eq!(v["error"]["kind"], "unauthorized");
    }

    #[tokio::test]
    async fn config_validate_rejects_wrong_token() {
        let response = post_with_auth("/config/validate", Some("wrong-token"), "{}").await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
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
    // (enforced at claim time in queue.rs; the shape rules live here)

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
    fn match_key_routes_by_prefix() {
        let entries = vec![
            ("banks/rbc/".to_string(), "spending".to_string()),
            ("shared/exports/".to_string(), "spending".to_string()),
            ("shared/exports/".to_string(), "audit".to_string()),
        ];
        // One upload fans out to every pipeline reading the folder.
        assert_eq!(
            match_key(&entries, "shared/exports/jan.csv"),
            vec!["audit".to_string(), "spending".to_string()]
        );
        assert_eq!(
            match_key(&entries, "banks/rbc/chequing/feb.csv"),
            vec!["spending".to_string()]
        );
        // No matching source: dropped.
        assert!(match_key(&entries, "unrelated/x.csv").is_empty());
        // Empty prefixes never match everything.
        let empty = vec![("".to_string(), "oops".to_string())];
        assert!(match_key(&empty, "anything.csv").is_empty());
    }

    #[test]
    fn urldecode_handles_percent_and_plus() {
        assert_eq!(urldecode("a%2Fb+c"), "a/b c");
        assert_eq!(urldecode("plain"), "plain");
        assert_eq!(urldecode("bad%zz"), "bad%zz");
    }
}
