# karet-worker

[![CI](https://github.com/karet-org/karet-worker/actions/workflows/ci.yml/badge.svg)](https://github.com/karet-org/karet-worker/actions/workflows/ci.yml)
[![Publish Docker image](https://github.com/karet-org/karet-worker/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/karet-org/karet-worker/actions/workflows/docker-publish.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-2b2c33)](./LICENSE)

Rust/Axum data pipeline worker for the Karet analytics platform. Ingests
source CSVs from S3 (karet-lake bucket), evaluates AST-JSON mapping
expressions (parse_date, cast, upper/lower/trim, arithmetic, comparisons,
`if`, `coalesce`, keyword lookups, etc.) with Polars, and writes
partitioned Parquet output to S3 (karet-warehouse bucket).

See the `compose.yml` in the [`karet`](https://github.com/karet-org/karet)
repo for the full stack (rustfs + worker + web).

## Environment variables

All required to start the worker; it fails fast if any is unset.

| Variable | Description |
|----------|-------------|
| `S3_BUCKET_PIPELINES` | Bucket for pipeline configs (default `karet-pipelines`). |
| `S3_BUCKET_LAKE` | Bucket for raw CSV data (default `karet-lake`). |
| `S3_BUCKET_WAREHOUSE` | Bucket for partitioned Parquet output (default `karet-warehouse`). |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | S3 credentials |
| `AWS_ENDPOINT_URL` | S3 endpoint URL (e.g. `http://rustfs:9000` for local dev, `https://s3.<region>.amazonaws.com` for real AWS). |
| `KARET_WORKER_TOKEN` | Shared bearer token required on `POST /config/validate`. Generate with `openssl rand -hex 32`; the web service must send the same value. |
| `REDIS_URL` | Valkey/Redis connection string (e.g. `redis://valkey:6379`). Jobs arrive via the `karet:jobs:stream` consumer group. |
| `KARET_WEBHOOK_SECRET` | Shared secret for `POST /events/s3` (send as `X-Karet-Webhook-Secret`, or via `RUSTFS_NOTIFY_WEBHOOK_AUTH_TOKEN_PRIMARY`). |
| `WORKER_CONCURRENCY` | Optional. Jobs processed concurrently per worker (default `1`). |
| `MAX_ATTEMPTS` | Optional. Delivery attempts before a job is terminally failed (default `3`). |
| `JOB_LOCK_TTL_MS` / `HEARTBEAT_MS` | Optional. Per-pipeline lock TTL and heartbeat interval (defaults `90000` / `30000`). |
| `PORT` | Optional HTTP server port (default `8080`). |

## Job queue

The Redis stream is the only job transport (design:
`karet-jobs-redis-design.html` in the workspace):

- Claims jobs from the `karet:jobs:stream` consumer group; a per-pipeline
  lock serializes runs; busy jobs defer to a delayed ZSET.
- Publishes live status + progress to `karet:jobs:live:<id>` hashes.
- Retries transient failures with exponential backoff, up to
  `MAX_ATTEMPTS`; crashed workers' entries are reclaimed automatically.
- Writes the terminal job record to S3
  (`pipelines/<slug>/jobs/<id>.json`), then acks.
- Debounces RustFS upload events (5s quiet / 30s max) and enqueues
  webhook-triggered jobs itself; point
  `RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_PRIMARY` at `http://worker:8080/events/s3`.
- Shuts down gracefully on SIGTERM: stops claiming, finishes in-flight
  jobs, then exits.

## HTTP API

`POST /config/validate` requires an `Authorization: Bearer
$KARET_WORKER_TOKEN` header; `POST /events/s3` enforces the webhook
secret; `GET /health` is open for liveness probes.

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Liveness/readiness: Redis status, queue depth, in-flight count |
| `POST` | `/config/validate` | Validate a candidate `Pipeline_Config` body |
| `POST` | `/events/s3` | RustFS object-created notifications (debounced into job runs) |

## Development

```sh
cargo test                        # unit + property tests
cargo run                         # start the worker locally
```

The integration test at `tests/integration_rustfs.rs` requires Docker
and is marked `#[ignore]` by default:

```sh
cargo test --test integration_rustfs -- --ignored --nocapture
```
