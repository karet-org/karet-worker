# karet-worker

[![CI](https://github.com/karet-org/karet-worker/actions/workflows/ci.yml/badge.svg)](https://github.com/karet-org/karet-worker/actions/workflows/ci.yml)
[![Publish Docker image](https://github.com/karet-org/karet-worker/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/karet-org/karet-worker/actions/workflows/docker-publish.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-2b2c33)](./LICENSE)

Pipeline runner for Karet, a self-hosted analytics stack. Reads CSV and NDJSON
from the lake bucket, evaluates the pipeline's mapping expressions with Polars,
and writes partitioned Parquet to the warehouse bucket, one numbered table
version per run. Rust and Axum.

The web app and the compose file for the whole stack are in
[karet](https://github.com/karet-org/karet).
Docs: [karet-docs.pages.dev](https://karet-docs.pages.dev)

## Environment variables

The first group is required: the worker fails fast listing whatever is unset.
See `REQUIRED_ENV_VARS` in `src/lib.rs`.

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | Postgres connection string. Pipeline configs, the version each run is pinned to, and job rows. |
| `REDIS_URL` | Valkey connection string, e.g. `redis://valkey:6379`. Jobs arrive on the `karet:jobs:stream` consumer group. |
| `S3_BUCKET_PIPELINES` | Bucket for dashboards and saved queries. |
| `S3_BUCKET_LAKE` | Bucket holding the source files a run reads. |
| `S3_BUCKET_WAREHOUSE` | Bucket the Parquet output is written to. |
| `AWS_ENDPOINT_URL` | S3 endpoint, `http://rustfs:9000` locally or `https://s3.<region>.amazonaws.com` for AWS. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | S3 credentials. |
| `KARET_WORKER_TOKEN` | Bearer token required on `POST /config/validate`; the web app must send the same value. `openssl rand -hex 32`. |
| `KARET_WEBHOOK_SECRET` | Shared secret for `POST /events/s3`, sent as `X-Karet-Webhook-Secret` or by RustFS as `RUSTFS_NOTIFY_WEBHOOK_AUTH_TOKEN_PRIMARY`. |

Optional:

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_CONCURRENCY` | `1` | Jobs this worker runs at once. |
| `MAX_ATTEMPTS` | `3` | Attempts before a job fails terminally. |
| `JOB_LOCK_TTL_MS` | `90000` | Per-pipeline lock TTL. |
| `HEARTBEAT_MS` | `30000` | How often a running job renews its lock. |
| `DATABASE_POOL_MAX` | `4` | Postgres connections this worker opens. |
| `PORT` | `8080` | Port to serve on. |
| `HOSTNAME` | `local` | Consumer name in the stream group; Docker sets it per container. |

## Job queue

The Redis stream is the only job transport:

- Claims jobs from the `karet:jobs:stream` consumer group; a per-pipeline
  lock serializes runs; busy jobs defer to a delayed ZSET.
- Publishes live status + progress to `karet:jobs:live:<id>` hashes.
- Retries transient failures with exponential backoff, up to
  `MAX_ATTEMPTS`; crashed workers' entries are reclaimed automatically.
- Writes the terminal job row to Postgres, then acks.
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
