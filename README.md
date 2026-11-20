# karet-worker

[![CI](https://github.com/karet-org/karet-worker/actions/workflows/ci.yml/badge.svg)](https://github.com/karet-org/karet-worker/actions/workflows/ci.yml)
[![Publish Docker image](https://github.com/karet-org/karet-worker/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/karet-org/karet-worker/actions/workflows/docker-publish.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-2b2c33)](./LICENSE)

Pipeline runner for Karet, a self-hosted analytics stack. Reads CSV and NDJSON
from the lake bucket, evaluates the pipeline's mapping expressions with Polars,
and writes the analytic tables to [DuckLake](https://ducklake.select): the
catalog in Postgres, the Parquet files in the warehouse bucket under `lake/`.
Each run commits one snapshot per table it changes. Rust and Axum.

The web app and the compose file for the whole stack are in
[karet](https://github.com/karet-org/karet).
Docs: [karet-docs.pages.dev](https://karet-docs.pages.dev)

## Environment variables

The first group is required: the worker fails fast listing whatever is unset.
See `REQUIRED_ENV_VARS` in `src/lib.rs`.

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | Postgres connection string. Pipeline configs, the version each run is pinned to, and job rows. The DuckLake catalog is the `LAKE_DATABASE` database on the same server, opened with the same credentials. |
| `REDIS_URL` | Valkey connection string, e.g. `redis://valkey:6379`. Jobs arrive on the `karet:jobs:stream` consumer group. |
| `S3_BUCKET_LAKE` | Bucket holding the source files a run reads. |
| `S3_BUCKET_WAREHOUSE` | Bucket the DuckLake data files are written to, under `lake/`. |
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
| `LAKE_DATABASE` | `karet_lake` | Postgres database holding the DuckLake catalog. The web app's setup creates it. |
| `PORT` | `8080` | Port to serve on. |
| `HOSTNAME` | `local` | Consumer name in the stream group; Docker sets it per container. |
| `LAKE_POLL_INTERVAL_SECS` | `0` | When above 0, list every source prefix in the lake bucket this often and start a run for new or replaced objects. For S3 stores that cannot send webhooks. `0` turns it off. |

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
- For stores without webhooks, set `LAKE_POLL_INTERVAL_SECS`. The worker
  lists each source prefix on that interval, compares the keys and ETags
  with the previous listing (saved in Redis under `karet:lake:seen:<prefix>`),
  and debounces a run for new or replaced objects. The first scan of a
  prefix only saves the listing.
- Shuts down gracefully on SIGTERM: stops claiming, finishes in-flight
  jobs, then exits.

## Tables

Each pipeline is a DuckLake schema named after its slug, and each analytic
table a DuckLake table. The `_karet_mapping` column names the mapping that
wrote a row and is the first partition key, so a mapping's full run replaces
only its own files. The `_karet.table_sources` table holds each mapping's
incremental-run record, written in the same transaction as the rows.

After each commit the worker merges the table's files under 8 MB, expires
snapshots outside the last 10 that changed each table's rows, and deletes data
files no retained snapshot reads once they are an hour old.

## HTTP API

`POST /config/validate` and `POST /tables/restore` require an
`Authorization: Bearer $KARET_WORKER_TOKEN` header; `POST /events/s3`
enforces the webhook secret; `GET /health` is open for liveness probes.

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Liveness/readiness: Redis status, queue depth, in-flight count |
| `POST` | `/config/validate` | Validate a candidate `Pipeline_Config` body |
| `POST` | `/tables/restore` | `{pipeline, table, version}`: make a retained version current, as a new snapshot. 409 while the pipeline runs. |
| `POST` | `/events/s3` | RustFS object-created notifications (debounced into job runs) |

## Development

The `duckdb` crate links against the DuckDB release library. Download
`libduckdb-linux-amd64.zip` for the version in `Cargo.toml` (1.5.6) from the
[DuckDB releases](https://github.com/duckdb/duckdb/releases), unzip it, and
point the build at it:

```sh
export DUCKDB_LIB_DIR=$HOME/libduckdb DUCKDB_INCLUDE_DIR=$HOME/libduckdb LD_LIBRARY_PATH=$HOME/libduckdb
cargo test                        # unit, property and local-catalog tests
cargo run                         # start the worker locally
```

`tests/lake_local.rs` runs against a DuckDB catalog file in a temporary folder
and downloads the ducklake extension on first use. The end-to-end run in
`tests/integration_lake.rs` needs Docker and is marked `#[ignore]`:

```sh
cargo test --test integration_lake -- --ignored --nocapture
```
