# karet-worker

[![Publish Docker image](https://github.com/karet-org/karet-worker/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/karet-org/karet-worker/actions/workflows/docker-publish.yml)

Rust/Axum data pipeline worker for the Karet analytics platform. Ingests
CSVs from S3, evaluates AST-JSON mapping expressions (parse_date, cast,
upper/lower/trim, arithmetic, comparisons, `if`, `coalesce`, keyword
lookups, etc.), and writes partitioned Parquet output back to S3.

See the `compose.yml` in the [`karet`](https://github.com/karet-org/karet)
repo for the full stack (rustfs + worker + web).

## Environment variables

All required to start the worker; it fails fast if any is unset.

| Variable | Description |
|----------|-------------|
| `S3_BUCKET` | S3 bucket name |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | S3 credentials |
| `AWS_ENDPOINT_URL` | S3 endpoint URL (e.g. `http://rustfs:9000` for local dev, `https://s3.<region>.amazonaws.com` for real AWS). |
| `PORT` | Optional HTTP server port (default `8080`). |
| `POLARS_MAX_THREADS` | Optional cap on Polars thread pool size. |

## HTTP API

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Liveness check |
| `POST` | `/config/validate` | Validate a candidate `Pipeline_Config` body |
| `POST` | `/jobs/run` | Execute a pipeline run for the given `pipeline_prefix` |

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
