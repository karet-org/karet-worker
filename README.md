# karet-worker

Rust/Axum data pipeline worker for the Karet analytics platform. Ingests
CSVs from S3, evaluates AST-JSON mapping expressions (including keyword
lookups), and writes partitioned Parquet output back to S3.

See the top-level `docker-compose.yaml` for the full stack (rustfs +
worker + web).

## Environment variables

All required to start the worker; it fails fast if any is unset.

| Variable | Description |
|----------|-------------|
| `S3_BUCKET` | S3 bucket name |
| `S3_ENDPOINT` | S3 endpoint URL (e.g. `http://rustfs:9000`) |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | S3 credentials |
| `PORT` | HTTP server port (default `8080`) |

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
