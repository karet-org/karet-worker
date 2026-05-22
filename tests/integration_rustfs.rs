//! End-to-end integration test: run the worker's ingestion pipeline against
//! a real S3-compatible store (RustFS) booted via `testcontainers`.
//!
//! # What this exercises
//!
//! - The worker reads CSVs from S3 under `raw/<container_name>/`.
//! - The worker writes Parquet output under
//!   `clean/<analytic_table_id>/year=YYYY/month=MM/data.parquet`.
//!
//! # Why it's `#[ignore]` by default
//!
//! The test spins up a Docker container, so it only runs when Docker is
//! available on the host. CI and Docker-less environments would otherwise
//! fail the standard `cargo test` invocation. Run it locally with:
//!
//! ```bash
//! cd src/karet-worker
//! cargo test --test integration_rustfs -- --ignored --nocapture
//! ```
//!
//! # Flow
//!
//! 1. Boot a `rustfs/rustfs:latest` container on an ephemeral host port.
//! 2. Build an `aws-sdk-s3` client pointing at the container's endpoint in
//!    path-style mode.
//! 3. Create the `karet-data` bucket.
//! 4. Seed the bucket with a `Pipeline_Config` JSON at `config/pipeline.json`
//!    and a few CSVs under `raw/visa/`.
//! 5. Fetch the CSVs back from S3, pipe them through `ingest_many` +
//!    `produce_partitions`, then upload the resulting Parquet partitions
//!    back to S3 via a thin `PartitionUploader` implementation that wraps
//!    the async SDK.
//! 6. List the `clean/transactions/` prefix on S3 and assert the expected
//!    `year=YYYY/month=MM/*.parquet` partition layout is present.

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use karet_worker::config::PipelineConfig;
use karet_worker::lookup;
use karet_worker::pipeline::{
    ingest_many, produce_partitions, upload_partitions, PartitionUploader,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

/// Bucket used by every fixture in this module. Matches the default in
/// `docker-compose.yaml` so mental models carry across environments.
const BUCKET: &str = "karet-data";

/// S3 key where the seeded `Pipeline_Config` lives.
const CONFIG_KEY: &str = "config/pipeline.json";

/// Pipeline_Config JSON for the test. Wires one source container
/// (`visa`, with a simple 3-column CSV schema), one mapping that
/// uppercases the description and passes date/amount through, and one
/// analytic table (`transactions`) partitioned by month on `date`.
///
/// Deliberately narrow -- we want to prove the S3 <-> worker seam, not
/// re-test every AST node. The full AST/evaluator behaviour is covered by
/// unit and property tests elsewhere.
const PIPELINE_CONFIG: &str = r#"{
    "version": 1,
    "source_containers": [
        {
            "id": "visa",
            "name": "Visa Statements",
            "path_prefix": "raw/visa/",
            "schema": [
                {"name": "date",        "type": "string"},
                {"name": "description", "type": "string"},
                {"name": "amount",      "type": "number"}
            ]
        }
    ],
    "lookup_mappings": [],
    "mappings": [
        {
            "id": "visa_to_tx",
            "source_container_id": "visa",
            "analytic_table_id": "transactions",
            "partition_by": {"column": "date", "granularity": "month"},
            "columns": [
                {
                    "name": "date",
                    "expr": {
                        "kind": "parse_date",
                        "input": {"kind": "col", "name": "date"},
                        "format": "%Y-%m-%d"
                    }
                },
                {
                    "name": "description",
                    "expr": {
                        "kind": "upper",
                        "input": {"kind": "col", "name": "description"}
                    }
                },
                {
                    "name": "amount",
                    "expr": {"kind": "col", "name": "amount"}
                }
            ]
        }
    ],
    "analytic_tables": [
        {
            "id": "transactions",
            "name": "Transactions",
            "output_prefix": "clean/transactions/",
            "schema": [
                {"name": "date",        "type": "date"},
                {"name": "description", "type": "string"},
                {"name": "amount",      "type": "number"}
            ]
        }
    ],
    "layout": {}
}"#;

/// Three tiny CSVs spanning January + February 2024. We deliberately
/// straddle a month boundary so the partition-coverage assertion has
/// something non-trivial to check.
///
/// Each tuple is `(s3_key, csv_body)`. Keys live under the container's
/// `path_prefix` (`raw/visa/`).
fn seed_csvs() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "raw/visa/january.csv",
            "date,description,amount\n\
             2024-01-05,starbucks,7.25\n\
             2024-01-18,uber,12.80\n",
        ),
        (
            "raw/visa/february.csv",
            "date,description,amount\n\
             2024-02-02,whole foods,45.10\n\
             2024-02-22,netflix,15.99\n",
        ),
        // A second January file: proves multi-file ingestion into the
        // same partition still works end-to-end (is
        // tangentially exercised too, even though this test is scoped
        // to 2.1 + 5.1).
        (
            "raw/visa/january_extra.csv",
            "date,description,amount\n\
             2024-01-29,blue bottle,5.00\n",
        ),
    ]
}

/// Thin [`PartitionUploader`] that wraps the async `aws-sdk-s3` client.
///
/// The `PartitionUploader` trait is synchronous (see `pipeline.rs`), so we
/// hold a reference to a Tokio runtime handle and `block_on` each `put_object`
/// call. For a test this is fine -- we're uploading a handful of tiny
/// Parquet files and already running inside a multi-threaded runtime, so
/// the `block_on` only blocks the caller's worker, not the whole runtime.
struct S3Uploader {
    client: Client,
    bucket: String,
    runtime: tokio::runtime::Handle,
}

impl PartitionUploader for S3Uploader {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), String> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = key.to_string();
        let body = bytes.to_vec();
        self.runtime
            .block_on(async move {
                client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from(body))
                    .send()
                    .await
                    .map(|_| ())
                    .map_err(|e| format!("{e:?}"))
            })
    }
}

/// Build an `aws-sdk-s3` client pointing at the given local endpoint with
/// path-style addressing and the RustFS default credentials.
///
/// Path-style is required because RustFS does not serve virtual-host-style
/// bucket URLs on localhost; every bucket lives as a sub-path of the root
/// endpoint. `force_path_style(true)` matches what the `docker-compose.yaml`
/// sets for the web service via `S3_FORCE_PATH_STYLE`.
async fn s3_client(endpoint: &str) -> Client {
    let creds = Credentials::new(
        "rustfsadmin", // default RustFS access key (matches docker-compose)
        "rustfsadmin", // default RustFS secret key
        None,
        None,
        "integration_test",
    );
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(creds)
        .endpoint_url(endpoint)
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();
    Client::from_conf(s3_config)
}

/// Read an object out of S3 as a `Vec<u8>`. Small helper so the test body
/// stays focused on the assertions rather than streaming plumbing.
async fn get_bytes(client: &Client, bucket: &str, key: &str) -> Vec<u8> {
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("get_object");
    resp.body
        .collect()
        .await
        .expect("collect body")
        .into_bytes()
        .to_vec()
}

/// List every object key under `prefix` in `bucket`. Handles pagination --
/// `list_objects_v2` returns up to 1000 keys per page, which is more than
/// enough for this test, but paginating explicitly keeps the helper
/// correct if someone later expands the seed set.
async fn list_keys(client: &Client, bucket: &str, prefix: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut continuation: Option<String> = None;
    loop {
        let mut req = client.list_objects_v2().bucket(bucket).prefix(prefix);
        if let Some(token) = &continuation {
            req = req.continuation_token(token);
        }
        let resp = req.send().await.expect("list_objects_v2");
        for obj in resp.contents() {
            if let Some(k) = obj.key() {
                out.push(k.to_string());
            }
        }
        if resp.is_truncated().unwrap_or(false) {
            continuation = resp.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }
    out
}

/// End-to-end pipeline run against a live RustFS container.
///
/// Marked `#[ignore]` so it only runs when an operator opts in with
/// `cargo test -- --ignored`. The body still type-checks on every build.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with `cargo test --test integration_rustfs -- --ignored`"]
async fn worker_reads_raw_csvs_and_writes_partitioned_parquet() {
    // ---- 1. Boot RustFS ---------------------------------------------------
    //
    // We expose port 9000 (the S3 API) and wait for RustFS's startup banner
    // before proceeding. The container is held for the duration of the test
    // via the `_container` binding; `Drop` tears it down automatically.
    //
    // Credentials are left at RustFS defaults to match the rest of the
    // project (docker-compose falls back to `rustfsadmin/rustfsadmin`).
    let _container = GenericImage::new("rustfs/rustfs", "latest")
        .with_exposed_port(9000.tcp())
        .with_wait_for(WaitFor::message_on_stdout("RustFS Object Storage Server"))
        .with_env_var("RUSTFS_VOLUMES", "/data")
        .with_env_var("RUSTFS_ADDRESS", "0.0.0.0:9000")
        .with_env_var("RUSTFS_ACCESS_KEY", "rustfsadmin")
        .with_env_var("RUSTFS_SECRET_KEY", "rustfsadmin")
        .start()
        .await
        .expect("start rustfs container");

    let host = _container.get_host().await.expect("container host");
    let port = _container
        .get_host_port_ipv4(9000)
        .await
        .expect("host port for 9000");
    let endpoint = format!("http://{host}:{port}");

    let client = s3_client(&endpoint).await;

    // ---- 2. Create the bucket --------------------------------------------
    //
    // RustFS starts empty; we need the bucket in place before we can seed
    // config + CSVs. `create_bucket` is idempotent in spirit here (we're
    // hitting a fresh container), so we don't bother swallowing
    // `BucketAlreadyExists`.
    client
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .expect("create bucket");

    // ---- 3. Seed Pipeline_Config + raw CSVs ------------------------------
    //
    // Both live in the same bucket under the prefixes the worker expects
    // (`config/pipeline.json` and `raw/visa/*.csv`). The config's
    // `path_prefix` must match `raw/visa/` for ingest_file's prefix-based
    // container resolution to work.
    client
        .put_object()
        .bucket(BUCKET)
        .key(CONFIG_KEY)
        .body(ByteStream::from(PIPELINE_CONFIG.as_bytes().to_vec()))
        .send()
        .await
        .expect("put pipeline config");

    for (key, body) in seed_csvs() {
        client
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from(body.as_bytes().to_vec()))
            .send()
            .await
            .expect("put raw csv");
    }

    // ---- 4. Load Pipeline_Config from S3 ---------------------------------
    //
    // Parse the bytes via serde so we're exercising the same path the
    // worker uses at startup. This is the read of
    // Pipeline_Config (technically 1.1 + 1.2, but both are prereqs for the
    // CSV ingestion that 2.1 scopes).
    let cfg_bytes = get_bytes(&client, BUCKET, CONFIG_KEY).await;
    let cfg: PipelineConfig =
        serde_json::from_slice(&cfg_bytes).expect("parse Pipeline_Config from S3");

    // ---- 5. Fetch raw CSVs from S3 ---------------------------------------
    //
    // : "THE Worker SHALL read Source_Files (CSV format)
    // from S3_Store under a path pattern defined in the Source_Container
    // configuration (e.g., `raw/<container_name>/`)."
    //
    // We list every key under `raw/visa/` (the container's `path_prefix`)
    // and pull each one into memory. In production the worker will stream
    // each CSV through Polars; for this integration harness the one-bucket
    // read-into-memory shape is enough to prove the seam.
    let container = &cfg.source_containers[0];
    assert_eq!(container.path_prefix, "raw/visa/", "sanity check");

    let raw_keys = list_keys(&client, BUCKET, &container.path_prefix).await;
    assert_eq!(
        raw_keys.len(),
        seed_csvs().len(),
        "expected one S3 object per seeded CSV; got {raw_keys:?}"
    );

    let mut files: Vec<(String, Vec<u8>)> = Vec::with_capacity(raw_keys.len());
    for key in raw_keys {
        let bytes = get_bytes(&client, BUCKET, &key).await;
        files.push((key, bytes));
    }

    // ---- 6. Run the worker's ingestion pipeline --------------------------
    //
    // Compile the lookup registry (empty for this config), ingest the
    // CSVs, partition the resulting frame, and write each partition back
    // to S3 via the uploader wrapper.
    let matchers = lookup::build_registry(&cfg.lookup_mappings);

    let lf = ingest_many(&files, &cfg, &matchers).expect("ingest_many succeeds on seeded CSVs");
    let df = lf.collect().expect("collect ingested frame");

    // Sanity check on row count -- 2 + 2 + 1 = 5 rows across the three CSVs.
    assert_eq!(
        df.height(),
        5,
        "ingested frame should contain every row from every seeded CSV",
    );

    // Produce per-partition Parquet bytes for the first (and only) mapping.
    let mapping = &cfg.mappings[0];
    let table = cfg
        .analytic_tables
        .iter()
        .find(|t| t.id == mapping.analytic_table_id)
        .expect("analytic table for mapping");

    let partitions = produce_partitions(&df, mapping, table).expect("produce partitions");

    // Month partitioning: 2024-01 and 2024-02 → exactly 2 partitions.
    assert_eq!(
        partitions.len(),
        2,
        "expected one partition per calendar month in the seed data; got {:?}",
        partitions.iter().map(|p| &p.key).collect::<Vec<_>>(),
    );

    // ---- 7. Upload partitions back to S3 ---------------------------------
    //
    // : "THE Worker SHALL write Analytic_Table output as
    // Parquet files to S3_Store under a configurable output prefix."
    //
    // We wrap the async S3 client in a sync-facing uploader so it
    // satisfies the existing `PartitionUploader` trait without requiring
    // changes to `pipeline.rs` purely for tests.
    let uploader = S3Uploader {
        client: client.clone(),
        bucket: BUCKET.to_string(),
        runtime: tokio::runtime::Handle::current(),
    };
    let _ = tokio::task::spawn_blocking({
        let partitions = partitions.clone();
        move || upload_partitions(&uploader, &partitions).expect("upload partitions")
    })
    .await
    .expect("spawn_blocking join");

    // ---- 8. Assert the output partition layout on S3 ---------------------
    //
    // Pattern: `clean/<analytic_table_id>/year=YYYY/month=MM/<uuid>.parquet`.
    // We assert:
    //   - Every uploaded key lives under `clean/transactions/`.
    //   - Exactly one key exists under `year=2024/month=01/`.
    //   - Exactly one key exists under `year=2024/month=02/`.
    //   - Each key ends with `.parquet` and the blob starts with `PAR1`.
    let clean_keys = list_keys(&client, BUCKET, "clean/transactions/").await;
    assert_eq!(
        clean_keys.len(),
        2,
        "expected exactly 2 Parquet objects under clean/transactions/ but found {clean_keys:?}",
    );

    let has_jan = clean_keys
        .iter()
        .any(|k| k.contains("year=2024/month=01/") && k.ends_with(".parquet"));
    let has_feb = clean_keys
        .iter()
        .any(|k| k.contains("year=2024/month=02/") && k.ends_with(".parquet"));
    assert!(
        has_jan,
        "missing January 2024 partition in uploaded keys: {clean_keys:?}",
    );
    assert!(
        has_feb,
        "missing February 2024 partition in uploaded keys: {clean_keys:?}",
    );

    // Spot-check the first returned object is really Parquet (PAR1 magic).
    let first_body = get_bytes(&client, BUCKET, &clean_keys[0]).await;
    assert!(
        first_body.len() > 4 && &first_body[..4] == b"PAR1",
        "uploaded object does not look like Parquet; first 8 bytes = {:02x?}",
        &first_body.iter().take(8).copied().collect::<Vec<_>>(),
    );

}
