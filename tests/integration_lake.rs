//! A pipeline run end to end: source CSVs in RustFS, the config in Postgres,
//! the table written to DuckLake with its catalog in Postgres.
//!
//! Ignored by default because it needs Docker:
//! `cargo test --test integration_lake -- --ignored`

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use karet_worker::job::{execute_job, JobContext, NoopProgress};
use karet_worker::lake::{self, Lake, LakeCatalog, LakeSettings};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const SCHEMA: &str = "
CREATE TABLE config_versions (id bigserial PRIMARY KEY, pipeline text NOT NULL, config jsonb NOT NULL);
CREATE TABLE pipelines_current (pipeline text PRIMARY KEY, config_version_id bigint NOT NULL);
";

const CONFIG: &str = r#"{
    "version": 1,
    "source_containers": [{
        "id": "visa", "name": "Visa", "path_prefix": "visa/",
        "schema": [
            {"name": "date", "type": "string"},
            {"name": "description", "type": "string"},
            {"name": "amount", "type": "number"}
        ]
    }],
    "dimensions": [],
    "mappings": [{
        "id": "visa_to_tx", "source_container_id": "visa", "analytic_table_id": "transactions",
        "columns": [
            {"name": "date", "expr": {"kind": "parse_date", "input": {"kind": "col", "name": "date"}, "format": "%Y-%m-%d"}},
            {"name": "description", "expr": {"kind": "upper", "input": {"kind": "col", "name": "description"}}},
            {"name": "amount", "expr": {"kind": "col", "name": "amount"}}
        ]
    }],
    "analytic_tables": [{
        "id": "transactions", "name": "Transactions",
        "schema": [
            {"name": "date", "type": "date"},
            {"name": "description", "type": "string"},
            {"name": "amount", "type": "number"}
        ],
        "dedup_keys": ["date", "description"]
    }],
    "layout": {}
}"#;

struct Stack {
    _pg: ContainerAsync<GenericImage>,
    _s3: ContainerAsync<GenericImage>,
    ctx: JobContext,
    s3: aws_sdk_s3::Client,
}

async fn start() -> Stack {
    start_with(CONFIG).await
}

async fn start_with(config: &str) -> Stack {
    let pg = GenericImage::new("postgres", "17-alpine")
        .with_exposed_port(5432.tcp())
        .with_env_var("POSTGRES_PASSWORD", "test")
        .start()
        .await
        .expect("start postgres");
    let pg_port = pg.get_host_port_ipv4(5432).await.unwrap();
    let s3c = GenericImage::new("rustfs/rustfs", "latest")
        .with_exposed_port(9000.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Starting:"))
        .with_env_var("RUSTFS_VOLUMES", "/data")
        .with_env_var("RUSTFS_ADDRESS", "0.0.0.0:9000")
        .with_env_var("RUSTFS_ACCESS_KEY", "rustfsadmin")
        .with_env_var("RUSTFS_SECRET_KEY", "rustfsadmin")
        .start()
        .await
        .expect("start rustfs");
    let s3_port = s3c.get_host_port_ipv4(9000).await.unwrap();
    let endpoint = format!("http://127.0.0.1:{s3_port}");

    let url = format!("postgres://postgres:test@127.0.0.1:{pg_port}/postgres");
    let mut pool = None;
    for _ in 0..60 {
        if let Ok(p) = karet_worker::db::connect(&url).await {
            if sqlx::query("SELECT 1").execute(&p).await.is_ok() {
                pool = Some(p);
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let pool = pool.expect("postgres accepts connections");
    sqlx::raw_sql(SCHEMA).execute(&pool).await.unwrap();
    sqlx::raw_sql("CREATE DATABASE karet_lake").execute(&pool).await.unwrap();
    let (id,): (i64,) = sqlx::query_as("INSERT INTO config_versions (pipeline, config) VALUES ('p-1', $1::jsonb) RETURNING id")
        .bind(config)
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO pipelines_current VALUES ('p-1', $1)").bind(id).execute(&pool).await.unwrap();

    let creds = Credentials::new("rustfsadmin", "rustfsadmin", None, None, "test");
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(creds)
        .endpoint_url(&endpoint)
        .load()
        .await;
    let s3 = aws_sdk_s3::Client::from_conf(aws_sdk_s3::config::Builder::from(&config).force_path_style(true).build());
    // RustFS logs nothing once it listens, so retry until it answers.
    for bucket in ["karet-lake", "karet-warehouse"] {
        let mut created = false;
        for _ in 0..60 {
            if s3.create_bucket().bucket(bucket).send().await.is_ok() {
                created = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        assert!(created, "create bucket {bucket}");
    }

    let settings = LakeSettings::from_urls(&url, "karet_lake", &endpoint, "karet-warehouse", "rustfsadmin", "rustfsadmin", "us-east-1")
        .unwrap();
    let ctx = JobContext {
        s3_client: s3.clone(),
        lake_bucket: "karet-lake".into(),
        db: pool,
        lake: Arc::new(Lake::new(LakeCatalog::Postgres(settings))),
        now: karet_worker::job::system_now,
    };
    Stack { _pg: pg, _s3: s3c, ctx, s3 }
}

impl Stack {
    async fn upload(&self, key: &str, body: &str) {
        self.s3
            .put_object()
            .bucket("karet-lake")
            .key(key)
            .body(ByteStream::from(body.as_bytes().to_vec()))
            .send()
            .await
            .expect("put csv");
    }

    async fn run(&self, clean: bool) -> karet_worker::job::JobOutcome {
        execute_job(&self.ctx, Some("job"), "p-1", None, clean, &NoopProgress, &AtomicBool::new(false))
            .await
            .expect("run")
    }

    fn query(&self, sql: &str) -> Vec<String> {
        let conn = self.ctx.lake.connection().unwrap();
        let mut stmt = conn.prepare(sql).unwrap();
        let mut rows: Vec<String> = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap().map(Result::unwrap).collect();
        rows.sort();
        rows
    }

    fn rows(&self) -> Vec<String> {
        self.query("SELECT concat_ws('|', date, description, amount) FROM lake.\"p-1\".transactions")
    }

    fn versions(&self) -> Vec<i64> {
        lake::table_versions(&self.ctx.lake.connection().unwrap(), "p-1", "transactions").unwrap()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with `cargo test --test integration_lake -- --ignored`"]
async fn runs_write_ducklake_and_append_only_new_files() {
    let stack = start().await;
    stack.upload("visa/1-january.csv", "date,description,amount\n2024-01-05,starbucks,7.25\n2024-01-18,uber,12.8\n").await;
    stack.upload("visa/2-february.csv", "date,description,amount\n2024-02-02,whole foods,45.1\n").await;

    let first = stack.run(false).await;
    assert!(first.errors.is_empty(), "{:?}", first.errors);
    assert_eq!(first.files_processed, 2);
    assert_eq!(
        stack.rows(),
        vec!["2024-01-05|STARBUCKS|7.25", "2024-01-18|UBER|12.8", "2024-02-02|WHOLE FOODS|45.1"]
    );
    assert_eq!(stack.versions().len(), 1);

    // Nothing new: no new version.
    let idle = stack.run(false).await;
    assert_eq!(idle.files_processed, 0);
    assert_eq!(stack.versions().len(), 1);

    // One new file, with one row that is already stored.
    stack.upload("visa/3-march.csv", "date,description,amount\n2024-01-05,starbucks,7.25\n2024-03-01,netflix,15.99\n").await;
    let append = stack.run(false).await;
    assert!(append.errors.is_empty(), "{:?}", append.errors);
    assert_eq!(append.files_processed, 1, "only the new file is read");
    assert_eq!(append.rows_deduped, 1);
    assert_eq!(stack.rows().len(), 4);
    let versions = stack.versions();
    assert_eq!(versions.len(), 2);

    // A clean run rebuilds the same rows from every file.
    let clean = stack.run(true).await;
    assert_eq!(clean.files_processed, 3);
    assert_eq!(stack.rows().len(), 4);

    let restored = lake::restore(&stack.ctx.lake.connection().unwrap(), "p-1", "transactions", versions[0]).unwrap();
    assert!(restored > versions[1]);
    assert_eq!(stack.rows().len(), 3);
    // Restore clears the records, so the next run is a full run.
    let after = stack.run(false).await;
    assert_eq!(after.files_processed, 3);
    assert_eq!(stack.rows().len(), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with `cargo test --test integration_lake -- --ignored`"]
async fn pipelines_commit_at_the_same_time_with_a_postgres_catalog() {
    let stack = start().await;
    let lake = stack.ctx.lake.clone();
    let dir = tempfile::tempdir().unwrap();
    let table = karet_worker::config::AnalyticTable {
        retention: None,
        id: "t".into(),
        name: "t".into(),
        schema: vec![],
        partition_keys: vec![],
        dedup_keys: vec!["k".into()],
    };
    let setup = lake.connection().unwrap();
    let mut files = Vec::new();
    for i in 0..4 {
        let path = dir.path().join(format!("{i}.parquet"));
        setup
            .execute_batch(&format!("COPY (SELECT 'a' AS k, {i} AS v) TO '{}' (FORMAT parquet)", path.display()))
            .unwrap();
        files.push(path);
    }
    for round in 0..5 {
        let barrier = Arc::new(std::sync::Barrier::new(files.len()));
        let handles: Vec<_> = files
            .iter()
            .enumerate()
            .map(|(i, file)| {
                let (lake, barrier, file, table) = (lake.clone(), barrier.clone(), file.clone(), table.clone());
                std::thread::spawn(move || {
                    let conn = lake.connection().unwrap();
                    let pipeline = format!("p-{i}");
                    let records = std::collections::HashMap::from([(
                        "m".to_string(),
                        karet_worker::incremental::MappingSources {
                            mapping_id: "m".into(),
                            fingerprint: "fp".into(),
                            files: vec![],
                            expired: vec![],
                        },
                    )]);
                    barrier.wait();
                    lake::commit_table(
                        &conn,
                        &lake::Commit {
                            pipeline: &pipeline,
                            table: &table,
                            writes: &[lake::Write { mapping_id: "m".into(), file: file.clone(), replace: true }],
                            records: &records,
                            clean: false,
                            job_id: None,
                        },
                    )
                })
            })
            .collect();
        for h in handles {
            if let Err(e) = h.join().unwrap() {
                panic!("round {round}: {e}");
            }
        }
    }
}

fn in_ten_days() -> i64 {
    karet_worker::job::system_now() + 10 * 86_400
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with `cargo test --test integration_lake -- --ignored`"]
async fn raw_files_past_retention_are_deleted_and_their_rows_kept() {
    let config = CONFIG.replace(r#""path_prefix": "visa/","#, r#""path_prefix": "visa/", "retention_days": 7,"#);
    assert_ne!(config, CONFIG);
    let mut stack = start_with(&config).await;
    stack.upload("visa/1-january.csv", "date,description,amount\n2024-01-05,starbucks,7.25\n2024-01-18,uber,12.8\n").await;
    stack.upload("visa/2-february.csv", "date,description,amount\n2024-02-02,whole foods,45.1\n").await;
    assert_eq!(stack.run(false).await.files_processed, 2);

    // Ten days on, both files are past the 7-day retention.
    stack.ctx.now = in_ten_days;
    let expire = stack.run(false).await;
    assert!(expire.errors.is_empty(), "{:?}", expire.errors);
    assert_eq!(expire.files_processed, 0);
    let left = stack.s3.list_objects_v2().bucket("karet-lake").prefix("visa/").send().await.unwrap();
    assert_eq!(left.contents().len(), 0, "the raw files are deleted");
    assert_eq!(stack.rows().len(), 3, "their rows stay");

    // The same file uploaded again is not read twice, and is deleted again.
    stack.upload("visa/2-february.csv", "date,description,amount\n2024-02-02,whole foods,45.1\n").await;
    let again = stack.run(false).await;
    assert_eq!(again.files_processed, 0);
    let left = stack.s3.list_objects_v2().bucket("karet-lake").prefix("visa/").send().await.unwrap();
    assert_eq!(left.contents().len(), 0);

    // A new file appends; the deleted files do not make it a full run.
    stack.upload("visa/3-march.csv", "date,description,amount\n2024-03-01,netflix,15.99\n").await;
    let append = stack.run(false).await;
    assert_eq!(append.files_processed, 1);
    assert_eq!(stack.rows().len(), 4);
}
