//! Shared pipeline-job executor.
//!
//! Both entry points — the legacy `POST /jobs/run` HTTP handler and the
//! Redis stream consumer — run jobs through [`execute_job`], so behavior
//! (clean_run semantics, per-mapping error containment, partition upload)
//! is identical regardless of transport.

use crate::assertions::validate_assertions;
use crate::config::{self, PipelineConfig};
use crate::dimension;
use crate::manifest;
use crate::pipeline;
use crate::s3 as s3mod;

/// Where a job run currently is; surfaced as live progress.
#[derive(Debug, Clone)]
pub enum Progress {
    /// Listing + downloading lake CSVs. `done`/`total` count files
    /// (total is 0 until listing finishes).
    Downloading { done: usize, total: usize },
    /// Evaluating mappings. `partitions_written` is a running total.
    Ingesting {
        mappings_done: usize,
        mappings_total: usize,
        partitions_written: usize,
    },
}

/// Sink for progress updates. Implementations must be cheap and
/// non-blocking (the Redis sink forwards over an unbounded channel).
pub trait ProgressSink: Send + Sync {
    fn update(&self, progress: Progress);
}

/// No-op sink for the legacy HTTP path and tests.
pub struct NoopProgress;

impl ProgressSink for NoopProgress {
    fn update(&self, _progress: Progress) {}
}

/// Terminal result of a successful (possibly partially failed) run.
#[derive(Debug, Clone, serde::Serialize)]
pub struct JobOutcome {
    pub partitions_written: usize,
    pub files_processed: usize,
    /// Duplicate rows dropped by table dedup keys across all mappings.
    pub rows_deduped: usize,
    /// Per-mapping errors; non-empty means a partial failure.
    pub errors: Vec<String>,
}

/// Failures that prevent a run from starting at all.
#[derive(Debug, thiserror::Error)]
pub enum JobError {
    /// The pipeline config object could not be fetched. Possibly
    /// transient (network) — the queue path retries these.
    #[error("config read failed: {0}")]
    ConfigRead(String),
    /// The config fetched but does not parse. Permanent.
    #[error("config parse failed: {0}")]
    ConfigParse(String),
    /// The config parsed but fails cross-reference validation. Permanent.
    #[error("config invalid: {0}")]
    ConfigInvalid(String),
    /// No source files found under any source container prefix.
    #[error("no CSV files found to process")]
    NoFiles,
    /// A dimension could not be built (duplicate key, row cap, bad file).
    #[error("dimension error: {0}")]
    Dimension(String),
    /// The run lock was lost to a newer attempt; abort without writing.
    #[error("cancelled: lock lost to a newer attempt")]
    Cancelled,
}

/// Everything `execute_job` needs from the environment.
#[derive(Clone)]
pub struct JobContext {
    pub s3_client: aws_sdk_s3::Client,
    pub pipelines_bucket: String,
    pub lake_bucket: String,
    pub warehouse_bucket: String,
    /// The control-plane database: where configs and job rows live.
    pub db: sqlx::PgPool,
}

/// Execute one pipeline run for `prefix` (validated by the caller).
///
/// Error containment matches the original handler: a failure in one
/// mapping is recorded in `errors` and the remaining mappings still run.
/// Unlike the original handler, the config is validated before execution;
/// a config that fails [`config::validate`] is rejected up front.
pub async fn execute_job(
    ctx: &JobContext,
    prefix: &str,
    pipeline: &str,
    config_version_id: Option<i64>,
    clean_run: bool,
    progress: &dyn ProgressSink,
    cancel: &std::sync::atomic::AtomicBool,
) -> Result<JobOutcome, JobError> {
    use std::sync::atomic::Ordering;
    // The run is pinned to a config version, so a save partway through cannot
    // change what this run does, and the job row records which config it used.
    let version_id = match config_version_id {
        Some(id) => id,
        None => crate::db::current_version_for(&ctx.db, pipeline)
            .await
            .map_err(|e| JobError::ConfigRead(format!("resolving live config version: {e}")))?
            .ok_or_else(|| {
                JobError::ConfigRead(format!("pipeline {pipeline} has no published config"))
            })?,
    };
    let config_json = crate::db::config_for_version(&ctx.db, version_id)
        .await
        .map_err(|e| JobError::ConfigRead(format!("reading config version {version_id}: {e}")))?
        .ok_or_else(|| JobError::ConfigRead(format!("config version {version_id} not found")))?;
    let cfg: PipelineConfig =
        serde_json::from_value(config_json).map_err(|e| JobError::ConfigParse(e.to_string()))?;
    if let Err(errs) = config::validate(&cfg) {
        let joined = errs
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        return Err(JobError::ConfigInvalid(joined));
    }

    // Per-table publish state. Writes land under `v<next>/` and become visible
    // only when the manifest and pointer are written after every mapping has
    // run, so a reader never sees one mapping's half of a union table.
    //
    // clean_run starts from an empty file list instead of deleting anything:
    // the old objects stay readable under their own version until vacuum
    // retires them, which is what makes a bad run recoverable.
    struct TablePublish {
        prefix: String,
        version: u64,
        files: Vec<manifest::ManifestFile>,
        touched: bool,
    }
    let mut publishes: std::collections::HashMap<String, TablePublish> =
        std::collections::HashMap::new();
    for table in &cfg.analytic_tables {
        let table_prefix = format!("{prefix}{}/", table.id);
        let current = manifest::read_current(&ctx.s3_client, &ctx.warehouse_bucket, &table_prefix)
            .await
            .map_err(JobError::ConfigRead)?;
        publishes.insert(
            table.id.clone(),
            TablePublish {
                prefix: table_prefix,
                version: current.version + 1,
                files: if clean_run { Vec::new() } else { current.files },
                touched: false,
            },
        );
    }

    // Download every raw CSV file under each source container's
    // path_prefix from the lake bucket.
    progress.update(Progress::Downloading { done: 0, total: 0 });
    let mut candidate_keys: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for sc in &cfg.source_containers {
        // path_prefix is an absolute lake key prefix; sources may point at
        // any folder in the lake, not just this pipeline's.
        match s3mod::list_keys(&ctx.s3_client, &ctx.lake_bucket, &sc.path_prefix).await {
            Ok(keys) => {
                let exts = sc.format.extensions();
                candidate_keys.extend(keys.into_iter().filter(|k| {
                    exts.iter().any(|e| k.ends_with(e)) && seen.insert(k.clone())
                }))
            }
            Err(e) => tracing::warn!("failed to list keys for {}: {e}", sc.path_prefix),
        }
    }

    let mut all_files: Vec<(String, Vec<u8>)> = Vec::new();
    let total = candidate_keys.len();
    for (i, key) in candidate_keys.iter().enumerate() {
        if cancel.load(Ordering::SeqCst) {
            return Err(JobError::Cancelled);
        }
        match s3mod::get_bytes(&ctx.s3_client, &ctx.lake_bucket, key).await {
            Ok(bytes) => {
                all_files.push((key.clone(), bytes));
            }
            Err(e) => tracing::warn!("failed to download {key}: {e}"),
        }
        progress.update(Progress::Downloading { done: i + 1, total });
    }

    if all_files.is_empty() {
        return Err(JobError::NoFiles);
    }

    // Precompile the dimension registry once per job; shared by every mapping.
    // Inline rows compile from config; file-backed rows are fetched from the
    // lake here, where S3 access lives.
    let mut matchers = dimension::build_inline_registry(&cfg.dimensions)
        .map_err(|e| JobError::Dimension(e.to_string()))?;
    for dim in &cfg.dimensions {
        if let crate::config::DimensionRows::File {
            path_prefix,
            key,
            values,
            priority_column,
        } = &dim.rows
        {
            let keys = s3mod::list_keys(&ctx.s3_client, &ctx.lake_bucket, path_prefix)
                .await
                .map_err(|e| JobError::Dimension(format!("listing {path_prefix}: {e}")))?;
            let mut rows = Vec::new();
            for object_key in keys.iter().filter(|k| k.ends_with(".csv")) {
                let bytes = s3mod::get_bytes(&ctx.s3_client, &ctx.lake_bucket, object_key)
                    .await
                    .map_err(|e| JobError::Dimension(format!("reading {object_key}: {e}")))?;
                rows.extend(
                    dimension::rows_from_csv(
                        dim,
                        key,
                        values,
                        priority_column.as_deref(),
                        &bytes,
                    )
                    .map_err(|e| JobError::Dimension(e.to_string()))?,
                );
            }
            let matcher = dimension::DimensionMatcher::new(dim, rows)
                .map_err(|e| JobError::Dimension(e.to_string()))?;
            matchers.insert(dim.id.clone(), std::sync::Arc::new(matcher));
        }
    }
    let matchers = matchers;

    let mut total_partitions = 0usize;
    let mut total_deduped: usize = 0;
    let mut errors: Vec<String> = Vec::new();
    let files_processed = all_files.len();
    let mappings_total = cfg.mappings.len();

    for (mapping_idx, mapping) in cfg.mappings.iter().enumerate() {
        if cancel.load(Ordering::SeqCst) {
            return Err(JobError::Cancelled);
        }
        progress.update(Progress::Ingesting {
            mappings_done: mapping_idx,
            mappings_total,
            partitions_written: total_partitions,
        });

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

        // Polars work is CPU-bound: run it on the blocking pool so
        // heartbeats and health checks stay responsive. Uploads happen
        // back on the async runtime.
        let cfg_cloned = cfg.clone();
        let mapping_id = mapping.id.clone();
        let table_id = mapping.analytic_table_id.clone();
        let matchers_cloned = matchers.clone();
        let partitions_result: Result<Result<(Vec<pipeline::PartitionOutput>, usize), String>, _> =
            tokio::task::spawn_blocking(move || {
                let mapping = cfg_cloned
                    .mappings
                    .iter()
                    .find(|m| m.id == mapping_id)
                    .expect("mapping exists; cloned from same config");
                let lf = pipeline::ingest_many(&mapping_files, &cfg_cloned, mapping, &matchers_cloned)
                    .map_err(|e| format!("ingest {}: {e}", mapping.id))?;
                let df = lf
                    .collect()
                    .map_err(|e| format!("collect {}: {e}", mapping.id))?;

                let table = cfg_cloned
                    .analytic_tables
                    .iter()
                    .find(|t| t.id == table_id)
                    .ok_or_else(|| format!("table {table_id} not found"))?;

                // Assertions: failure fails this mapping only; others run.
                let violations = validate_assertions(&df, table);
                if !violations.is_empty() {
                    let msgs: Vec<String> = violations
                        .iter()
                        .map(|v| format!("assertion {}: {v}", mapping.id))
                        .collect();
                    tracing::warn!(
                        mapping = %mapping.id,
                        count = violations.len(),
                        "assertion violations; skipping upload",
                    );
                    return Err(msgs.join("; "));
                }

                let (df, dropped) = pipeline::dedup_rows(&df, table)
                    .map_err(|e| format!("dedup {}: {e}", mapping.id))?;
                if dropped > 0 {
                    tracing::info!(mapping = %mapping.id, dropped, "dedup dropped duplicate rows");
                }

                pipeline::produce_partitions(&df, mapping, table)
                    .map_err(|e| format!("partition {}: {e}", mapping.id))
                    .map(|parts| (parts, dropped))
            })
            .await;

        let (partitions, dropped) = match partitions_result {
            Ok(Ok(p)) => p,
            Ok(Err(msg)) => {
                errors.push(msg);
                continue;
            }
            Err(join_err) => {
                errors.push(format!("mapping {} task panicked: {join_err}", mapping.id));
                continue;
            }
        };

        total_deduped += dropped;
        let Some(publish) = publishes.get_mut(&mapping.analytic_table_id) else {
            errors.push(format!(
                "mapping {}: table {} not declared",
                mapping.id, mapping.analytic_table_id
            ));
            continue;
        };
        match upload_partitions_async(ctx, prefix, publish.version, &mapping.id, &partitions).await {
            Ok(written) => {
                total_partitions += written.len();
                publish.files =
                    manifest::apply_mapping_writes(&publish.files, &mapping.id, written);
                publish.touched = true;
            }
            Err(e) => errors.push(format!("upload {}: {e}", mapping.id)),
        }
    }

    // Publish each table that got new output, then collect what no retained
    // version references. A table whose every mapping failed is left on its
    // previous version.
    for (table_id, publish) in publishes.iter() {
        if !publish.touched {
            continue;
        }
        let manifest = manifest::TableManifest {
            version: publish.version,
            created_at: chrono::Utc::now().to_rfc3339(),
            job_id: None,
            files: publish.files.clone(),
        };
        if let Err(e) =
            manifest::publish(&ctx.s3_client, &ctx.warehouse_bucket, &publish.prefix, &manifest)
                .await
        {
            errors.push(format!("publish {table_id}: {e}"));
            continue;
        }
        tracing::info!(
            table = %table_id,
            version = manifest.version,
            files = manifest.files.len(),
            "published table version",
        );
        match manifest::vacuum(
            &ctx.s3_client,
            &ctx.warehouse_bucket,
            &publish.prefix,
            manifest.version,
        )
        .await
        {
            Ok(0) => {}
            Ok(n) => tracing::info!(table = %table_id, deleted = n, "vacuumed unreferenced objects"),
            Err(e) => tracing::warn!(table = %table_id, "vacuum skipped: {e}"),
        }
    }

    progress.update(Progress::Ingesting {
        mappings_done: mappings_total,
        mappings_total,
        partitions_written: total_partitions,
    });

    Ok(JobOutcome {
        partitions_written: total_partitions,
        files_processed,
        rows_deduped: total_deduped,
        errors,
    })
}

/// Upload partitions under `prefix`, into the table's staging version.
///
/// `PartitionOutput::key` is `<table>/<hive segments>/<mapping>.parquet`; the
/// version is spliced in after the table so the hive segments stay in the path
/// for readers to re-materialize partition columns from.
async fn upload_partitions_async(
    ctx: &JobContext,
    prefix: &str,
    version: u64,
    mapping_id: &str,
    partitions: &[pipeline::PartitionOutput],
) -> Result<Vec<manifest::ManifestFile>, String> {
    let mut written = Vec::with_capacity(partitions.len());
    for p in partitions {
        let (table_id, rest) = p
            .key
            .split_once('/')
            .ok_or_else(|| format!("partition key {} has no table segment", p.key))?;
        let relative = format!("{}{rest}", manifest::version_prefix(version));
        let full_key = format!("{prefix}{table_id}/{relative}");
        ctx.s3_client
            .put_object()
            .bucket(&ctx.warehouse_bucket)
            .key(&full_key)
            .body(aws_sdk_s3::primitives::ByteStream::from(p.bytes.clone()))
            .content_type("application/octet-stream")
            .send()
            .await
            .map_err(|e| format!("S3 PutObject failed for {full_key}: {}", s3mod::err_chain(&e)))?;
        written.push(manifest::ManifestFile {
            key: relative,
            mapping_id: mapping_id.to_string(),
            bytes: p.bytes.len() as u64,
        });
    }
    Ok(written)
}
