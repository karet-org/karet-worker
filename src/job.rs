//! Shared pipeline-job executor.
//!
//! Both entry points — the legacy `POST /jobs/run` HTTP handler and the
//! Redis stream consumer — run jobs through [`execute_job`], so behavior
//! (clean_run semantics, per-mapping error containment, partition upload)
//! is identical regardless of transport.

use crate::assertions::validate_assertions;
use crate::config::{self, PipelineConfig};
use crate::lookup;
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
    /// No CSV files found under any source container prefix.
    #[error("no CSV files found to process")]
    NoFiles,
}

/// Everything `execute_job` needs from the environment.
#[derive(Clone)]
pub struct JobContext {
    pub s3_client: aws_sdk_s3::Client,
    pub pipelines_bucket: String,
    pub lake_bucket: String,
    pub warehouse_bucket: String,
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
    clean_run: bool,
    progress: &dyn ProgressSink,
) -> Result<JobOutcome, JobError> {
    let config_key = format!("{prefix}pipeline.json");
    let config_bytes = s3mod::get_bytes(&ctx.s3_client, &ctx.pipelines_bucket, &config_key)
        .await
        .map_err(JobError::ConfigRead)?;
    let cfg: PipelineConfig =
        serde_json::from_slice(&config_bytes).map_err(|e| JobError::ConfigParse(e.to_string()))?;
    if let Err(errs) = config::validate(&cfg) {
        let joined = errs
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        return Err(JobError::ConfigInvalid(joined));
    }

    // clean_run: delete existing warehouse output under the tables the
    // current config declares (so stale tables from prior configs aren't
    // wiped).
    if clean_run {
        for table in &cfg.analytic_tables {
            let table_prefix = format!("{prefix}{}/", table.id);
            match s3mod::list_keys(&ctx.s3_client, &ctx.warehouse_bucket, &table_prefix).await {
                Ok(keys) => {
                    for key in keys {
                        let _ = ctx
                            .s3_client
                            .delete_object()
                            .bucket(&ctx.warehouse_bucket)
                            .key(&key)
                            .send()
                            .await;
                    }
                    tracing::info!("clean_run: deleted existing clean output under {table_prefix}");
                }
                Err(e) => tracing::warn!(
                    "clean_run: failed to list clean keys under {table_prefix}: {e}"
                ),
            }
        }
    }

    // Download every raw CSV file under each source container's
    // path_prefix from the lake bucket.
    progress.update(Progress::Downloading { done: 0, total: 0 });
    let mut candidate_keys: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for sc in &cfg.source_containers {
        let raw_prefix = format!("{prefix}{}", sc.path_prefix);
        match s3mod::list_keys(&ctx.s3_client, &ctx.lake_bucket, &raw_prefix).await {
            Ok(keys) => candidate_keys.extend(
                keys.into_iter()
                    .filter(|k| k.ends_with(".csv") && seen.insert(k.clone())),
            ),
            Err(e) => tracing::warn!("failed to list keys for {raw_prefix}: {e}"),
        }
    }

    let mut all_files: Vec<(String, Vec<u8>)> = Vec::new();
    let total = candidate_keys.len();
    for (i, key) in candidate_keys.iter().enumerate() {
        match s3mod::get_bytes(&ctx.s3_client, &ctx.lake_bucket, key).await {
            Ok(bytes) => {
                // Strip pipeline prefix so the key matches path_prefix.
                let rel_key = key.strip_prefix(prefix).unwrap_or(key).to_string();
                all_files.push((rel_key, bytes));
            }
            Err(e) => tracing::warn!("failed to download {key}: {e}"),
        }
        progress.update(Progress::Downloading { done: i + 1, total });
    }

    if all_files.is_empty() {
        return Err(JobError::NoFiles);
    }

    // Precompile the lookup registry once per job; shared by every mapping.
    let matchers = lookup::build_registry(&cfg.lookup_mappings);

    let mut total_partitions = 0usize;
    let mut errors: Vec<String> = Vec::new();
    let files_processed = all_files.len();
    let mappings_total = cfg.mappings.len();

    for (mapping_idx, mapping) in cfg.mappings.iter().enumerate() {
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

        // The Polars pipeline for one mapping (parse, evaluate, encode
        // Parquet) is CPU-bound; run it on the blocking pool so heartbeats
        // and health checks stay responsive during large collects. Uploads
        // happen back on the async runtime: the sync `PartitionUploader`
        // bridge uses `block_in_place`, which panics on blocking-pool
        // threads.
        let cfg_cloned = cfg.clone();
        let mapping_id = mapping.id.clone();
        let table_id = mapping.analytic_table_id.clone();
        let matchers_cloned = matchers.clone();
        let partitions_result: Result<Result<Vec<pipeline::PartitionOutput>, String>, _> =
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

                pipeline::produce_partitions(&df, mapping, table)
                    .map_err(|e| format!("partition {}: {e}", mapping.id))
            })
            .await;

        let partitions = match partitions_result {
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

        match upload_partitions_async(ctx, prefix, &partitions).await {
            Ok(count) => total_partitions += count,
            Err(e) => errors.push(format!("upload {}: {e}", mapping.id)),
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
        errors,
    })
}

/// Async equivalent of [`pipeline::upload_partitions`] +
/// [`s3mod::S3PartitionUploader`]: same key layout, same
/// short-circuit-on-first-failure semantics, same error text, but native
/// `await` instead of the `block_in_place` bridge.
async fn upload_partitions_async(
    ctx: &JobContext,
    prefix: &str,
    partitions: &[pipeline::PartitionOutput],
) -> Result<usize, String> {
    let mut uploaded = 0usize;
    for p in partitions {
        let full_key = format!("{prefix}{}", p.key);
        ctx.s3_client
            .put_object()
            .bucket(&ctx.warehouse_bucket)
            .key(&full_key)
            .body(aws_sdk_s3::primitives::ByteStream::from(p.bytes.clone()))
            .content_type("application/octet-stream")
            .send()
            .await
            .map_err(|e| format!("S3 PutObject failed for {full_key}: {}", s3mod::err_chain(&e)))?;
        uploaded += 1;
    }
    Ok(uploaded)
}
