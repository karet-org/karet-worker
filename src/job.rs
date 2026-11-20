//! Shared pipeline-job executor.
//!
//! Both entry points — the legacy `POST /jobs/run` HTTP handler and the
//! Redis stream consumer — run jobs through [`execute_job`], so behavior
//! (clean_run semantics, per-mapping error containment, partition upload)
//! is identical regardless of transport.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use polars::prelude::DataFrame;

use crate::assertions::validate_assertions;
use crate::config::{self, PipelineConfig};
use crate::dimension;
use crate::incremental::{self, SourceObject};
use crate::lake::{self, Lake};
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
    pub lake_bucket: String,
    /// The control-plane database: where configs and job rows live.
    pub db: sqlx::PgPool,
    /// The DuckLake catalog the analytic tables are written to.
    pub lake: Arc<Lake>,
}

/// Run `f` with a lake connection on the blocking pool.
async fn with_lake<T: Send + 'static>(
    lake: &Arc<Lake>,
    f: impl FnOnce(&duckdb::Connection) -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    let lake = lake.clone();
    tokio::task::spawn_blocking(move || f(&lake.connection()?))
        .await
        .map_err(|e| format!("lake task panicked: {e}"))?
}

/// Execute one pipeline run.
///
/// Error containment matches the original handler: a failure in one
/// mapping is recorded in `errors` and the remaining mappings still run.
/// Unlike the original handler, the config is validated before execution;
/// a config that fails [`config::validate`] is rejected up front.
#[allow(clippy::too_many_arguments)]
pub async fn execute_job(
    ctx: &JobContext,
    job_id: Option<&str>,
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

    // Each mapping's rows are staged in local Parquet files and written to the
    // lake in one transaction per table after every mapping has run, so a
    // reader never sees one mapping's half of a union table.
    let staging = tempfile::tempdir().map_err(|e| JobError::ConfigRead(format!("staging folder: {e}")))?;
    let mut publishes: HashMap<String, TablePublish> = HashMap::new();
    for table in &cfg.analytic_tables {
        let sources = if clean_run {
            HashMap::new()
        } else {
            let (p, t) = (pipeline.to_string(), table.id.clone());
            with_lake(&ctx.lake, move |c| lake::read_records(c, &p, &t))
                .await
                .map_err(JobError::ConfigRead)?
        };
        publishes.insert(
            table.id.clone(),
            TablePublish { sources, writes: Vec::new(), touched: false },
        );
    }

    progress.update(Progress::Downloading { done: 0, total: 0 });
    let mut listings: HashMap<String, Vec<SourceObject>> = HashMap::new();
    let mut seen = HashSet::new();
    for sc in &cfg.source_containers {
        // path_prefix is an absolute lake key prefix; sources may point at
        // any folder in the lake, not just this pipeline's.
        match s3mod::list_objects(&ctx.s3_client, &ctx.lake_bucket, &sc.path_prefix).await {
            Ok(objects) => {
                let exts = sc.format.extensions();
                let mut listing: Vec<SourceObject> = objects
                    .into_iter()
                    .filter(|o| exts.iter().any(|e| o.key.ends_with(e)) && seen.insert(o.key.clone()))
                    .collect();
                listing.sort();
                listings.insert(sc.id.clone(), listing);
            }
            Err(e) => tracing::warn!("failed to list keys for {}: {e}", sc.path_prefix),
        }
    }
    if listings.values().all(|l| l.is_empty()) {
        return Err(JobError::NoFiles);
    }

    // Precompile the dimension registry once per job; shared by every mapping.
    // Inline rows compile from config; file-backed rows are fetched from the
    // lake here, where S3 access lives.
    let mut dimension_files: Vec<SourceObject> = Vec::new();
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
            let objects = s3mod::list_objects(&ctx.s3_client, &ctx.lake_bucket, path_prefix)
                .await
                .map_err(|e| JobError::Dimension(format!("listing {path_prefix}: {e}")))?;
            let objects: Vec<SourceObject> =
                objects.into_iter().filter(|o| o.key.ends_with(".csv")).collect();
            let keys: Vec<String> = objects.iter().map(|o| o.key.clone()).collect();
            dimension_files.extend(objects);
            let mut rows = Vec::new();
            for object_key in &keys {
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
    dimension_files.sort();

    let empty: Vec<SourceObject> = Vec::new();
    let mut plans: Vec<(incremental::Plan, String)> = Vec::with_capacity(cfg.mappings.len());
    for mapping in &cfg.mappings {
        let fingerprint = incremental::fingerprint(&cfg, mapping, &dimension_files);
        let listing = listings.get(&mapping.source_container_id).unwrap_or(&empty);
        let order_matters = cfg
            .analytic_tables
            .iter()
            .find(|t| t.id == mapping.analytic_table_id)
            .is_none_or(|t| !t.dedup_keys.is_empty());
        let plan = if clean_run {
            incremental::Plan::Full(incremental::FullReason::Requested)
        } else {
            let previous = publishes
                .get(&mapping.analytic_table_id)
                .and_then(|p| p.sources.get(&mapping.id));
            incremental::plan(previous, &fingerprint, listing, order_matters)
        };
        tracing::info!(mapping = %mapping.id, "{plan}");
        plans.push((plan, fingerprint));
    }

    let mut needed: Vec<String> = Vec::new();
    let mut wanted = HashSet::new();
    for (mapping, (plan, _)) in cfg.mappings.iter().zip(&plans) {
        let keys: &[SourceObject] = match plan {
            incremental::Plan::Full(_) => listings.get(&mapping.source_container_id).unwrap_or(&empty),
            incremental::Plan::Append(new) => new,
            incremental::Plan::Unchanged => &[],
        };
        needed.extend(keys.iter().filter(|o| wanted.insert(o.key.clone())).map(|o| o.key.clone()));
    }
    needed.sort();

    let mut downloaded: HashMap<String, Vec<u8>> = HashMap::new();
    let total = needed.len();
    for (i, key) in needed.iter().enumerate() {
        if cancel.load(Ordering::SeqCst) {
            return Err(JobError::Cancelled);
        }
        download(ctx, key, &mut downloaded).await;
        progress.update(Progress::Downloading { done: i + 1, total });
    }

    if !needed.is_empty() && downloaded.is_empty() {
        return Err(JobError::NoFiles);
    }

    let mut total_partitions = 0usize;
    let mut total_deduped: usize = 0;
    let mut errors: Vec<String> = Vec::new();
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

        if !cfg
            .source_containers
            .iter()
            .any(|s| s.id == mapping.source_container_id)
        {
            continue;
        }
        let listing = listings.get(&mapping.source_container_id).unwrap_or(&empty);
        let (plan, fingerprint) = &plans[mapping_idx];
        let Some(publish) = publishes.get_mut(&mapping.analytic_table_id) else {
            errors.push(format!(
                "mapping {}: table {} not declared",
                mapping.id, mapping.analytic_table_id
            ));
            continue;
        };

        match plan {
            incremental::Plan::Unchanged => continue,
            incremental::Plan::Full(_) => {}
            incremental::Plan::Append(new) => {
                match append_mapping(ctx, pipeline, &cfg, mapping, new, &downloaded, &matchers, staging.path())
                    .await
                {
                    Ok(appended) => {
                        total_partitions += appended.partitions;
                        total_deduped += appended.dropped;
                        if let Some(file) = appended.file {
                            publish.writes.push(StagedWrite { mapping_id: mapping.id.clone(), file, replace: false });
                        }
                        let previous =
                            publish.sources.get(&mapping.id).map(|s| s.files.as_slice()).unwrap_or(&[]);
                        let files = incremental::record_files(previous, appended.read);
                        publish.record(&mapping.id, fingerprint, files);
                        continue;
                    }
                    Err(Failure::Mapping(msg)) => {
                        errors.push(msg);
                        continue;
                    }
                    Err(Failure::Fallback(msg)) => {
                        tracing::warn!(mapping = %mapping.id, "incremental run failed, running in full: {msg}");
                        for o in listing {
                            if !downloaded.contains_key(&o.key) {
                                download(ctx, &o.key, &mut downloaded).await;
                            }
                        }
                    }
                }
            }
        }

        let read: Vec<SourceObject> = listing
            .iter()
            .filter(|o| downloaded.contains_key(&o.key))
            .cloned()
            .collect();
        if read.is_empty() {
            continue;
        }
        let mapping_files: Vec<(String, Vec<u8>)> = read
            .iter()
            .map(|o| (o.key.clone(), downloaded[&o.key].clone()))
            .collect();

        // Polars work is CPU-bound: run it on the blocking pool so
        // heartbeats and health checks stay responsive.
        let (cfg_c, mapping_c, matchers_c) = (cfg.clone(), mapping.clone(), matchers.clone());
        let file = staging.path().join(format!("{mapping_idx}.parquet"));
        let file_c = file.clone();
        let staged = tokio::task::spawn_blocking(move || {
            let (mut df, dropped) = ingest_checked(&mapping_files, &cfg_c, &mapping_c, &matchers_c)?
                .ok_or_else(|| {
                    Failure::Mapping(format!(
                        "ingest {}: {}",
                        mapping_c.id,
                        crate::error::PipelineError::NoFilesSucceeded
                    ))
                })?;
            let table = table_for(&cfg_c, &mapping_c)?;
            let partitions = pipeline::count_partitions(&df, table)
                .map_err(|e| Failure::Mapping(format!("partition {}: {e}", mapping_c.id)))?;
            stage(&mut df, &file_c).map_err(|e| Failure::Mapping(format!("stage {}: {e}", mapping_c.id)))?;
            Ok::<_, Failure>((partitions, dropped))
        })
        .await;

        match staged {
            Ok(Ok((partitions, dropped))) => {
                total_partitions += partitions;
                total_deduped += dropped;
                publish.writes.push(StagedWrite { mapping_id: mapping.id.clone(), file, replace: true });
                publish.record(&mapping.id, fingerprint, read);
            }
            Ok(Err(Failure::Mapping(msg) | Failure::Fallback(msg))) => errors.push(msg),
            Err(join_err) => errors.push(format!("mapping {} task panicked: {join_err}", mapping.id)),
        }
    }
    let files_processed = downloaded.len();

    // Commit each table that got new output, then tidy it. A table whose
    // every mapping failed is left as it was.
    for table in &cfg.analytic_tables {
        let Some(publish) = publishes.remove(&table.id) else { continue };
        if !publish.touched {
            continue;
        }
        let (p, t, job) = (pipeline.to_string(), table.clone(), job_id.map(str::to_string));
        let committed = with_lake(&ctx.lake, move |c| {
            let writes: Vec<lake::Write> = publish
                .writes
                .iter()
                .map(|w| lake::Write { mapping_id: &w.mapping_id, file: &w.file, replace: w.replace })
                .collect();
            lake::commit_table(
                c,
                &lake::Commit {
                    pipeline: &p,
                    table: &t,
                    writes: &writes,
                    records: &publish.sources,
                    clean: clean_run,
                    job_id: job.as_deref(),
                },
            )
        })
        .await;
        match committed {
            Ok(snapshot) => tracing::info!(table = %table.id, snapshot, "published table version"),
            Err(e) => {
                errors.push(format!("publish {}: {e}", table.id));
                continue;
            }
        }
        let (p, t) = (pipeline.to_string(), table.id.clone());
        match with_lake(&ctx.lake, move |c| lake::maintain(c, &p, &t)).await {
            Ok(0) => {}
            Ok(n) => tracing::info!(table = %table.id, expired = n, "expired old snapshots"),
            Err(e) => tracing::warn!(table = %table.id, "maintenance skipped: {e}"),
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

/// One table's next version, built up while the mappings run.
struct TablePublish {
    /// What each mapping's rows were built from, by mapping id.
    sources: HashMap<String, incremental::MappingSources>,
    writes: Vec<StagedWrite>,
    touched: bool,
}

/// One mapping's rows, staged in a local Parquet file.
struct StagedWrite {
    mapping_id: String,
    file: std::path::PathBuf,
    /// Replace the mapping's stored rows, or add to them.
    replace: bool,
}

fn stage(df: &mut DataFrame, path: &std::path::Path) -> Result<(), String> {
    let bytes = pipeline::write_parquet_bytes(df).map_err(|e| e.to_string())?;
    std::fs::write(path, bytes).map_err(|e| format!("{}: {e}", path.display()))
}

impl TablePublish {
    fn record(&mut self, mapping_id: &str, fingerprint: &str, files: Vec<SourceObject>) {
        self.sources.insert(
            mapping_id.to_string(),
            incremental::MappingSources {
                mapping_id: mapping_id.to_string(),
                fingerprint: fingerprint.to_string(),
                files,
            },
        );
        self.touched = true;
    }
}

async fn download(ctx: &JobContext, key: &str, into: &mut HashMap<String, Vec<u8>>) {
    match s3mod::get_bytes(&ctx.s3_client, &ctx.lake_bucket, key).await {
        Ok(bytes) => {
            into.insert(key.to_string(), bytes);
        }
        Err(e) => tracing::warn!("failed to download {key}: {e}"),
    }
}

/// How a mapping's run failed.
enum Failure {
    /// The mapping fails this run and keeps its previous files.
    Mapping(String),
    /// An incremental run hit something a full run does not depend on, such
    /// as a stored file it could not read; the mapping runs in full instead.
    Fallback(String),
}

fn table_for<'a>(
    cfg: &'a PipelineConfig,
    mapping: &crate::config::Mapping,
) -> Result<&'a crate::config::AnalyticTable, Failure> {
    cfg.analytic_tables
        .iter()
        .find(|t| t.id == mapping.analytic_table_id)
        .ok_or_else(|| Failure::Mapping(format!("table {} not found", mapping.analytic_table_id)))
}

/// Ingest, assert and dedup one mapping's files. `None` when no file could be
/// ingested. Assertion failures are [`Failure::Mapping`]; the rest are
/// [`Failure::Fallback`], which a full run reports like a mapping failure.
fn ingest_checked(
    files: &[(String, Vec<u8>)],
    cfg: &PipelineConfig,
    mapping: &crate::config::Mapping,
    matchers: &HashMap<String, Arc<dimension::DimensionMatcher>>,
) -> Result<Option<(DataFrame, usize)>, Failure> {
    let lf = match pipeline::ingest_many(files, cfg, mapping, matchers) {
        Ok(lf) => lf,
        Err(crate::error::PipelineError::NoFilesSucceeded) => return Ok(None),
        Err(e) => return Err(Failure::Fallback(format!("ingest {}: {e}", mapping.id))),
    };
    let df = lf
        .collect()
        .map_err(|e| Failure::Fallback(format!("collect {}: {e}", mapping.id)))?;
    let table = table_for(cfg, mapping)?;

    let violations = validate_assertions(&df, table);
    if !violations.is_empty() {
        tracing::warn!(
            mapping = %mapping.id,
            count = violations.len(),
            "assertion violations; skipping upload",
        );
        let msgs: Vec<String> = violations
            .iter()
            .map(|v| format!("assertion {}: {v}", mapping.id))
            .collect();
        return Err(Failure::Mapping(msgs.join("; ")));
    }

    let (df, dropped) = pipeline::dedup_rows(&df, table)
        .map_err(|e| Failure::Fallback(format!("dedup {}: {e}", mapping.id)))?;
    if dropped > 0 {
        tracing::info!(mapping = %mapping.id, dropped, "dedup dropped duplicate rows");
    }
    Ok(Some((df, dropped)))
}

struct Appended {
    /// The staged new rows; `None` when none are left after dedup.
    file: Option<std::path::PathBuf>,
    partitions: usize,
    dropped: usize,
    read: Vec<SourceObject>,
}

/// Ingest `new` files and stage the rows whose dedup key is not stored yet.
#[allow(clippy::too_many_arguments)]
async fn append_mapping(
    ctx: &JobContext,
    pipeline: &str,
    cfg: &PipelineConfig,
    mapping: &crate::config::Mapping,
    new: &[SourceObject],
    downloaded: &HashMap<String, Vec<u8>>,
    matchers: &HashMap<String, Arc<dimension::DimensionMatcher>>,
    staging: &std::path::Path,
) -> Result<Appended, Failure> {
    let read: Vec<SourceObject> = new
        .iter()
        .filter(|o| downloaded.contains_key(&o.key))
        .cloned()
        .collect();
    let files: Vec<(String, Vec<u8>)> = read
        .iter()
        .map(|o| (o.key.clone(), downloaded[&o.key].clone()))
        .collect();
    let table = table_for(cfg, mapping)?.clone();

    let ingested_path = staging.join(format!("{}.new.parquet", sanitize(&mapping.id)));
    let kept_path = staging.join(format!("{}.kept.parquet", sanitize(&mapping.id)));
    let (cfg_c, mapping_c, matchers_c, table_c, ingested_c) =
        (cfg.clone(), mapping.clone(), matchers.clone(), table.clone(), ingested_path.clone());
    let ingested = tokio::task::spawn_blocking(move || {
        let Some((mut rows, dropped)) = ingest_checked(&files, &cfg_c, &mapping_c, &matchers_c)? else {
            return Ok(None);
        };
        pipeline::count_partitions(&rows, &table_c)
            .map_err(|e| Failure::Mapping(format!("partition {}: {e}", mapping_c.id)))?;
        if rows.height() == 0 {
            return Ok(Some(dropped));
        }
        stage(&mut rows, &ingested_c).map_err(Failure::Fallback)?;
        Ok(Some(dropped))
    })
    .await
    .map_err(|e| Failure::Fallback(format!("ingest task panicked: {e}")))??;
    let Some(dropped) = ingested else {
        return Ok(Appended { file: None, partitions: 0, dropped: 0, read });
    };
    if !ingested_path.exists() {
        return Ok(Appended { file: None, partitions: 0, dropped, read });
    }

    let (p, m, t, i, k) = (pipeline.to_string(), mapping.id.clone(), table.clone(), ingested_path, kept_path.clone());
    let (stored_dupes, partitions) = with_lake(&ctx.lake, move |c| lake::drop_stored_keys(c, &p, &t, &m, &i, &k))
        .await
        .map_err(Failure::Fallback)?;
    Ok(Appended {
        file: (partitions > 0).then_some(kept_path),
        partitions,
        dropped: dropped + stored_dupes,
        read,
    })
}

/// A mapping id as a file name.
fn sanitize(id: &str) -> String {
    id.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' }).collect()
}
