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

    // Writes land under `v<next>/` and become visible only when the manifest
    // and pointer are written after every mapping has run, so a reader never
    // sees one mapping's half of a union table.
    //
    // clean_run starts from an empty file list instead of deleting anything:
    // the old objects stay readable under their own version until vacuum
    // retires them, which is what makes a bad run recoverable.
    let mut publishes: HashMap<String, TablePublish> = HashMap::new();
    for table in &cfg.analytic_tables {
        let table_prefix = format!("{prefix}{}/", table.id);
        let current = manifest::read_current(&ctx.s3_client, &ctx.warehouse_bucket, &table_prefix)
            .await
            .map_err(JobError::ConfigRead)?;
        let sources = if clean_run {
            HashMap::new()
        } else {
            incremental::read_record(&ctx.s3_client, &ctx.warehouse_bucket, &table_prefix, &current)
                .await
        };
        publishes.insert(
            table.id.clone(),
            TablePublish {
                prefix: table_prefix,
                version: current.version + 1,
                files: if clean_run { Vec::new() } else { current.files },
                sources,
                touched: false,
            },
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
                match append_mapping(ctx, prefix, &cfg, mapping, new, &downloaded, &matchers, publish)
                    .await
                {
                    Ok(appended) => {
                        total_partitions += appended.files.len();
                        total_deduped += appended.dropped;
                        publish.files = manifest::apply_partition_writes(
                            &publish.files,
                            &mapping.id,
                            appended.files,
                        );
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
        // heartbeats and health checks stay responsive. Uploads happen
        // back on the async runtime.
        let (cfg_c, mapping_c, matchers_c) = (cfg.clone(), mapping.clone(), matchers.clone());
        let partitions_result = tokio::task::spawn_blocking(move || {
            let (df, dropped) = ingest_checked(&mapping_files, &cfg_c, &mapping_c, &matchers_c)?
                .ok_or_else(|| {
                    Failure::Mapping(format!(
                        "ingest {}: {}",
                        mapping_c.id,
                        crate::error::PipelineError::NoFilesSucceeded
                    ))
                })?;
            let table = table_for(&cfg_c, &mapping_c)?;
            pipeline::produce_partitions(&df, &mapping_c, table)
                .map_err(|e| Failure::Mapping(format!("partition {}: {e}", mapping_c.id)))
                .map(|parts| (parts, dropped))
        })
        .await;

        let (partitions, dropped) = match partitions_result {
            Ok(Ok(p)) => p,
            Ok(Err(Failure::Mapping(msg) | Failure::Fallback(msg))) => {
                errors.push(msg);
                continue;
            }
            Err(join_err) => {
                errors.push(format!("mapping {} task panicked: {join_err}", mapping.id));
                continue;
            }
        };

        total_deduped += dropped;
        match upload_partitions_async(ctx, prefix, publish.version, &mapping.id, &partitions).await {
            Ok(written) => {
                total_partitions += written.len();
                publish.files =
                    manifest::apply_mapping_writes(&publish.files, &mapping.id, written);
                publish.record(&mapping.id, fingerprint, read);
            }
            Err(e) => errors.push(format!("upload {}: {e}", mapping.id)),
        }
    }
    let files_processed = downloaded.len();

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
        // A failed write only costs a full run next time: the record will not
        // match the manifest.
        if let Err(e) = incremental::write_record(
            &ctx.s3_client,
            &ctx.warehouse_bucket,
            &publish.prefix,
            &manifest,
            &publish.sources,
        )
        .await
        {
            tracing::warn!(table = %table_id, "source record not written: {e}");
        }
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

/// One table's next version, built up while the mappings run.
struct TablePublish {
    prefix: String,
    version: u64,
    files: Vec<manifest::ManifestFile>,
    /// What each mapping's files were built from, by mapping id.
    sources: HashMap<String, incremental::MappingSources>,
    touched: bool,
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
    files: Vec<manifest::ManifestFile>,
    dropped: usize,
    read: Vec<SourceObject>,
}

/// Ingest `new` files and merge their rows into the partitions they fall
/// into. Other partitions are left for [`manifest::apply_partition_writes`]
/// to carry forward.
#[allow(clippy::too_many_arguments)]
async fn append_mapping(
    ctx: &JobContext,
    prefix: &str,
    cfg: &PipelineConfig,
    mapping: &crate::config::Mapping,
    new: &[SourceObject],
    downloaded: &HashMap<String, Vec<u8>>,
    matchers: &HashMap<String, Arc<dimension::DimensionMatcher>>,
    publish: &TablePublish,
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

    let (cfg_c, mapping_c, matchers_c) = (cfg.clone(), mapping.clone(), matchers.clone());
    let ingested = tokio::task::spawn_blocking(move || ingest_checked(&files, &cfg_c, &mapping_c, &matchers_c))
        .await
        .map_err(|e| Failure::Fallback(format!("ingest task panicked: {e}")))??;
    let Some((mut rows, mut dropped)) = ingested else {
        return Ok(Appended { files: Vec::new(), dropped: 0, read });
    };

    let own: Vec<&manifest::ManifestFile> =
        publish.files.iter().filter(|f| f.mapping_id == mapping.id).collect();
    let fetch = |key: String| async move {
        s3mod::get_bytes(&ctx.s3_client, &ctx.warehouse_bucket, &format!("{}{key}", publish.prefix))
            .await
            .map_err(Failure::Fallback)
    };

    // Dedup keys need not include the partition keys, so a duplicate can be
    // in any of the mapping's partitions.
    if !table.dedup_keys.is_empty() && rows.height() > 0 && !own.is_empty() {
        let mut stored = Vec::with_capacity(own.len());
        for f in &own {
            stored.push((f.key.clone(), fetch(f.key.clone()).await?));
        }
        let table_c = table.clone();
        let (kept, d) = tokio::task::spawn_blocking(move || {
            let keys = incremental::stored_keys(&stored, &table_c)?;
            incremental::drop_stored_keys(&rows, &keys, &table_c.dedup_keys)
        })
        .await
        .map_err(|e| Failure::Fallback(format!("dedup task panicked: {e}")))?
        .map_err(Failure::Fallback)?;
        rows = kept;
        dropped += d;
    }

    if rows.height() == 0 {
        return Ok(Appended { files: Vec::new(), dropped, read });
    }

    let frames = pipeline::partition_frames(&rows, mapping, &table)
        .map_err(|e| Failure::Fallback(e.to_string()))?;
    let mut merges = Vec::with_capacity(frames.len());
    for (key, frame) in frames {
        let path = key.split_once('/').map(|(_, rest)| rest).unwrap_or_default();
        let existing = match own.iter().find(|f| manifest::partition_path(&f.key) == path) {
            Some(f) => Some(fetch(f.key.clone()).await?),
            None => None,
        };
        merges.push((key, frame, existing));
    }
    let partitions = tokio::task::spawn_blocking(move || {
        merges
            .into_iter()
            .map(|(key, frame, existing)| {
                let mut merged = match existing {
                    Some(bytes) => incremental::merge_partition(&bytes, &frame)?,
                    None => frame,
                };
                let bytes = pipeline::write_parquet_bytes(&mut merged).map_err(|e| e.to_string())?;
                Ok(pipeline::PartitionOutput { key, bytes })
            })
            .collect::<Result<Vec<_>, String>>()
    })
    .await
    .map_err(|e| Failure::Fallback(format!("merge task panicked: {e}")))?
    .map_err(Failure::Fallback)?;

    let files = upload_partitions_async(ctx, prefix, publish.version, &mapping.id, &partitions)
        .await
        .map_err(|e| Failure::Mapping(format!("upload {}: {e}", mapping.id)))?;
    Ok(Appended { files, dropped, read })
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
