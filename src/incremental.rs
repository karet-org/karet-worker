//! Incremental runs: ingest only the source files a mapping has not read.
//!
//! Each table version has a record at `_sources/<version>.json`: per mapping,
//! a fingerprint of the config that decides its output and the key and ETag of
//! every file read. With a matching record and only new files, a run ingests
//! the new files, drops rows whose dedup key is already stored, and rewrites
//! the partitions the rest fall into. That equals a full run as long as new
//! files sort after old ones, since dedup keeps the first row in key order.

use std::collections::{HashMap, HashSet};
use std::io::Cursor;

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{AnalyticTable, Mapping, PipelineConfig};
use crate::manifest::{self, ManifestFile, TableManifest};

/// Bump when the worker changes what a mapping produces from the same inputs.
const OUTPUT_FORMAT: u32 = 1;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceObject {
    pub key: String,
    pub etag: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MappingSources {
    pub mapping_id: String,
    pub fingerprint: String,
    /// Sorted by key.
    pub files: Vec<SourceObject>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct TableSources {
    version: u64,
    /// [`files_digest`] of the manifest written with this record.
    files_digest: String,
    mappings: Vec<MappingSources>,
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|b| format!("{b:02x}")).collect()
}

fn files_digest(files: &[ManifestFile]) -> String {
    let mut keys: Vec<&str> = files.iter().map(|f| f.key.as_str()).collect();
    keys.sort_unstable();
    sha256_hex(keys.join("\n").as_bytes())
}

/// The record for `current`, by mapping id. Empty when it is missing or was
/// not written with this manifest (older versions, web restores, a run that
/// died before publishing), which makes every mapping run in full.
pub async fn read_record(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    table_prefix: &str,
    current: &TableManifest,
) -> HashMap<String, MappingSources> {
    if current.version == 0 {
        return HashMap::new();
    }
    let key = format!("{table_prefix}{}", manifest::sources_key(current.version));
    let Ok(bytes) = crate::s3::get_bytes(client, bucket, &key).await else {
        return HashMap::new();
    };
    match serde_json::from_slice::<TableSources>(&bytes) {
        Ok(r) if r.version == current.version && r.files_digest == files_digest(&current.files) => {
            r.mappings.into_iter().map(|m| (m.mapping_id.clone(), m)).collect()
        }
        Ok(_) => HashMap::new(),
        Err(e) => {
            tracing::warn!("{key} is not a source record: {e}");
            HashMap::new()
        }
    }
}

/// Write the record for `manifest`. Call before publishing it.
pub async fn write_record(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    table_prefix: &str,
    manifest: &TableManifest,
    sources: &HashMap<String, MappingSources>,
) -> Result<(), String> {
    let mut mappings: Vec<MappingSources> = sources.values().cloned().collect();
    mappings.sort_by(|a, b| a.mapping_id.cmp(&b.mapping_id));
    let record = TableSources {
        version: manifest.version,
        files_digest: files_digest(&manifest.files),
        mappings,
    };
    let key = format!("{table_prefix}{}", manifest::sources_key(manifest.version));
    let body = serde_json::to_vec(&record).map_err(|e| format!("serialize source record: {e}"))?;
    client
        .put_object()
        .bucket(bucket)
        .key(&key)
        .body(aws_sdk_s3::primitives::ByteStream::from(body))
        .content_type("application/json")
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("PutObject {key}: {}", crate::s3::err_chain(&e)))
}

/// Hash of everything besides the source files that decides `mapping`'s
/// output. `dimension_files` must be sorted.
pub fn fingerprint(cfg: &PipelineConfig, mapping: &Mapping, dimension_files: &[SourceObject]) -> String {
    let source = cfg.source_containers.iter().find(|s| s.id == mapping.source_container_id);
    let table = cfg.analytic_tables.iter().find(|t| t.id == mapping.analytic_table_id);
    // serde_json's map sorts keys, so the encoding is stable.
    let doc = serde_json::json!({
        "output_format": OUTPUT_FORMAT,
        "mapping": mapping,
        "source": source,
        "table": table,
        "dimensions": cfg.dimensions,
        "dimension_files": dimension_files,
    });
    sha256_hex(doc.to_string().as_bytes())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FullReason {
    Requested,
    NoRecord,
    ConfigChanged,
    Replaced(String),
    Removed(String),
    OutOfOrder(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    Full(FullReason),
    Unchanged,
    Append(Vec<SourceObject>),
}

impl std::fmt::Display for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Plan::Full(FullReason::Requested) => write!(f, "full run: requested"),
            Plan::Full(FullReason::NoRecord) => write!(f, "full run: no source record"),
            Plan::Full(FullReason::ConfigChanged) => write!(f, "full run: config changed"),
            Plan::Full(FullReason::Replaced(k)) => write!(f, "full run: {k} was replaced"),
            Plan::Full(FullReason::Removed(k)) => write!(f, "full run: {k} was removed"),
            Plan::Full(FullReason::OutOfOrder(k)) => {
                write!(f, "full run: {k} sorts before a file already read")
            }
            Plan::Unchanged => write!(f, "no new source files"),
            Plan::Append(new) => write!(f, "incremental run: {} new files", new.len()),
        }
    }
}

/// `listing` must be sorted by key. `order_matters` is whether the table has
/// dedup keys.
pub fn plan(
    previous: Option<&MappingSources>,
    fingerprint: &str,
    listing: &[SourceObject],
    order_matters: bool,
) -> Plan {
    let Some(previous) = previous else {
        return Plan::Full(FullReason::NoRecord);
    };
    if previous.fingerprint != fingerprint {
        return Plan::Full(FullReason::ConfigChanged);
    }
    let listed: HashMap<&str, &str> =
        listing.iter().map(|o| (o.key.as_str(), o.etag.as_str())).collect();
    for old in &previous.files {
        match listed.get(old.key.as_str()) {
            None => return Plan::Full(FullReason::Removed(old.key.clone())),
            Some(etag) if *etag != old.etag => return Plan::Full(FullReason::Replaced(old.key.clone())),
            Some(_) => {}
        }
    }
    let seen: HashSet<&str> = previous.files.iter().map(|o| o.key.as_str()).collect();
    let new: Vec<SourceObject> =
        listing.iter().filter(|o| !seen.contains(o.key.as_str())).cloned().collect();
    if new.is_empty() {
        return Plan::Unchanged;
    }
    if order_matters {
        if let Some(last) = previous.files.last() {
            if let Some(early) = new.iter().find(|o| o.key < last.key) {
                return Plan::Full(FullReason::OutOfOrder(early.key.clone()));
            }
        }
    }
    Plan::Append(new)
}

pub fn record_files(previous: &[SourceObject], read: Vec<SourceObject>) -> Vec<SourceObject> {
    let mut out: Vec<SourceObject> = previous.to_vec();
    out.extend(read);
    out.sort();
    out.dedup_by(|a, b| a.key == b.key);
    out
}

/// Inverse of the `%XX` escaping in partition paths.
fn decode_segment(value: &str) -> Result<String, String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = value.get(i + 1..i + 3).ok_or_else(|| format!("truncated escape in `{value}`"))?;
            out.push(u8::from_str_radix(hex, 16).map_err(|_| format!("bad escape in `{value}`"))?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| format!("`{value}` is not UTF-8 once decoded"))
}

fn partition_value(key: &str, name: &str) -> Result<String, String> {
    let prefix = format!("{name}=");
    let raw = manifest::partition_path(key)
        .split('/')
        .find_map(|s| s.strip_prefix(prefix.as_str()))
        .ok_or_else(|| format!("{key} has no `{name}` segment"))?;
    decode_segment(raw)
}

pub fn read_parquet(bytes: &[u8]) -> Result<DataFrame, String> {
    ParquetReader::new(Cursor::new(bytes))
        .finish()
        .map_err(|e| format!("read parquet: {e}"))
}

/// Keys are compared as strings: partition values come back from the path as
/// strings, and both sides go through the same cast.
fn key_strings(df: &DataFrame, keys: &[String]) -> Result<DataFrame, String> {
    let columns = keys
        .iter()
        .map(|k| {
            df.column(k)
                .and_then(|c| c.cast(&DataType::String))
                .map_err(|e| format!("dedup key `{k}`: {e}"))
        })
        .collect::<Result<Vec<Column>, String>>()?;
    DataFrame::new(df.height(), columns).map_err(|e| e.to_string())
}

/// The dedup key tuples in stored Parquet files, given as `(key, bytes)`.
/// Partition key columns are read from the path, since the payload omits them.
pub fn stored_keys(files: &[(String, Vec<u8>)], table: &AnalyticTable) -> Result<DataFrame, String> {
    let mut out: Option<DataFrame> = None;
    for (key, bytes) in files {
        let df = read_parquet(bytes)?;
        let columns = table
            .dedup_keys
            .iter()
            .map(|k| {
                if table.partition_keys.contains(k) {
                    let value = partition_value(key, k)?;
                    Ok(Column::new(k.as_str().into(), vec![value; df.height()]))
                } else {
                    df.column(k)
                        .and_then(|c| c.cast(&DataType::String))
                        .map_err(|e| format!("{key}: dedup key `{k}`: {e}"))
                }
            })
            .collect::<Result<Vec<Column>, String>>()?;
        let keys = DataFrame::new(df.height(), columns).map_err(|e| e.to_string())?;
        out = Some(match out {
            None => keys,
            Some(acc) => acc.vstack(&keys).map_err(|e| e.to_string())?,
        });
    }
    Ok(out.unwrap_or_default())
}

/// Drop rows of `new_rows` whose key is in `stored`, keeping order.
/// `new_rows` must already be deduplicated. Returns the rows and the number
/// dropped.
pub fn drop_stored_keys(
    new_rows: &DataFrame,
    stored: &DataFrame,
    keys: &[String],
) -> Result<(DataFrame, usize), String> {
    if keys.is_empty() || stored.height() == 0 || new_rows.height() == 0 {
        return Ok((new_rows.clone(), 0));
    }
    const ROW: &str = "__karet_new_row";
    let fresh = key_strings(new_rows, keys)?
        .with_row_index(ROW.into(), None)
        .map_err(|e| e.to_string())?;
    let mut old_columns = vec![Column::full_null(ROW.into(), stored.height(), &IDX_DTYPE)];
    for k in keys {
        old_columns.push(stored.column(k).map_err(|e| e.to_string())?.clone());
    }
    let old = DataFrame::new(stored.height(), old_columns).map_err(|e| e.to_string())?;
    // Stored rows first so they win, as they do in a full run. unique_stable
    // also treats null keys as equal, matching dedup_rows.
    let unique = old
        .vstack(&fresh)
        .and_then(|all| all.unique_stable(Some(keys), UniqueKeepStrategy::First, None))
        .map_err(|e| e.to_string())?;
    let kept: Vec<IdxSize> = unique
        .column(ROW)
        .and_then(|c| c.idx().cloned())
        .map_err(|e| e.to_string())?
        .into_iter()
        .flatten()
        .collect();
    let rows = new_rows
        .take(&IdxCa::from_vec(ROW.into(), kept))
        .map_err(|e| e.to_string())?;
    let dropped = new_rows.height() - rows.height();
    Ok((rows, dropped))
}

/// Stored rows followed by `new_rows`, cast to the stored types.
pub fn merge_partition(existing: &[u8], new_rows: &DataFrame) -> Result<DataFrame, String> {
    let old = read_parquet(existing)?;
    if new_rows.width() != old.width() {
        return Err(format!(
            "stored partition has {} columns, new rows have {}",
            old.width(),
            new_rows.width()
        ));
    }
    let columns = old
        .schema()
        .iter_fields()
        .map(|field| {
            new_rows
                .column(field.name())
                .and_then(|c| c.cast(field.dtype()))
                .map_err(|e| format!("column `{}`: {e}", field.name()))
        })
        .collect::<Result<Vec<Column>, String>>()?;
    let aligned = DataFrame::new(new_rows.height(), columns).map_err(|e| e.to_string())?;
    old.vstack(&aligned).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::{dedup_rows, partition_frames, write_parquet_bytes};

    fn obj(key: &str, etag: &str) -> SourceObject {
        SourceObject { key: key.into(), etag: etag.into() }
    }

    fn record(files: &[SourceObject]) -> MappingSources {
        MappingSources { mapping_id: "m".into(), fingerprint: "fp".into(), files: files.to_vec() }
    }

    #[test]
    fn no_record_means_full() {
        assert_eq!(plan(None, "fp", &[obj("a", "1")], true), Plan::Full(FullReason::NoRecord));
    }

    #[test]
    fn changed_fingerprint_means_full() {
        let prev = record(&[obj("a", "1")]);
        assert_eq!(plan(Some(&prev), "other", &[obj("a", "1")], false), Plan::Full(FullReason::ConfigChanged));
    }

    #[test]
    fn nothing_new_is_unchanged() {
        let prev = record(&[obj("a", "1")]);
        assert_eq!(plan(Some(&prev), "fp", &[obj("a", "1")], true), Plan::Unchanged);
    }

    #[test]
    fn new_files_after_old_ones_append() {
        let prev = record(&[obj("a", "1"), obj("b", "1")]);
        let listing = [obj("a", "1"), obj("b", "1"), obj("c", "1")];
        assert_eq!(plan(Some(&prev), "fp", &listing, true), Plan::Append(vec![obj("c", "1")]));
    }

    #[test]
    fn a_replaced_file_means_full() {
        let prev = record(&[obj("a", "1")]);
        let listing = [obj("a", "2"), obj("b", "1")];
        assert_eq!(plan(Some(&prev), "fp", &listing, false), Plan::Full(FullReason::Replaced("a".into())));
    }

    #[test]
    fn a_removed_file_means_full() {
        let prev = record(&[obj("a", "1"), obj("b", "1")]);
        assert_eq!(
            plan(Some(&prev), "fp", &[obj("b", "1")], false),
            Plan::Full(FullReason::Removed("a".into()))
        );
    }

    #[test]
    fn an_early_new_file_means_full_only_with_dedup_keys() {
        let prev = record(&[obj("b", "1")]);
        let listing = [obj("a", "1"), obj("b", "1")];
        assert_eq!(plan(Some(&prev), "fp", &listing, true), Plan::Full(FullReason::OutOfOrder("a".into())));
        assert_eq!(plan(Some(&prev), "fp", &listing, false), Plan::Append(vec![obj("a", "1")]));
    }

    #[test]
    fn record_files_merges_and_sorts() {
        let merged = record_files(&[obj("b", "1")], vec![obj("c", "1"), obj("a", "1")]);
        assert_eq!(merged, vec![obj("a", "1"), obj("b", "1"), obj("c", "1")]);
    }

    fn cfg_with(table: AnalyticTable) -> PipelineConfig {
        PipelineConfig {
            version: 1,
            source_containers: vec![],
            dimensions: vec![],
            mappings: vec![mapping()],
            analytic_tables: vec![table],
            layout: Default::default(),
        }
    }

    fn mapping() -> Mapping {
        Mapping {
            id: "m".into(),
            name: String::new(),
            source_container_id: "src".into(),
            analytic_table_id: "t".into(),
            where_: None,
            columns: vec![],
        }
    }

    fn table(partition_keys: &[&str], dedup_keys: &[&str]) -> AnalyticTable {
        AnalyticTable {
            id: "t".into(),
            name: "T".into(),
            schema: vec![],
            partition_keys: partition_keys.iter().map(|s| s.to_string()).collect(),
            dedup_keys: dedup_keys.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn fingerprint_ignores_layout_but_not_the_table() {
        let mut a = cfg_with(table(&["month"], &["id"]));
        let base = fingerprint(&a, &mapping(), &[]);
        a.layout.insert("m".into(), crate::config::LayoutPosition { x: 1.0, y: 2.0 });
        assert_eq!(fingerprint(&a, &mapping(), &[]), base, "moving a node is not a config change");
        a.analytic_tables[0].dedup_keys.push("ts".into());
        assert_ne!(fingerprint(&a, &mapping(), &[]), base);
        let b = cfg_with(table(&["month"], &["id"]));
        assert_ne!(fingerprint(&b, &mapping(), &[obj("dim.csv", "1")]), base, "a dimension file is an input");
    }

    #[test]
    fn decode_reverses_the_partition_encoding() {
        assert_eq!(decode_segment("visa%20gold").unwrap(), "visa gold");
        assert_eq!(decode_segment("2026-09-01").unwrap(), "2026-09-01");
        assert!(decode_segment("bad%2").is_err());
    }

    fn frame() -> DataFrame {
        df![
            "month" => ["2026-09", "2026-09", "2026-10"],
            "id" => [1i64, 2, 3],
            "v" => ["a", "b", "c"],
        ]
        .unwrap()
    }

    /// `df` partitioned and encoded as stored files under `v1/`.
    fn stored_files(df: &DataFrame, t: &AnalyticTable) -> Vec<(String, Vec<u8>)> {
        partition_frames(df, &mapping(), t)
            .unwrap()
            .into_iter()
            .map(|(key, mut f)| {
                let rest = key.split_once('/').unwrap().1;
                (format!("v1/{rest}"), write_parquet_bytes(&mut f).unwrap())
            })
            .collect()
    }

    #[test]
    fn stored_keys_reads_partition_keys_from_the_path() {
        let t = table(&["month"], &["month", "id"]);
        let files = stored_files(&frame(), &t);
        let keys = stored_keys(&files, &t).unwrap();
        assert_eq!(keys.height(), 3);
        let months: Vec<Option<&str>> = keys.column("month").unwrap().str().unwrap().into_iter().collect();
        assert_eq!(months, vec![Some("2026-09"), Some("2026-09"), Some("2026-10")]);
    }

    #[test]
    fn stored_rows_win_over_new_rows_with_the_same_key() {
        let t = table(&["month"], &["id"]);
        let files = stored_files(&frame(), &t);
        let stored = stored_keys(&files, &t).unwrap();
        let new_rows = df![
            "month" => ["2026-10", "2026-10"],
            "id" => [3i64, 4],
            "v" => ["dup", "d"],
        ]
        .unwrap();
        let (kept, dropped) = drop_stored_keys(&new_rows, &stored, &t.dedup_keys).unwrap();
        assert_eq!(dropped, 1);
        let ids: Vec<Option<i64>> = kept.column("id").unwrap().i64().unwrap().into_iter().collect();
        assert_eq!(ids, vec![Some(4)]);
    }

    #[test]
    fn null_keys_match_like_a_full_run_dedup() {
        let t = table(&[], &["id"]);
        let stored = df!["id" => [None::<&str>]].unwrap();
        let new_rows = df!["id" => [None::<i64>, Some(5)], "v" => ["x", "y"]].unwrap();
        let (kept, dropped) = drop_stored_keys(&new_rows, &stored, &t.dedup_keys).unwrap();
        assert_eq!((kept.height(), dropped), (1, 1));
    }

    #[test]
    fn merge_appends_after_stored_rows() {
        let t = table(&["month"], &[]);
        let files = stored_files(&frame(), &t);
        let (_, sept) = files.iter().find(|(k, _)| k.contains("2026-09")).unwrap();
        let new_rows = df!["id" => [9i32], "v" => ["z"]].unwrap();
        let merged = merge_partition(sept, &new_rows).unwrap();
        let ids: Vec<Option<i64>> = merged.column("id").unwrap().i64().unwrap().into_iter().collect();
        assert_eq!(ids, vec![Some(1), Some(2), Some(9)], "new int32 ids are cast to the stored int64");
    }

    #[test]
    fn merge_refuses_a_different_column_set() {
        let t = table(&[], &[]);
        let files = stored_files(&frame(), &t);
        let extra = df!["month" => ["x"], "id" => [1i64], "v" => ["a"], "w" => ["b"]].unwrap();
        assert!(merge_partition(&files[0].1, &extra).is_err());
    }

    /// Storing one batch and appending a second gives the same table as a
    /// full run over both.
    #[test]
    fn appending_equals_a_full_run() {
        let t = table(&["month"], &["id"]);
        let first = df![
            "month" => ["2026-09", "2026-09", "2026-10"],
            "id" => [1i64, 2, 3],
            "v" => ["a", "b", "c"],
        ]
        .unwrap();
        let second = df![
            "month" => ["2026-10", "2026-11", "2026-11", "2026-09"],
            "id" => [3i64, 4, 4, 5],
            "v" => ["c2", "d", "d2", "e"],
        ]
        .unwrap();

        // Full run over both batches.
        let (full, _) = dedup_rows(&first.vstack(&second).unwrap(), &t).unwrap();
        let mut expected: Vec<(String, DataFrame)> = partition_frames(&full, &mapping(), &t).unwrap();
        expected.sort_by(|a, b| a.0.cmp(&b.0));

        // Incremental: store the first batch, then append the second.
        let (first_deduped, _) = dedup_rows(&first, &t).unwrap();
        let stored = stored_files(&first_deduped, &t);
        let (second_deduped, _) = dedup_rows(&second, &t).unwrap();
        let (fresh, dropped) =
            drop_stored_keys(&second_deduped, &stored_keys(&stored, &t).unwrap(), &t.dedup_keys).unwrap();
        assert_eq!(dropped, 1, "id 3 is already stored");
        let mut got: Vec<(String, DataFrame)> = Vec::new();
        let mut written: HashSet<String> = HashSet::new();
        for (key, rows) in partition_frames(&fresh, &mapping(), &t).unwrap() {
            let rest = key.split_once('/').unwrap().1.to_string();
            let merged = match stored.iter().find(|(k, _)| crate::manifest::partition_path(k) == rest) {
                Some((_, bytes)) => merge_partition(bytes, &rows).unwrap(),
                None => rows,
            };
            written.insert(rest);
            got.push((key, merged));
        }
        for (k, bytes) in &stored {
            let rest = crate::manifest::partition_path(k).to_string();
            if !written.contains(&rest) {
                got.push((format!("t/{rest}"), read_parquet(bytes).unwrap()));
            }
        }
        got.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(got.len(), expected.len());
        for ((gk, gdf), (ek, edf)) in got.iter().zip(expected.iter()) {
            assert_eq!(gk, ek);
            assert!(gdf.equals_missing(edf), "{gk}: {gdf:?} vs {edf:?}");
        }
    }
}
