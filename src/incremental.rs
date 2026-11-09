//! Incremental runs: ingest only the source files a mapping has not seen.
//!
//! A full run re-reads every file under a mapping's source prefix. When the
//! only change since the last run is new files, a full run would produce the
//! previous output plus the new files' rows, minus any new row whose dedup key
//! is already stored. An incremental run computes that directly: it ingests the
//! new files, drops rows whose key is already in the table, and rewrites only
//! the partitions the remaining rows fall into.
//!
//! What each mapping's output was built from is stored per table version at
//! `_sources/<version>.json`, next to the manifest: a fingerprint of the
//! config that decides the output, and the key and ETag of every source file
//! read. A mapping falls back to a full run when:
//!
//! - the run asked for one (`clean_run`);
//! - the current version has no source record, or the record does not match
//!   the manifest (a version written before this existed, or a restore made
//!   by the web app);
//! - the fingerprint changed (the mapping, its source, its table, a
//!   dimension, or a dimension file);
//! - a file it already read was removed or replaced;
//! - the table has dedup keys and a new file sorts before a file already read.
//!   Dedup keeps the first row in key order, so appending is only the same as
//!   a full run when new files come after old ones.

use std::collections::HashSet;
use std::io::Cursor;

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{AnalyticTable, Mapping, PipelineConfig};
use crate::manifest::ManifestFile;

/// Change this when a worker change alters what a mapping produces from the
/// same inputs, so the next run rebuilds rather than appending to old output.
const OUTPUT_FORMAT: u32 = 1;

/// One object in a lake listing.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceObject {
    pub key: String,
    pub etag: String,
}

/// What one mapping's files in a table version were built from.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MappingSources {
    pub mapping_id: String,
    pub fingerprint: String,
    /// Every source file read, sorted by key.
    pub files: Vec<SourceObject>,
}

/// The `_sources/<version>.json` record.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct TableSources {
    pub version: u64,
    /// [`files_digest`] of the manifest this record was written with. A record
    /// whose digest does not match the live manifest is ignored.
    pub files_digest: String,
    pub mappings: Vec<MappingSources>,
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Hash of a manifest's file list.
pub fn files_digest(files: &[ManifestFile]) -> String {
    let mut keys: Vec<&str> = files.iter().map(|f| f.key.as_str()).collect();
    keys.sort_unstable();
    sha256_hex(keys.join("\n").as_bytes())
}

/// Hash of everything other than the source files that decides `mapping`'s
/// output. `dimension_files` must be sorted.
pub fn fingerprint(
    cfg: &PipelineConfig,
    mapping: &Mapping,
    dimension_files: &[SourceObject],
) -> String {
    let source = cfg
        .source_containers
        .iter()
        .find(|s| s.id == mapping.source_container_id);
    let table = cfg
        .analytic_tables
        .iter()
        .find(|t| t.id == mapping.analytic_table_id);
    // serde_json's map is sorted by key, so the encoding is stable.
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

/// Why a mapping is running in full.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FullReason {
    Requested,
    NoRecord,
    ConfigChanged,
    Replaced(String),
    Removed(String),
    OutOfOrder(String),
}

impl std::fmt::Display for FullReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FullReason::Requested => write!(f, "a full run was requested"),
            FullReason::NoRecord => write!(f, "no source record for the current version"),
            FullReason::ConfigChanged => write!(f, "the config or a dimension file changed"),
            FullReason::Replaced(k) => write!(f, "{k} was replaced"),
            FullReason::Removed(k) => write!(f, "{k} was removed"),
            FullReason::OutOfOrder(k) => {
                write!(f, "{k} sorts before a file already read and the table has dedup keys")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    Full(FullReason),
    /// No new files and nothing changed.
    Unchanged,
    /// Ingest these files and merge their rows into the existing output.
    Append(Vec<SourceObject>),
}

/// Decide how to run one mapping. `listing` is the source prefix's current
/// contents sorted by key; `order_matters` is whether the table has dedup keys.
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
    let listed: std::collections::HashMap<&str, &str> = listing
        .iter()
        .map(|o| (o.key.as_str(), o.etag.as_str()))
        .collect();
    for old in &previous.files {
        match listed.get(old.key.as_str()) {
            None => return Plan::Full(FullReason::Removed(old.key.clone())),
            Some(etag) if *etag != old.etag => {
                return Plan::Full(FullReason::Replaced(old.key.clone()))
            }
            Some(_) => {}
        }
    }
    let seen: HashSet<&str> = previous.files.iter().map(|o| o.key.as_str()).collect();
    let new: Vec<SourceObject> = listing
        .iter()
        .filter(|o| !seen.contains(o.key.as_str()))
        .cloned()
        .collect();
    if new.is_empty() {
        return Plan::Unchanged;
    }
    if order_matters {
        if let Some(last) = previous.files.iter().map(|o| o.key.as_str()).max() {
            if let Some(early) = new.iter().find(|o| o.key.as_str() < last) {
                return Plan::Full(FullReason::OutOfOrder(early.key.clone()));
            }
        }
    }
    Plan::Append(new)
}

/// Merge newly read files into a mapping's file list, sorted by key.
pub fn record_files(previous: &[SourceObject], read: Vec<SourceObject>) -> Vec<SourceObject> {
    let mut out: Vec<SourceObject> = previous.to_vec();
    out.extend(read);
    out.sort();
    out.dedup_by(|a, b| a.key == b.key);
    out
}

/// Inverse of the partition path encoding: `%XX` back to the byte.
fn decode_segment(value: &str) -> Result<String, String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = value
                .get(i + 1..i + 3)
                .ok_or_else(|| format!("truncated escape in `{value}`"))?;
            out.push(u8::from_str_radix(hex, 16).map_err(|_| format!("bad escape in `{value}`"))?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| format!("`{value}` is not UTF-8 once decoded"))
}

/// The partition values encoded in a file key such as
/// `v3/year=2026/month=9/m.parquet`, as strings.
fn partition_values(key: &str, partition_keys: &[String]) -> Result<Vec<(String, String)>, String> {
    let segments: Vec<&str> = crate::manifest::partition_path(key).split('/').collect();
    let dirs = &segments[..segments.len().saturating_sub(1)];
    partition_keys
        .iter()
        .map(|name| {
            let prefix = format!("{name}=");
            let raw = dirs
                .iter()
                .find_map(|s| s.strip_prefix(prefix.as_str()))
                .ok_or_else(|| format!("{key} has no `{name}` segment"))?;
            Ok((name.clone(), decode_segment(raw)?))
        })
        .collect()
}

pub fn read_parquet(bytes: &[u8]) -> Result<DataFrame, String> {
    ParquetReader::new(Cursor::new(bytes))
        .finish()
        .map_err(|e| format!("read parquet: {e}"))
}

/// The dedup key columns of `df`, each cast to a string. Both sides of the
/// comparison go through the same cast, and partition values come back from
/// the path as strings, so comparing strings needs no type bookkeeping.
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

/// The dedup key tuples stored in one existing Parquet file of the table, as
/// strings. Key columns that are partition keys come from the file's path,
/// since they are not in the payload.
pub fn stored_keys(bytes: &[u8], key: &str, table: &AnalyticTable) -> Result<DataFrame, String> {
    let df = read_parquet(bytes)?;
    let from_path: Vec<(String, String)> = partition_values(key, &table.partition_keys)?
        .into_iter()
        .filter(|(name, _)| table.dedup_keys.contains(name))
        .collect();
    let columns = table
        .dedup_keys
        .iter()
        .map(|k| match from_path.iter().find(|(name, _)| name == k) {
            Some((_, value)) => Ok(Column::new(
                k.as_str().into(),
                vec![value.as_str(); df.height()],
            )),
            None => df
                .column(k)
                .and_then(|c| c.cast(&DataType::String))
                .map_err(|e| format!("{key}: dedup key `{k}`: {e}")),
        })
        .collect::<Result<Vec<Column>, String>>()?;
    DataFrame::new(df.height(), columns).map_err(|e| e.to_string())
}

/// Drop rows of `new_rows` whose dedup key tuple is in `stored` (string
/// columns named by `keys`). `new_rows` must already be deduplicated among
/// itself. Keeps the order of `new_rows`. Returns the kept rows and how many
/// were dropped.
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
    // Stored rows first, so a key present in both keeps the stored copy, the
    // same row a full run keeps since old files sort first.
    let combined = old.vstack(&fresh).map_err(|e| e.to_string())?;
    let unique = combined
        .unique_stable(Some(keys), UniqueKeepStrategy::First, None)
        .map_err(|e| e.to_string())?;
    let kept: Vec<IdxSize> = unique
        .column(ROW)
        .and_then(|c| c.idx().cloned())
        .map_err(|e| e.to_string())?
        .into_iter()
        .flatten()
        .collect();
    let kept = IdxCa::from_vec(ROW.into(), kept);
    let rows = new_rows.take(&kept).map_err(|e| e.to_string())?;
    let dropped = new_rows.height() - rows.height();
    Ok((rows, dropped))
}

/// Append `new_rows` to an existing partition file's rows. New columns are
/// cast to the stored types; a mismatch that cannot be cast is an error, and
/// the caller falls back to a full run.
pub fn merge_partition(existing: &[u8], new_rows: &DataFrame) -> Result<DataFrame, String> {
    let old = read_parquet(existing)?;
    let mut columns = Vec::with_capacity(old.width());
    for field in old.schema().iter_fields() {
        let column = new_rows
            .column(field.name())
            .and_then(|c| c.cast(field.dtype()))
            .map_err(|e| format!("column `{}`: {e}", field.name()))?;
        columns.push(column);
    }
    if new_rows.width() != old.width() {
        return Err(format!(
            "stored partition has {} columns, new rows have {}",
            old.width(),
            new_rows.width()
        ));
    }
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

    /// Partition `df` and return each file's key with a `v1/` prefix and bytes,
    /// as they would be stored.
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

    fn all_stored_keys(files: &[(String, Vec<u8>)], t: &AnalyticTable) -> DataFrame {
        let mut out: Option<DataFrame> = None;
        for (key, bytes) in files {
            let k = stored_keys(bytes, key, t).unwrap();
            out = Some(match out {
                None => k,
                Some(acc) => acc.vstack(&k).unwrap(),
            });
        }
        out.unwrap()
    }

    #[test]
    fn stored_keys_reads_partition_keys_from_the_path() {
        let t = table(&["month"], &["month", "id"]);
        let files = stored_files(&frame(), &t);
        let keys = all_stored_keys(&files, &t);
        assert_eq!(keys.height(), 3);
        let months: Vec<Option<&str>> = keys.column("month").unwrap().str().unwrap().into_iter().collect();
        assert_eq!(months, vec![Some("2026-09"), Some("2026-09"), Some("2026-10")]);
    }

    #[test]
    fn stored_rows_win_over_new_rows_with_the_same_key() {
        let t = table(&["month"], &["id"]);
        let files = stored_files(&frame(), &t);
        let stored = all_stored_keys(&files, &t);
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
        // unique_stable treats nulls as equal, so a full run keeps one row per
        // null key. The comparison against stored rows must agree.
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

    /// The property incremental runs rest on: for files appended in key
    /// order, storing the first batch and then merging the second gives the
    /// same table as one full run over both.
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
            drop_stored_keys(&second_deduped, &all_stored_keys(&stored, &t), &t.dedup_keys).unwrap();
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
