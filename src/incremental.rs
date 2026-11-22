//! Incremental runs: ingest only the source files a mapping has not read.
//!
//! Each mapping has a record, stored with the table (see
//! [`crate::lake::read_records`]): a fingerprint of the config that decides
//! its output and the key and ETag of every file read. With a matching record
//! and only new files, a run ingests the new files, drops rows whose dedup key
//! is already stored, and adds the rest. That equals a full run as long as new
//! files sort after old ones, since dedup keeps the first row in key order.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{Mapping, PipelineConfig};

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

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|b| format!("{b:02x}")).collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AnalyticTable;

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
}
