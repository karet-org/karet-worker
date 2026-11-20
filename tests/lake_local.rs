//! The DuckLake writes against a local catalog: a DuckDB catalog file and
//! data files in a temporary folder. Needs network access the first time, to
//! install the ducklake extension.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use duckdb::Connection;
use karet_worker::config::AnalyticTable;
use karet_worker::incremental::{MappingSources, SourceObject};
use karet_worker::lake::{self, Commit, Lake, LakeCatalog, RestoreError, Write};

struct Fixture {
    dir: tempfile::TempDir,
    conn: Connection,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let lake = Lake::new(LakeCatalog::Local(dir.path().join("lake")));
        std::fs::create_dir_all(dir.path().join("lake")).unwrap();
        let conn = lake.connection().unwrap();
        Fixture { dir, conn }
    }

    /// Stage `select` (a SELECT producing the table's columns) as a Parquet file.
    fn stage(&self, name: &str, select: &str) -> PathBuf {
        let path = self.dir.path().join(format!("{name}.parquet"));
        self.conn
            .execute_batch(&format!("COPY ({select}) TO '{}' (FORMAT parquet)", path.display()))
            .unwrap();
        path
    }

    fn rows(&self, sql: &str) -> Vec<String> {
        let mut stmt = self.conn.prepare(sql).unwrap();
        let mut out: Vec<String> = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap().map(Result::unwrap).collect();
        out.sort();
        out
    }

    fn table_rows(&self) -> Vec<String> {
        self.rows("SELECT concat_ws('|', _karet_mapping, k, coalesce(v, 'NULL')) FROM lake.\"p-1\".\"t\"")
    }

    fn commit(&self, writes: &[Write], records: &HashMap<String, MappingSources>, clean: bool) -> Result<i64, String> {
        lake::commit_table(
            &self.conn,
            &Commit { pipeline: "p-1", table: &table(), writes, records, clean, job_id: Some("job-1") },
        )
    }
}

fn table() -> AnalyticTable {
    AnalyticTable {
        id: "t".into(),
        name: "t".into(),
        schema: vec![],
        partition_keys: vec!["k".into()],
        dedup_keys: vec!["k".into()],
    }
}

fn write<'a>(mapping_id: &'a str, file: &'a Path, replace: bool) -> Write<'a> {
    Write { mapping_id, file, replace }
}

fn record(mapping_id: &str, keys: &[&str]) -> HashMap<String, MappingSources> {
    let files = keys.iter().map(|k| SourceObject { key: k.to_string(), etag: "e".into() }).collect();
    HashMap::from([(
        mapping_id.to_string(),
        MappingSources { mapping_id: mapping_id.into(), fingerprint: "fp".into(), files },
    )])
}

const AB: &str = "SELECT * FROM (VALUES ('a', 'first'), ('b', 'first')) t(k, v)";

#[test]
fn append_drops_stored_keys_and_equals_a_full_run() {
    let f = Fixture::new();
    let first = f.stage("first", AB);
    f.commit(&[write("m", &first, true)], &record("m", &["one.csv"]), false).unwrap();

    let new = f.stage("new", "SELECT * FROM (VALUES ('b', 'second'), ('c', 'second')) t(k, v)");
    let kept = f.dir.path().join("kept.parquet");
    let (dropped, partitions) = lake::drop_stored_keys(&f.conn, "p-1", &table(), "m", &new, &kept).unwrap();
    assert_eq!((dropped, partitions), (1, 1));
    f.commit(&[write("m", &kept, false)], &record("m", &["one.csv", "two.csv"]), false).unwrap();

    // A full run keeps the first row per key, in file order.
    assert_eq!(f.table_rows(), vec!["m|a|first", "m|b|first", "m|c|second"]);
    let records = lake::read_records(&f.conn, "p-1", "t").unwrap();
    assert_eq!(records["m"].files.len(), 2);
}

#[test]
fn null_keys_match_each_other() {
    let f = Fixture::new();
    let first = f.stage("first", "SELECT NULL::VARCHAR AS k, 'first' AS v");
    f.commit(&[write("m", &first, true)], &HashMap::new(), false).unwrap();
    let new = f.stage("new", "SELECT NULL::VARCHAR AS k, 'second' AS v");
    let kept = f.dir.path().join("kept.parquet");
    let (dropped, _) = lake::drop_stored_keys(&f.conn, "p-1", &table(), "m", &new, &kept).unwrap();
    assert_eq!(dropped, 1);
}

#[test]
fn stored_keys_of_another_mapping_do_not_count() {
    let f = Fixture::new();
    let first = f.stage("first", AB);
    f.commit(&[write("m1", &first, true)], &HashMap::new(), false).unwrap();
    let new = f.stage("new", AB);
    let kept = f.dir.path().join("kept.parquet");
    let (dropped, _) = lake::drop_stored_keys(&f.conn, "p-1", &table(), "m2", &new, &kept).unwrap();
    assert_eq!(dropped, 0);
}

#[test]
fn replacing_a_mapping_leaves_the_others_and_drops_whole_files() {
    let f = Fixture::new();
    let a = f.stage("a", AB);
    let b = f.stage("b", "SELECT 'z' AS k, 'other' AS v");
    f.commit(&[write("m1", &a, true), write("m2", &b, true)], &HashMap::new(), false).unwrap();
    let a2 = f.stage("a2", "SELECT 'a' AS k, 'again' AS v");
    f.commit(&[write("m1", &a2, true)], &HashMap::new(), false).unwrap();

    assert_eq!(f.table_rows(), vec!["m1|a|again", "m2|z|other"]);
    let delete_files = f.rows(
        "SELECT count(delete_file)::VARCHAR FROM ducklake_list_files('lake', 't', schema => 'p-1')",
    );
    assert_eq!(delete_files, vec!["0"]);
}

#[test]
fn a_clean_run_removes_rows_of_mappings_that_did_not_write() {
    let f = Fixture::new();
    let a = f.stage("a", AB);
    let b = f.stage("b", "SELECT 'z' AS k, 'other' AS v");
    f.commit(&[write("m1", &a, true), write("m2", &b, true)], &HashMap::new(), false).unwrap();
    f.commit(&[write("m1", &a, true)], &HashMap::new(), true).unwrap();
    assert_eq!(f.table_rows(), vec!["m1|a|first", "m1|b|first"]);
}

#[test]
fn a_clean_run_recreates_the_table_when_its_columns_change() {
    let f = Fixture::new();
    let a = f.stage("a", AB);
    f.commit(&[write("m", &a, true)], &HashMap::new(), false).unwrap();
    let b = f.stage("b", "SELECT 'a' AS k, 1::BIGINT AS v");
    let err = f.commit(&[write("m", &b, true)], &HashMap::new(), false).unwrap_err();
    assert!(err.contains("stored as VARCHAR"), "{err}");
    f.commit(&[write("m", &b, true)], &HashMap::new(), true).unwrap();
    let cols = lake::table_columns(&f.conn, "p-1", "t").unwrap().unwrap();
    assert!(cols.contains(&("v".to_string(), "BIGINT".to_string())), "{cols:?}");
}

#[test]
fn a_failed_commit_changes_neither_rows_nor_records() {
    let f = Fixture::new();
    let a = f.stage("a", AB);
    f.commit(&[write("m", &a, true)], &record("m", &["one.csv"]), false).unwrap();
    let bad = f.stage("bad", "SELECT 'a' AS k, 1::BIGINT AS v");
    assert!(f.commit(&[write("m", &bad, true)], &record("m", &["two.csv"]), false).is_err());
    assert_eq!(f.table_rows(), vec!["m|a|first", "m|b|first"]);
    assert_eq!(lake::read_records(&f.conn, "p-1", "t").unwrap()["m"].files[0].key, "one.csv");
}

#[test]
fn restore_brings_back_an_earlier_version_and_clears_the_records() {
    let f = Fixture::new();
    let a = f.stage("a", AB);
    let v1 = f.commit(&[write("m", &a, true)], &record("m", &["one.csv"]), false).unwrap();
    let c = f.stage("c", "SELECT 'c' AS k, 'later' AS v");
    f.commit(&[write("m", &c, true)], &record("m", &["two.csv"]), false).unwrap();
    assert_eq!(f.table_rows(), vec!["m|c|later"]);

    let versions = lake::table_versions(&f.conn, "p-1", "t").unwrap();
    assert!(versions.contains(&v1), "{versions:?}");
    let restored = lake::restore(&f.conn, "p-1", "t", v1).unwrap();
    assert!(restored > v1);
    assert_eq!(f.table_rows(), vec!["m|a|first", "m|b|first"]);
    assert!(lake::read_records(&f.conn, "p-1", "t").unwrap().is_empty());

    assert!(matches!(lake::restore(&f.conn, "p-1", "t", 999_999), Err(RestoreError::NotFound(999_999))));
}

#[test]
fn restore_works_when_the_older_version_shares_files_with_the_live_one() {
    // An append keeps the earlier files live, so the restore's own delete
    // must not hide the rows it is about to copy back.
    let f = Fixture::new();
    let a = f.stage("a", AB);
    let v1 = f.commit(&[write("m", &a, true)], &HashMap::new(), false).unwrap();
    let c = f.stage("c", "SELECT 'c' AS k, 'later' AS v");
    f.commit(&[write("m", &c, false)], &HashMap::new(), false).unwrap();
    lake::restore(&f.conn, "p-1", "t", v1).unwrap();
    assert_eq!(f.table_rows(), vec!["m|a|first", "m|b|first"]);
}

#[test]
fn maintenance_keeps_the_current_rows() {
    let f = Fixture::new();
    for i in 0..3 {
        let file = f.stage(&format!("r{i}"), &format!("SELECT 'k{i}' AS k, 'v' AS v"));
        f.commit(&[write("m", &file, false)], &HashMap::new(), false).unwrap();
    }
    lake::maintain(&f.conn, "p-1", "t").unwrap();
    assert_eq!(f.table_rows(), vec!["m|k0|v", "m|k1|v", "m|k2|v"]);
}
