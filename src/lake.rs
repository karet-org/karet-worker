//! Analytic tables in DuckLake: the catalog lives in Postgres, the Parquet in
//! the warehouse bucket.
//!
//! Each pipeline is a DuckLake schema named after its slug, and each analytic
//! table a DuckLake table named after its id. A table has one extra column,
//! [`MAPPING_COLUMN`], naming the mapping that wrote the row, so one mapping's
//! rows can be replaced without touching another's. It is the first partition
//! key, which keeps every Parquet file to one mapping, so replacing a mapping
//! drops whole files instead of writing delete files.
//!
//! Each pipeline schema also holds [`RECORDS_TABLE`], the incremental-run
//! records of its tables. It is per pipeline because only runs of the same
//! pipeline share a run lock: two pipelines writing one shared table would
//! conflict and one commit would be lost.
//!
//! A run commits one DuckLake transaction per table. A table version shown in
//! the web app is the id of a snapshot that changed the table's rows.
//!
//! DuckDB's API is synchronous: call these from a blocking task.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Mutex;

use duckdb::Connection;

use crate::config::AnalyticTable;
use crate::incremental::MappingSources;

pub const CATALOG: &str = "lake";
pub const MAPPING_COLUMN: &str = "_karet_mapping";
/// Table in each pipeline schema holding the incremental-run records, see
/// [`read_records`]. The prefix keeps it apart from analytic table ids.
pub const RECORDS_TABLE: &str = "_karet_sources";
const METADATA: &str = "\"__ducklake_metadata_lake\".\"ducklake\"";
/// Data-changing snapshots kept per table, for time travel and restore.
pub const RETAINED_VERSIONS: usize = 10;
/// Files at or above this size are left alone by the merge after each run.
/// It must stay well below the size of a table's main file: at 8 MB, a 7 MB
/// partition was rewritten by every 10-minute append. Small appends still
/// merge with each other until their file reaches this size.
const MERGE_MAX_FILE_SIZE: u64 = 1024 * 1024;
/// Inserts of 10 rows or fewer are stored in the catalog rather than in a
/// Parquet file. Once a table has this many such rows, maintenance writes
/// them out, so they do not pile up in Postgres.
pub const FLUSH_INLINED_ROWS: usize = 1000;

/// What the worker's DuckDB may use, beside Polars in the same process.
#[derive(Debug, Clone)]
pub struct Resources {
    pub memory_limit: String,
    pub threads: usize,
    /// Where extensions are installed. The image sets this to the copies it
    /// ships, so a job never downloads an extension.
    pub extension_directory: Option<String>,
}

impl Default for Resources {
    fn default() -> Self {
        Self { memory_limit: "1GB".into(), threads: 2, extension_directory: None }
    }
}

impl Resources {
    /// `DUCKDB_MEMORY_LIMIT`, `DUCKDB_THREADS` and `DUCKDB_EXTENSION_DIRECTORY`.
    pub fn from_env() -> Self {
        let default = Self::default();
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        Self {
            memory_limit: var("DUCKDB_MEMORY_LIMIT").unwrap_or(default.memory_limit),
            threads: var("DUCKDB_THREADS").and_then(|v| v.parse().ok()).filter(|n| *n > 0).unwrap_or(default.threads),
            extension_directory: var("DUCKDB_EXTENSION_DIRECTORY"),
        }
    }

    fn setup_sql(&self) -> Vec<String> {
        let mut out = vec![
            format!("SET memory_limit = {}", lit(&self.memory_limit)),
            format!("SET threads = {}", self.threads),
        ];
        if let Some(dir) = &self.extension_directory {
            out.push(format!("SET extension_directory = {}", lit(dir)));
        }
        out
    }
}
/// Keys of a snapshot's `changes` map that mean a table's rows changed.
const DATA_CHANGES: &[&str] = &["tables_inserted_into", "tables_deleted_from", "inlined_insert", "inlined_delete"];

/// Connection settings, from the worker's environment.
#[derive(Debug, Clone)]
pub struct LakeSettings {
    pub pg_host: String,
    pub pg_port: u16,
    pub pg_user: String,
    pub pg_password: String,
    pub pg_database: String,
    pub warehouse_bucket: String,
    pub s3_endpoint: String,
    pub s3_use_ssl: bool,
    pub s3_key_id: String,
    pub s3_secret: String,
    pub s3_region: String,
}

impl LakeSettings {
    /// `database_url` is the control-plane URL; the lake catalog is the
    /// `pg_database` database on the same server, with the same credentials.
    pub fn from_urls(
        database_url: &str,
        pg_database: &str,
        endpoint_url: &str,
        warehouse_bucket: &str,
        s3_key_id: &str,
        s3_secret: &str,
        s3_region: &str,
    ) -> Result<Self, String> {
        let db = url::Url::parse(database_url).map_err(|e| format!("DATABASE_URL: {e}"))?;
        let decode = |s: &str| {
            percent_decode(s).map_err(|e| format!("DATABASE_URL: {e}"))
        };
        let endpoint = url::Url::parse(endpoint_url).map_err(|e| format!("AWS_ENDPOINT_URL: {e}"))?;
        let host = endpoint.host_str().ok_or("AWS_ENDPOINT_URL has no host")?;
        let s3_endpoint = match endpoint.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        };
        Ok(Self {
            pg_host: db.host_str().ok_or("DATABASE_URL has no host")?.to_string(),
            pg_port: db.port().unwrap_or(5432),
            pg_user: decode(db.username())?,
            pg_password: decode(db.password().unwrap_or(""))?,
            pg_database: pg_database.to_string(),
            warehouse_bucket: warehouse_bucket.to_string(),
            s3_endpoint,
            s3_use_ssl: endpoint.scheme() == "https",
            s3_key_id: s3_key_id.to_string(),
            s3_secret: s3_secret.to_string(),
            s3_region: s3_region.to_string(),
        })
    }

    /// The statements that load the extensions and attach the catalog.
    fn setup_sql(&self) -> Vec<String> {
        vec![
            "INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres; INSTALL httpfs; LOAD httpfs".into(),
            "SET allow_persistent_secrets = false".into(),
            format!(
                "CREATE SECRET karet_s3 (TYPE s3, KEY_ID {}, SECRET {}, REGION {}, ENDPOINT {}, URL_STYLE 'path', USE_SSL {})",
                lit(&self.s3_key_id),
                lit(&self.s3_secret),
                lit(&self.s3_region),
                lit(&self.s3_endpoint),
                self.s3_use_ssl,
            ),
            format!(
                "CREATE SECRET karet_lake_pg (TYPE postgres, HOST {}, PORT {}, DATABASE {}, USER {}, PASSWORD {})",
                lit(&self.pg_host),
                self.pg_port,
                lit(&self.pg_database),
                lit(&self.pg_user),
                lit(&self.pg_password),
            ),
            format!(
                "ATTACH 'ducklake:postgres:' AS {CATALOG} (DATA_PATH {}, METADATA_SCHEMA 'ducklake', META_SECRET 'karet_lake_pg')",
                lit(&format!("s3://{}/lake/", self.warehouse_bucket)),
            ),
        ]
    }
}

/// Where the catalog and the data live.
#[derive(Debug, Clone)]
pub enum LakeCatalog {
    Postgres(LakeSettings),
    /// A DuckDB catalog file and data files in a local folder, for tests.
    Local(std::path::PathBuf),
}

impl LakeCatalog {
    fn setup_sql(&self) -> Vec<String> {
        match self {
            LakeCatalog::Postgres(settings) => settings.setup_sql(),
            LakeCatalog::Local(dir) => {
                vec![
                    "INSTALL ducklake; LOAD ducklake".to_string(),
                    format!(
                        "ATTACH {} AS {CATALOG} (DATA_PATH {}, METADATA_SCHEMA 'ducklake')",
                        lit(&format!("ducklake:{}", dir.join("catalog.ducklake").display())),
                        lit(&format!("{}/", dir.join("data").display())),
                    ),
                ]
            }
        }
    }
}

fn percent_decode(s: &str) -> Result<String, String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = s.get(i + 1..i + 3).ok_or("truncated percent escape")?;
            out.push(u8::from_str_radix(hex, 16).map_err(|_| "bad percent escape")?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| "not UTF-8 once decoded".into())
}

/// One DuckDB database with the catalog attached, opened on first use so a
/// worker that never runs a job never connects. Jobs take their own
/// connection from it.
pub struct Lake {
    catalog: LakeCatalog,
    resources: Resources,
    base: Mutex<Option<Connection>>,
}

impl Lake {
    pub fn new(catalog: LakeCatalog) -> Self {
        Self::with_resources(catalog, Resources::default())
    }

    pub fn with_resources(catalog: LakeCatalog, resources: Resources) -> Self {
        Self { catalog, resources, base: Mutex::new(None) }
    }

    pub fn connection(&self) -> Result<Connection, String> {
        let mut base = self.base.lock().map_err(|_| "lake connection poisoned".to_string())?;
        if base.is_none() {
            let conn = Connection::open_in_memory().map_err(|e| format!("open DuckDB: {e}"))?;
            for stmt in self.resources.setup_sql().into_iter().chain(self.catalog.setup_sql()) {
                conn.execute_batch(&stmt).map_err(|e| format!("lake setup: {e}"))?;
            }
            *base = Some(conn);
        }
        base.as_ref().expect("set above").try_clone().map_err(|e| format!("lake connection: {e}"))
    }
}

/// A double-quoted identifier.
pub fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A single-quoted string literal.
pub fn lit(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn table_ref(pipeline: &str, table_id: &str) -> String {
    format!("{CATALOG}.{}.{}", ident(pipeline), ident(table_id))
}

fn records_table(pipeline: &str) -> String {
    table_ref(pipeline, RECORDS_TABLE)
}

fn file_ref(path: &Path) -> String {
    format!("read_parquet({})", lit(&path.to_string_lossy()))
}

fn err(context: &str) -> impl Fn(duckdb::Error) -> String + '_ {
    move |e| format!("{context}: {e}")
}

/// Column names and DuckDB types of the table, or `None` when it does not exist.
pub fn table_columns(conn: &Connection, pipeline: &str, table_id: &str) -> Result<Option<Vec<(String, String)>>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT column_name, data_type FROM duckdb_columns() \
             WHERE database_name = ? AND schema_name = ? AND table_name = ? ORDER BY column_index",
        )
        .map_err(err("list columns"))?;
    let cols = stmt
        .query_map([CATALOG, pipeline, table_id], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
        .map_err(err("list columns"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err("list columns"))?;
    Ok(if cols.is_empty() { None } else { Some(cols) })
}

fn file_columns(conn: &Connection, path: &Path) -> Result<Vec<(String, String)>, String> {
    let mut stmt = conn
        .prepare(&format!("DESCRIBE SELECT * FROM {}", file_ref(path)))
        .map_err(err("describe staged rows"))?;
    let cols = stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
        .map_err(err("describe staged rows"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err("describe staged rows"))?;
    Ok(cols)
}

fn count(conn: &Connection, sql: &str) -> Result<usize, String> {
    conn.query_row(sql, [], |r| r.get::<_, i64>(0))
        .map(|n| n as usize)
        .map_err(err("count rows"))
}

/// The incremental-run record of each mapping of the table, by mapping id.
/// Written in the same transaction as the table's rows, so a record always
/// describes the rows that are stored.
pub fn read_records(conn: &Connection, pipeline: &str, table_id: &str) -> Result<HashMap<String, MappingSources>, String> {
    if table_columns(conn, pipeline, RECORDS_TABLE)?.is_none() {
        return Ok(HashMap::new());
    }
    let mut stmt = conn
        .prepare(&format!(
            "SELECT mapping_id, fingerprint, files FROM {} WHERE table_id = ?",
            records_table(pipeline)
        ))
        .map_err(err("read source records"))?;
    let rows = stmt
        .query_map([table_id], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))
        })
        .map_err(err("read source records"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err("read source records"))?;
    let mut out = HashMap::new();
    for (mapping_id, fingerprint, files) in rows {
        let files = serde_json::from_str(&files).map_err(|e| format!("source record {mapping_id}: {e}"))?;
        out.insert(mapping_id.clone(), MappingSources { mapping_id, fingerprint, files });
    }
    Ok(out)
}

/// Copy the rows of `staged` whose dedup key is not already stored for
/// `mapping_id` into `out`. Returns the number of rows dropped and the number
/// of partitions the kept rows fall into. Keys are compared as strings and
/// null keys match each other, as in a full run.
pub fn drop_stored_keys(
    conn: &Connection,
    pipeline: &str,
    table: &AnalyticTable,
    mapping_id: &str,
    staged: &Path,
    out: &Path,
) -> Result<(usize, usize), String> {
    let total = count(conn, &format!("SELECT count(*) FROM {}", file_ref(staged)))?;
    let stored_cols = table_columns(conn, pipeline, &table.id)?;
    let has_keys = !table.dedup_keys.is_empty()
        && stored_cols.as_ref().is_some_and(|cols| table.dedup_keys.iter().all(|k| cols.iter().any(|(c, _)| c == k)));
    let filter = if has_keys {
        let conds = table
            .dedup_keys
            .iter()
            .map(|k| format!("CAST(s.{0} AS VARCHAR) IS NOT DISTINCT FROM CAST(n.{0} AS VARCHAR)", ident(k)))
            .collect::<Vec<_>>()
            .join(" AND ");
        format!(
            " WHERE NOT EXISTS (SELECT 1 FROM {} s WHERE s.{} = {} AND {conds})",
            table_ref(pipeline, &table.id),
            ident(MAPPING_COLUMN),
            lit(mapping_id),
        )
    } else {
        String::new()
    };
    conn.execute_batch(&format!(
        "COPY (SELECT n.* FROM {} n{filter}) TO {} (FORMAT parquet)",
        file_ref(staged),
        lit(&out.to_string_lossy()),
    ))
    .map_err(err("drop stored keys"))?;
    let kept = count(conn, &format!("SELECT count(*) FROM {}", file_ref(out)))?;
    let partitions = if kept == 0 {
        0
    } else if table.partition_keys.is_empty() {
        1
    } else {
        let keys = table.partition_keys.iter().map(|k| ident(k)).collect::<Vec<_>>().join(", ");
        count(conn, &format!("SELECT count(*) FROM (SELECT DISTINCT {keys} FROM {})", file_ref(out)))?
    };
    Ok((total - kept, partitions))
}

/// One mapping's staged rows for a table.
#[derive(Debug)]
pub struct Write<'a> {
    pub mapping_id: &'a str,
    pub file: &'a Path,
    /// Replace the mapping's stored rows, or add to them.
    pub replace: bool,
}

/// What [`commit_table`] needs to know besides the rows.
pub struct Commit<'a> {
    pub pipeline: &'a str,
    pub table: &'a AnalyticTable,
    pub writes: &'a [Write<'a>],
    /// Every mapping's record after this run; replaces the stored ones.
    pub records: &'a HashMap<String, MappingSources>,
    /// Remove every stored row first, including rows of mappings that no
    /// longer exist. Also lets the table be recreated when its columns change.
    pub clean: bool,
    pub job_id: Option<&'a str>,
}

/// Write one table's rows and records in one transaction. Returns the
/// snapshot id that holds them.
pub fn commit_table(conn: &Connection, commit: &Commit) -> Result<i64, String> {
    let wanted = wanted_columns(conn, commit.writes)?;
    conn.execute_batch(&format!(
        "CREATE SCHEMA IF NOT EXISTS {CATALOG}.{}; \
         CREATE TABLE IF NOT EXISTS {} (table_id VARCHAR, mapping_id VARCHAR, fingerprint VARCHAR, files VARCHAR)",
        ident(commit.pipeline),
        records_table(commit.pipeline),
    ))
    .map_err(err("create schema"))?;
    let existing = table_columns(conn, commit.pipeline, &commit.table.id)?;
    conn.execute_batch("BEGIN TRANSACTION").map_err(err("begin"))?;
    match write_table(conn, commit, &wanted, existing) {
        Ok(()) => {
            conn.execute_batch("COMMIT").map_err(err("commit"))?;
            current_snapshot(conn)
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

/// Union of the staged files' columns, in first-seen order.
fn wanted_columns(conn: &Connection, writes: &[Write]) -> Result<Vec<(String, String)>, String> {
    let mut out: Vec<(String, String)> = Vec::new();
    for w in writes {
        for (name, ty) in file_columns(conn, w.file)? {
            match out.iter().find(|(n, _)| *n == name) {
                Some((_, t)) if *t != ty => {
                    return Err(format!("column `{name}` is {t} in one mapping and {ty} in another"));
                }
                Some(_) => {}
                None => out.push((name, ty)),
            }
        }
    }
    Ok(out)
}

fn same_columns(a: &[(String, String)], b: &[(String, String)]) -> bool {
    let mut a = a.to_vec();
    let mut b = b.to_vec();
    a.sort();
    b.sort();
    a == b
}

fn write_table(
    conn: &Connection,
    commit: &Commit,
    wanted: &[(String, String)],
    existing: Option<Vec<(String, String)>>,
) -> Result<(), String> {
    let table = table_ref(commit.pipeline, &commit.table.id);
    let mut wanted_all: Vec<(String, String)> = wanted.to_vec();
    wanted_all.push((MAPPING_COLUMN.to_string(), "VARCHAR".to_string()));
    let exec = |sql: String| conn.execute_batch(&sql).map_err(|e| format!("{sql}: {e}"));

    let create = match &existing {
        None => true,
        Some(cols) if commit.clean && !wanted.is_empty() && !same_columns(cols, &wanted_all) => {
            exec(format!("DROP TABLE {table}"))?;
            true
        }
        Some(cols) => {
            for (name, ty) in wanted {
                match cols.iter().find(|(n, _)| n == name) {
                    None => exec(format!("ALTER TABLE {table} ADD COLUMN {} {ty}", ident(name)))?,
                    Some((_, t)) if t != ty => {
                        return Err(format!(
                            "column `{name}` is stored as {t} but the new rows are {ty}; run the pipeline in full"
                        ))
                    }
                    Some(_) => {}
                }
            }
            false
        }
    };
    if create {
        if wanted.is_empty() {
            return Err("no rows to create the table from".into());
        }
        let cols = wanted_all.iter().map(|(n, t)| format!("{} {t}", ident(n))).collect::<Vec<_>>().join(", ");
        exec(format!("CREATE TABLE {table} ({cols})"))?;
    }
    if create || commit.clean {
        let keys: Vec<String> = std::iter::once(MAPPING_COLUMN.to_string())
            .chain(commit.table.partition_keys.iter().cloned())
            .filter(|k| wanted_all.iter().any(|(n, _)| n == k))
            .map(|k| ident(&k))
            .collect();
        exec(format!("ALTER TABLE {table} SET PARTITIONED BY ({})", keys.join(", ")))?;
    }
    if commit.clean && !create {
        exec(format!("DELETE FROM {table}"))?;
    }

    for w in commit.writes {
        if w.replace && !commit.clean {
            exec(format!("DELETE FROM {table} WHERE {} = {}", ident(MAPPING_COLUMN), lit(w.mapping_id)))?;
        }
        exec(format!(
            "INSERT INTO {table} BY NAME SELECT *, {} AS {} FROM {}",
            lit(w.mapping_id),
            ident(MAPPING_COLUMN),
            file_ref(w.file),
        ))?;
    }

    exec(format!(
        "DELETE FROM {} WHERE table_id = {}",
        records_table(commit.pipeline),
        lit(&commit.table.id),
    ))?;
    let mut records: Vec<&MappingSources> = commit.records.values().collect();
    records.sort_by(|a, b| a.mapping_id.cmp(&b.mapping_id));
    for r in records {
        let files = serde_json::to_string(&r.files).map_err(|e| format!("serialize source record: {e}"))?;
        exec(format!(
            "INSERT INTO {} VALUES ({}, {}, {}, {})",
            records_table(commit.pipeline),
            lit(&commit.table.id),
            lit(&r.mapping_id),
            lit(&r.fingerprint),
            lit(&files),
        ))?;
    }
    let message = match commit.job_id {
        Some(id) => format!("job {id}"),
        None => "run".to_string(),
    };
    exec(format!("CALL {CATALOG}.set_commit_message('karet-worker', {})", lit(&message)))
}

fn current_snapshot(conn: &Connection) -> Result<i64, String> {
    conn.query_row(&format!("SELECT id FROM {CATALOG}.current_snapshot()"), [], |r| r.get(0))
        .map_err(err("current snapshot"))
}

/// The DuckLake id of a table, or `None` when it does not exist.
fn table_id_in_catalog(conn: &Connection, pipeline: &str, table_id: &str) -> Result<Option<i64>, String> {
    let sql = format!(
        "SELECT t.table_id FROM {METADATA}.ducklake_table t JOIN {METADATA}.ducklake_schema s USING (schema_id) \
         WHERE s.schema_name = ? AND t.table_name = ? AND t.end_snapshot IS NULL AND s.end_snapshot IS NULL"
    );
    let mut stmt = conn.prepare(&sql).map_err(err("find table"))?;
    let mut rows = stmt.query([pipeline, table_id]).map_err(err("find table"))?;
    match rows.next().map_err(err("find table"))? {
        Some(r) => Ok(Some(r.get(0).map_err(err("find table"))?)),
        None => Ok(None),
    }
}

/// Every snapshot id with the ids of the tables whose rows it changed.
fn data_snapshots(conn: &Connection) -> Result<Vec<(i64, BTreeSet<i64>)>, String> {
    let keys = DATA_CHANGES.iter().map(|k| lit(k)).collect::<Vec<_>>().join(", ");
    let sql = format!(
        "SELECT snapshot_id, list_distinct(flatten(list_transform([{keys}], k -> coalesce(changes[k], [])))) \
         FROM {CATALOG}.snapshots() ORDER BY snapshot_id"
    );
    let mut stmt = conn.prepare(&sql).map_err(err("list snapshots"))?;
    let rows = stmt
        .query_map([], |r| {
            let id: i64 = r.get(0)?;
            let tables: duckdb::types::Value = r.get(1)?;
            Ok((id, tables))
        })
        .map_err(err("list snapshots"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err("list snapshots"))?;
    Ok(rows
        .into_iter()
        .map(|(id, value)| {
            let tables = match value {
                duckdb::types::Value::List(items) => items
                    .into_iter()
                    .filter_map(|v| match v {
                        duckdb::types::Value::Text(s) => s.parse().ok(),
                        _ => None,
                    })
                    .collect(),
                _ => BTreeSet::new(),
            };
            (id, tables)
        })
        .collect())
}

/// The snapshots that changed this table's rows, oldest first.
pub fn table_versions(conn: &Connection, pipeline: &str, table_id: &str) -> Result<Vec<i64>, String> {
    let Some(id) = table_id_in_catalog(conn, pipeline, table_id)? else {
        return Ok(Vec::new());
    };
    Ok(data_snapshots(conn)?.into_iter().filter(|(_, t)| t.contains(&id)).map(|(s, _)| s).collect())
}

/// DuckLake ids of the tables that exist now.
fn live_table_ids(conn: &Connection) -> Result<BTreeSet<i64>, String> {
    let mut stmt = conn
        .prepare(&format!("SELECT table_id FROM {METADATA}.ducklake_table WHERE end_snapshot IS NULL"))
        .map_err(err("list tables"))?;
    let ids = stmt
        .query_map([], |r| r.get::<_, i64>(0))
        .map_err(err("list tables"))?
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(err("list tables"))?;
    Ok(ids)
}

/// Snapshots to expire: all but the latest, and the last `keep` that changed
/// the rows of each table in `live`. A dropped or recreated table keeps no
/// snapshots, so its files can be deleted.
pub fn snapshots_to_expire(snapshots: &[(i64, BTreeSet<i64>)], live: &BTreeSet<i64>, keep: usize) -> Vec<i64> {
    let mut kept: BTreeSet<i64> = BTreeSet::new();
    if let Some((latest, _)) = snapshots.iter().max_by_key(|(id, _)| *id) {
        kept.insert(*latest);
    }
    let mut per_table: HashMap<i64, Vec<i64>> = HashMap::new();
    for (id, tables) in snapshots {
        for t in tables.intersection(live) {
            per_table.entry(*t).or_default().push(*id);
        }
    }
    for mut ids in per_table.into_values() {
        ids.sort_unstable();
        kept.extend(ids.iter().rev().take(keep));
    }
    snapshots.iter().map(|(id, _)| *id).filter(|id| !kept.contains(id)).collect()
}

/// Rows of the table stored in the catalog that no later snapshot deleted.
fn inlined_rows(conn: &Connection, pipeline: &str, table_id: &str) -> Result<usize, String> {
    let Some(id) = table_id_in_catalog(conn, pipeline, table_id)? else {
        return Ok(0);
    };
    let mut stmt = conn
        .prepare(&format!("SELECT table_name FROM {METADATA}.ducklake_inlined_data_tables WHERE table_id = ?"))
        .map_err(err("list inlined tables"))?;
    let names = stmt
        .query_map([id], |r| r.get::<_, String>(0))
        .map_err(err("list inlined tables"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err("list inlined tables"))?;
    let mut total = 0;
    for name in names {
        total += count(conn, &format!("SELECT count(*) FROM {METADATA}.{} WHERE end_snapshot IS NULL", ident(&name)))?;
    }
    Ok(total)
}

/// Write the table's catalog-stored rows to Parquet once there are at least
/// `min_rows`. Returns the rows written.
pub fn flush_inlined(conn: &Connection, pipeline: &str, table_id: &str, min_rows: usize) -> Result<usize, String> {
    let rows = inlined_rows(conn, pipeline, table_id)?;
    if rows == 0 || rows < min_rows {
        return Ok(0);
    }
    conn.execute_batch(&format!(
        "CALL ducklake_flush_inlined_data({}, schema_name => {}, table_name => {})",
        lit(CATALOG),
        lit(pipeline),
        lit(table_id),
    ))
    .map_err(err("flush inlined rows"))?;
    Ok(rows)
}

/// Write out piled-up catalog-stored rows, merge the table's small files,
/// then [`expire`].
pub fn maintain(conn: &Connection, pipeline: &str, table_id: &str) -> Result<usize, String> {
    flush_inlined(conn, pipeline, table_id, FLUSH_INLINED_ROWS)?;
    conn.execute_batch(&format!(
        "CALL ducklake_merge_adjacent_files({}, {}, schema => {}, max_file_size => {MERGE_MAX_FILE_SIZE})",
        lit(CATALOG),
        lit(table_id),
        lit(pipeline),
    ))
    .map_err(err("merge files"))?;
    expire(conn)
}

/// Expire old snapshots and delete the files no retained snapshot reads.
/// Files are deleted an hour after they stop being read, so a query that
/// started on an older snapshot can finish. Returns the snapshots expired.
pub fn expire(conn: &Connection) -> Result<usize, String> {
    let expire = snapshots_to_expire(&data_snapshots(conn)?, &live_table_ids(conn)?, RETAINED_VERSIONS);
    if !expire.is_empty() {
        let list = expire.iter().map(i64::to_string).collect::<Vec<_>>().join(", ");
        conn.execute_batch(&format!("CALL ducklake_expire_snapshots({}, versions => [{list}])", lit(CATALOG)))
            .map_err(err("expire snapshots"))?;
    }
    conn.execute_batch(&format!(
        "CALL ducklake_cleanup_old_files({}, older_than => now()::TIMESTAMP - INTERVAL 1 HOUR)",
        lit(CATALOG)
    ))
    .map_err(err("delete old files"))?;
    Ok(expire.len())
}

/// Drop a deleted pipeline's schema with its tables and records, then
/// expire the snapshots that read them so their files are deleted.
pub fn drop_pipeline(conn: &Connection, pipeline: &str) -> Result<(), String> {
    let exists = conn
        .query_row(
            "SELECT count(*) FROM duckdb_schemas() WHERE database_name = ? AND schema_name = ?",
            [CATALOG, pipeline],
            |r| r.get::<_, i64>(0),
        )
        .map_err(err("find schema"))?;
    if exists == 0 {
        return Ok(());
    }
    conn.execute_batch(&format!("DROP SCHEMA {CATALOG}.{} CASCADE", ident(pipeline)))
        .map_err(err("drop schema"))?;
    expire(conn).map(|_| ())
}

#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    #[error("version {0} of this table is not retained")]
    NotFound(i64),
    #[error("{0}")]
    Failed(String),
}

/// Make `version` the table's current rows, as a new snapshot. The source
/// records are cleared, so the next run of each mapping is a full run.
pub fn restore(conn: &Connection, pipeline: &str, table_id: &str, version: i64) -> Result<i64, RestoreError> {
    let versions = table_versions(conn, pipeline, table_id).map_err(RestoreError::Failed)?;
    if !versions.contains(&version) {
        return Err(RestoreError::NotFound(version));
    }
    let table = table_ref(pipeline, table_id);
    // Copied out first: inside the transaction, the delete below also hides
    // the rows of any file the old version shares with the live one.
    let staging = tempfile::tempdir().map_err(|e| RestoreError::Failed(format!("staging folder: {e}")))?;
    let rows = staging.path().join("restore.parquet");
    conn.execute_batch(&format!(
        "COPY (SELECT * FROM {table} AT (VERSION => {version})) TO {} (FORMAT parquet)",
        lit(&rows.to_string_lossy())
    ))
    .map_err(|e| RestoreError::Failed(format!("read version {version}: {e}")))?;
    let run = || -> Result<(), String> {
        let exec = |sql: String| conn.execute_batch(&sql).map_err(|e| format!("{sql}: {e}"));
        exec(format!("DELETE FROM {table}"))?;
        exec(format!("INSERT INTO {table} BY NAME SELECT * FROM {}", file_ref(&rows)))?;
        exec(format!("DELETE FROM {} WHERE table_id = {}", records_table(pipeline), lit(table_id)))?;
        exec(format!("CALL {CATALOG}.set_commit_message('karet-worker', {})", lit(&format!("restore {version}"))))
    };
    conn.execute_batch("BEGIN TRANSACTION").map_err(|e| RestoreError::Failed(e.to_string()))?;
    match run() {
        Ok(()) => {
            conn.execute_batch("COMMIT").map_err(|e| RestoreError::Failed(format!("commit: {e}")))?;
            current_snapshot(conn).map_err(RestoreError::Failed)
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(RestoreError::Failed(e))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snaps(list: &[(i64, &[i64])]) -> Vec<(i64, BTreeSet<i64>)> {
        list.iter().map(|(id, t)| (*id, t.iter().copied().collect())).collect()
    }

    #[test]
    fn keeps_the_latest_snapshots_of_each_table() {
        // Table 1 changes often, table 2 rarely: table 2's old versions stay.
        let s = snaps(&[(1, &[]), (2, &[2]), (3, &[1]), (4, &[1]), (5, &[1]), (6, &[])]);
        assert_eq!(snapshots_to_expire(&s, &BTreeSet::from([1, 2]), 2), vec![1, 3]);
    }

    #[test]
    fn keeps_nothing_for_a_dropped_table() {
        let s = snaps(&[(1, &[7]), (2, &[7]), (3, &[8])]);
        assert_eq!(snapshots_to_expire(&s, &BTreeSet::from([8]), 10), vec![1, 2]);
    }

    #[test]
    fn always_keeps_the_latest_snapshot() {
        let s = snaps(&[(1, &[1]), (2, &[])]);
        let live = BTreeSet::from([1]);
        assert_eq!(snapshots_to_expire(&s, &live, 1), Vec::<i64>::new());
        assert_eq!(snapshots_to_expire(&s, &live, 0), vec![1]);
    }

    #[test]
    fn quotes_identifiers_and_literals() {
        assert_eq!(ident("p-1\"x"), "\"p-1\"\"x\"");
        assert_eq!(lit("it's"), "'it''s'");
    }

    #[test]
    fn settings_come_from_the_urls() {
        let s = LakeSettings::from_urls(
            "postgres://karet:p%40ss@postgres:5432/karet",
            "karet_lake",
            "http://rustfs:9000",
            "karet-warehouse",
            "k",
            "s",
            "us-east-1",
        )
        .unwrap();
        assert_eq!((s.pg_host.as_str(), s.pg_port, s.pg_user.as_str()), ("postgres", 5432, "karet"));
        assert_eq!(s.pg_password, "p@ss");
        assert_eq!((s.s3_endpoint.as_str(), s.s3_use_ssl), ("rustfs:9000", false));
        let sql = s.setup_sql().join("\n");
        assert!(sql.contains("DATA_PATH 's3://karet-warehouse/lake/'"), "{sql}");
        assert!(sql.contains("DATABASE 'karet_lake'"), "{sql}");
    }
}
