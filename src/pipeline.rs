//! Pipeline execution: CSV ingestion, mapping evaluation, partitioned Parquet output.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use polars::prelude::*;

use crate::config::{AnalyticTable, ColumnSchema, Mapping, PipelineConfig, SourceContainer};
use crate::error::PipelineError;
use crate::evaluator::{compile, CompileCtx};
use crate::lookup::LookupMatcher;

/// Check a CSV header row against a source container's declared schema.
///
/// Returns `Ok(())` iff every column named in `schema` is present in
/// `headers`. Otherwise returns `Err(missing)` where `missing` lists the
/// schema column names not found in `headers`, in schema declaration order.
///
/// Extra columns present in `headers` but not in `schema` are not an error --
/// they are ignored by the caller.
pub fn validate_csv_headers(
    headers: &[String],
    schema: &[ColumnSchema],
) -> Result<(), Vec<String>> {
    let missing: Vec<String> = schema
        .iter()
        .filter(|col| !headers.iter().any(|h| h == &col.name))
        .map(|col| col.name.clone())
        .collect();

    if missing.is_empty() {
        Ok(())
    } else {
        Err(missing)
    }
}

/// Project a [`DataFrame`] down to only the columns named in `schema`, in
/// schema-declaration order.
///
/// Extra columns present in `df` but not in `schema` are dropped so the
/// evaluator only sees the columns it was configured for.
///
/// # Preconditions
///
/// The caller is expected to have already run [`validate_csv_headers`] against
/// the source's headers, so every column named in `schema` is present in `df`.
/// If that precondition is violated, `select` will surface the underlying
/// polars error.
pub fn project_schema_columns(
    df: &DataFrame,
    schema: &[ColumnSchema],
) -> PolarsResult<DataFrame> {
    let names: Vec<&str> = schema.iter().map(|c| c.name.as_str()).collect();
    df.select(names)
}

/// Resolve the [`SourceContainer`] whose `path_prefix` is a prefix of `key`.
///
/// Walks `cfg.source_containers` in declaration order and returns the first
/// match. Returns [`PipelineError::UnknownSourceContainer`] if none match --
/// we refuse to guess a schema for an unknown key.
fn resolve_source_container<'a>(
    key: &str,
    cfg: &'a PipelineConfig,
) -> Result<&'a SourceContainer, PipelineError> {
    cfg.source_containers
        .iter()
        .find(|sc| key.starts_with(&sc.path_prefix))
        .ok_or_else(|| PipelineError::UnknownSourceContainer {
            key: key.to_string(),
        })
}

/// Find the first [`Mapping`] targeting `source_container_id`.
///
/// Scoped to single-mapping ingestion: a source container with multiple
/// mappings uses the first in declaration order. If none target this
/// container, returns [`PipelineError::NoMapping`].
fn resolve_mapping<'a>(
    source_container_id: &str,
    cfg: &'a PipelineConfig,
) -> Result<&'a Mapping, PipelineError> {
    cfg.mappings
        .iter()
        .find(|m| m.source_container_id == source_container_id)
        .ok_or_else(|| PipelineError::NoMapping {
            source_container_id: source_container_id.to_string(),
        })
}

/// Ingest a single CSV file through one mapping and return the output
/// [`DataFrame`].
///
/// Resolves the source container by path prefix, picks the first mapping
/// targeting it, validates headers against the declared schema (extras
/// allowed, missing flagged), projects to schema columns, and compiles +
/// executes each `MappingColumn.expr` via Polars. `matchers` is the
/// per-job precompiled lookup registry produced by
/// [`crate::lookup::build_registry`].
pub fn ingest_file(
    key: &str,
    csv_bytes: &[u8],
    cfg: &PipelineConfig,
    matchers: &HashMap<String, Arc<LookupMatcher>>,
) -> Result<DataFrame, PipelineError> {
    let source_container = resolve_source_container(key, cfg)?;
    let mapping = resolve_mapping(&source_container.id, cfg)?;

    let df = CsvReadOptions::default()
        .with_has_header(true)
        .into_reader_with_file_handle(Cursor::new(csv_bytes))
        .finish()
        .map_err(|e| PipelineError::polars(key, e))?;

    // Collect header names as owned strings so the borrow against `df` is
    // released before `df.select` below.
    let headers: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    if let Err(missing) = validate_csv_headers(&headers, &source_container.schema) {
        return Err(PipelineError::MissingColumns {
            key: key.to_string(),
            missing,
        });
    }

    let projected = project_schema_columns(&df, &source_container.schema)
        .map_err(|e| PipelineError::polars(key, e))?;

    // Compile every mapping column against the registry, aliasing to the
    // declared output name. Errors are collected eagerly so they point at
    // the specific failing column via the `EvalError` chain.
    let ctx = CompileCtx::new(matchers);
    let mut compiled_exprs: Vec<Expr> = Vec::with_capacity(mapping.columns.len());
    for column in &mapping.columns {
        let expr = compile(&column.expr, &ctx).map_err(|e| PipelineError::eval(key, e))?;
        compiled_exprs.push(expr.alias(column.name.as_str()));
    }

    projected
        .lazy()
        .select(compiled_exprs)
        .collect()
        .map_err(|e| PipelineError::polars(key, e))
}

/// Ingest many CSV files through their respective mappings and return the
/// union of their rows as a single [`LazyFrame`].
///
/// Per-file failures are logged and skipped -- a single malformed or
/// schema-violating CSV must not abort the whole job. If **every** file
/// fails we return [`PipelineError::NoFilesSucceeded`].
pub fn ingest_many(
    files: &[(String, Vec<u8>)],
    cfg: &PipelineConfig,
    matchers: &HashMap<String, Arc<LookupMatcher>>,
) -> Result<LazyFrame, PipelineError> {
    let mut frames: Vec<LazyFrame> = Vec::with_capacity(files.len());

    for (key, csv_bytes) in files {
        match ingest_file(key, csv_bytes, cfg, matchers) {
            Ok(df) => frames.push(df.lazy()),
            Err(e) => {
                tracing::warn!(key = %key, error = %e, "skipping file during multi-file ingestion");
            }
        }
    }

    if frames.is_empty() {
        return Err(PipelineError::NoFilesSucceeded);
    }

    concat(frames, UnionArgs::default()).map_err(|e| PipelineError::polars("<multi>", e))
}

// ===========================================================================
// Partitioning and Parquet output
// ===========================================================================

/// A single partition's worth of Parquet-encoded output.
#[derive(Debug, Clone)]
pub struct PartitionOutput {
    /// S3 object key, e.g. `clean/transactions/year=2024/month=01/data.parquet`.
    pub key: String,
    /// Parquet-encoded bytes ready to upload.
    pub bytes: Vec<u8>,
}

/// Partition a [`DataFrame`] by `(year, month)` of a date-typed column.
///
/// Returns one `((year, month), sub_df)` entry per distinct calendar month
/// present in `partition_col`. Order is unspecified -- callers that need
/// stable ordering should sort by the key themselves.
pub fn partition_by_month(
    df: &DataFrame,
    partition_col: &str,
) -> Result<Vec<((i32, u32), DataFrame)>, PolarsError> {
    let partitions = df
        .clone()
        .lazy()
        .select([
            col(partition_col).dt().year().alias("__year"),
            col(partition_col).dt().month().alias("__month"),
        ])
        .unique(None, UniqueKeepStrategy::First)
        .collect()?;

    // `dt().year()` → Int32, `dt().month()` → Int8.
    let years = partitions.column("__year")?.i32()?;
    let months = partitions.column("__month")?.i8()?;

    let mut out: Vec<((i32, u32), DataFrame)> = Vec::with_capacity(partitions.height());
    for i in 0..partitions.height() {
        // Skip null partition-key values -- a null date can't be assigned
        // to a `(year, month)` partition.
        let (Some(year), Some(month)) = (years.get(i), months.get(i)) else {
            continue;
        };

        let sub_df = df
            .clone()
            .lazy()
            .filter(
                col(partition_col)
                    .dt()
                    .year()
                    .eq(year)
                    .and(col(partition_col).dt().month().eq(month)),
            )
            .sort([partition_col], Default::default())
            .collect()?;

        out.push(((year, month as u32), sub_df));
    }

    Ok(out)
}

/// Serialize a [`DataFrame`] to Parquet-encoded bytes.
pub fn write_parquet_bytes(df: &mut DataFrame) -> Result<Vec<u8>, PolarsError> {
    let mut buf = Cursor::new(Vec::new());
    ParquetWriter::new(&mut buf).finish(df)?;
    Ok(buf.into_inner())
}

/// Build the S3 object key for a `(year, month)` partition.
/// Format: `clean/<analytic_table_id>/year=YYYY/month=MM/<mapping_id>.parquet`.
///
/// The mapping id is in the filename so multiple mappings writing to the
/// same analytic table don't overwrite each other's partitions; re-running
/// the same mapping still overwrites its own previous output in place.
pub fn partition_key(
    analytic_table_id: &str,
    mapping_id: &str,
    year: i32,
    month: u32,
) -> String {
    format!(
        "clean/{analytic_table_id}/year={year:04}/month={month:02}/{mapping_id}.parquet"
    )
}

/// Build the S3 object key for an unpartitioned output.
/// Format: `clean/<analytic_table_id>/<mapping_id>.parquet`.
fn unpartitioned_key(analytic_table_id: &str, mapping_id: &str) -> String {
    format!("clean/{analytic_table_id}/{mapping_id}.parquet")
}

/// Produce one [`PartitionOutput`] per partition of `df` according to the
/// mapping's `partition_by` configuration.
///
/// - `partition_by == None`: the whole frame becomes a single output
///   under `clean/<table_id>/data.parquet`.
/// - `partition_by.granularity == "month"`: one output per `(year,
///   month)` of the declared date column.
/// - Any other granularity returns [`PipelineError::UnsupportedGranularity`].
pub fn produce_partitions(
    df: &DataFrame,
    mapping: &Mapping,
    table: &AnalyticTable,
) -> Result<Vec<PartitionOutput>, PipelineError> {
    debug_assert_eq!(
        mapping.analytic_table_id, table.id,
        "produce_partitions: mapping.analytic_table_id (`{}`) must equal table.id (`{}`)",
        mapping.analytic_table_id, table.id
    );

    match &mapping.partition_by {
        None => {
            let mut owned = df.clone();
            let bytes = write_parquet_bytes(&mut owned).map_err(|e| PipelineError::polars("<partition>", e))?;
            Ok(vec![PartitionOutput {
                key: unpartitioned_key(&table.id, &mapping.id),
                bytes,
            }])
        }

        Some(pb) if pb.granularity == "month" => {
            let groups = partition_by_month(df, &pb.column)
                .map_err(|e| PipelineError::polars("<partition>", e))?;

            let mut out: Vec<PartitionOutput> = Vec::with_capacity(groups.len());
            for ((year, month), mut sub_df) in groups {
                let bytes = write_parquet_bytes(&mut sub_df)
                    .map_err(|e| PipelineError::polars("<partition>", e))?;
                out.push(PartitionOutput {
                    key: partition_key(&table.id, &mapping.id, year, month),
                    bytes,
                });
            }
            Ok(out)
        }

        Some(pb) => Err(PipelineError::UnsupportedGranularity {
            got: pb.granularity.clone(),
        }),
    }
}

// ===========================================================================
// Partition upload
// ===========================================================================

/// Abstraction over the partition uploader.
///
/// The worker's real S3 client implements this trait; tests provide a
/// mock. The interface is synchronous -- pulling in `async_trait` solely
/// for test doubles would be premature.
///
/// The `bytes` slice is borrowed so callers can pass a reference into a
/// [`PartitionOutput`] without cloning its `Vec<u8>`. On failure,
/// implementations return a `String` that [`upload_partitions`] wraps
/// into [`PipelineError::PartitionUploadFailed`] alongside the key.
pub trait PartitionUploader {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), String>;
}

/// Upload a list of [`PartitionOutput`]s via the given uploader.
///
/// Short-circuits on the **first** per-partition failure so the caller
/// gets an unambiguous pointer to the partition that needs attention.
/// Successful uploads are returned in input order.
pub fn upload_partitions(
    uploader: &dyn PartitionUploader,
    partitions: &[PartitionOutput],
) -> Result<Vec<String>, PipelineError> {
    let mut uploaded = Vec::with_capacity(partitions.len());
    for p in partitions {
        uploader
            .put(&p.key, &p.bytes)
            .map_err(|message| PipelineError::PartitionUploadFailed {
                key: p.key.clone(),
                message,
            })?;
        uploaded.push(p.key.clone());
    }
    Ok(uploaded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ColumnSchema;
    use proptest::prelude::*;

    fn arb_column_name() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9_]{0,7}".prop_map(|s| s)
    }

    fn arb_schema() -> impl Strategy<Value = Vec<ColumnSchema>> {
        // 0..=8 columns, possibly empty
        proptest::collection::vec(
            arb_column_name().prop_map(|name| ColumnSchema {
                name,
                type_: "string".to_string(),
                nullable: None,
                assertions: None,
            }),
            0..=8,
        )
    }

    fn arb_headers() -> impl Strategy<Value = Vec<String>> {
        proptest::collection::vec(arb_column_name(), 0..=12)
    }

    proptest! {
        // CSV schema validation
        #[test]
        fn header_validation_accepts_iff_all_schema_cols_present(
            schema in arb_schema(),
            headers in arb_headers(),
        ) {
            let result = validate_csv_headers(&headers, &schema);

            // Compute the expected missing list: schema cols not in headers,
            // in schema order.
            let expected_missing: Vec<String> = schema.iter()
                .filter(|c| !headers.iter().any(|h| h == &c.name))
                .map(|c| c.name.clone())
                .collect();

            if expected_missing.is_empty() {
                prop_assert!(result.is_ok());
            } else {
                let missing = result.unwrap_err();
                prop_assert_eq!(missing, expected_missing);
            }
        }
    }

    /// Build a single-row [`Column`] for a string-valued field.
    ///
    /// Broken out so the two call sites (with-extras and without-extras) use
    /// identical machinery; this keeps the property test's equality check
    /// honest -- any projection difference is attributable to
    /// `project_schema_columns`, not to how we built the inputs.
    fn str_column(name: &str, val: &str) -> Column {
        Column::new(name.into(), &[val])
    }

    proptest! {
        // Extra CSV columns do not affect output
        //
        // Given a DataFrame whose columns are `schema_names ∪ extras`, projecting
        // it through the schema yields the same DataFrame as projecting a
        // DataFrame built from `schema_names` alone. In other words, extras are
        // invisible to the downstream evaluator -- which is how 
        // manifests at this layer of the pipeline.
        #[test]
        fn project_schema_columns_ignores_extras(
            schema_names in proptest::collection::vec(arb_column_name(), 1..=5),
            extra_names_raw in proptest::collection::vec(arb_column_name(), 0..=5),
        ) {
            // Dedup schema names while preserving first-seen order. We can't
            // use `sort + dedup` because column order matters for DataFrame
            // equality; we need the *same* order across the two frames.
            let mut seen = std::collections::HashSet::new();
            let schema_names: Vec<String> = schema_names
                .into_iter()
                .filter(|n| seen.insert(n.clone()))
                .collect();
            prop_assume!(!schema_names.is_empty());

            // Extras: drop any that collide with schema names, and dedup
            // amongst themselves. Polars rejects DataFrames with duplicate
            // column names, so this is a correctness requirement on the
            // inputs, not a test-quality concern.
            let mut extra_seen = std::collections::HashSet::new();
            let extras: Vec<String> = extra_names_raw
                .into_iter()
                .filter(|n| !schema_names.contains(n) && extra_seen.insert(n.clone()))
                .collect();

            let schema: Vec<ColumnSchema> = schema_names
                .iter()
                .map(|name| ColumnSchema {
                    name: name.clone(),
                    type_: "string".to_string(),
                    nullable: None,
                    assertions: None,
                })
                .collect();

            // Build a DataFrame with schema columns first, then extras.
            let mut cols_with_extras: Vec<Column> = Vec::new();
            for n in &schema_names {
                cols_with_extras.push(str_column(n, "schema_val"));
            }
            for n in &extras {
                cols_with_extras.push(str_column(n, "extra_val"));
            }

            // The "clean" DataFrame has only the schema columns, in the same
            // order.
            let mut cols_clean: Vec<Column> = Vec::new();
            for n in &schema_names {
                cols_clean.push(str_column(n, "schema_val"));
            }

            let df_extras = DataFrame::new(1, cols_with_extras).unwrap();
            let df_clean = DataFrame::new(1, cols_clean).unwrap();

            let out_extras = project_schema_columns(&df_extras, &schema).unwrap();
            let out_clean = project_schema_columns(&df_clean, &schema).unwrap();

            prop_assert!(
                out_extras.equals(&out_clean),
                "projected output should be identical whether or not the source \
                 DataFrame had extra columns; got {:?} vs {:?}",
                out_extras,
                out_clean,
            );
        }
    }

    // -----------------------------------------------------------------------
    // ingest_file
    // -----------------------------------------------------------------------

    use crate::ast::AstNode;
    use crate::config::{AnalyticTable, Mapping, MappingColumn, PipelineConfig, SourceContainer};
    use std::collections::HashMap;

    /// Build a minimal config with one source container, one mapping that
    /// produces a single `upper_desc` column by uppercasing `description`,
    /// and one analytic table. Used by the ingest_file tests below.
    fn simple_config() -> PipelineConfig {
        PipelineConfig {
            version: 1,
            source_containers: vec![SourceContainer {
                id: "src".into(),
                name: "Src".into(),
                path_prefix: "raw/src/".into(),
                schema: vec![
                    ColumnSchema {
                        name: "date".into(),
                        type_: "string".into(),
                        nullable: None,
                        assertions: None,
                    },
                    ColumnSchema {
                        name: "description".into(),
                        type_: "string".into(),
                        nullable: None,
                        assertions: None,
                    },
                    ColumnSchema {
                        name: "amount".into(),
                        type_: "number".into(),
                        nullable: None,
                        assertions: None,
                    },
                ],
            }],
            lookup_mappings: vec![],
            mappings: vec![Mapping {
                id: "m".into(),
                name: String::new(),
                source_container_id: "src".into(),
                analytic_table_id: "t".into(),
                partition_by: None,
                columns: vec![MappingColumn {
                    name: "upper_desc".into(),
                    expr: AstNode::Upper {
                        input: Box::new(AstNode::Col {
                            name: "description".into(),
                        }),
                    },
                }],
            }],
            analytic_tables: vec![AnalyticTable {
                id: "t".into(),
                name: "T".into(),
                output_prefix: "clean/t/".into(),
                schema: vec![ColumnSchema {
                    name: "upper_desc".into(),
                    type_: "string".into(),
                    nullable: None,
                    assertions: None,
                }],
            }],
            layout: HashMap::new(),
        }
    }

    #[test]
    fn ingest_file_uppercases_description() {
        let cfg = simple_config();
        let matchers = HashMap::new();
        let csv = b"date,description,amount\n2024-01-01,hello,10.0\n";

        let df = ingest_file("raw/src/file.csv", csv, &cfg, &matchers)
            .expect("ingest should succeed");

        assert_eq!(df.height(), 1);
        let col = df.column("upper_desc").expect("upper_desc column").as_materialized_series();
        let s = col.str().expect("string column");
        assert_eq!(s.get(0), Some("HELLO"));
    }

    #[test]
    fn ingest_file_rejects_unknown_key() {
        let cfg = simple_config();
        let matchers = HashMap::new();
        let csv = b"date,description,amount\n2024-01-01,hello,10.0\n";

        let err = ingest_file("raw/other/file.csv", csv, &cfg, &matchers).unwrap_err();
        assert!(
            matches!(err, PipelineError::UnknownSourceContainer { ref key } if key == "raw/other/file.csv"),
            "expected UnknownSourceContainer, got {err:?}"
        );
    }

    #[test]
    fn ingest_file_reports_missing_columns() {
        let cfg = simple_config();
        let matchers = HashMap::new();
        // Missing `amount`.
        let csv = b"date,description\n2024-01-01,hello\n";

        let err = ingest_file("raw/src/file.csv", csv, &cfg, &matchers).unwrap_err();
        match err {
            PipelineError::MissingColumns { key, missing } => {
                assert_eq!(key, "raw/src/file.csv");
                assert_eq!(missing, vec!["amount".to_string()]);
            }
            other => panic!("expected MissingColumns, got {other:?}"),
        }
    }

    #[test]
    fn ingest_file_ignores_extra_csv_columns() {
        // CSV has an extra `memo` column not in the schema. :
        // extras must not affect the output.
        let cfg = simple_config();
        let matchers = HashMap::new();
        let csv = b"date,description,amount,memo\n2024-01-01,hello,10.0,ignored\n";

        let df = ingest_file("raw/src/file.csv", csv, &cfg, &matchers)
            .expect("ingest should succeed");

        let names: Vec<&str> = df
            .get_column_names()
            .iter()
            .map(|n| n.as_str())
            .collect();
        assert_eq!(names, vec!["upper_desc"]);
        let col = df.column("upper_desc").unwrap().as_materialized_series();
        assert_eq!(col.str().unwrap().get(0), Some("HELLO"));
    }

    #[test]
    fn ingest_file_errors_when_no_mapping_targets_container() {
        // Config has a source container but no mapping for it.
        let mut cfg = simple_config();
        cfg.mappings.clear();
        let matchers = HashMap::new();
        let csv = b"date,description,amount\n2024-01-01,hello,10.0\n";

        let err = ingest_file("raw/src/file.csv", csv, &cfg, &matchers).unwrap_err();
        assert!(
            matches!(err, PipelineError::NoMapping { ref source_container_id } if source_container_id == "src"),
            "expected NoMapping, got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // ingest_many
    // -----------------------------------------------------------------------

    #[test]
    fn ingest_many_concats_rows() {
        // Two well-formed CSVs under the same source container should concat
        // into a single LazyFrame with the sum of the per-file row counts.
        let cfg = simple_config();
        let matchers = HashMap::new();
        let files: Vec<(String, Vec<u8>)> = vec![
            (
                "raw/src/a.csv".into(),
                b"date,description,amount\n2024-01-01,hello,10.0\n".to_vec(),
            ),
            (
                "raw/src/b.csv".into(),
                b"date,description,amount\n2024-02-01,world,20.0\n".to_vec(),
            ),
        ];

        let lf = ingest_many(&files, &cfg, &matchers).expect("ingest_many should succeed");
        let df = lf.collect().expect("collect");

        assert_eq!(df.height(), 2);
        let col = df.column("upper_desc").unwrap().as_materialized_series();
        let s = col.str().unwrap();
        let vals: Vec<Option<&str>> = (0..df.height()).map(|i| s.get(i)).collect();
        // Order preserved across inputs in declaration order.
        assert_eq!(vals, vec![Some("HELLO"), Some("WORLD")]);
    }

    #[test]
    fn ingest_many_skips_failing_file() {
        // One good file + one missing `amount` → only the good file is
        // represented in the output, and ingest_many does not surface the
        // per-file failure as an error.
        let cfg = simple_config();
        let matchers = HashMap::new();
        let files: Vec<(String, Vec<u8>)> = vec![
            (
                "raw/src/good.csv".into(),
                b"date,description,amount\n2024-01-01,hello,10.0\n".to_vec(),
            ),
            (
                "raw/src/bad.csv".into(),
                b"date,description\n2024-01-01,hello\n".to_vec(),
            ),
        ];

        let lf = ingest_many(&files, &cfg, &matchers).expect("ingest_many should succeed");
        let df = lf.collect().expect("collect");

        assert_eq!(df.height(), 1);
        let col = df.column("upper_desc").unwrap().as_materialized_series();
        assert_eq!(col.str().unwrap().get(0), Some("HELLO"));
    }

    proptest! {
        // Multi-file ingestion is the union of single-file ingestions
        //
        // For N CSVs conforming to the same schema, the multiset of rows
        // produced by `ingest_many` equals the multiset union of the rows
        // produced by calling `ingest_file` on each CSV individually. We
        // compare as sorted multisets because `concat` does not promise
        // ordering once we strip the file-boundary structure, and the
        // requirement (2.5) is about coverage, not order.
        #[test]
        fn multi_file_ingest_is_union_of_single_file(
            files_rows in proptest::collection::vec(
                proptest::collection::vec("[a-zA-Z]{1,8}", 1..=5),
                1..=4,
            ),
        ) {
            let cfg = simple_config();
            let matchers = HashMap::new();

            // Build the `(key, csv_bytes)` pairs for ingest_many and, while
            // we're at it, pre-compute the expected uppercased descriptions.
            // Keeping these in lock-step keeps the property test's ground
            // truth obvious: whatever we fed in, uppercased, must come out.
            let mut files: Vec<(String, Vec<u8>)> = Vec::new();
            let mut expected_upper: Vec<String> = Vec::new();
            for (i, rows) in files_rows.iter().enumerate() {
                let mut csv = String::from("date,description,amount\n");
                for desc in rows {
                    csv.push_str("2024-01-01,");
                    csv.push_str(desc);
                    csv.push_str(",0.0\n");
                    expected_upper.push(desc.to_uppercase());
                }
                files.push((format!("raw/src/f{i}.csv"), csv.into_bytes()));
            }

            // Multi-file path: one concat'd LazyFrame, collected.
            let lf = ingest_many(&files, &cfg, &matchers).expect("ingest_many should succeed");
            let df = lf.collect().expect("collect multi");
            let col = df.column("upper_desc").unwrap().as_materialized_series();
            let s = col.str().unwrap();
            let mut got: Vec<String> = (0..df.height())
                .map(|i| s.get(i).unwrap().to_string())
                .collect();

            // Single-file path: call ingest_file per input and append rows
            // into a single Vec -- this is the explicit multiset-union.
            let mut single_file_sum: Vec<String> = Vec::new();
            for (key, bytes) in &files {
                let single_df = ingest_file(key, bytes, &cfg, &matchers).unwrap();
                let c = single_df.column("upper_desc").unwrap().as_materialized_series();
                let st = c.str().unwrap();
                for i in 0..single_df.height() {
                    single_file_sum.push(st.get(i).unwrap().to_string());
                }
            }

            // Multiset equality: sort both sides and compare.
            got.sort();
            single_file_sum.sort();
            prop_assert_eq!(&got, &single_file_sum);

            // Cross-check against the ground-truth expected uppercased rows
            // so we know the invariant isn't being vacuously satisfied by
            // both sides returning the same empty/garbage result.
            let mut expected_sorted = expected_upper.clone();
            expected_sorted.sort();
            prop_assert_eq!(got, expected_sorted);
        }
    }

    #[test]
    fn ingest_many_errors_when_all_files_fail() {
        // Every file is malformed (missing columns) → no successful frame to
        // return, so ingest_many must surface NoFilesSucceeded rather than
        // producing an empty LazyFrame.
        let cfg = simple_config();
        let matchers = HashMap::new();
        let files: Vec<(String, Vec<u8>)> = vec![(
            "raw/src/bad.csv".into(),
            b"date,description\n2024-01-01,hello\n".to_vec(),
        )];

        let err = ingest_many(&files, &cfg, &matchers)
            .err()
            .expect("ingest_many should fail when every file fails");
        assert!(
            matches!(err, PipelineError::NoFilesSucceeded),
            "expected NoFilesSucceeded, got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // produce_partitions
    // -----------------------------------------------------------------------

    use crate::config::PartitionBy;

    /// Build a minimal AnalyticTable for tests.
    fn test_table(id: &str) -> AnalyticTable {
        AnalyticTable {
            id: id.into(),
            name: id.into(),
            output_prefix: format!("clean/{id}/"),
            schema: vec![],
        }
    }

    /// Build a mapping targeting `table_id` with the given (optional)
    /// partition configuration. The mapping's columns list is empty because
    /// these tests operate on DataFrames built by hand -- produce_partitions
    /// doesn't inspect `columns`.
    fn test_mapping(table_id: &str, partition_by: Option<PartitionBy>) -> Mapping {
        Mapping {
            id: "m".into(),
            name: String::new(),
            source_container_id: "src".into(),
            analytic_table_id: table_id.into(),
            partition_by,
            columns: vec![],
        }
    }

    /// Build a DataFrame with a Date-typed `d` column from `&[&str]` values
    /// like `"2024-01-15"`. Uses Polars' str→Date conversion so the schema
    /// matches what the evaluator produces via `parse_date`.
    fn df_with_dates(dates: &[&str]) -> DataFrame {
        let df = DataFrame::new(
            dates.len(),
            vec![Column::new("d".into(), dates)],
        )
        .unwrap();
        df.lazy()
            .with_column(col("d").str().to_date(StrptimeOptions {
                format: Some("%Y-%m-%d".into()),
                strict: true,
                ..Default::default()
            }))
            .collect()
            .unwrap()
    }

    #[test]
    fn produce_partitions_no_partitioning() {
        // With `partition_by: None`, the whole frame becomes a single output
        // under `clean/<id>/<uuid>.parquet`. We can't pin the UUID, but we
        // can assert the prefix/suffix and that exactly one output came out.
        let df = df_with_dates(&["2024-01-15", "2024-02-01"]);
        let table = test_table("orders");
        let mapping = test_mapping("orders", None);

        let outs = produce_partitions(&df, &mapping, &table).unwrap();
        assert_eq!(outs.len(), 1);

        let key = &outs[0].key;
        assert!(
            key.starts_with("clean/orders/") && key.ends_with(".parquet"),
            "unexpected key `{key}`"
        );
        assert!(
            !key.contains("year="),
            "unpartitioned key should not contain a year= segment; got `{key}`"
        );
        assert!(
            !outs[0].bytes.is_empty(),
            "parquet bytes should be non-empty for a non-empty frame"
        );
        // Parquet magic header (PAR1) -- a cheap sanity check that we
        // actually produced a parquet file and not some other encoding.
        assert_eq!(&outs[0].bytes[..4], b"PAR1");
    }

    #[test]
    fn produce_partitions_by_month() {
        // Rows dated 2024-01-15, 2024-02-01, 2024-02-28 → 2 partitions
        // (January and February 2024). Assert the two partition keys
        // contain the expected `year=YYYY/month=MM/` fragments.
        let df = df_with_dates(&["2024-01-15", "2024-02-01", "2024-02-28"]);
        let table = test_table("transactions");
        let mapping = test_mapping(
            "transactions",
            Some(PartitionBy {
                column: "d".into(),
                granularity: "month".into(),
            }),
        );

        let outs = produce_partitions(&df, &mapping, &table).unwrap();
        assert_eq!(outs.len(), 2, "expected 2 month partitions, got {}", outs.len());

        // Order of partitions isn't guaranteed by `unique()`, so check
        // membership of the key fragments rather than positional indexing.
        let keys: Vec<&String> = outs.iter().map(|o| &o.key).collect();
        let has_jan = keys
            .iter()
            .any(|k| k.contains("year=2024/month=01/") && k.contains("clean/transactions/"));
        let has_feb = keys
            .iter()
            .any(|k| k.contains("year=2024/month=02/") && k.contains("clean/transactions/"));
        assert!(has_jan, "missing January partition in keys {:?}", keys);
        assert!(has_feb, "missing February partition in keys {:?}", keys);

        for out in &outs {
            assert!(out.key.ends_with(".parquet"));
            assert_eq!(&out.bytes[..4], b"PAR1");
        }
    }

    #[test]
    fn produce_partitions_distinct_keys_per_mapping() {
        // Two mappings writing the same analytic table at the same (year,
        // month) must produce different keys, otherwise the second upload
        // overwrites the first.
        let df = df_with_dates(&["2024-01-15"]);
        let table = test_table("transactions");

        let mut visa = test_mapping(
            "transactions",
            Some(PartitionBy { column: "d".into(), granularity: "month".into() }),
        );
        visa.id = "scotia_visa_mapping".into();

        let mut chq = test_mapping(
            "transactions",
            Some(PartitionBy { column: "d".into(), granularity: "month".into() }),
        );
        chq.id = "scotia_chq_mapping".into();

        let visa_key = produce_partitions(&df, &visa, &table).unwrap()[0].key.clone();
        let chq_key = produce_partitions(&df, &chq, &table).unwrap()[0].key.clone();

        assert_ne!(visa_key, chq_key);
        assert!(visa_key.contains("scotia_visa_mapping"));
        assert!(chq_key.contains("scotia_chq_mapping"));
    }

    #[test]
    fn produce_partitions_rejects_unsupported_granularity() {
        // `granularity = "day"` isn't implemented → `UnsupportedGranularity`.
        let df = df_with_dates(&["2024-01-15"]);
        let table = test_table("t");
        let mapping = test_mapping(
            "t",
            Some(PartitionBy {
                column: "d".into(),
                granularity: "day".into(),
            }),
        );

        let err = produce_partitions(&df, &mapping, &table).unwrap_err();
        match err {
            PipelineError::UnsupportedGranularity { got } => {
                assert_eq!(got, "day");
            }
            other => panic!("expected UnsupportedGranularity, got {other:?}"),
        }
    }

    proptest! {
        // Output partitions cover exactly the input date range
        //
        // Generate 1..=20 random dates within 2022-01-01..=2025-12-31 and
        // assert that the set of `(year, month)` keys emitted by
        // `produce_partitions` equals the set `{ (year(v), month(v)) | v in dates }`.
        //
        // We restrict day-of-month to 1..=28 so every `(year, month, day)`
        // triple is a valid calendar date regardless of month length or
        // leap-year rules -- we're testing partition coverage here, not date
        // parsing.
        #[test]
        fn partitions_cover_input_date_range(
            dates in proptest::collection::vec(
                (2022i32..=2025, 1u32..=12, 1u32..=28),
                1..=20,
            ),
        ) {
            // Format the (y, m, d) triples into YYYY-MM-DD strings for the
            // DataFrame builder. Kept as `String` owned values so their
            // lifetimes extend across the subsequent borrow as `&[&str]`.
            let date_strs: Vec<String> = dates.iter()
                .map(|(y, m, d)| format!("{y:04}-{m:02}-{d:02}"))
                .collect();
            let date_refs: Vec<&str> = date_strs.iter().map(|s| s.as_str()).collect();

            let df = df_with_dates(&date_refs);
            let table = test_table("t");
            let mapping = test_mapping(
                "t",
                Some(PartitionBy {
                    column: "d".into(),
                    granularity: "month".into(),
                }),
            );

            let outs = produce_partitions(&df, &mapping, &table).unwrap();

            // Extract `(year, month)` pairs from the output keys. The key
            // format `clean/<id>/year=YYYY/month=MM/<uuid>.parquet` is pinned
            // by `partition_key()`; we locate the `year=` and `month=` tags
            // and read their fixed-width numeric values.
            let mut got: std::collections::HashSet<(i32, u32)> = std::collections::HashSet::new();
            for out in &outs {
                let y_start = out.key.find("year=").expect("key has year=") + 5;
                let y = out.key[y_start..y_start + 4].parse::<i32>().unwrap();
                let m_start = out.key.find("month=").expect("key has month=") + 6;
                let m = out.key[m_start..m_start + 2].parse::<u32>().unwrap();
                got.insert((y, m));
            }

            let expected: std::collections::HashSet<(i32, u32)> = dates.iter()
                .map(|(y, m, _)| (*y, *m))
                .collect();

            prop_assert_eq!(got, expected);
        }
    }

    // -----------------------------------------------------------------------
    // upload_partitions -- partition upload failures identify the bad key.
    // -----------------------------------------------------------------------

    #[test]
    fn upload_failure_identifies_partition() {
        // Mock uploader that fails on one specific key.
        struct MockUploader {
            fail_on: String,
        }
        impl PartitionUploader for MockUploader {
            fn put(&self, key: &str, _bytes: &[u8]) -> Result<(), String> {
                if key == self.fail_on {
                    Err("simulated S3 failure".to_string())
                } else {
                    Ok(())
                }
            }
        }

        let partitions = vec![
            PartitionOutput {
                key: "clean/t/year=2024/month=01/a.parquet".into(),
                bytes: vec![0u8],
            },
            PartitionOutput {
                key: "clean/t/year=2024/month=02/b.parquet".into(),
                bytes: vec![0u8],
            },
            PartitionOutput {
                key: "clean/t/year=2024/month=03/c.parquet".into(),
                bytes: vec![0u8],
            },
        ];
        let uploader = MockUploader {
            fail_on: "clean/t/year=2024/month=02/b.parquet".into(),
        };

        let err = upload_partitions(&uploader, &partitions).unwrap_err();
        match err {
            PipelineError::PartitionUploadFailed { key, message } => {
                assert_eq!(key, "clean/t/year=2024/month=02/b.parquet");
                assert!(
                    message.contains("simulated"),
                    "expected failure message to carry uploader's text; got `{message}`"
                );
            }
            other => panic!("expected PartitionUploadFailed, got {other:?}"),
        }
    }

    #[test]
    fn upload_success_returns_all_keys() {
        // Happy path: every put succeeds → returned Vec lists every input
        // key in declaration order. Guards against accidentally swallowing
        // or reordering keys during the loop.
        struct OkUploader;
        impl PartitionUploader for OkUploader {
            fn put(&self, _key: &str, _bytes: &[u8]) -> Result<(), String> {
                Ok(())
            }
        }

        let partitions = vec![
            PartitionOutput {
                key: "a.parquet".into(),
                bytes: vec![0u8],
            },
            PartitionOutput {
                key: "b.parquet".into(),
                bytes: vec![0u8],
            },
        ];

        let keys = upload_partitions(&OkUploader, &partitions).unwrap();
        assert_eq!(keys, vec!["a.parquet".to_string(), "b.parquet".to_string()]);
    }
}
