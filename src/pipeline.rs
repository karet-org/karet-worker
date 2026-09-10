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
/// Extra columns present in `headers` but not in `schema` are not an error,
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

/// Project `df` to the schema's columns, in declaration order; extras are
/// dropped. Callers run [`validate_csv_headers`] first, so missing columns
/// surface as polars errors.
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
/// match. Returns [`PipelineError::UnknownSourceContainer`] if none match,
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

/// Read a CSV source file (header row, comma-delimited) into a [`DataFrame`].
///
/// The schema is inferred from the data; downstream mapping expressions
/// handle any type coercion (e.g. `parse_date`, `cast`).
fn read_source(key: &str, bytes: &[u8]) -> Result<DataFrame, PipelineError> {
    CsvReadOptions::default()
        .with_has_header(true)
        .into_reader_with_file_handle(Cursor::new(bytes))
        .finish()
        .map_err(|e| PipelineError::polars(key, e))
}

/// Ingest one source file through one mapping: resolve the container by
/// prefix, validate headers, project to schema columns, evaluate each
/// column expr via Polars. The mapping is caller-supplied, not re-derived
/// from the container, so multi-mapping containers ingest per mapping.
pub fn ingest_file(
    key: &str,
    csv_bytes: &[u8],
    cfg: &PipelineConfig,
    mapping: &Mapping,
    matchers: &HashMap<String, Arc<LookupMatcher>>,
) -> Result<DataFrame, PipelineError> {
    let source_container = resolve_source_container(key, cfg)?;

    // JSON sources bind schema columns to paths inside each record, so the
    // reader already emits exactly the declared columns: there are no headers
    // to validate and nothing to project.
    let projected = if source_container.format == crate::config::SourceFormat::Csv {
        let df = read_source(key, csv_bytes)?;

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

        project_schema_columns(&df, &source_container.schema)
            .map_err(|e| PipelineError::polars(key, e))?
    } else {
        let (df, skipped) = crate::json_source::read_json_source(csv_bytes, source_container)
            .map_err(|message| PipelineError::JsonRead {
                key: key.to_string(),
                message,
            })?;
        if skipped > 0 {
            tracing::warn!(key, skipped, "skipped unparseable JSON records");
        }
        // Record-level filter runs before the column expressions so unrelated
        // entries in a shared log stream never reach the mapping.
        if let Some(filter) = &source_container.record_filter {
            let ctx = CompileCtx::new(matchers);
            let expr = compile(filter, &ctx).map_err(|e| PipelineError::eval(key, e))?;
            df.lazy()
                .filter(expr)
                .collect()
                .map_err(|e| PipelineError::polars(key, e))?
        } else {
            df
        }
    };

    // Compile every mapping column against the registry, aliasing to the
    // declared output name. Errors are collected eagerly so they point at
    // the specific failing column via the `EvalError` chain.
    let ctx = CompileCtx::new(matchers);
    let mut compiled_exprs: Vec<Expr> = Vec::with_capacity(mapping.columns.len());
    for column in &mapping.columns {
        let expr = compile(&column.expr, &ctx).map_err(|e| PipelineError::eval(key, e))?;
        compiled_exprs.push(expr.alias(column.name.as_str()));
    }

    let mut lazy = projected.lazy().select(compiled_exprs);

    // After the projection, so the predicate sees output column names.
    if let Some(predicate) = &mapping.where_ {
        let expr = compile(predicate, &ctx).map_err(|e| PipelineError::eval(key, e))?;
        lazy = lazy.filter(expr);
    }

    lazy.collect().map_err(|e| PipelineError::polars(key, e))
}

/// Ingest many CSV files through their respective mappings and return the
/// union of their rows as a single [`LazyFrame`].
///
/// Per-file failures are logged and skipped, a single malformed or
/// schema-violating CSV must not abort the whole job. If **every** file
/// fails we return [`PipelineError::NoFilesSucceeded`].
pub fn ingest_many(
    files: &[(String, Vec<u8>)],
    cfg: &PipelineConfig,
    mapping: &Mapping,
    matchers: &HashMap<String, Arc<LookupMatcher>>,
) -> Result<LazyFrame, PipelineError> {
    let mut frames: Vec<LazyFrame> = Vec::with_capacity(files.len());

    for (key, csv_bytes) in files {
        match ingest_file(key, csv_bytes, cfg, mapping, matchers) {
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
    /// S3 object key, e.g. `transactions/year=2024/month=01/data.parquet`.
    pub key: String,
    /// Parquet-encoded bytes ready to upload.
    pub bytes: Vec<u8>,
}

/// Percent-encode a partition value for use as a hive path segment.
/// Characters outside `[A-Za-z0-9._-]` are `%XX`-escaped.
fn encode_segment(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Render one partition-key cell as its path segment string.
/// Ints plain, dates ISO, bools true/false, strings percent-encoded.
fn segment_value(av: &AnyValue) -> Result<String, PipelineError> {
    match av {
        AnyValue::Int8(v) => Ok(v.to_string()),
        AnyValue::Int16(v) => Ok(v.to_string()),
        AnyValue::Int32(v) => Ok(v.to_string()),
        AnyValue::Int64(v) => Ok(v.to_string()),
        AnyValue::UInt8(v) => Ok(v.to_string()),
        AnyValue::UInt16(v) => Ok(v.to_string()),
        AnyValue::UInt32(v) => Ok(v.to_string()),
        AnyValue::UInt64(v) => Ok(v.to_string()),
        AnyValue::Boolean(v) => Ok(v.to_string()),
        AnyValue::Date(days) => {
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                .ok_or_else(|| PipelineError::Partition {
                    message: format!("date value {days} out of range"),
                })?;
            Ok(date.format("%Y-%m-%d").to_string())
        }
        AnyValue::String(v) => Ok(encode_segment(v)),
        AnyValue::StringOwned(v) => Ok(encode_segment(v)),
        other => Err(PipelineError::Partition {
            message: format!("unsupported partition key value: {other:?}"),
        }),
    }
}

/// Serialize a [`DataFrame`] to Parquet-encoded bytes.
pub fn write_parquet_bytes(df: &mut DataFrame) -> Result<Vec<u8>, PolarsError> {
    let mut buf = Cursor::new(Vec::new());
    ParquetWriter::new(&mut buf).finish(df)?;
    Ok(buf.into_inner())
}

/// Build the S3 object key for an unpartitioned output.
/// Format: `<analytic_table_id>/<mapping_id>.parquet`.
fn unpartitioned_key(analytic_table_id: &str, mapping_id: &str) -> String {
    format!("{analytic_table_id}/{mapping_id}.parquet")
}

/// Deduplicate `df` on the table's `dedup_keys`, keeping the first row in
/// ingest order. Returns the deduped frame and the number of dropped rows.
/// No keys means no work.
pub fn dedup_rows(
    df: &DataFrame,
    table: &AnalyticTable,
) -> Result<(DataFrame, usize), PipelineError> {
    if table.dedup_keys.is_empty() {
        return Ok((df.clone(), 0));
    }
    let deduped = df
        .unique_stable(Some(&table.dedup_keys), UniqueKeepStrategy::First, None)
        .map_err(|e| PipelineError::polars("<dedup>", e))?;
    let dropped = df.height() - deduped.height();
    Ok((deduped, dropped))
}

/// One [`PartitionOutput`] per distinct tuple of the table's
/// `partition_keys` at `<table_id>/<k>=<v>/../<mapping_id>.parquet`; no
/// keys means one whole-frame output. Key columns live in the path only
/// (hive reading restores them); null keys fail the mapping (`coalesce`
/// is the escape hatch). The mapping id in the filename keeps mappings
/// sharing a table from overwriting each other.
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

    if table.partition_keys.is_empty() {
        let mut owned = df.clone();
        let bytes =
            write_parquet_bytes(&mut owned).map_err(|e| PipelineError::polars("<partition>", e))?;
        return Ok(vec![PartitionOutput {
            key: unpartitioned_key(&table.id, &mapping.id),
            bytes,
        }]);
    }

    // Fail fast on null key values, naming the column and row count.
    for key in &table.partition_keys {
        let column = df
            .column(key)
            .map_err(|e| PipelineError::polars("<partition>", e))?;
        let nulls = column.null_count();
        if nulls > 0 {
            return Err(PipelineError::Partition {
                message: format!(
                    "partition key `{key}` has {nulls} null value(s); \
                     rows cannot be assigned to a partition (coalesce in the expression to supply a default)"
                ),
            });
        }
    }

    let keys: Vec<PlSmallStr> = table
        .partition_keys
        .iter()
        .map(|k| PlSmallStr::from_str(k))
        .collect();
    let groups = df
        .partition_by_stable(keys.clone(), true)
        .map_err(|e| PipelineError::polars("<partition>", e))?;

    let mut out: Vec<PartitionOutput> = Vec::with_capacity(groups.len());
    for group in groups {
        let mut segments: Vec<String> = Vec::with_capacity(table.partition_keys.len());
        for key in &table.partition_keys {
            let av = group
                .column(key)
                .and_then(|c| c.get(0))
                .map_err(|e| PipelineError::polars("<partition>", e))?;
            segments.push(format!("{key}={}", segment_value(&av)?));
        }
        let mut sub = group
            .drop_many(keys.iter().cloned());
        let bytes =
            write_parquet_bytes(&mut sub).map_err(|e| PipelineError::polars("<partition>", e))?;
        out.push(PartitionOutput {
            key: format!("{}/{}/{}.parquet", table.id, segments.join("/"), mapping.id),
            bytes,
        });
    }
    Ok(out)
}

// ===========================================================================
// Partition upload
// ===========================================================================

/// Keys of a mapping's previous outputs that this run did not rewrite:
/// stale layouts after a partition-key change, and partitions whose
/// source rows vanished. `existing` is the current listing under the
/// table prefix; `uploaded` the full keys this run just wrote.
pub fn stale_keys(
    existing: &[String],
    uploaded: &std::collections::HashSet<String>,
    mapping_id: &str,
) -> Vec<String> {
    let suffix = format!("/{mapping_id}.parquet");
    existing
        .iter()
        .filter(|k| k.ends_with(&suffix) && !uploaded.contains(*k))
        .cloned()
        .collect()
}

/// Upload seam: production is async in `job.rs`; this sync trait lets
/// tests run the pipeline against in-memory uploaders.
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
                path: None,
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
    /// honest, any projection difference is attributable to
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
        // invisible to the downstream evaluator, which is how 
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
                    path: None,
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
    use crate::config::{AnalyticTable, Mapping, MappingColumn, PipelineConfig, SourceContainer, SourceFormat};
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
                format: SourceFormat::Csv,
                record_filter: None,
                schema: vec![
                    ColumnSchema {
                        name: "date".into(),
                        type_: "string".into(),
                        nullable: None,
                        assertions: None,
                        path: None,
                    },
                    ColumnSchema {
                        name: "description".into(),
                        type_: "string".into(),
                        nullable: None,
                        assertions: None,
                        path: None,
                    },
                    ColumnSchema {
                        name: "amount".into(),
                        type_: "number".into(),
                        nullable: None,
                        assertions: None,
                        path: None,
                    },
                ],
            }],
            lookup_mappings: vec![],
            mappings: vec![Mapping {
                id: "m".into(),
                name: String::new(),
                source_container_id: "src".into(),
                analytic_table_id: "t".into(),
                where_: None,
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
                schema: vec![ColumnSchema {
                    name: "upper_desc".into(),
                    type_: "string".into(),
                    nullable: None,
                    assertions: None,
                    path: None,
                }],
                partition_keys: vec![],
                dedup_keys: vec![],
            }],
            layout: HashMap::new(),
        }
    }

    #[test]
    fn ingest_file_uppercases_description() {
        let cfg = simple_config();
        let matchers = HashMap::new();
        let csv = b"date,description,amount\n2024-01-01,hello,10.0\n";

        let df = ingest_file("raw/src/file.csv", csv, &cfg, &cfg.mappings[0], &matchers)
            .expect("ingest should succeed");

        assert_eq!(df.height(), 1);
        let col = df.column("upper_desc").expect("upper_desc column").as_materialized_series();
        let s = col.str().expect("string column");
        assert_eq!(s.get(0), Some("HELLO"));
    }

    #[test]
    fn where_drops_non_matching_rows() {
        // Predicate references the mapping's *output* column (upper_desc),
        // proving the filter runs after the projection.
        let mut cfg = simple_config();
        cfg.mappings[0].where_ = Some(AstNode::Eq {
            left: Box::new(AstNode::Col { name: "upper_desc".into() }),
            right: Box::new(AstNode::Str { value: "KEEP".into() }),
        });
        let matchers = HashMap::new();
        let csv = b"date,description,amount\n2024-01-01,keep,1.0\n2024-01-02,drop,2.0\n";

        let df = ingest_file("raw/src/f.csv", csv, &cfg, &cfg.mappings[0], &matchers)
            .expect("ingest should succeed");

        assert_eq!(df.height(), 1);
        let s = df.column("upper_desc").unwrap().as_materialized_series().str().unwrap().get(0);
        assert_eq!(s, Some("KEEP"));
    }

    #[test]
    fn where_composes_with_and_or_not() {
        let mut cfg = simple_config();
        // NOT(upper_desc == "DROP") AND (upper_desc == "A" OR upper_desc == "B")
        let is = |v: &str| AstNode::Eq {
            left: Box::new(AstNode::Col { name: "upper_desc".into() }),
            right: Box::new(AstNode::Str { value: v.into() }),
        };
        cfg.mappings[0].where_ = Some(AstNode::And {
            left: Box::new(AstNode::Not { input: Box::new(is("DROP")) }),
            right: Box::new(AstNode::Or {
                left: Box::new(is("A")),
                right: Box::new(is("B")),
            }),
        });
        let matchers = HashMap::new();
        let csv = b"date,description,amount\n2024-01-01,a,1\n2024-01-02,b,2\n2024-01-03,drop,3\n2024-01-04,c,4\n";

        let df = ingest_file("raw/src/f.csv", csv, &cfg, &cfg.mappings[0], &matchers)
            .expect("ingest should succeed");

        assert_eq!(df.height(), 2, "only A and B survive");
    }

    #[test]
    fn absent_where_keeps_every_row() {
        let cfg = simple_config();
        let matchers = HashMap::new();
        let csv = b"date,description,amount\n2024-01-01,a,1\n2024-01-02,b,2\n";
        let df = ingest_file("raw/src/f.csv", csv, &cfg, &cfg.mappings[0], &matchers).unwrap();
        assert_eq!(df.height(), 2);
    }

    #[test]
    fn ingest_file_rejects_unknown_key() {
        let cfg = simple_config();
        let matchers = HashMap::new();
        let csv = b"date,description,amount\n2024-01-01,hello,10.0\n";

        let err = ingest_file("raw/other/file.csv", csv, &cfg, &cfg.mappings[0], &matchers).unwrap_err();
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

        let err = ingest_file("raw/src/file.csv", csv, &cfg, &cfg.mappings[0], &matchers).unwrap_err();
        match err {
            PipelineError::MissingColumns { key, missing } => {
                assert_eq!(key, "raw/src/file.csv");
                assert_eq!(missing, vec!["amount".to_string()]);
            }
            other => panic!("expected MissingColumns, got {other:?}"),
        }
    }

    #[test]
    fn ingest_file_uses_the_supplied_mapping_not_the_first() {
        // Regression for the v1/v2 review finding: two mappings share one
        // source container; ingestion previously re-derived "the first
        // mapping for the container", so mapping B's ingest ran with
        // mapping A's columns and B's data landed under A's shape.
        let mut cfg = simple_config();
        let mut second = cfg.mappings[0].clone();
        second.id = "second_mapping".into();
        second.analytic_table_id = cfg.analytic_tables[0].id.clone();
        // Same container, different output: only the description, renamed.
        second.columns = vec![crate::config::MappingColumn {
            name: "description_upper".into(),
            expr: crate::ast::AstNode::Upper {
                input: Box::new(crate::ast::AstNode::Col {
                    name: "description".into(),
                }),
            },
        }];
        cfg.mappings.push(second);

        let matchers = HashMap::new();
        let csv = b"date,description,amount\n2024-01-01,hello,10.0\n";

        // Ingesting with mapping #2 must produce mapping #2's columns.
        let df = ingest_file("raw/src/file.csv", csv, &cfg, &cfg.mappings[1], &matchers)
            .expect("ingest with the second mapping succeeds");
        let cols: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
        assert_eq!(cols, vec!["description_upper".to_string()]);
        let v = df.column("description_upper").unwrap().str().unwrap().get(0);
        assert_eq!(v, Some("HELLO"));

        // And mapping #1 still produces its own columns.
        let df1 = ingest_file("raw/src/file.csv", csv, &cfg, &cfg.mappings[0], &matchers)
            .expect("ingest with the first mapping succeeds");
        assert_ne!(
            df1.get_column_names(),
            df.get_column_names(),
            "the two mappings must not produce identical shapes in this fixture"
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

        let lf = ingest_many(&files, &cfg, &cfg.mappings[0], &matchers).expect("ingest_many should succeed");
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

        let lf = ingest_many(&files, &cfg, &cfg.mappings[0], &matchers).expect("ingest_many should succeed");
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
            let lf = ingest_many(&files, &cfg, &cfg.mappings[0], &matchers).expect("ingest_many should succeed");
            let df = lf.collect().expect("collect multi");
            let col = df.column("upper_desc").unwrap().as_materialized_series();
            let s = col.str().unwrap();
            let mut got: Vec<String> = (0..df.height())
                .map(|i| s.get(i).unwrap().to_string())
                .collect();

            // Single-file path: call ingest_file per input and append rows
            // into a single Vec, this is the explicit multiset-union.
            let mut single_file_sum: Vec<String> = Vec::new();
            for (key, bytes) in &files {
                let single_df = ingest_file(key, bytes, &cfg, &cfg.mappings[0], &matchers).unwrap();
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

        let err = ingest_many(&files, &cfg, &cfg.mappings[0], &matchers)
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

    /// Build a minimal AnalyticTable for tests.
    fn test_table(id: &str, partition_keys: &[&str], dedup_keys: &[&str]) -> AnalyticTable {
        AnalyticTable {
            id: id.into(),
            name: id.into(),
            schema: vec![],
            partition_keys: partition_keys.iter().map(|k| k.to_string()).collect(),
            dedup_keys: dedup_keys.iter().map(|k| k.to_string()).collect(),
        }
    }

    /// Build a mapping targeting `table_id`. The columns list is empty
    /// because these tests operate on DataFrames built by hand.
    fn test_mapping(table_id: &str) -> Mapping {
        Mapping {
            id: "m".into(),
            name: String::new(),
            source_container_id: "src".into(),
            where_: None,
            analytic_table_id: table_id.into(),
            columns: vec![],
        }
    }

    /// DataFrame with int64 `year`/`month` key columns plus a `v` payload.
    fn df_with_keys(rows: &[(i64, i64, &str)]) -> DataFrame {
        let years: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let months: Vec<i64> = rows.iter().map(|r| r.1).collect();
        let vals: Vec<&str> = rows.iter().map(|r| r.2).collect();
        df!["year" => years, "month" => months, "v" => vals].unwrap()
    }

    #[test]
    fn produce_partitions_no_partitioning() {
        let df = df_with_keys(&[(2024, 1, "a"), (2024, 2, "b")]);
        let table = test_table("orders", &[], &[]);
        let mapping = test_mapping("orders");

        let outs = produce_partitions(&df, &mapping, &table).unwrap();
        assert_eq!(outs.len(), 1);
        assert_eq!(outs[0].key, "orders/m.parquet");
        assert_eq!(&outs[0].bytes[..4], b"PAR1");
    }

    #[test]
    fn produce_partitions_groups_by_key_tuple_and_drops_key_columns() {
        let df = df_with_keys(&[(2024, 1, "a"), (2024, 2, "b"), (2024, 2, "c")]);
        let table = test_table("transactions", &["year", "month"], &[]);
        let mapping = test_mapping("transactions");

        let outs = produce_partitions(&df, &mapping, &table).unwrap();
        assert_eq!(outs.len(), 2, "expected 2 partitions, got {}", outs.len());

        let keys: Vec<&String> = outs.iter().map(|o| &o.key).collect();
        assert!(keys.iter().any(|k| *k == "transactions/year=2024/month=1/m.parquet"), "{keys:?}");
        assert!(keys.iter().any(|k| *k == "transactions/year=2024/month=2/m.parquet"), "{keys:?}");

        // Key columns must not be inside the parquet payload; hive
        // re-materializes them from the path on read.
        for out in &outs {
            let cursor = std::io::Cursor::new(out.bytes.clone());
            let read = ParquetReader::new(cursor).finish().unwrap();
            let names: Vec<String> = read
                .get_column_names()
                .iter()
                .map(|n| n.to_string())
                .collect();
            assert_eq!(names, vec!["v".to_string()], "key columns leaked into {names:?}");
        }
    }

    #[test]
    fn produce_partitions_encodes_values_per_type() {
        // string keys percent-encode; bools render true/false.
        let df = df![
            "account" => ["visa gold", "chequing"],
            "flag" => [true, false],
            "v" => ["a", "b"]
        ]
        .unwrap();
        let table = test_table("t", &["account", "flag"], &[]);
        let outs = produce_partitions(&df, &test_mapping("t"), &table).unwrap();
        let keys: Vec<&String> = outs.iter().map(|o| &o.key).collect();
        assert!(
            keys.iter().any(|k| *k == "t/account=visa%20gold/flag=true/m.parquet"),
            "{keys:?}"
        );
        assert!(
            keys.iter().any(|k| *k == "t/account=chequing/flag=false/m.parquet"),
            "{keys:?}"
        );
    }

    #[test]
    fn produce_partitions_null_key_fails_with_column_and_count() {
        let df = df![
            "year" => [Some(2024i64), None, None],
            "v" => ["a", "b", "c"]
        ]
        .unwrap();
        let table = test_table("t", &["year"], &[]);
        let err = produce_partitions(&df, &test_mapping("t"), &table).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("`year`") && msg.contains("2 null"), "{msg}");
    }

    #[test]
    fn produce_partitions_distinct_keys_per_mapping() {
        // Two mappings writing the same table at the same tuple must
        // produce different keys, otherwise the second overwrites the first.
        let df = df_with_keys(&[(2024, 1, "a")]);
        let table = test_table("transactions", &["year", "month"], &[]);

        let mut visa = test_mapping("transactions");
        visa.id = "scotia_visa_mapping".into();
        let mut chq = test_mapping("transactions");
        chq.id = "scotia_chq_mapping".into();

        let visa_key = produce_partitions(&df, &visa, &table).unwrap()[0].key.clone();
        let chq_key = produce_partitions(&df, &chq, &table).unwrap()[0].key.clone();
        assert_ne!(visa_key, chq_key);
    }

    #[test]
    fn stale_keys_reaps_only_this_mappings_unwritten_outputs() {
        let existing: Vec<String> = [
            "pipelines/p/t/year=2026/month=1/m.parquet", // rewritten
            "pipelines/p/t/year=2026/month=2/m.parquet", // vanished partition
            "pipelines/p/t/account=visa/m.parquet",      // old layout
            "pipelines/p/t/year=2026/month=1/other.parquet", // other mapping
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let uploaded: std::collections::HashSet<String> =
            ["pipelines/p/t/year=2026/month=1/m.parquet".to_string()].into();
        let mut stale = stale_keys(&existing, &uploaded, "m");
        stale.sort();
        assert_eq!(
            stale,
            vec![
                "pipelines/p/t/account=visa/m.parquet".to_string(),
                "pipelines/p/t/year=2026/month=2/m.parquet".to_string(),
            ]
        );
    }

    #[test]
    fn dedup_rows_keeps_first_and_counts() {
        let df = df![
            "k" => ["a", "b", "a", "a"],
            "v" => [1i64, 2, 3, 4]
        ]
        .unwrap();
        let table = test_table("t", &[], &["k"]);
        let (deduped, dropped) = dedup_rows(&df, &table).unwrap();
        assert_eq!(dropped, 2);
        assert_eq!(deduped.height(), 2);
        // first occurrence survives (v=1 for k=a)
        let v = deduped.column("v").unwrap().i64().unwrap();
        let ks = deduped.column("k").unwrap().str().unwrap();
        for i in 0..deduped.height() {
            if ks.get(i) == Some("a") {
                assert_eq!(v.get(i), Some(1));
            }
        }
        // no keys: identity
        let none = test_table("t", &[], &[]);
        let (same, zero) = dedup_rows(&df, &none).unwrap();
        assert_eq!(zero, 0);
        assert_eq!(same.height(), df.height());
    }

    proptest! {
        // Output partitions cover exactly the distinct key tuples of the
        // input frame, whatever the values are.
        #[test]
        fn partitions_cover_distinct_tuples(
            rows in proptest::collection::vec((2022i64..=2025, 1i64..=12), 1..=20),
        ) {
            let data: Vec<(i64, i64, &str)> =
                rows.iter().map(|(y, m)| (*y, *m, "x")).collect();
            let df = df_with_keys(&data);
            let table = test_table("t", &["year", "month"], &[]);
            let outs = produce_partitions(&df, &test_mapping("t"), &table).unwrap();

            let mut got: std::collections::HashSet<String> = std::collections::HashSet::new();
            for out in &outs {
                got.insert(out.key.clone());
            }
            let expected: std::collections::HashSet<String> = rows.iter()
                .map(|(y, m)| format!("t/year={y}/month={m}/m.parquet"))
                .collect();
            prop_assert_eq!(got, expected);
        }
    }

    // -----------------------------------------------------------------------
    // upload_partitions, partition upload failures identify the bad key.
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
                key: "t/year=2024/month=01/a.parquet".into(),
                bytes: vec![0u8],
            },
            PartitionOutput {
                key: "t/year=2024/month=02/b.parquet".into(),
                bytes: vec![0u8],
            },
            PartitionOutput {
                key: "t/year=2024/month=03/c.parquet".into(),
                bytes: vec![0u8],
            },
        ];
        let uploader = MockUploader {
            fail_on: "t/year=2024/month=02/b.parquet".into(),
        };

        let err = upload_partitions(&uploader, &partitions).unwrap_err();
        match err {
            PipelineError::PartitionUploadFailed { key, message } => {
                assert_eq!(key, "t/year=2024/month=02/b.parquet");
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
