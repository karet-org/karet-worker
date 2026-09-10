//! Rollup execution: group-by + aggregate an analytic table into a smaller one.
//!
//! The input is the **table**, not the run's batch: the traffic shipper uploads
//! every ten minutes, so a rollup that saw only the in-run frame would be wrong
//! in all but the first run of each day. Runs therefore re-read the source
//! partitions they touched and recompute the target partitions those cover,
//! which validation makes sound by requiring the target's partition keys to be
//! a subset of the source's (see `config::validate`).
//!
//! Partition key columns live in the object path, not in the Parquet, so
//! reading a partition means restoring them from its hive segments.

use std::collections::BTreeSet;
use std::io::Cursor;

use polars::prelude::*;

use crate::config::{AggFn, AnalyticTable, Rollup, RollupAggregate};
use crate::evaluator::{compile, CompileCtx};

#[derive(Debug, thiserror::Error)]
pub enum RollupError {
    #[error("rollup `{rollup}`: {message}")]
    Plan { rollup: String, message: String },
    #[error("rollup `{rollup}`: reading {key}: {source}")]
    Read {
        rollup: String,
        key: String,
        source: PolarsError,
    },
    #[error("rollup `{rollup}`: partition path `{path}` is not hive-encoded")]
    Path { rollup: String, path: String },
}

/// Output column names an aggregate contributes to the target table.
///
/// `avg` is stored as a sum/count pair so a coarser rollup can be built from a
/// finer one; a stored mean cannot be re-averaged without the weights.
pub fn output_columns(agg: &RollupAggregate) -> Vec<String> {
    match agg.fn_ {
        AggFn::Avg => vec![format!("{}_sum", agg.name), format!("{}_count", agg.name)],
        _ => vec![agg.name.clone()],
    }
}

/// True for aggregates that are correct at their own grain but can never be
/// re-aggregated upward. The editor labels these "grain-locked".
pub fn is_grain_locked(agg: &RollupAggregate) -> bool {
    matches!(agg.fn_, AggFn::CountDistinct | AggFn::Median)
}

/// Build the aggregate expressions for one rollup.
fn agg_exprs(rollup: &Rollup) -> Result<Vec<Expr>, RollupError> {
    let mut out = Vec::with_capacity(rollup.aggregates.len() + 1);
    for agg in &rollup.aggregates {
        // An aggregate may count or sum a subset of the group's rows.
        let masked = |e: Expr| -> Result<Expr, RollupError> {
            match &agg.where_ {
                None => Ok(e),
                Some(pred) => {
                    let ctx = CompileCtx::empty();
                    let cond = compile(pred, &ctx).map_err(|err| RollupError::Plan {
                        rollup: rollup.id.clone(),
                        message: format!("aggregate `{}` where: {err}", agg.name),
                    })?;
                    Ok(e.filter(cond))
                }
            }
        };

        let column = || -> Result<Expr, RollupError> {
            let name = agg.column.as_deref().ok_or_else(|| RollupError::Plan {
                rollup: rollup.id.clone(),
                message: format!("aggregate `{}` needs a column", agg.name),
            })?;
            masked(col(name))
        };

        let expr = match agg.fn_ {
            // `len()` cannot be filtered inside an aggregation (it yields a
            // list), so a conditional count sums the predicate instead.
            AggFn::Count => match &agg.where_ {
                None => len().cast(DataType::Int64).alias(agg.name.as_str()),
                Some(pred) => {
                    let ctx = CompileCtx::empty();
                    let cond = compile(pred, &ctx).map_err(|err| RollupError::Plan {
                        rollup: rollup.id.clone(),
                        message: format!("aggregate `{}` where: {err}", agg.name),
                    })?;
                    cond.cast(DataType::Int64)
                        .sum()
                        .alias(agg.name.as_str())
                }
            },
            AggFn::Sum => column()?.sum().alias(agg.name.as_str()),
            AggFn::Min => column()?.min().alias(agg.name.as_str()),
            AggFn::Max => column()?.max().alias(agg.name.as_str()),
            AggFn::CountDistinct => column()?
                .n_unique()
                .cast(DataType::Int64)
                .alias(agg.name.as_str()),
            AggFn::Median => column()?.median().alias(agg.name.as_str()),
            AggFn::Avg => {
                // Two columns, so the pair stays re-aggregatable.
                out.push(column()?.sum().alias(format!("{}_sum", agg.name)));
                column()?
                    .count()
                    .cast(DataType::Int64)
                    .alias(format!("{}_count", agg.name))
            }
        };
        out.push(expr);
    }
    Ok(out)
}

/// Group `input` by the rollup's `group_by` and apply its aggregates.
pub fn plan(input: LazyFrame, rollup: &Rollup) -> Result<LazyFrame, RollupError> {
    if rollup.group_by.is_empty() {
        return Err(RollupError::Plan {
            rollup: rollup.id.clone(),
            message: "group_by is empty".to_string(),
        });
    }
    let keys: Vec<Expr> = rollup.group_by.iter().map(|c| col(c.as_str())).collect();
    Ok(input.group_by(keys).agg(agg_exprs(rollup)?))
}

/// One hive path segment pair, e.g. `date=2026-09-09`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct HiveSegment {
    pub key: String,
    pub value: String,
}

/// Percent-decode a hive path segment value (`produce_partitions` escapes
/// anything outside `[A-Za-z0-9._-]`).
fn decode_segment(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(&value[i + 1..i + 3], 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Split `<table>/k1=v1/k2=v2/` into its segments. An unpartitioned
/// directory yields none.
pub fn parse_hive_dir(dir: &str) -> Vec<HiveSegment> {
    dir.split('/')
        .filter_map(|part| {
            let (key, value) = part.split_once('=')?;
            Some(HiveSegment {
                key: key.to_string(),
                value: decode_segment(value),
            })
        })
        .collect()
}

/// Directory prefixes (relative to the warehouse pipeline prefix) the run
/// wrote under `table_id`, e.g. `traffic/date=2026-09-09/`. Object keys
/// carry the partition, so the run's own uploads say what to recompute.
pub fn affected_dirs(uploaded_keys: &[String], table_id: &str) -> BTreeSet<String> {
    let table_prefix = format!("{table_id}/");
    uploaded_keys
        .iter()
        .filter(|k| k.starts_with(&table_prefix))
        .filter_map(|k| k.rfind('/').map(|i| k[..=i].to_string()))
        .collect()
}

/// Restore a partition key column dropped into the object path, typed from
/// the source table's schema so group-by keys match the target's declared
/// types.
fn segment_literal(seg: &HiveSegment, table: &AnalyticTable) -> Expr {
    let declared = table
        .schema
        .iter()
        .find(|c| c.name == seg.key)
        .map(|c| c.type_.as_str())
        .unwrap_or("string");
    let value = seg.value.as_str();
    let expr = match declared {
        "int64" => value
            .parse::<i64>()
            .map(lit)
            .unwrap_or_else(|_| lit(NULL).cast(DataType::Int64)),
        "float64" => value
            .parse::<f64>()
            .map(lit)
            .unwrap_or_else(|_| lit(NULL).cast(DataType::Float64)),
        "bool" => value
            .parse::<bool>()
            .map(lit)
            .unwrap_or_else(|_| lit(NULL).cast(DataType::Boolean)),
        "date" => lit(value).str().to_date(StrptimeOptions {
            format: Some("%Y-%m-%d".into()),
            strict: false,
            ..Default::default()
        }),
        _ => lit(value),
    };
    expr.alias(seg.key.as_str())
}

/// Read one partition's Parquet objects and re-attach its hive key columns.
pub fn read_partition(
    rollup: &Rollup,
    source: &AnalyticTable,
    dir: &str,
    objects: &[(String, Vec<u8>)],
) -> Result<Option<DataFrame>, RollupError> {
    let segments = parse_hive_dir(dir);
    if !source.partition_keys.is_empty() && segments.is_empty() {
        return Err(RollupError::Path {
            rollup: rollup.id.clone(),
            path: dir.to_string(),
        });
    }

    let mut frames: Vec<LazyFrame> = Vec::with_capacity(objects.len());
    for (key, bytes) in objects {
        let df = ParquetReader::new(Cursor::new(bytes.as_slice()))
            .finish()
            .map_err(|source| RollupError::Read {
                rollup: rollup.id.clone(),
                key: key.clone(),
                source,
            })?;
        let mut lf = df.lazy();
        if !segments.is_empty() {
            lf = lf.with_columns(
                segments
                    .iter()
                    .map(|s| segment_literal(s, source))
                    .collect::<Vec<_>>(),
            );
        }
        frames.push(lf);
    }
    if frames.is_empty() {
        return Ok(None);
    }

    // Mappings sharing a table may write different column subsets.
    let args = UnionArgs {
        rechunk: true,
        diagonal: true,
        ..Default::default()
    };
    let lf = concat(&frames, args).map_err(|source| RollupError::Read {
        rollup: rollup.id.clone(),
        key: dir.to_string(),
        source,
    })?;
    let df = plan(lf, rollup)
        .and_then(|planned| {
            planned.collect().map_err(|e| RollupError::Read {
                rollup: rollup.id.clone(),
                key: dir.to_string(),
                source: e,
            })
        })
        .map_err(|e| e)?;
    Ok(Some(df))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ColumnSchema;

    fn column(name: &str, type_: &str) -> ColumnSchema {
        ColumnSchema {
            name: name.to_string(),
            type_: type_.to_string(),
            nullable: None,
            assertions: None,
            path: None,
        }
    }

    fn table(id: &str, partition_keys: &[&str]) -> AnalyticTable {
        AnalyticTable {
            id: id.to_string(),
            name: id.to_string(),
            schema: vec![
                column("date", "date"),
                column("host", "string"),
                column("bytes", "int64"),
                column("status", "int64"),
            ],
            partition_keys: partition_keys.iter().map(|s| s.to_string()).collect(),
            dedup_keys: vec![],
        }
    }

    fn agg(name: &str, fn_: AggFn, column: Option<&str>) -> RollupAggregate {
        RollupAggregate {
            name: name.to_string(),
            fn_,
            column: column.map(|s| s.to_string()),
            where_: None,
        }
    }

    fn rollup(aggregates: Vec<RollupAggregate>) -> Rollup {
        Rollup {
            id: "daily".to_string(),
            name: Some("Daily".to_string()),
            source_table_id: "traffic".to_string(),
            analytic_table_id: "traffic_daily".to_string(),
            group_by: vec!["host".to_string()],
            aggregates,
        }
    }

    fn input() -> LazyFrame {
        df![
            "host" => ["a", "a", "b"],
            "bytes" => [10i64, 30, 5],
            "status" => [200i64, 500, 200],
        ]
        .unwrap()
        .lazy()
    }

    fn value(df: &DataFrame, column: &str, host: &str) -> f64 {
        let idx = df
            .column("host")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .position(|v| v == Some(host))
            .unwrap();
        df.column(column)
            .unwrap()
            .get(idx)
            .unwrap()
            .try_extract::<f64>()
            .unwrap()
    }

    #[test]
    fn count_and_sum_group_per_key() {
        let r = rollup(vec![
            agg("requests", AggFn::Count, None),
            agg("bytes", AggFn::Sum, Some("bytes")),
        ]);
        let out = plan(input(), &r).unwrap().collect().unwrap();
        assert_eq!(out.height(), 2);
        assert_eq!(value(&out, "requests", "a"), 2.0);
        assert_eq!(value(&out, "bytes", "a"), 40.0);
        assert_eq!(value(&out, "requests", "b"), 1.0);
    }

    #[test]
    fn avg_is_stored_as_a_sum_count_pair() {
        let r = rollup(vec![agg("size", AggFn::Avg, Some("bytes"))]);
        let out = plan(input(), &r).unwrap().collect().unwrap();
        assert!(out.column("size").is_err(), "no bare mean column");
        assert_eq!(value(&out, "size_sum", "a"), 40.0);
        assert_eq!(value(&out, "size_count", "a"), 2.0);
        assert_eq!(
            output_columns(&agg("size", AggFn::Avg, Some("bytes"))),
            vec!["size_sum".to_string(), "size_count".to_string()]
        );
    }

    #[test]
    fn a_where_clause_narrows_one_aggregate_only() {
        let mut errors = agg("errors", AggFn::Count, None);
        errors.where_ = Some(crate::ast::AstNode::Ge {
            left: Box::new(crate::ast::AstNode::Col {
                name: "status".to_string(),
            }),
            right: Box::new(crate::ast::AstNode::Num { value: 500.0 }),
        });
        let r = rollup(vec![agg("requests", AggFn::Count, None), errors]);
        let out = plan(input(), &r).unwrap().collect().unwrap();
        assert_eq!(value(&out, "requests", "a"), 2.0);
        assert_eq!(value(&out, "errors", "a"), 1.0);
        assert_eq!(value(&out, "errors", "b"), 0.0);
    }

    #[test]
    fn distinct_and_median_are_grain_locked() {
        assert!(is_grain_locked(&agg(
            "visitors",
            AggFn::CountDistinct,
            Some("host")
        )));
        assert!(is_grain_locked(&agg("p50", AggFn::Median, Some("bytes"))));
        assert!(!is_grain_locked(&agg("n", AggFn::Count, None)));
        assert!(!is_grain_locked(&agg("b", AggFn::Sum, Some("bytes"))));
    }

    #[test]
    fn an_aggregate_without_its_column_is_rejected() {
        let r = rollup(vec![agg("bytes", AggFn::Sum, None)]);
        let err = match plan(input(), &r) {
            Err(e) => e,
            Ok(_) => panic!("an aggregate without a column must be rejected"),
        };
        assert!(err.to_string().contains("needs a column"), "{err}");
    }

    #[test]
    fn hive_dirs_round_trip_through_parsing() {
        let segs = parse_hive_dir("traffic/date=2026-09-09/host=etl.joeyshi.xyz/");
        assert_eq!(
            segs,
            vec![
                HiveSegment { key: "date".into(), value: "2026-09-09".into() },
                HiveSegment { key: "host".into(), value: "etl.joeyshi.xyz".into() },
            ]
        );
        // Percent-escaped values decode back.
        let escaped = parse_hive_dir("t/host=a%2Fb/");
        assert_eq!(escaped[0].value, "a/b");
        assert!(parse_hive_dir("traffic/").is_empty());
    }

    #[test]
    fn affected_dirs_are_the_partitions_the_run_wrote() {
        let uploaded = vec![
            "traffic/date=2026-09-09/m.parquet".to_string(),
            "traffic/date=2026-09-09/m2.parquet".to_string(),
            "traffic/date=2026-09-10/m.parquet".to_string(),
            "other/date=2026-09-09/m.parquet".to_string(),
        ];
        let dirs = affected_dirs(&uploaded, "traffic");
        assert_eq!(
            dirs.into_iter().collect::<Vec<_>>(),
            vec![
                "traffic/date=2026-09-09/".to_string(),
                "traffic/date=2026-09-10/".to_string(),
            ]
        );
    }

    #[test]
    fn reading_a_partition_restores_its_key_columns() {
        // Parquet on disk lacks `date`; the path supplies it, and the rollup
        // groups by it.
        let mut df = df!["host" => ["a", "a"], "bytes" => [1i64, 2]].unwrap();
        let bytes = crate::pipeline::write_parquet_bytes(&mut df).unwrap();
        let source = table("traffic", &["date"]);
        let mut r = rollup(vec![agg("requests", AggFn::Count, None)]);
        r.group_by = vec!["date".to_string(), "host".to_string()];

        let out = read_partition(
            &r,
            &source,
            "traffic/date=2026-09-09/",
            &[("k".to_string(), bytes)],
        )
        .unwrap()
        .unwrap();
        assert_eq!(out.height(), 1);
        assert_eq!(
            out.column("date").unwrap().dtype(),
            &DataType::Date,
            "restored key uses the declared type"
        );
        assert_eq!(value(&out, "requests", "a"), 2.0);
    }
}
