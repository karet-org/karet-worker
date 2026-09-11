//! AST evaluator that compiles `AstNode` trees into Polars `Expr` values.
//!
//! Compilation is pure: [`compile`] walks an [`AstNode`] and returns a
//! Polars [`Expr`] that evaluates the node vectorized over a `DataFrame`.
//! [`CompileCtx`] carries a registry of precompiled [`DimensionMatcher`]s
//! keyed by dotted lookup id for nodes that can't be expressed as pure
//! Polars operators.

use std::collections::HashMap;
use std::sync::Arc;

use polars::prelude::*;

use crate::ast::{AstNode, CastType};
use crate::error::EvalError;
use crate::dimension::DimensionMatcher;

/// Context passed to [`compile`].
///
/// Carries a registry of [`DimensionMatcher`]s keyed by dimension id
/// (`"categories"`, `"categories.merchants"`, …). The registry is typically
/// built once per job from the `Pipeline_Config` via
/// [`crate::dimension::build_inline_registry`] and shared by reference across every
/// mapping column compiled in that job.
pub struct CompileCtx<'a> {
    pub dimensions: &'a HashMap<String, Arc<DimensionMatcher>>,
}

impl<'a> CompileCtx<'a> {
    /// Create a new context wrapping a reference to the lookup registry.
    pub fn new(dimensions: &'a HashMap<String, Arc<DimensionMatcher>>) -> Self {
        Self { dimensions }
    }
}

/// Compile an AST node to a Polars expression.
pub fn compile(node: &AstNode, ctx: &CompileCtx) -> Result<Expr, EvalError> {
    match node {
        // --- References and literals ---
        AstNode::Col { name } => Ok(col(name.as_str())),
        AstNode::Str { value } => Ok(lit(value.as_str())),
        AstNode::Num { value } => Ok(lit(*value)),
        AstNode::Bool { value } => Ok(lit(*value)),
        AstNode::Null => Ok(lit(NULL)),

        // --- Arithmetic ---
        AstNode::Add { left, right } => Ok(compile(left, ctx)? + compile(right, ctx)?),
        AstNode::Sub { left, right } => Ok(compile(left, ctx)? - compile(right, ctx)?),
        AstNode::Mul { left, right } => Ok(compile(left, ctx)? * compile(right, ctx)?),
        AstNode::Div { left, right } => Ok(compile(left, ctx)? / compile(right, ctx)?),

        // --- Comparisons ---
        AstNode::Eq { left, right } => Ok(compile(left, ctx)?.eq(compile(right, ctx)?)),
        AstNode::Ne { left, right } => Ok(compile(left, ctx)?.neq(compile(right, ctx)?)),
        AstNode::Gt { left, right } => Ok(compile(left, ctx)?.gt(compile(right, ctx)?)),
        AstNode::Lt { left, right } => Ok(compile(left, ctx)?.lt(compile(right, ctx)?)),
        AstNode::Ge { left, right } => Ok(compile(left, ctx)?.gt_eq(compile(right, ctx)?)),
        AstNode::Le { left, right } => Ok(compile(left, ctx)?.lt_eq(compile(right, ctx)?)),
        AstNode::Contains { input, pattern } => {
            Ok(compile(input, ctx)?
                .str()
                .contains_literal(compile(pattern, ctx)?))
        }

        AstNode::FromUnix { input, unit } => {
            // Seconds (default) or milliseconds since the epoch -> Date.
            let millis = match unit.as_deref() {
                Some("ms") => compile(input, ctx)?,
                _ => compile(input, ctx)? * lit(1000.0),
            };
            Ok(millis
                .cast(DataType::Int64)
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .cast(DataType::Date))
        }

        // --- Boolean composition ---
        AstNode::And { left, right } => Ok(compile(left, ctx)?.and(compile(right, ctx)?)),
        AstNode::Or { left, right } => Ok(compile(left, ctx)?.or(compile(right, ctx)?)),
        AstNode::Not { input } => Ok(compile(input, ctx)?.not()),

        // --- String ops ---
        AstNode::Upper { input } => Ok(compile(input, ctx)?.str().to_uppercase()),
        AstNode::Lower { input } => Ok(compile(input, ctx)?.str().to_lowercase()),
        // `strip_chars(lit(NULL))` strips ASCII whitespace, matching SQL-style
        // TRIM semantics.
        AstNode::Trim { input } => Ok(compile(input, ctx)?.str().strip_chars(lit(NULL))),
        AstNode::Substring { input, start, length } => {
            // Polars `str().slice(offset: Expr, length: Expr)`, `lit(NULL)`
            // for length means "to end".
            let len_expr = match length {
                Some(l) => lit(*l),
                None => lit(NULL),
            };
            Ok(compile(input, ctx)?.str().slice(lit(*start), len_expr))
        }
        AstNode::Concat { sep, args } => {
            let arg_exprs: Vec<Expr> = args
                .iter()
                .map(|a| compile(a, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(concat_str(arg_exprs, sep.as_str(), true))
        }

        // --- Control flow ---
        AstNode::If { cond, then, r#else } => Ok(when(compile(cond, ctx)?)
            .then(compile(then, ctx)?)
            .otherwise(compile(r#else, ctx)?)),

        // Empty `args` -> NULL literal; otherwise Polars `coalesce`.
        AstNode::Coalesce { args } => {
            if args.is_empty() {
                return Ok(lit(NULL));
            }
            let exprs: Vec<Expr> = args
                .iter()
                .map(|a| compile(a, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(coalesce(&exprs))
        }

        // --- Cast ---
        AstNode::Cast { input, to } => {
            let dtype = match to {
                CastType::Int64 => DataType::Int64,
                CastType::Float64 => DataType::Float64,
                CastType::String => DataType::String,
                CastType::Date => DataType::Date,
            };
            Ok(compile(input, ctx)?.cast(dtype))
        }

        // --- Date parsing ---
        // Polars 0.53's `StrptimeOptions::strict` defaults to `true`, which
        // errors on malformed input. We set it to `false` so malformed
        // strings parse to null instead of failing the whole pipeline
        // (Req 3.6, malformed AST structures surface as JSON parse errors
        // at config-load time, malformed *data* must not).
        AstNode::Year { input } => Ok(compile(input, ctx)?
            .dt()
            .year()
            .cast(DataType::Int64)),
        AstNode::Month { input } => Ok(compile(input, ctx)?
            .dt()
            .month()
            .cast(DataType::Int64)),
        AstNode::Day { input } => Ok(compile(input, ctx)?
            .dt()
            .day()
            .cast(DataType::Int64)),

        AstNode::ParseDate { input, format } => {
            let options = StrptimeOptions {
                format: Some(format.as_str().into()),
                strict: false,
                ..Default::default()
            };
            Ok(compile(input, ctx)?.str().to_date(options))
        }

        // --- Lookup ---
        //
        // Resolve the dotted lookup id against the registry; compile the
        // `input` expression; wrap the matcher in a Polars `map` closure that
        // Dimension probe over each string in the input column, returning the
        // requested value column (or `on_miss`).
        //
        // The closure must be `Fn + Send + Sync + 'static`, so we clone an
        // `Arc<DimensionMatcher>` into it rather than capturing `ctx`.
        AstNode::DimRef { dim_id, value, input } => {
            let matcher = ctx
                .dimensions
                .get(dim_id)
                .ok_or_else(|| EvalError::UnknownDimension { id: dim_id.clone() })?
                .clone();
            let value_index = matcher.value_index(value.as_deref()).ok_or_else(|| {
                EvalError::UnknownDimensionValue {
                    id: dim_id.clone(),
                    value: value.clone().unwrap_or_default(),
                }
            })?;
            let input_expr = compile(input, ctx)?;
            Ok(input_expr.map(
                move |column| {
                    let result: StringChunked = column
                        .str()?
                        .iter()
                        .map(|s_opt| s_opt.and_then(|s| matcher.lookup(s, value_index)))
                        .collect();
                    Ok(result.into_column())
                },
                |_schema, field| Ok(Field::new(field.name().clone(), DataType::String)),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::AstNode;
    use proptest::prelude::*;

    // Malformed date strings must parse to null rather than erroring.
    // Mixed input `["2024-01-01", "not-a-date"]` should yield
    // `[Some(_), None]`.
    #[test]
    fn date_parts_extract_as_int64() {
        let df = df!["d" => ["2026-09-06", "2024-01-31"]].unwrap();
        let registry = HashMap::new();
        let ctx = CompileCtx::new(&registry);
        let parse = AstNode::ParseDate {
            input: Box::new(AstNode::Col { name: "d".into() }),
            format: "%Y-%m-%d".into(),
        };
        let out = df
            .lazy()
            .select([
                compile(&AstNode::Year { input: Box::new(parse.clone()) }, &ctx)
                    .unwrap()
                    .alias("y"),
                compile(&AstNode::Month { input: Box::new(parse.clone()) }, &ctx)
                    .unwrap()
                    .alias("m"),
                compile(&AstNode::Day { input: Box::new(parse) }, &ctx)
                    .unwrap()
                    .alias("day"),
            ])
            .collect()
            .unwrap();
        assert_eq!(out.column("y").unwrap().i64().unwrap().get(0), Some(2026));
        assert_eq!(out.column("m").unwrap().i64().unwrap().get(0), Some(9));
        assert_eq!(out.column("day").unwrap().i64().unwrap().get(1), Some(31));
    }

    #[test]
    fn from_unix_converts_epoch_seconds_and_millis() {
        // 1788934940.86 s and the same instant in ms -> 2026-09-09 (UTC).
        let df = df!["s" => [1788934940.86_f64], "ms" => [1788934940860.0_f64]].unwrap();
        let registry = HashMap::new();
        let ctx = CompileCtx::new(&registry);
        let out = df
            .lazy()
            .select([
                compile(
                    &AstNode::FromUnix {
                        input: Box::new(AstNode::Col { name: "s".into() }),
                        unit: None,
                    },
                    &ctx,
                )
                .unwrap()
                .alias("from_s"),
                compile(
                    &AstNode::FromUnix {
                        input: Box::new(AstNode::Col { name: "ms".into() }),
                        unit: Some("ms".into()),
                    },
                    &ctx,
                )
                .unwrap()
                .alias("from_ms"),
            ])
            .collect()
            .unwrap();

        let a = out.column("from_s").unwrap();
        let b = out.column("from_ms").unwrap();
        assert_eq!(a.dtype(), &DataType::Date);
        assert_eq!(
            a.as_materialized_series().get(0).unwrap(),
            b.as_materialized_series().get(0).unwrap(),
            "seconds and millis of the same instant agree"
        );
        // Sanity-check the actual date via year/month/day.
        let parts = out
            .lazy()
            .select([
                col("from_s").dt().year().alias("y"),
                col("from_s").dt().month().alias("m"),
                col("from_s").dt().day().alias("d"),
            ])
            .collect()
            .unwrap();
        assert_eq!(parts.column("y").unwrap().i32().unwrap().get(0), Some(2026));
        assert_eq!(parts.column("m").unwrap().i8().unwrap().get(0), Some(9));
        assert_eq!(parts.column("d").unwrap().i8().unwrap().get(0), Some(9));
    }

    #[test]
    fn parse_date_produces_null_on_malformed() {
        let df = DataFrame::new(
            2,
            vec![Column::new("d".into(), &["2024-01-01", "not-a-date"])],
        )
        .unwrap();

        let ast = AstNode::ParseDate {
            input: Box::new(AstNode::Col { name: "d".into() }),
            format: "%Y-%m-%d".into(),
        };
        let registry = HashMap::new();
        let expr = compile(&ast, &CompileCtx::new(&registry)).unwrap();

        let out = df.lazy().select([expr.alias("v")]).collect().unwrap();
        let series = out.column("v").unwrap().as_materialized_series();
        let dates = series.date().unwrap();

        assert!(dates.phys.get(0).is_some(), "valid date should parse");
        assert!(dates.phys.get(1).is_none(), "malformed date should be null");
    }

    // Two-row coalesce: `[("HIT", _), (null, "FB")]` -> `["HIT", "FB"]`.
    #[test]
    fn coalesce_returns_first_non_null() {
        let df = DataFrame::new(
            2,
            vec![
                Column::new("a".into(), &[Some("HIT"), None]),
                Column::new("b".into(), &["FALLBACK", "FALLBACK"]),
            ],
        )
        .unwrap();

        let ast = AstNode::Coalesce {
            args: vec![
                AstNode::Col { name: "a".into() },
                AstNode::Col { name: "b".into() },
            ],
        };
        let registry = HashMap::new();
        let expr = compile(&ast, &CompileCtx::new(&registry)).unwrap();

        let out = df.lazy().select([expr.alias("v")]).collect().unwrap();
        let series = out.column("v").unwrap().as_materialized_series();
        let s = series.str().unwrap();
        assert_eq!(s.get(0), Some("HIT"));
        assert_eq!(s.get(1), Some("FALLBACK"));
    }

    #[test]
    fn coalesce_all_null_returns_null() {
        let df = DataFrame::new(
            1,
            vec![Column::new("a".into(), &[None::<&str>])],
        )
        .unwrap();

        let ast = AstNode::Coalesce {
            args: vec![
                AstNode::Col { name: "a".into() },
                AstNode::Null,
            ],
        };
        let registry = HashMap::new();
        let expr = compile(&ast, &CompileCtx::new(&registry)).unwrap();

        let out = df.lazy().select([expr.alias("v")]).collect().unwrap();
        let series = out.column("v").unwrap().as_materialized_series();
        let s = series.str().unwrap();
        assert_eq!(s.get(0), None);
    }

    #[test]
    fn coalesce_empty_args_is_null() {
        let df = DataFrame::new(
            1,
            vec![Column::new("a".into(), &["irrelevant"])],
        )
        .unwrap();

        let ast = AstNode::Coalesce { args: vec![] };
        let registry = HashMap::new();
        let expr = compile(&ast, &CompileCtx::new(&registry)).unwrap();

        let out = df.lazy().select([expr.alias("v")]).collect().unwrap();
        let series = out.column("v").unwrap().as_materialized_series();
        assert!(series.is_null().get(0).unwrap_or(false));
    }

    #[test]
    fn concat_ignores_null_args() {
        let df = DataFrame::new(
            2,
            vec![
                Column::new("a".into(), &["FROM", "FROM"]),
                Column::new("b".into(), &[Some("ACCT"), None]),
            ],
        )
        .unwrap();

        let ast = AstNode::Concat {
            sep: " ".into(),
            args: vec![
                AstNode::Col { name: "a".into() },
                AstNode::Col { name: "b".into() },
            ],
        };
        let registry = HashMap::new();
        let expr = compile(&ast, &CompileCtx::new(&registry)).unwrap();

        let out = df.lazy().select([expr.alias("v")]).collect().unwrap();
        let series = out.column("v").unwrap().as_materialized_series();
        let s = series.str().unwrap();
        assert_eq!(s.get(0), Some("FROM ACCT"));
        assert_eq!(s.get(1), Some("FROM"));
    }

    fn arb_column_name() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9_]{0,7}".prop_map(|s| s)
    }

    proptest! {
        // Col AST evaluation returns the row's column value
        //
        // For any row R and any column name n present in R, evaluating
        // `Col { name: n }` on R returns R[n]. We test this by building a
        // single-row DataFrame where each column has a unique, column-indexed
        // value (`val_{i}`), compiling `Col { name: cols[target] }` to a
        // Polars expression, projecting the frame through it, and asserting
        // that the resulting scalar matches the target column's value.
        #[test]
        fn col_returns_row_value(
            cols in proptest::collection::vec(arb_column_name(), 1..=4)
                .prop_map(|v| {
                    // Dedup while preserving first-seen order (DataFrame
                    // rejects duplicate column names, and column order affects
                    // which index maps to which value).
                    let mut seen = std::collections::HashSet::new();
                    v.into_iter().filter(|n| seen.insert(n.clone())).collect::<Vec<_>>()
                }),
            target_idx in 0usize..4,
        ) {
            prop_assume!(!cols.is_empty());
            let target = target_idx % cols.len();

            // Each column gets a distinct value based on its position so we
            // can prove the compiled expression picked out the right one.
            let mut cols_built: Vec<Column> = Vec::new();
            for (i, name) in cols.iter().enumerate() {
                let val = format!("val_{i}");
                cols_built.push(Column::new(name.as_str().into(), &[val.as_str()]));
            }
            let df = DataFrame::new(1, cols_built).unwrap();

            let target_name = cols[target].clone();
            let expected = format!("val_{target}");

            let expr = compile(
                &AstNode::Col { name: target_name.clone() },
                &CompileCtx::new(&HashMap::new()),
            )
            .unwrap();
            let out = df.lazy().select([expr.alias("v")]).collect().unwrap();

            let series = out.column("v").unwrap().as_materialized_series();
            let s = series.str().unwrap();
            prop_assert_eq!(s.get(0), Some(expected.as_str()));
        }

        // dim_ref AST evaluation equals a direct matcher call.
        //
        // Build a single-row DataFrame with `"x" = input`, compile
        // `DimRef { dim_id: "l", input: Col { name: "x" } }` against a registry
        // built from a randomly generated inline dimension, and require the
        // collected result to equal `DimensionMatcher::lookup(&input, 0)`. This
        // pins the contract that the Polars `map` wiring in the DimRef compile
        // arm is a faithful lift of the matcher.
        #[test]
        fn dim_ref_eval_equals_direct_call(
            case_insensitive in any::<bool>(),
            rows in proptest::collection::vec(
                (proptest::collection::vec("[a-zA-Z]{1,8}", 1..=3), "[A-Z]{1,8}"),
                1..=5,
            ),
            input in ".{0,30}",
        ) {
            let dim_rows: Vec<crate::config::InlineDimensionRow> = rows.iter().map(|(pats, out)| {
                crate::config::InlineDimensionRow {
                    patterns: pats.clone(),
                    values: vec![out.clone()],
                    priority: 0,
                }
            }).collect();

            let cfg = crate::config::Dimension {
                id: "l".into(),
                name: None,
                match_: crate::config::MatchMode::KeywordSubstring,
                case_insensitive: Some(case_insensitive),
                on_miss: crate::config::OnMiss::Null,
                rows: crate::config::DimensionRows::Inline {
                    values: vec!["v".into()],
                    rows: dim_rows,
                },
            };

            let registry = crate::dimension::build_inline_registry(std::slice::from_ref(&cfg)).unwrap();
            let direct_matcher = registry.get("l").unwrap().clone();

            let ast = AstNode::DimRef {
                dim_id: "l".to_string(),
                value: None,
                input: Box::new(AstNode::Col { name: "x".to_string() }),
            };
            let ctx = CompileCtx::new(&registry);
            let expr = compile(&ast, &ctx).unwrap();

            let df = DataFrame::new(
                1,
                vec![Column::new("x".into(), &[input.as_str()])],
            ).unwrap();
            let out = df.lazy().select([expr.alias("v")]).collect().unwrap();

            let series = out.column("v").unwrap().as_materialized_series();
            let s = series.str().unwrap();
            let got: Option<String> = s.get(0).map(|v| v.to_string());

            let expected: Option<String> = direct_matcher.lookup(&input, 0);

            prop_assert_eq!(got, expected);
        }
    }
}
