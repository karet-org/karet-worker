//! AST evaluator that compiles `AstNode` trees into Polars `Expr` values.
//!
//! Compilation is pure: [`compile`] walks an [`AstNode`] and returns a
//! Polars [`Expr`] that evaluates the node vectorized over a `DataFrame`.
//! [`CompileCtx`] carries a registry of precompiled [`LookupMatcher`]s
//! keyed by dotted lookup id for nodes that can't be expressed as pure
//! Polars operators.

use std::collections::HashMap;
use std::sync::Arc;

use polars::prelude::*;

use crate::ast::{AstNode, CastType};
use crate::error::EvalError;
use crate::lookup::LookupMatcher;

/// Context passed to [`compile`].
///
/// Carries a registry of [`LookupMatcher`]s keyed by dotted lookup id
/// (`"categories"`, `"categories.merchants"`, …). The registry is typically
/// built once per job from the `Pipeline_Config` via
/// [`crate::lookup::build_registry`] and shared by reference across every
/// mapping column compiled in that job.
pub struct CompileCtx<'a> {
    pub lookups: &'a HashMap<String, Arc<LookupMatcher>>,
}

impl<'a> CompileCtx<'a> {
    /// Create a new context wrapping a reference to the lookup registry.
    pub fn new(lookups: &'a HashMap<String, Arc<LookupMatcher>>) -> Self {
        Self { lookups }
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

        // --- String ops ---
        AstNode::Upper { input } => Ok(compile(input, ctx)?.str().to_uppercase()),
        AstNode::Lower { input } => Ok(compile(input, ctx)?.str().to_lowercase()),
        // `strip_chars(lit(NULL))` strips ASCII whitespace, matching SQL-style
        // TRIM semantics.
        AstNode::Trim { input } => Ok(compile(input, ctx)?.str().strip_chars(lit(NULL))),
        AstNode::Substring { input, start, length } => {
            // Polars `str().slice(offset: Expr, length: Expr)` -- `lit(NULL)`
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
            Ok(concat_str(arg_exprs, sep.as_str(), false))
        }

        // --- Control flow ---
        AstNode::If { cond, then, r#else } => Ok(when(compile(cond, ctx)?)
            .then(compile(then, ctx)?)
            .otherwise(compile(r#else, ctx)?)),

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
        // (Req 3.6 -- malformed AST structures surface as JSON parse errors
        // at config-load time, malformed *data* must not).
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
        // runs `match_first` over each string in the input column. On a hit
        // we return the matcher's `output` value; on a miss we yield `None`
        //.
        //
        // The closure must be `Fn + Send + Sync + 'static`, so we clone an
        // `Arc<LookupMatcher>` into it rather than capturing `ctx`.
        AstNode::LookupRef { lookup_id, input } => {
            let matcher = ctx
                .lookups
                .get(lookup_id)
                .ok_or_else(|| EvalError::UnknownLookup {
                    id: lookup_id.clone(),
                })?
                .clone();
            let input_expr = compile(input, ctx)?;
            Ok(input_expr.map(
                move |column| {
                    let result: StringChunked = column
                        .str()?
                        .iter()
                        .map(|s_opt| s_opt.and_then(|s| matcher.match_first(s).map(|h| h.output)))
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

    // Malformed date strings must parse to null rather than erroring
    //. Mixed input `["2024-01-01", "not-a-date"]` should yield
    // `[Some(_), None]`.
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

        // lookup_ref AST evaluation equals direct matcher call
        //
        // Building a single-row DataFrame with `"x" = input`, compiling
        // `LookupRef { lookup_id: "l", input: Col { name: "x" } }` against a
        // registry built from a randomly generated flat `LookupMapping`, and
        // collecting the result must yield the same scalar as calling
        // `LookupMatcher::from_config(&cfg).match_first(&input).map(|h| h.output)`
        // directly. This pins down the contract that the Polars `map` wiring
        // in the `LookupRef` compile arm is a faithful lift of the matcher.
        //
        #[test]
        fn lookup_ref_eval_equals_direct_call(
            case_insensitive in any::<bool>(),
            rows in proptest::collection::vec(
                (proptest::collection::vec("[a-zA-Z]{1,8}", 1..=3), "[A-Z]{1,8}"),
                1..=5,
            ),
            input in ".{0,30}",
        ) {
            let cfg_rows: Vec<crate::config::LookupRow> = rows.iter().map(|(pats, out)| {
                crate::config::LookupRow {
                    input_patterns: pats.clone(),
                    output: out.clone(),
                    parent_output: None,
                }
            }).collect();

            let cfg = crate::config::LookupMapping {
                id: "l".into(),
                name: None,
                match_: Some("keyword_substring".into()),
                case_insensitive: Some(case_insensitive),
                rows: cfg_rows,
                children: vec![],
                parent_output_column: None,
                catch_all: None,
            };

            let direct_matcher = crate::lookup::LookupMatcher::from_config(&cfg);
            let registry = crate::lookup::build_registry(std::slice::from_ref(&cfg));

            let ast = AstNode::LookupRef {
                lookup_id: "l".to_string(),
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

            let expected: Option<String> = direct_matcher.match_first(&input).map(|h| h.output);

            prop_assert_eq!(got, expected);
        }
    }
}
