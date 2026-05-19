//! Data-quality assertions on analytic table columns.
//!
//! Runs after mapping expressions produce the DataFrame and before
//! partitions are written. A failed assertion fails the mapping only.
//! Every assertion is evaluated independently so violations are reported
//! together.

use polars::prelude::*;

use crate::config::{AnalyticTable, ColumnAssertions};

#[derive(Debug, Clone, PartialEq)]
pub struct AssertionViolation {
    pub column: String,
    pub kind: &'static str,
    pub message: String,
}

impl std::fmt::Display for AssertionViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "column `{}` {}: {}", self.column, self.kind, self.message)
    }
}

/// Run every column assertion on `table.schema` against `df`. Returns the
/// full list of violations (empty on success). A missing column produces
/// a `missing_column` violation rather than silently passing.
pub fn validate_assertions(df: &DataFrame, table: &AnalyticTable) -> Vec<AssertionViolation> {
    let mut out = Vec::new();

    for col in &table.schema {
        let Some(a) = col.assertions.as_ref() else {
            continue;
        };
        let series = match df.column(&col.name) {
            Ok(s) => s,
            Err(_) => {
                out.push(AssertionViolation {
                    column: col.name.clone(),
                    kind: "missing_column",
                    message: "column not present in produced DataFrame".into(),
                });
                continue;
            }
        };
        check_column(&col.name, series, a, &mut out);
    }

    out
}

fn check_column(
    name: &str,
    series: &Column,
    a: &ColumnAssertions,
    out: &mut Vec<AssertionViolation>,
) {
    if a.not_null == Some(true) {
        let nulls = series.null_count();
        if nulls > 0 {
            out.push(AssertionViolation {
                column: name.into(),
                kind: "not_null",
                message: format!("{nulls} null value(s) found"),
            });
        }
    }

    if a.unique == Some(true) {
        // Compare n_unique against the non-null count so nulls don't trigger
        // false duplicates.
        let non_null = series.len() - series.null_count();
        match series.n_unique() {
            Ok(n) if n < non_null => {
                out.push(AssertionViolation {
                    column: name.into(),
                    kind: "unique",
                    message: format!("{} duplicate value(s)", non_null - n),
                });
            }
            Err(e) => {
                out.push(AssertionViolation {
                    column: name.into(),
                    kind: "unique",
                    message: format!("uniqueness check failed: {e}"),
                });
            }
            _ => {}
        }
    }

    if let Some(allowed) = a.accepted_values.as_deref() {
        // Empty list is ambiguous; treat as "no check" (matches web editor).
        if !allowed.is_empty() {
            match series.as_materialized_series().cast(&DataType::String) {
                Ok(casted) => {
                    let str_chunked = match casted.str() {
                        Ok(s) => s.clone(),
                        Err(e) => {
                            out.push(AssertionViolation {
                                column: name.into(),
                                kind: "accepted_values",
                                message: format!("cast to string failed: {e}"),
                            });
                            return;
                        }
                    };
                    let mut bad = 0usize;
                    for v in str_chunked.into_iter().flatten() {
                        if !allowed.iter().any(|s| s == v) {
                            bad += 1;
                        }
                    }
                    if bad > 0 {
                        out.push(AssertionViolation {
                            column: name.into(),
                            kind: "accepted_values",
                            message: format!(
                                "{bad} value(s) not in accepted_values ({} allowed)",
                                allowed.len()
                            ),
                        });
                    }
                }
                Err(e) => out.push(AssertionViolation {
                    column: name.into(),
                    kind: "accepted_values",
                    message: format!("cast to string failed: {e}"),
                }),
            }
        }
    }

    if a.min.is_some() || a.max.is_some() {
        match series.as_materialized_series().cast(&DataType::Float64) {
            Ok(casted) => {
                let floats = match casted.f64() {
                    Ok(f) => f.clone(),
                    Err(e) => {
                        out.push(AssertionViolation {
                            column: name.into(),
                            kind: "range",
                            message: format!("cast to float64 failed: {e}"),
                        });
                        return;
                    }
                };
                let mut below = 0usize;
                let mut above = 0usize;
                for v in floats.into_iter().flatten() {
                    if let Some(lo) = a.min {
                        if v < lo {
                            below += 1;
                            continue;
                        }
                    }
                    if let Some(hi) = a.max {
                        if v > hi {
                            above += 1;
                        }
                    }
                }
                if below > 0 || above > 0 {
                    out.push(AssertionViolation {
                        column: name.into(),
                        kind: "range",
                        message: format!(
                            "{below} below min, {above} above max (min={:?}, max={:?})",
                            a.min, a.max
                        ),
                    });
                }
            }
            Err(e) => out.push(AssertionViolation {
                column: name.into(),
                kind: "range",
                message: format!("cast to float64 failed: {e}"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ColumnSchema;

    fn table_with(name: &str, type_: &str, a: ColumnAssertions) -> AnalyticTable {
        AnalyticTable {
            id: "t".into(),
            name: "T".into(),
            output_prefix: "clean/t/".into(),
            schema: vec![ColumnSchema {
                name: name.into(),
                type_: type_.into(),
                nullable: None,
                assertions: Some(a),
            }],
        }
    }

    #[test]
    fn no_assertions_produces_no_violations() {
        let df = df!["a" => [1, 2, 3]].unwrap();
        let table = AnalyticTable {
            id: "t".into(),
            name: "T".into(),
            output_prefix: "clean/t/".into(),
            schema: vec![ColumnSchema {
                name: "a".into(),
                type_: "int64".into(),
                nullable: None,
                assertions: None,
            }],
        };
        assert!(validate_assertions(&df, &table).is_empty());
    }

    #[test]
    fn not_null_catches_nulls() {
        let df = df!["a" => &[Some(1i64), None, Some(3)]].unwrap();
        let t = table_with(
            "a",
            "int64",
            ColumnAssertions {
                not_null: Some(true),
                ..Default::default()
            },
        );
        let v = validate_assertions(&df, &t);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].kind, "not_null");
    }

    #[test]
    fn unique_catches_duplicates() {
        let df = df!["a" => [1i64, 2, 2, 3]].unwrap();
        let t = table_with(
            "a",
            "int64",
            ColumnAssertions {
                unique: Some(true),
                ..Default::default()
            },
        );
        let v = validate_assertions(&df, &t);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].kind, "unique");
    }

    #[test]
    fn accepted_values_catches_off_list() {
        let df = df!["a" => ["FOOD", "TRAVEL", "UNKNOWN"]].unwrap();
        let t = table_with(
            "a",
            "string",
            ColumnAssertions {
                accepted_values: Some(vec!["FOOD".into(), "TRAVEL".into()]),
                ..Default::default()
            },
        );
        let v = validate_assertions(&df, &t);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].kind, "accepted_values");
    }

    #[test]
    fn accepted_values_empty_list_is_treated_as_no_check() {
        let df = df!["a" => ["FOOD", "TRAVEL"]].unwrap();
        let t = table_with(
            "a",
            "string",
            ColumnAssertions {
                accepted_values: Some(vec![]),
                ..Default::default()
            },
        );
        assert!(validate_assertions(&df, &t).is_empty());
    }

    #[test]
    fn range_catches_out_of_bounds() {
        let df = df!["a" => [0.0, 5.0, 11.0]].unwrap();
        let t = table_with(
            "a",
            "float64",
            ColumnAssertions {
                min: Some(1.0),
                max: Some(10.0),
                ..Default::default()
            },
        );
        let v = validate_assertions(&df, &t);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].kind, "range");
    }

    #[test]
    fn missing_column_reported() {
        let df = df!["b" => [1i64]].unwrap();
        let t = table_with(
            "a",
            "int64",
            ColumnAssertions {
                not_null: Some(true),
                ..Default::default()
            },
        );
        let v = validate_assertions(&df, &t);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].kind, "missing_column");
    }

    #[test]
    fn multiple_assertions_all_reported() {
        let df = df!["a" => [1i64, 1, 100]].unwrap();
        let t = table_with(
            "a",
            "int64",
            ColumnAssertions {
                unique: Some(true),
                max: Some(50.0),
                ..Default::default()
            },
        );
        let v = validate_assertions(&df, &t);
        let kinds: Vec<&str> = v.iter().map(|x| x.kind).collect();
        assert!(kinds.contains(&"unique"));
        assert!(kinds.contains(&"range"));
    }
}
