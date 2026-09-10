//! Dimension matcher: replaces the old Lookup_Mapping node.
//!
//! A dimension maps a key to one or more value columns. Rows come either
//! inline from the config (small, hand-edited tables) or from a file in the
//! lake (large or externally-maintained reference data). Matching is exact
//! or keyword-substring; ties break on `priority` then definition order.

use std::collections::HashMap;
use std::sync::Arc;

use crate::config::{Dimension, DimensionRows, MatchMode, OnMiss};

/// Hard cap on dimension rows, enforced when loading file-backed rows.
/// A reference table that quietly grows must fail loudly, not OOM a worker.
pub const MAX_DIMENSION_ROWS: usize = 1_000_000;

#[derive(Debug, Clone)]
struct CompiledRow {
    /// Exact keys or substring patterns, already case-folded when the
    /// dimension is case-insensitive.
    patterns: Vec<String>,
    /// One entry per value column, positionally matching `value_columns`.
    values: Vec<Option<String>>,
    priority: i64,
}

/// Precompiled dimension ready for expression evaluation.
#[derive(Debug)]
pub struct DimensionMatcher {
    rows: Vec<CompiledRow>,
    /// Value column names, in declaration order.
    value_columns: Vec<String>,
    match_mode: MatchMode,
    case_insensitive: bool,
    on_miss: OnMiss,
    /// Exact-match fast path: pattern -> row index.
    exact_index: HashMap<String, usize>,
}

/// Failure while building a dimension.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum DimensionError {
    #[error("dimension `{id}` has duplicate key `{key}`")]
    DuplicateKey { id: String, key: String },
    #[error("dimension `{id}` exceeds the {max}-row cap ({rows} rows)")]
    TooManyRows { id: String, rows: usize, max: usize },
    #[error("dimension `{id}` file is missing column `{column}`")]
    MissingColumn { id: String, column: String },
    #[error("dimension `{id}` file could not be read: {message}")]
    FileRead { id: String, message: String },
}

impl DimensionMatcher {
    /// Compile a dimension whose rows are already resolved to
    /// `(patterns, values, priority)` triples.
    pub fn new(
        dim: &Dimension,
        rows: Vec<(Vec<String>, Vec<Option<String>>, i64)>,
    ) -> Result<Self, DimensionError> {
        if rows.len() > MAX_DIMENSION_ROWS {
            return Err(DimensionError::TooManyRows {
                id: dim.id.clone(),
                rows: rows.len(),
                max: MAX_DIMENSION_ROWS,
            });
        }
        let case_insensitive = dim.case_insensitive.unwrap_or(false);
        let fold = |s: &str| {
            if case_insensitive {
                s.to_lowercase()
            } else {
                s.to_string()
            }
        };

        let compiled: Vec<CompiledRow> = rows
            .into_iter()
            .map(|(patterns, values, priority)| CompiledRow {
                patterns: patterns.iter().map(|p| fold(p)).collect(),
                values,
                priority,
            })
            .collect();

        // Exact dimensions are keyed reference data: a repeated key is a
        // data-quality error, never a silent last-wins.
        let mut exact_index = HashMap::new();
        if dim.match_ == MatchMode::Exact {
            for (i, row) in compiled.iter().enumerate() {
                for pattern in &row.patterns {
                    if exact_index.insert(pattern.clone(), i).is_some() {
                        return Err(DimensionError::DuplicateKey {
                            id: dim.id.clone(),
                            key: pattern.clone(),
                        });
                    }
                }
            }
        }

        Ok(Self {
            rows: compiled,
            value_columns: dim.value_columns().to_vec(),
            match_mode: dim.match_,
            case_insensitive,
            on_miss: dim.on_miss.clone(),
            exact_index,
        })
    }

    /// Index of a value column, for the evaluator to capture once.
    pub fn value_index(&self, name: Option<&str>) -> Option<usize> {
        match name {
            None => Some(0),
            Some(n) => self.value_columns.iter().position(|c| c == n),
        }
    }

    pub fn value_columns(&self) -> &[String] {
        &self.value_columns
    }

    /// Resolve `input` to the value at `value_index`, applying `on_miss`.
    pub fn lookup(&self, input: &str, value_index: usize) -> Option<String> {
        let folded = if self.case_insensitive {
            input.to_lowercase()
        } else {
            input.to_string()
        };

        let hit = match self.match_mode {
            MatchMode::Exact => self.exact_index.get(&folded).map(|i| &self.rows[*i]),
            MatchMode::KeywordSubstring => {
                // Highest priority among matching rows; ties keep definition
                // order, preserving first-match-wins for equal priorities.
                let mut best: Option<&CompiledRow> = None;
                for row in &self.rows {
                    if row.patterns.iter().any(|p| folded.contains(p.as_str())) {
                        match best {
                            Some(current) if current.priority >= row.priority => {}
                            _ => best = Some(row),
                        }
                    }
                }
                best
            }
        };

        match hit {
            Some(row) => row.values.get(value_index).cloned().flatten(),
            None => match &self.on_miss {
                OnMiss::Null => None,
                OnMiss::Passthrough => Some(input.to_string()),
                OnMiss::Literal { literal } => Some(literal.clone()),
            },
        }
    }
}

/// Compile every dimension whose rows are inline. File-backed dimensions are
/// resolved by the job runner, which has S3 access, and merged in afterwards.
pub fn build_inline_registry(
    dimensions: &[Dimension],
) -> Result<HashMap<String, Arc<DimensionMatcher>>, DimensionError> {
    let mut registry = HashMap::new();
    for dim in dimensions {
        if let DimensionRows::Inline { rows, .. } = &dim.rows {
            let triples = rows
                .iter()
                .map(|r| {
                    (
                        r.patterns.clone(),
                        r.values.iter().map(|v| Some(v.clone())).collect(),
                        r.priority,
                    )
                })
                .collect();
            registry.insert(dim.id.clone(), Arc::new(DimensionMatcher::new(dim, triples)?));
        }
    }
    Ok(registry)
}

/// Parse a CSV reference file into dimension rows: one key column plus the
/// declared value columns, with an optional priority column.
pub fn rows_from_csv(
    dim: &Dimension,
    key: &str,
    values: &[String],
    priority_column: Option<&str>,
    bytes: &[u8],
) -> Result<Vec<(Vec<String>, Vec<Option<String>>, i64)>, DimensionError> {
    let text = std::str::from_utf8(bytes).map_err(|e| DimensionError::FileRead {
        id: dim.id.clone(),
        message: format!("invalid UTF-8: {e}"),
    })?;

    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    let header: Vec<String> = match lines.next() {
        Some(h) => split_csv_line(h),
        None => {
            return Err(DimensionError::FileRead {
                id: dim.id.clone(),
                message: "file is empty".to_string(),
            })
        }
    };
    let index_of = |name: &str| header.iter().position(|h| h == name);

    let key_idx = index_of(key).ok_or_else(|| DimensionError::MissingColumn {
        id: dim.id.clone(),
        column: key.to_string(),
    })?;
    let value_idxs: Vec<usize> = values
        .iter()
        .map(|v| {
            index_of(v).ok_or_else(|| DimensionError::MissingColumn {
                id: dim.id.clone(),
                column: v.clone(),
            })
        })
        .collect::<Result<_, _>>()?;
    let priority_idx = match priority_column {
        Some(p) => Some(index_of(p).ok_or_else(|| DimensionError::MissingColumn {
            id: dim.id.clone(),
            column: p.to_string(),
        })?),
        None => None,
    };

    let mut out = Vec::new();
    for line in lines {
        let fields = split_csv_line(line);
        let key_value = fields.get(key_idx).cloned().unwrap_or_default();
        if key_value.is_empty() {
            continue;
        }
        let row_values = value_idxs
            .iter()
            .map(|i| fields.get(*i).cloned())
            .collect::<Vec<_>>();
        let priority = priority_idx
            .and_then(|i| fields.get(i))
            .and_then(|p| p.parse::<i64>().ok())
            .unwrap_or(0);
        out.push((vec![key_value], row_values, priority));
        if out.len() > MAX_DIMENSION_ROWS {
            return Err(DimensionError::TooManyRows {
                id: dim.id.clone(),
                rows: out.len(),
                max: MAX_DIMENSION_ROWS,
            });
        }
    }
    Ok(out)
}

/// Split one CSV line, honouring double-quoted fields with `""` escapes.
fn split_csv_line(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                field.push('"');
                chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => out.push(std::mem::take(&mut field)),
            _ => field.push(c),
        }
    }
    out.push(field.trim_end_matches('\r').to_string());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DimensionRows, InlineDimensionRow};

    fn inline(id: &str, match_: MatchMode, on_miss: OnMiss, rows: Vec<InlineDimensionRow>) -> Dimension {
        Dimension {
            id: id.into(),
            name: None,
            match_,
            case_insensitive: Some(true),
            on_miss,
            rows: DimensionRows::Inline {
                values: vec!["category".into()],
                rows,
            },
        }
    }

    fn row(patterns: &[&str], value: &str, priority: i64) -> InlineDimensionRow {
        InlineDimensionRow {
            patterns: patterns.iter().map(|p| p.to_string()).collect(),
            values: vec![value.to_string()],
            priority,
        }
    }

    #[test]
    fn substring_match_is_case_insensitive_and_priority_ordered() {
        let dim = inline(
            "categories",
            MatchMode::KeywordSubstring,
            OnMiss::Null,
            vec![
                row(&["safeway"], "Groceries", 0),
                row(&["safeway gas"], "Fuel", 10),
            ],
        );
        let reg = build_inline_registry(std::slice::from_ref(&dim)).unwrap();
        let m = reg.get("categories").unwrap();
        assert_eq!(m.lookup("SAFEWAY GAS #123", 0).as_deref(), Some("Fuel"));
        assert_eq!(m.lookup("SAFEWAY #99", 0).as_deref(), Some("Groceries"));
        assert_eq!(m.lookup("UNRELATED", 0), None);
    }

    #[test]
    fn on_miss_passthrough_and_literal() {
        let pass = inline("d", MatchMode::Exact, OnMiss::Passthrough, vec![row(&["CA"], "Canada", 0)]);
        let reg = build_inline_registry(std::slice::from_ref(&pass)).unwrap();
        let m = reg.get("d").unwrap();
        assert_eq!(m.lookup("ca", 0).as_deref(), Some("Canada"));
        assert_eq!(m.lookup("XK", 0).as_deref(), Some("XK"), "unknown key passes through");

        let lit = inline(
            "d",
            MatchMode::Exact,
            OnMiss::Literal { literal: "Uncategorized".into() },
            vec![row(&["CA"], "Canada", 0)],
        );
        let reg = build_inline_registry(std::slice::from_ref(&lit)).unwrap();
        assert_eq!(reg.get("d").unwrap().lookup("ZZ", 0).as_deref(), Some("Uncategorized"));
    }

    #[test]
    fn duplicate_exact_key_is_an_error() {
        let dim = inline(
            "codes",
            MatchMode::Exact,
            OnMiss::Null,
            vec![row(&["A"], "first", 0), row(&["A"], "second", 0)],
        );
        let err = build_inline_registry(std::slice::from_ref(&dim)).unwrap_err();
        assert_eq!(
            err,
            DimensionError::DuplicateKey { id: "codes".into(), key: "a".into() }
        );
    }

    #[test]
    fn csv_rows_resolve_key_values_and_priority() {
        let dim = Dimension {
            id: "geo".into(),
            name: None,
            match_: MatchMode::Exact,
            case_insensitive: None,
            on_miss: OnMiss::Null,
            rows: DimensionRows::File {
                path_prefix: "dimensions/geo/".into(),
                key: "code".into(),
                values: vec!["country".into(), "region".into()],
                priority_column: None,
            },
        };
        let csv = b"code,country,region\nCA,Canada,Americas\nDE,\"Germany, Fed. Rep.\",Europe\n";
        let rows = rows_from_csv(&dim, "code", &["country".into(), "region".into()], None, csv).unwrap();
        assert_eq!(rows.len(), 2);
        let m = DimensionMatcher::new(&dim, rows).unwrap();
        assert_eq!(m.lookup("CA", 0).as_deref(), Some("Canada"));
        assert_eq!(m.lookup("CA", 1).as_deref(), Some("Americas"));
        assert_eq!(
            m.lookup("DE", 0).as_deref(),
            Some("Germany, Fed. Rep."),
            "quoted field with a comma survives"
        );
    }

    #[test]
    fn csv_missing_declared_column_is_an_error() {
        let dim = Dimension {
            id: "geo".into(),
            name: None,
            match_: MatchMode::Exact,
            case_insensitive: None,
            on_miss: OnMiss::Null,
            rows: DimensionRows::File {
                path_prefix: "d/".into(),
                key: "code".into(),
                values: vec!["region".into()],
                priority_column: None,
            },
        };
        let err = rows_from_csv(&dim, "code", &["region".into()], None, b"code,country\nCA,Canada\n")
            .unwrap_err();
        assert_eq!(err, DimensionError::MissingColumn { id: "geo".into(), column: "region".into() });
    }

    // --- Behaviours ported from the Lookup matcher this node replaces ---

    #[test]
    fn matches_first_in_definition_order_at_equal_priority() {
        let dim = inline(
            "d",
            MatchMode::KeywordSubstring,
            OnMiss::Null,
            vec![row(&["ab"], "first", 0), row(&["abc"], "second", 0)],
        );
        let reg = build_inline_registry(std::slice::from_ref(&dim)).unwrap();
        assert_eq!(reg.get("d").unwrap().lookup("abcdef", 0).as_deref(), Some("first"));
    }

    #[test]
    fn case_sensitive_dimension_does_not_match_different_case() {
        let mut dim = inline("d", MatchMode::KeywordSubstring, OnMiss::Null, vec![row(&["UBER"], "T", 0)]);
        dim.case_insensitive = Some(false);
        let reg = build_inline_registry(std::slice::from_ref(&dim)).unwrap();
        let m = reg.get("d").unwrap();
        assert_eq!(m.lookup("uber eats", 0), None);
        assert_eq!(m.lookup("UBER EATS", 0).as_deref(), Some("T"));
    }

    #[test]
    fn negative_priority_is_deprioritised() {
        let dim = inline(
            "d",
            MatchMode::KeywordSubstring,
            OnMiss::Null,
            vec![row(&["x"], "low", -5), row(&["x"], "high", 0)],
        );
        let reg = build_inline_registry(std::slice::from_ref(&dim)).unwrap();
        assert_eq!(reg.get("d").unwrap().lookup("xyz", 0).as_deref(), Some("high"));
    }

    #[test]
    fn priority_only_compares_matching_rows() {
        // The high-priority row does not match, so the lower one still wins.
        let dim = inline(
            "d",
            MatchMode::KeywordSubstring,
            OnMiss::Null,
            vec![row(&["zzz"], "unmatched", 100), row(&["x"], "matched", 0)],
        );
        let reg = build_inline_registry(std::slice::from_ref(&dim)).unwrap();
        assert_eq!(reg.get("d").unwrap().lookup("xyz", 0).as_deref(), Some("matched"));
    }

    #[test]
    fn on_miss_does_not_fire_when_a_row_matches() {
        let dim = inline(
            "d",
            MatchMode::KeywordSubstring,
            OnMiss::Literal { literal: "fallback".into() },
            vec![row(&["x"], "hit", 0)],
        );
        let reg = build_inline_registry(std::slice::from_ref(&dim)).unwrap();
        assert_eq!(reg.get("d").unwrap().lookup("xyz", 0).as_deref(), Some("hit"));
    }

    #[test]
    fn exact_mode_requires_whole_value_equality() {
        let dim = inline("d", MatchMode::Exact, OnMiss::Null, vec![row(&["CA"], "Canada", 0)]);
        let reg = build_inline_registry(std::slice::from_ref(&dim)).unwrap();
        let m = reg.get("d").unwrap();
        assert_eq!(m.lookup("CA", 0).as_deref(), Some("Canada"));
        assert_eq!(m.lookup("CANADA", 0), None, "substring must not match in exact mode");
    }

    #[test]
    fn duplicate_substring_patterns_are_allowed() {
        // Only exact dimensions are keyed; substring tables legitimately
        // repeat patterns across rows and resolve by priority/order.
        let dim = inline(
            "d",
            MatchMode::KeywordSubstring,
            OnMiss::Null,
            vec![row(&["x"], "a", 0), row(&["x"], "b", 0)],
        );
        assert!(build_inline_registry(std::slice::from_ref(&dim)).is_ok());
    }

    #[test]
    fn row_cap_is_enforced() {
        let dim = Dimension {
            id: "big".into(),
            name: None,
            match_: MatchMode::Exact,
            case_insensitive: None,
            on_miss: OnMiss::Null,
            rows: DimensionRows::File {
                path_prefix: "d/".into(),
                key: "k".into(),
                values: vec!["v".into()],
                priority_column: None,
            },
        };
        let rows = vec![(vec!["k".to_string()], vec![Some("v".to_string())], 0); MAX_DIMENSION_ROWS + 1];
        let err = DimensionMatcher::new(&dim, rows).unwrap_err();
        assert!(matches!(err, DimensionError::TooManyRows { .. }), "{err:?}");
    }

    #[test]
    fn value_index_resolves_by_name() {
        let dim = Dimension {
            id: "geo".into(),
            name: None,
            match_: MatchMode::Exact,
            case_insensitive: None,
            on_miss: OnMiss::Null,
            rows: DimensionRows::File {
                path_prefix: "d/".into(),
                key: "code".into(),
                values: vec!["country".into(), "region".into()],
                priority_column: None,
            },
        };
        let m = DimensionMatcher::new(&dim, vec![]).unwrap();
        assert_eq!(m.value_index(None), Some(0));
        assert_eq!(m.value_index(Some("region")), Some(1));
        assert_eq!(m.value_index(Some("nope")), None);
    }
}
