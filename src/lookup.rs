//! Lookup_Mapping matcher (keyword substring, case-insensitive, hierarchical).
//!
//! Children are consulted before the parent's own rows, so a more-specific
//! child classification wins over a broader parent bucket.

use std::collections::HashMap;
use std::sync::Arc;

use crate::config::LookupMapping;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupHit {
    pub output: String,
    pub parent_output: Option<String>,
}

/// Precompiled lookup table; scan order follows definition order.
pub struct LookupMatcher {
    rows: Vec<CompiledRow>,
    case_insensitive: bool,
    children: Vec<LookupMatcher>,
    /// Fallback emitted by [`match_first`] when the child+row scan finds
    /// nothing. `None` preserves the historical behavior: a miss maps to
    /// `None` (and null in the output column).
    catch_all: Option<LookupHit>,
}

struct CompiledRow {
    patterns: Vec<String>,
    output: String,
    parent_output: Option<String>,
}

impl LookupMatcher {
    pub fn from_config(cfg: &LookupMapping) -> Self {
        let case_insensitive = cfg.case_insensitive.unwrap_or(false);
        let rows = cfg
            .rows
            .iter()
            .map(|row| CompiledRow {
                patterns: row
                    .input_patterns
                    .iter()
                    .map(|p| {
                        if case_insensitive {
                            p.to_lowercase()
                        } else {
                            p.clone()
                        }
                    })
                    .collect(),
                output: row.output.clone(),
                parent_output: row.parent_output.clone(),
            })
            .collect();
        let children = cfg.children.iter().map(LookupMatcher::from_config).collect();
        let catch_all = cfg.catch_all.as_ref().map(|ca| LookupHit {
            output: ca.output.clone(),
            parent_output: ca.parent_output.clone(),
        });
        Self {
            rows,
            case_insensitive,
            children,
            catch_all,
        }
    }

    /// Return the first matching row's output/parent_output, consulting
    /// children before our own rows. Falls back to the matcher's
    /// [`Self::catch_all`] when nothing matches.
    pub fn match_first(&self, input: &str) -> Option<LookupHit> {
        if let Some(hit) = self.scan(input) {
            return Some(hit);
        }
        self.catch_all.clone()
    }

    /// Row + child scan without consulting the catch-all. Used
    /// recursively for child matchers so a child's catch-all only fires
    /// when the user queried the child directly (by dotted lookup id),
    /// not while the parent is walking its children for row matches.
    fn scan(&self, input: &str) -> Option<LookupHit> {
        for child in &self.children {
            if let Some(hit) = child.scan(input) {
                return Some(hit);
            }
        }

        let haystack: String;
        let haystack_ref: &str = if self.case_insensitive {
            haystack = input.to_lowercase();
            &haystack
        } else {
            input
        };

        for row in &self.rows {
            if row.patterns.iter().any(|p| haystack_ref.contains(p.as_str())) {
                return Some(LookupHit {
                    output: row.output.clone(),
                    parent_output: row.parent_output.clone(),
                });
            }
        }
        None
    }
}

/// Build a registry of compiled matchers keyed by dotted lookup path
/// (e.g. `"categories"`, `"categories.merchants"`).
pub fn build_registry(lookups: &[LookupMapping]) -> HashMap<String, Arc<LookupMatcher>> {
    let mut registry = HashMap::new();
    for lookup in lookups {
        insert_recursive(&mut registry, lookup, None);
    }
    registry
}

fn insert_recursive(
    registry: &mut HashMap<String, Arc<LookupMatcher>>,
    lookup: &LookupMapping,
    parent_path: Option<&str>,
) {
    let path = match parent_path {
        Some(p) => format!("{p}.{}", lookup.id),
        None => lookup.id.clone(),
    };
    registry.insert(path.clone(), Arc::new(LookupMatcher::from_config(lookup)));
    for child in &lookup.children {
        insert_recursive(registry, child, Some(&path));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LookupRow;

    fn mapping(case_insensitive: bool, rows: Vec<LookupRow>) -> LookupMapping {
        LookupMapping {
            id: "l".into(),
            name: None,
            match_: Some("keyword_substring".into()),
            case_insensitive: Some(case_insensitive),
            rows,
            children: vec![],
            parent_output_column: None,
            catch_all: None,
        }
    }

    fn row(patterns: &[&str], output: &str) -> LookupRow {
        LookupRow {
            input_patterns: patterns.iter().map(|p| (*p).to_string()).collect(),
            output: output.to_string(),
            parent_output: None,
        }
    }

    #[test]
    fn matches_first_in_definition_order() {
        let cfg = mapping(
            false,
            vec![
                row(&["FOO"], "FIRST"),
                row(&["FOO", "BAR"], "SECOND"),
            ],
        );
        let m = LookupMatcher::from_config(&cfg);
        assert_eq!(
            m.match_first("FOO BAR"),
            Some(LookupHit {
                output: "FIRST".into(),
                parent_output: None,
            })
        );
    }

    #[test]
    fn case_insensitive_match() {
        let cfg = mapping(true, vec![row(&["Starbucks"], "FOOD")]);
        let m = LookupMatcher::from_config(&cfg);
        assert_eq!(
            m.match_first("visit STARBUCKS today"),
            Some(LookupHit {
                output: "FOOD".into(),
                parent_output: None,
            })
        );
    }

    #[test]
    fn case_sensitive_does_not_match_different_case() {
        let cfg = mapping(false, vec![row(&["Starbucks"], "FOOD")]);
        let m = LookupMatcher::from_config(&cfg);
        assert_eq!(m.match_first("visit STARBUCKS today"), None);
    }

    #[test]
    fn no_match_returns_none() {
        let cfg = mapping(true, vec![row(&["uber"], "TRANSPORT")]);
        let m = LookupMatcher::from_config(&cfg);
        assert_eq!(m.match_first("grocery store"), None);
    }

    #[test]
    fn parent_output_is_preserved_on_hit() {
        let cfg = mapping(
            true,
            vec![LookupRow {
                input_patterns: vec!["uber".into()],
                output: "UBER".into(),
                parent_output: Some("TRANSPORT".into()),
            }],
        );
        let m = LookupMatcher::from_config(&cfg);
        assert_eq!(
            m.match_first("UBER TRIP"),
            Some(LookupHit {
                output: "UBER".into(),
                parent_output: Some("TRANSPORT".into()),
            })
        );
    }

    // -----------------------------------------------------------------------
    // Hierarchical parent/child matching
    // -----------------------------------------------------------------------

    fn mapping_with_children(
        case_insensitive: bool,
        rows: Vec<LookupRow>,
        children: Vec<LookupMapping>,
    ) -> LookupMapping {
        LookupMapping {
            id: "l".into(),
            name: None,
            match_: Some("keyword_substring".into()),
            case_insensitive: Some(case_insensitive),
            rows,
            children,
            parent_output_column: None,
            catch_all: None,
        }
    }

    fn child_row(patterns: &[&str], output: &str, parent_output: &str) -> LookupRow {
        LookupRow {
            input_patterns: patterns.iter().map(|p| (*p).to_string()).collect(),
            output: output.to_string(),
            parent_output: Some(parent_output.to_string()),
        }
    }

    #[test]
    fn child_match_wins_over_parent() {
        // Parent would bucket anything mentioning FOOD; child recognizes the
        // more specific STARBUCKS merchant and should take precedence.
        let child = mapping_with_children(
            true,
            vec![child_row(&["STARBUCKS"], "STARBUCKS", "FOOD")],
            vec![],
        );
        let parent = mapping_with_children(
            true,
            vec![row(&["FOOD"], "FOOD")],
            vec![child],
        );
        let m = LookupMatcher::from_config(&parent);
        assert_eq!(
            m.match_first("MY STARBUCKS"),
            Some(LookupHit {
                output: "STARBUCKS".into(),
                parent_output: Some("FOOD".into()),
            })
        );
    }

    #[test]
    fn parent_match_when_no_child_matches() {
        // Child only knows TAXI; input mentions UBER, which only the parent's
        // own rows catch. Parent rows have no parent_output so result is None.
        let child = mapping_with_children(
            true,
            vec![child_row(&["TAXI"], "TAXI", "TRANSPORT")],
            vec![],
        );
        let parent = mapping_with_children(
            true,
            vec![row(&["UBER"], "TRANSPORT")],
            vec![child],
        );
        let m = LookupMatcher::from_config(&parent);
        assert_eq!(
            m.match_first("UBER RIDE"),
            Some(LookupHit {
                output: "TRANSPORT".into(),
                parent_output: None,
            })
        );
    }

    #[test]
    fn nested_grandchild_match() {
        // parent -> child -> grandchild. Input matches only the grandchild;
        // recursion must walk all the way down.
        let grandchild = mapping_with_children(
            true,
            vec![child_row(&["BLUE_BOTTLE"], "BLUE_BOTTLE", "COFFEE")],
            vec![],
        );
        let child = mapping_with_children(
            true,
            vec![child_row(&["STARBUCKS"], "STARBUCKS", "COFFEE")],
            vec![grandchild],
        );
        let parent = mapping_with_children(
            true,
            vec![row(&["FOOD"], "FOOD")],
            vec![child],
        );
        let m = LookupMatcher::from_config(&parent);
        assert_eq!(
            m.match_first("BLUE_BOTTLE COFFEE"),
            Some(LookupHit {
                output: "BLUE_BOTTLE".into(),
                parent_output: Some("COFFEE".into()),
            })
        );
    }

    #[test]
    fn children_iterated_before_parent() {
        // Both parent and child would match the same input; child must win.
        let child = mapping_with_children(
            true,
            vec![child_row(&["UBER"], "UBER", "TRANSPORT")],
            vec![],
        );
        let parent = mapping_with_children(
            true,
            vec![row(&["UBER"], "GENERIC")],
            vec![child],
        );
        let m = LookupMatcher::from_config(&parent);
        assert_eq!(
            m.match_first("TOOK AN UBER"),
            Some(LookupHit {
                output: "UBER".into(),
                parent_output: Some("TRANSPORT".into()),
            })
        );
    }

    // -----------------------------------------------------------------------
    // catch_all
    // -----------------------------------------------------------------------

    fn mapping_with_catch_all(
        rows: Vec<LookupRow>,
        catch_all: Option<crate::config::LookupCatchAll>,
    ) -> LookupMapping {
        LookupMapping {
            id: "l".into(),
            name: None,
            match_: Some("keyword_substring".into()),
            case_insensitive: Some(true),
            rows,
            children: vec![],
            parent_output_column: None,
            catch_all,
        }
    }

    #[test]
    fn catch_all_fires_when_no_row_matches() {
        let cfg = mapping_with_catch_all(
            vec![row(&["UBER"], "TRANSPORT")],
            Some(crate::config::LookupCatchAll {
                output: "OTHER".into(),
                parent_output: None,
            }),
        );
        let m = LookupMatcher::from_config(&cfg);
        assert_eq!(
            m.match_first("grocery store"),
            Some(LookupHit {
                output: "OTHER".into(),
                parent_output: None,
            }),
        );
    }

    #[test]
    fn catch_all_does_not_fire_when_a_row_matches() {
        let cfg = mapping_with_catch_all(
            vec![row(&["UBER"], "TRANSPORT")],
            Some(crate::config::LookupCatchAll {
                output: "OTHER".into(),
                parent_output: None,
            }),
        );
        let m = LookupMatcher::from_config(&cfg);
        assert_eq!(
            m.match_first("UBER TRIP"),
            Some(LookupHit {
                output: "TRANSPORT".into(),
                parent_output: None,
            }),
        );
    }

    #[test]
    fn catch_all_carries_parent_output() {
        let cfg = mapping_with_catch_all(
            vec![row(&["UBER"], "TRANSPORT")],
            Some(crate::config::LookupCatchAll {
                output: "UNKNOWN_MERCHANT".into(),
                parent_output: Some("OTHER".into()),
            }),
        );
        let m = LookupMatcher::from_config(&cfg);
        assert_eq!(
            m.match_first("grocery store"),
            Some(LookupHit {
                output: "UNKNOWN_MERCHANT".into(),
                parent_output: Some("OTHER".into()),
            }),
        );
    }

    #[test]
    fn child_catch_all_does_not_shadow_parent_rows() {
        // Child has a catch-all and one row; parent has a row the child
        // doesn't know about. When we query the parent with input the
        // child doesn't match, the parent's row must still win —
        // otherwise a single child catch-all would blackhole every
        // parent input.
        let child = LookupMapping {
            id: "child".into(),
            name: None,
            match_: Some("keyword_substring".into()),
            case_insensitive: Some(true),
            rows: vec![child_row(&["TAXI"], "TAXI", "TRANSPORT")],
            children: vec![],
            parent_output_column: None,
            catch_all: Some(crate::config::LookupCatchAll {
                output: "CHILD_OTHER".into(),
                parent_output: None,
            }),
        };
        let parent = LookupMapping {
            id: "parent".into(),
            name: None,
            match_: Some("keyword_substring".into()),
            case_insensitive: Some(true),
            rows: vec![row(&["UBER"], "TRANSPORT")],
            children: vec![child],
            parent_output_column: None,
            catch_all: None,
        };

        let m = LookupMatcher::from_config(&parent);
        // "UBER RIDE" matches the parent's row, not the child's.
        assert_eq!(
            m.match_first("UBER RIDE"),
            Some(LookupHit {
                output: "TRANSPORT".into(),
                parent_output: None,
            }),
        );
    }

    #[test]
    fn no_catch_all_still_returns_none_on_miss() {
        let cfg = mapping_with_catch_all(
            vec![row(&["UBER"], "TRANSPORT")],
            None,
        );
        let m = LookupMatcher::from_config(&cfg);
        assert_eq!(m.match_first("grocery store"), None);
    }

    // -----------------------------------------------------------------------
    // Property tests
    // -----------------------------------------------------------------------

    use proptest::prelude::*;

    proptest! {
        // Lookup returns first-substring-match in definition order or None.
        #[test]
        fn lookup_matcher_first_substring_or_null(
            case_insensitive in any::<bool>(),
            rows in proptest::collection::vec(
                (
                    proptest::collection::vec("[a-zA-Z]{1,8}", 1..=3),
                    "[A-Z]{1,8}",
                ),
                1..=5,
            ),
            input in ".{0,30}",
        ) {
            let cfg_rows: Vec<LookupRow> = rows
                .iter()
                .map(|(pats, out)| LookupRow {
                    input_patterns: pats.clone(),
                    output: out.clone(),
                    parent_output: None,
                })
                .collect();

            let cfg = LookupMapping {
                id: "l".into(),
                name: None,
                match_: Some("keyword_substring".into()),
                case_insensitive: Some(case_insensitive),
                rows: cfg_rows,
                children: vec![],
                parent_output_column: None,
                catch_all: None,
            };

            let matcher = LookupMatcher::from_config(&cfg);
            let got = matcher.match_first(&input);

            // Reference implementation: walk rows in definition order and
            // return the first row whose any pattern is a substring of the
            // (possibly-lowercased) input.
            let haystack_ref: String = if case_insensitive {
                input.to_lowercase()
            } else {
                input.clone()
            };
            let mut expected: Option<LookupHit> = None;
            for (pats, out) in &rows {
                let hit = pats.iter().any(|p| {
                    let p_ref: String = if case_insensitive {
                        p.to_lowercase()
                    } else {
                        p.clone()
                    };
                    haystack_ref.contains(&p_ref)
                });
                if hit {
                    expected = Some(LookupHit {
                        output: out.clone(),
                        parent_output: None,
                    });
                    break;
                }
            }

            prop_assert_eq!(got, expected);
        }
    }

    proptest! {
        // Hierarchical lookup: child rows win, parent rows fall back.
        #[test]
        fn hierarchical_lookup_matches_design(
            case_insensitive in any::<bool>(),
            parent_rows in proptest::collection::vec(
                (proptest::collection::vec("[a-zA-Z]{1,8}", 1..=3), "[A-Z]{1,8}"),
                1..=3,
            ),
            child_rows in proptest::collection::vec(
                (proptest::collection::vec("[a-zA-Z]{1,8}", 1..=3), "[A-Z]{1,8}", "[A-Z]{1,8}"),
                1..=3,
            ),
            input in ".{0,30}",
        ) {
            let parent_cfg_rows: Vec<LookupRow> = parent_rows.iter().map(|(pats, out)| LookupRow {
                input_patterns: pats.clone(),
                output: out.clone(),
                parent_output: None,
            }).collect();

            let child_cfg_rows: Vec<LookupRow> = child_rows.iter().map(|(pats, out, parent_out)| LookupRow {
                input_patterns: pats.clone(),
                output: out.clone(),
                parent_output: Some(parent_out.clone()),
            }).collect();

            let child = LookupMapping {
                id: "child".into(),
                name: None,
                match_: None,
                case_insensitive: Some(case_insensitive),
                rows: child_cfg_rows,
                children: vec![],
                parent_output_column: Some("category".into()),
                catch_all: None,
            };

            let parent = LookupMapping {
                id: "parent".into(),
                name: None,
                match_: Some("keyword_substring".into()),
                case_insensitive: Some(case_insensitive),
                rows: parent_cfg_rows,
                children: vec![child],
                parent_output_column: None,
                catch_all: None,
            };

            let m = LookupMatcher::from_config(&parent);
            let got = m.match_first(&input);

            // Reference impl:
            let haystack: String = if case_insensitive { input.to_lowercase() } else { input.clone() };
            let transform = |s: &str| -> String {
                if case_insensitive { s.to_lowercase() } else { s.to_string() }
            };

            // Try child rows first, in order.
            let mut expected: Option<LookupHit> = None;
            for (pats, out, parent_out) in &child_rows {
                if pats.iter().any(|p| haystack.contains(&transform(p))) {
                    expected = Some(LookupHit {
                        output: out.clone(),
                        parent_output: Some(parent_out.clone()),
                    });
                    break;
                }
            }
            // If no child matched, try parent rows.
            if expected.is_none() {
                for (pats, out) in &parent_rows {
                    if pats.iter().any(|p| haystack.contains(&transform(p))) {
                        expected = Some(LookupHit {
                            output: out.clone(),
                            parent_output: None,
                        });
                        break;
                    }
                }
            }

            prop_assert_eq!(got, expected);
        }
    }
}
