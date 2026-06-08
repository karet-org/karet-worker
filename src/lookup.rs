//! Lookup_Mapping matcher (keyword substring, case-insensitive, hierarchical).
//!
//! Children are consulted before the parent's own rows, so a more-specific
//! child classification wins over a broader parent bucket.

use std::collections::HashMap;
use std::sync::Arc;

use crate::config::LookupMapping;

/// Precompiled lookup table; scan order follows definition order.
pub struct LookupMatcher {
    rows: Vec<CompiledRow>,
    case_insensitive: bool,
    children: Vec<LookupMatcher>,
    /// Fallback emitted by [`match_first`] when the child+row scan finds
    /// nothing. `None` preserves the historical behavior: a miss maps to
    /// `None` (and null in the output column).
    catch_all: Option<String>,
}

struct CompiledRow {
    patterns: Vec<String>,
    output: String,
    priority: i64,
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
                priority: row.priority,
            })
            .collect();
        let children = cfg.children.iter().map(LookupMatcher::from_config).collect();
        let catch_all = cfg.catch_all.as_ref().map(|ca| ca.output.clone());
        Self {
            rows,
            case_insensitive,
            children,
            catch_all,
        }
    }

    /// Return the first matching row's output, consulting children before
    /// our own rows. Falls back to the matcher's [`Self::catch_all`] when
    /// nothing matches.
    pub fn match_first(&self, input: &str) -> Option<String> {
        if let Some(hit) = self.scan(input) {
            return Some(hit);
        }
        self.catch_all.clone()
    }

    /// Row + child scan without consulting the catch-all. Used
    /// recursively for child matchers so a child's catch-all only fires
    /// when the user queried the child directly (by dotted lookup id),
    /// not while the parent is walking its children for row matches.
    fn scan(&self, input: &str) -> Option<String> {
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

        // Among our own rows, pick the matching row with the highest
        // `priority`. Ties resolve to definition order (the first row at the
        // winning priority wins), so a config with all-equal priorities keeps
        // the historical first-match-wins behavior.
        let mut best: Option<&CompiledRow> = None;
        for row in &self.rows {
            if row.patterns.iter().any(|p| haystack_ref.contains(p.as_str())) {
                match best {
                    Some(current) if current.priority >= row.priority => {}
                    _ => best = Some(row),
                }
            }
        }
        best.map(|row| row.output.clone())
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
            catch_all: None,
        }
    }

    fn row(patterns: &[&str], output: &str) -> LookupRow {
        LookupRow {
            input_patterns: patterns.iter().map(|p| (*p).to_string()).collect(),
            output: output.to_string(),
            priority: 0,
        }
    }

    fn row_p(patterns: &[&str], output: &str, priority: i64) -> LookupRow {
        LookupRow {
            input_patterns: patterns.iter().map(|p| (*p).to_string()).collect(),
            output: output.to_string(),
            priority,
        }
    }

    #[test]
    fn matches_first_in_definition_order() {
        let cfg = mapping(
            false,
            vec![row(&["FOO"], "FIRST"), row(&["FOO", "BAR"], "SECOND")],
        );
        let m = LookupMatcher::from_config(&cfg);
        assert_eq!(m.match_first("FOO BAR"), Some("FIRST".to_string()));
    }

    #[test]
    fn case_insensitive_match() {
        let cfg = mapping(true, vec![row(&["Starbucks"], "FOOD")]);
        let m = LookupMatcher::from_config(&cfg);
        assert_eq!(
            m.match_first("visit STARBUCKS today"),
            Some("FOOD".to_string())
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

    // -----------------------------------------------------------------------
    // Priority selection
    // -----------------------------------------------------------------------

    #[test]
    fn higher_priority_row_wins_over_earlier_row() {
        // Regression for the Amazon-payroll bug: SHOPPING (AMAZON) is defined
        // before INCOME (PAYROLL), but INCOME has the higher priority, so an
        // "AMAZON PAYROLL" deposit must resolve to INCOME.
        let cfg = mapping(
            true,
            vec![
                row_p(&["AMAZON"], "SHOPPING", 0),
                row_p(&["PAYROLL", "DEPOSIT"], "INCOME", 10),
            ],
        );
        let m = LookupMatcher::from_config(&cfg);
        assert_eq!(
            m.match_first("AMAZON PAYROLL DEPOSIT"),
            Some("INCOME".to_string())
        );
    }

    #[test]
    fn equal_priority_falls_back_to_definition_order() {
        let cfg = mapping(
            true,
            vec![row_p(&["FOO"], "FIRST", 5), row_p(&["FOO", "BAR"], "SECOND", 5)],
        );
        let m = LookupMatcher::from_config(&cfg);
        assert_eq!(m.match_first("FOO BAR"), Some("FIRST".to_string()));
    }

    #[test]
    fn priority_only_compares_matching_rows() {
        // A high-priority row that doesn't match must not suppress a
        // lower-priority row that does.
        let cfg = mapping(
            true,
            vec![
                row_p(&["NETFLIX"], "ENTERTAINMENT", 100),
                row_p(&["UBER"], "TRANSPORT", 1),
            ],
        );
        let m = LookupMatcher::from_config(&cfg);
        assert_eq!(m.match_first("TOOK AN UBER"), Some("TRANSPORT".to_string()));
    }

    #[test]
    fn negative_priority_is_deprioritized() {
        let cfg = mapping(
            true,
            vec![row_p(&["AMAZON"], "SHOPPING", -1), row_p(&["AMAZON"], "DEFAULT", 0)],
        );
        let m = LookupMatcher::from_config(&cfg);
        assert_eq!(m.match_first("AMAZON.COM"), Some("DEFAULT".to_string()));
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
            catch_all: None,
        }
    }

    #[test]
    fn child_match_wins_over_parent() {
        // Parent would bucket anything mentioning FOOD; child recognizes the
        // more specific STARBUCKS merchant and should take precedence.
        let child = mapping_with_children(true, vec![row(&["STARBUCKS"], "STARBUCKS")], vec![]);
        let parent = mapping_with_children(true, vec![row(&["FOOD"], "FOOD")], vec![child]);
        let m = LookupMatcher::from_config(&parent);
        assert_eq!(m.match_first("MY STARBUCKS"), Some("STARBUCKS".to_string()));
    }

    #[test]
    fn parent_match_when_no_child_matches() {
        // Child only knows TAXI; input mentions UBER, which only the parent's
        // own rows catch.
        let child = mapping_with_children(true, vec![row(&["TAXI"], "TAXI")], vec![]);
        let parent = mapping_with_children(true, vec![row(&["UBER"], "TRANSPORT")], vec![child]);
        let m = LookupMatcher::from_config(&parent);
        assert_eq!(m.match_first("UBER RIDE"), Some("TRANSPORT".to_string()));
    }

    #[test]
    fn nested_grandchild_match() {
        // parent -> child -> grandchild. Input matches only the grandchild;
        // recursion must walk all the way down.
        let grandchild =
            mapping_with_children(true, vec![row(&["BLUE_BOTTLE"], "BLUE_BOTTLE")], vec![]);
        let child =
            mapping_with_children(true, vec![row(&["STARBUCKS"], "STARBUCKS")], vec![grandchild]);
        let parent = mapping_with_children(true, vec![row(&["FOOD"], "FOOD")], vec![child]);
        let m = LookupMatcher::from_config(&parent);
        assert_eq!(
            m.match_first("BLUE_BOTTLE COFFEE"),
            Some("BLUE_BOTTLE".to_string())
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
            catch_all,
        }
    }

    #[test]
    fn catch_all_fires_when_no_row_matches() {
        let cfg = mapping_with_catch_all(
            vec![row(&["UBER"], "TRANSPORT")],
            Some(crate::config::LookupCatchAll {
                output: "OTHER".into(),
            }),
        );
        let m = LookupMatcher::from_config(&cfg);
        assert_eq!(m.match_first("grocery store"), Some("OTHER".to_string()));
    }

    #[test]
    fn catch_all_does_not_fire_when_a_row_matches() {
        let cfg = mapping_with_catch_all(
            vec![row(&["UBER"], "TRANSPORT")],
            Some(crate::config::LookupCatchAll {
                output: "OTHER".into(),
            }),
        );
        let m = LookupMatcher::from_config(&cfg);
        assert_eq!(m.match_first("UBER TRIP"), Some("TRANSPORT".to_string()));
    }

    #[test]
    fn child_catch_all_does_not_shadow_parent_rows() {
        // Child has a catch-all and one row; parent has a row the child
        // doesn't know about. When we query the parent with input the
        // child doesn't match, the parent's row must still win --
        // otherwise a single child catch-all would blackhole every
        // parent input.
        let child = LookupMapping {
            id: "child".into(),
            name: None,
            match_: Some("keyword_substring".into()),
            case_insensitive: Some(true),
            rows: vec![row(&["TAXI"], "TAXI")],
            children: vec![],
            catch_all: Some(crate::config::LookupCatchAll {
                output: "CHILD_OTHER".into(),
            }),
        };
        let parent = LookupMapping {
            id: "parent".into(),
            name: None,
            match_: Some("keyword_substring".into()),
            case_insensitive: Some(true),
            rows: vec![row(&["UBER"], "TRANSPORT")],
            children: vec![child],
            catch_all: None,
        };

        let m = LookupMatcher::from_config(&parent);
        // "UBER RIDE" matches the parent's row, not the child's.
        assert_eq!(m.match_first("UBER RIDE"), Some("TRANSPORT".to_string()));
    }

    #[test]
    fn no_catch_all_still_returns_none_on_miss() {
        let cfg = mapping_with_catch_all(vec![row(&["UBER"], "TRANSPORT")], None);
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
                    priority: 0,
                })
                .collect();

            let cfg = LookupMapping {
                id: "l".into(),
                name: None,
                match_: Some("keyword_substring".into()),
                case_insensitive: Some(case_insensitive),
                rows: cfg_rows,
                children: vec![],
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
            let mut expected: Option<String> = None;
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
                    expected = Some(out.clone());
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
                (proptest::collection::vec("[a-zA-Z]{1,8}", 1..=3), "[A-Z]{1,8}"),
                1..=3,
            ),
            input in ".{0,30}",
        ) {
            let parent_cfg_rows: Vec<LookupRow> = parent_rows.iter().map(|(pats, out)| LookupRow {
                input_patterns: pats.clone(),
                output: out.clone(),
                priority: 0,
            }).collect();

            let child_cfg_rows: Vec<LookupRow> = child_rows.iter().map(|(pats, out)| LookupRow {
                input_patterns: pats.clone(),
                output: out.clone(),
                priority: 0,
            }).collect();

            let child = LookupMapping {
                id: "child".into(),
                name: None,
                match_: None,
                case_insensitive: Some(case_insensitive),
                rows: child_cfg_rows,
                children: vec![],
                catch_all: None,
            };

            let parent = LookupMapping {
                id: "parent".into(),
                name: None,
                match_: Some("keyword_substring".into()),
                case_insensitive: Some(case_insensitive),
                rows: parent_cfg_rows,
                children: vec![child],
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
            let mut expected: Option<String> = None;
            for (pats, out) in &child_rows {
                if pats.iter().any(|p| haystack.contains(&transform(p))) {
                    expected = Some(out.clone());
                    break;
                }
            }
            // If no child matched, try parent rows.
            if expected.is_none() {
                for (pats, out) in &parent_rows {
                    if pats.iter().any(|p| haystack.contains(&transform(p))) {
                        expected = Some(out.clone());
                        break;
                    }
                }
            }

            prop_assert_eq!(got, expected);
        }
    }
}
