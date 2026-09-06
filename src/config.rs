//! `Pipeline_Config` types and validation.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::ast::AstNode;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PipelineConfig {
    pub version: u32,
    pub source_containers: Vec<SourceContainer>,
    pub lookup_mappings: Vec<LookupMapping>,
    pub mappings: Vec<Mapping>,
    pub analytic_tables: Vec<AnalyticTable>,
    #[serde(default)]
    pub layout: HashMap<String, LayoutPosition>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SourceContainer {
    pub id: String,
    pub name: String,
    pub path_prefix: String,
    pub schema: Vec<ColumnSchema>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ColumnSchema {
    pub name: String,
    /// Logical type name: `"string"`, `"number"`, `"int64"`, `"float64"`,
    /// `"date"`, `"bool"`.
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(default)]
    pub nullable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assertions: Option<ColumnAssertions>,
}

/// Declarative data-quality checks on a column. Applied post-mapping,
/// pre-write. A failed check fails this mapping only; others still run.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct ColumnAssertions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_null: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LookupMapping {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    /// Matching strategy, e.g. `"keyword_substring"`. Parent-only.
    #[serde(default, rename = "match")]
    pub match_: Option<String>,
    #[serde(default)]
    pub case_insensitive: Option<bool>,
    pub rows: Vec<LookupRow>,
    #[serde(default)]
    pub children: Vec<LookupMapping>,
    /// Fallback hit emitted when no row's patterns match (after children
    /// have also missed). Unset = miss yields `None` (null in the output
    /// column), preserving pre-catch-all behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catch_all: Option<LookupCatchAll>,
}

/// Output for a [`LookupMapping::catch_all`] fallback.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LookupCatchAll {
    pub output: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LookupRow {
    pub input_patterns: Vec<String>,
    pub output: String,
    /// Tie-breaker when more than one row in the same lookup node matches an
    /// input. The matcher picks the matching row with the highest `priority`;
    /// ties fall back to definition order. Defaults to `0`, so omitting the
    /// field preserves first-match-wins behavior.
    #[serde(default)]
    pub priority: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
    pub id: String,
    #[serde(default)]
    pub name: String,
    pub source_container_id: String,
    pub analytic_table_id: String,
    pub columns: Vec<MappingColumn>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct MappingColumn {
    pub name: String,
    pub expr: AstNode,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct AnalyticTable {
    pub id: String,
    pub name: String,
    pub schema: Vec<ColumnSchema>,
    /// Ordered hive partition keys; each names a schema column. Empty
    /// means unpartitioned. Capped at 2 by validation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_keys: Vec<String>,
    /// Row-identity columns; rows sharing the tuple collapse to one at
    /// write time (first in ingest order survives). Empty = no dedup.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dedup_keys: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LayoutPosition {
    pub x: f64,
    pub y: f64,
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

use std::collections::HashSet;

use thiserror::Error;

/// A single validation error. `path` is a JSON Pointer into the config doc.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConfigError {
    #[error("duplicate {kind} id `{id}` at {path}")]
    DuplicateId {
        path: String,
        kind: String,
        id: String,
    },

    #[error("dangling {kind} reference to `{target}` at {path}")]
    DanglingReference {
        path: String,
        kind: String,
        target: String,
    },

    #[error("schema error at {path}: {message}")]
    Schema { path: String, message: String },

    #[error("missing field `{field}` at {path}")]
    MissingField { path: String, field: String },
}

/// Validate a [`PipelineConfig`]. Collects every error (no short-circuit).
///
/// Checks:
/// - Unique non-empty ids/names per collection.
/// - Every mapping reference (source, table, lookup, columns) resolves.
/// - Table `partition_keys` (max 2, non-float) and `dedup_keys` name schema columns.
/// - Source `path_prefix` is a well-formed lake key prefix; no source's prefix
///   is a prefix of another's within the pipeline.
pub fn validate(cfg: &PipelineConfig) -> Result<(), Vec<ConfigError>> {
    let mut errors: Vec<ConfigError> = Vec::new();

    // Uniqueness and required fields per collection.

    check_unique_and_required_ids(
        &cfg.source_containers,
        "source_containers",
        "source_container",
        |s| &s.id,
        |s| Some(&s.name),
        &mut errors,
    );
    check_unique_and_required_ids(
        &cfg.lookup_mappings,
        "lookup_mappings",
        "lookup_mapping",
        |l| &l.id,
        |_| None,
        &mut errors,
    );
    check_unique_and_required_ids(
        &cfg.mappings,
        "mappings",
        "mapping",
        |m| &m.id,
        |_| None,
        &mut errors,
    );
    check_unique_and_required_ids(
        &cfg.analytic_tables,
        "analytic_tables",
        "analytic_table",
        |t| &t.id,
        |t| Some(&t.name),
        &mut errors,
    );

    // Reference checks on every mapping.

    let source_ids: HashSet<&str> = cfg
        .source_containers
        .iter()
        .map(|s| s.id.as_str())
        .collect();
    let table_ids: HashSet<&str> = cfg
        .analytic_tables
        .iter()
        .map(|t| t.id.as_str())
        .collect();

    for (i, mapping) in cfg.mappings.iter().enumerate() {
        if !mapping.source_container_id.is_empty()
            && !source_ids.contains(mapping.source_container_id.as_str())
        {
            errors.push(ConfigError::DanglingReference {
                path: format!("/mappings/{i}/source_container_id"),
                kind: "source_container".to_string(),
                target: mapping.source_container_id.clone(),
            });
        }

        if !mapping.analytic_table_id.is_empty()
            && !table_ids.contains(mapping.analytic_table_id.as_str())
        {
            errors.push(ConfigError::DanglingReference {
                path: format!("/mappings/{i}/analytic_table_id"),
                kind: "analytic_table".to_string(),
                target: mapping.analytic_table_id.clone(),
            });
        }

        for (j, column) in mapping.columns.iter().enumerate() {
            let expr_path = format!("/mappings/{i}/columns/{j}/expr");
            walk_ast(&column.expr, &mut |node| {
                if let AstNode::LookupRef { lookup_id, .. } = node {
                    if resolve_lookup(lookup_id, &cfg.lookup_mappings).is_none() {
                        errors.push(ConfigError::DanglingReference {
                            path: expr_path.clone(),
                            kind: "lookup".to_string(),
                            target: lookup_id.clone(),
                        });
                    }
                }
            });
        }

        // Cross-entity column checks.

        // Mapping output columns must appear in the target table schema.
        if let Some(table) = cfg
            .analytic_tables
            .iter()
            .find(|t| t.id == mapping.analytic_table_id)
        {
            let table_cols: HashSet<&str> =
                table.schema.iter().map(|c| c.name.as_str()).collect();
            for (j, column) in mapping.columns.iter().enumerate() {
                if !table_cols.contains(column.name.as_str()) {
                    errors.push(ConfigError::DanglingReference {
                        path: format!("/mappings/{i}/columns/{j}/name"),
                        kind: "analytic_table_column".to_string(),
                        target: format!("{}.{}", table.id, column.name),
                    });
                }
            }
        }

        // Every `Col` ref must resolve to a source schema column.
        if let Some(src) = cfg
            .source_containers
            .iter()
            .find(|s| s.id == mapping.source_container_id)
        {
            let src_cols: HashSet<&str> =
                src.schema.iter().map(|c| c.name.as_str()).collect();
            for (j, column) in mapping.columns.iter().enumerate() {
                let expr_path = format!("/mappings/{i}/columns/{j}/expr");
                walk_ast(&column.expr, &mut |node| {
                    if let AstNode::Col { name } = node {
                        if !src_cols.contains(name.as_str()) {
                            errors.push(ConfigError::DanglingReference {
                                path: expr_path.clone(),
                                kind: "source_column".to_string(),
                                target: format!("{}.{}", src.id, name),
                            });
                        }
                    }
                });
            }
        }
    }

    // Table partition and dedup keys reference schema columns.
    for (i, table) in cfg.analytic_tables.iter().enumerate() {
        let schema_cols: HashMap<&str, &str> = table
            .schema
            .iter()
            .map(|c| (c.name.as_str(), c.type_.as_str()))
            .collect();

        if table.partition_keys.len() > 2 {
            errors.push(ConfigError::Schema {
                path: format!("/analytic_tables/{i}/partition_keys"),
                message: format!(
                    "{} partition keys; at most 2 are supported",
                    table.partition_keys.len()
                ),
            });
        }
        let mut seen: HashSet<&str> = HashSet::new();
        for key in &table.partition_keys {
            if !seen.insert(key.as_str()) {
                errors.push(ConfigError::Schema {
                    path: format!("/analytic_tables/{i}/partition_keys"),
                    message: format!("duplicate partition key `{key}`"),
                });
                continue;
            }
            match schema_cols.get(key.as_str()) {
                None => errors.push(ConfigError::DanglingReference {
                    path: format!("/analytic_tables/{i}/partition_keys"),
                    kind: "schema_column".to_string(),
                    target: key.clone(),
                }),
                Some(&"float64") | Some(&"number") => errors.push(ConfigError::Schema {
                    path: format!("/analytic_tables/{i}/partition_keys"),
                    message: format!(
                        "partition key `{key}` is float-capable; floats make unstable path segments"
                    ),
                }),
                Some(_) => {}
            }
        }

        let mut seen: HashSet<&str> = HashSet::new();
        for key in &table.dedup_keys {
            if !seen.insert(key.as_str()) {
                errors.push(ConfigError::Schema {
                    path: format!("/analytic_tables/{i}/dedup_keys"),
                    message: format!("duplicate dedup key `{key}`"),
                });
                continue;
            }
            if !schema_cols.contains_key(key.as_str()) {
                errors.push(ConfigError::DanglingReference {
                    path: format!("/analytic_tables/{i}/dedup_keys"),
                    kind: "schema_column".to_string(),
                    target: key.clone(),
                });
            }
        }
    }

    // Source prefixes: well-formed absolute lake key prefixes, and no
    // source's prefix may be a prefix of another's (ingest resolves a
    // file's source by prefix match; nesting makes that ambiguous).
    for (i, sc) in cfg.source_containers.iter().enumerate() {
        let path = format!("/source_containers/{i}/path_prefix");
        let p = sc.path_prefix.as_str();
        if p.is_empty() {
            errors.push(ConfigError::MissingField {
                path,
                field: "path_prefix".to_string(),
            });
            continue;
        }
        if p.starts_with('/') || !p.ends_with('/') || p.split('/').any(|seg| seg == "..") {
            errors.push(ConfigError::Schema {
                path,
                message: format!(
                    "path_prefix `{p}` must be a relative key prefix ending in `/` without `..` segments"
                ),
            });
        }
    }
    for (i, a) in cfg.source_containers.iter().enumerate() {
        for (j, b) in cfg.source_containers.iter().enumerate() {
            if i != j && !a.path_prefix.is_empty() && b.path_prefix.starts_with(&a.path_prefix) {
                errors.push(ConfigError::Schema {
                    path: format!("/source_containers/{j}/path_prefix"),
                    message: format!(
                        "prefix `{}` is nested under `{}` (source `{}`); source prefixes must not overlap",
                        b.path_prefix, a.path_prefix, a.id
                    ),
                });
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Verify `id` is non-empty, names are non-empty, and ids are unique.
fn check_unique_and_required_ids<T, FId, FName>(
    items: &[T],
    collection: &str,
    kind: &str,
    id_of: FId,
    name_of: FName,
    errors: &mut Vec<ConfigError>,
) where
    FId: Fn(&T) -> &String,
    FName: Fn(&T) -> Option<&String>,
{
    let mut seen: HashSet<&str> = HashSet::new();
    for (i, item) in items.iter().enumerate() {
        let id = id_of(item);
        if id.is_empty() {
            errors.push(ConfigError::MissingField {
                path: format!("/{collection}/{i}/id"),
                field: "id".to_string(),
            });
        } else if !seen.insert(id.as_str()) {
            errors.push(ConfigError::DuplicateId {
                path: format!("/{collection}/{i}/id"),
                kind: kind.to_string(),
                id: id.clone(),
            });
        }

        if let Some(name) = name_of(item) {
            if name.is_empty() {
                errors.push(ConfigError::MissingField {
                    path: format!("/{collection}/{i}/name"),
                    field: "name".to_string(),
                });
            }
        }
    }
}

/// Depth-first walk over an [`AstNode`] tree. Invokes `visit` on every node.
fn walk_ast(node: &AstNode, visit: &mut impl FnMut(&AstNode)) {
    visit(node);
    match node {
        AstNode::Col { .. }
        | AstNode::Str { .. }
        | AstNode::Num { .. }
        | AstNode::Bool { .. }
        | AstNode::Null => {}

        AstNode::Add { left, right }
        | AstNode::Sub { left, right }
        | AstNode::Mul { left, right }
        | AstNode::Div { left, right }
        | AstNode::Eq { left, right }
        | AstNode::Ne { left, right }
        | AstNode::Gt { left, right }
        | AstNode::Lt { left, right }
        | AstNode::Ge { left, right }
        | AstNode::Le { left, right } => {
            walk_ast(left, visit);
            walk_ast(right, visit);
        }

        AstNode::Concat { args, .. } => {
            for arg in args {
                walk_ast(arg, visit);
            }
        }

        AstNode::Coalesce { args } => {
            for arg in args {
                walk_ast(arg, visit);
            }
        }

        AstNode::Upper { input }
        | AstNode::Lower { input }
        | AstNode::Trim { input }
        | AstNode::Substring { input, .. }
        | AstNode::ParseDate { input, .. }
        | AstNode::Year { input }
        | AstNode::Month { input }
        | AstNode::Day { input }
        | AstNode::LookupRef { input, .. }
        | AstNode::Cast { input, .. } => {
            walk_ast(input, visit);
        }

        AstNode::Contains { input, pattern } => {
            walk_ast(input, visit);
            walk_ast(pattern, visit);
        }

        AstNode::If { cond, then, r#else } => {
            walk_ast(cond, visit);
            walk_ast(then, visit);
            walk_ast(r#else, visit);
        }
    }
}

/// Resolve a dotted lookup path (e.g. `categories.merchants`) against the
/// top-level lookups. Returns `None` if any segment fails to resolve.
fn resolve_lookup<'a>(path: &str, lookups: &'a [LookupMapping]) -> Option<&'a LookupMapping> {
    let mut segments = path.split('.');
    let head = segments.next()?;
    if head.is_empty() {
        return None;
    }
    let mut current: &LookupMapping = lookups.iter().find(|l| l.id == head)?;
    for segment in segments {
        if segment.is_empty() {
            return None;
        }
        current = current.children.iter().find(|c| c.id == segment)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal valid config: one source, one lookup (with a child), one
    /// mapping referencing them, one analytic table.
    fn minimal_valid_config() -> PipelineConfig {
        PipelineConfig {
            version: 1,
            source_containers: vec![SourceContainer {
                id: "src".to_string(),
                name: "Src".to_string(),
                path_prefix: "raw/src/".to_string(),
                schema: vec![ColumnSchema {
                    name: "a".to_string(),
                    type_: "string".to_string(),
                    nullable: None,
                    assertions: None,
                }],
            }],
            lookup_mappings: vec![LookupMapping {
                id: "categories".to_string(),
                name: Some("Categories".to_string()),
                match_: Some("keyword_substring".to_string()),
                case_insensitive: Some(true),
                rows: vec![LookupRow {
                    input_patterns: vec!["UBER".to_string()],
                    output: "TRANSPORT".to_string(),
                    priority: 0,
                }],
                children: vec![LookupMapping {
                    id: "merchants".to_string(),
                    name: None,
                    match_: None,
                    case_insensitive: None,
                    rows: vec![LookupRow {
                        input_patterns: vec!["UBER".to_string()],
                        output: "UBER".to_string(),
                        priority: 0,
                    }],
                    children: vec![],
                    catch_all: None,
                }],
                catch_all: None,
            }],
            mappings: vec![Mapping {
                id: "m".to_string(),
                name: String::new(),
                source_container_id: "src".to_string(),
                analytic_table_id: "t".to_string(),
                columns: vec![
                    MappingColumn {
                        name: "cat".to_string(),
                        expr: AstNode::LookupRef {
                            lookup_id: "categories".to_string(),
                            input: Box::new(AstNode::Col {
                                name: "a".to_string(),
                            }),
                        },
                    },
                    MappingColumn {
                        name: "mer".to_string(),
                        expr: AstNode::LookupRef {
                            lookup_id: "categories.merchants".to_string(),
                            input: Box::new(AstNode::Col {
                                name: "a".to_string(),
                            }),
                        },
                    },
                ],
            }],
            analytic_tables: vec![AnalyticTable {
                id: "t".to_string(),
                name: "T".to_string(),
                schema: vec![
                    ColumnSchema {
                        name: "cat".to_string(),
                        type_: "string".to_string(),
                        nullable: Some(true),
                        assertions: None,
                    },
                    ColumnSchema {
                        name: "mer".to_string(),
                        type_: "string".to_string(),
                        nullable: Some(true),
                        assertions: None,
                    },
                ],
                partition_keys: vec![],
                dedup_keys: vec![],
            }],
            layout: HashMap::new(),
        }
    }

    #[test]
    fn valid_config_passes() {
        let cfg = minimal_valid_config();
        assert_eq!(validate(&cfg), Ok(()));
    }

    #[test]
    fn duplicate_source_container_id_is_reported() {
        let mut cfg = minimal_valid_config();
        cfg.source_containers.push(cfg.source_containers[0].clone());
        let errs = validate(&cfg).unwrap_err();
        assert!(errs.iter().any(|e| matches!(
            e,
            ConfigError::DuplicateId { kind, id, .. } if kind == "source_container" && id == "src"
        )));
    }

    #[test]
    fn dangling_source_container_reference_is_reported() {
        let mut cfg = minimal_valid_config();
        cfg.mappings[0].source_container_id = "missing".to_string();
        let errs = validate(&cfg).unwrap_err();
        assert!(errs.iter().any(|e| matches!(
            e,
            ConfigError::DanglingReference { kind, target, .. }
                if kind == "source_container" && target == "missing"
        )));
    }

    #[test]
    fn dangling_lookup_reference_is_reported() {
        let mut cfg = minimal_valid_config();
        cfg.mappings[0].columns[0].expr = AstNode::LookupRef {
            lookup_id: "does_not_exist".to_string(),
            input: Box::new(AstNode::Col {
                name: "a".to_string(),
            }),
        };
        let errs = validate(&cfg).unwrap_err();
        assert!(errs.iter().any(|e| matches!(
            e,
            ConfigError::DanglingReference { kind, target, .. }
                if kind == "lookup" && target == "does_not_exist"
        )));
    }

    #[test]
    fn dangling_child_lookup_reference_is_reported() {
        let mut cfg = minimal_valid_config();
        cfg.mappings[0].columns[1].expr = AstNode::LookupRef {
            lookup_id: "categories.missing_child".to_string(),
            input: Box::new(AstNode::Col {
                name: "a".to_string(),
            }),
        };
        let errs = validate(&cfg).unwrap_err();
        assert!(errs.iter().any(|e| matches!(
            e,
            ConfigError::DanglingReference { kind, target, .. }
                if kind == "lookup" && target == "categories.missing_child"
        )));
    }

    #[test]
    fn empty_id_is_reported_as_missing_field() {
        let mut cfg = minimal_valid_config();
        cfg.source_containers[0].id = String::new();
        // The mapping's source_container_id still says "src", so the empty id
        // manifests as MissingField on the container. Break the mapping ref so
        // the test doesn't also depend on the dangling-reference variant.
        cfg.mappings[0].source_container_id = String::new();
        let errs = validate(&cfg).unwrap_err();
        assert!(errs.iter().any(|e| matches!(
            e,
            ConfigError::MissingField { path, field }
                if path == "/source_containers/0/id" && field == "id"
        )));
    }

    /// A config with zero lookup_mappings must validate when no mapping
    /// references a lookup.
    #[test]
    fn valid_config_without_lookups_passes() {
        let cfg = PipelineConfig {
            version: 1,
            source_containers: vec![SourceContainer {
                id: "src".to_string(),
                name: "Src".to_string(),
                path_prefix: "raw/src/".to_string(),
                schema: vec![ColumnSchema {
                    name: "a".to_string(),
                    type_: "string".to_string(),
                    nullable: None,
                    assertions: None,
                }],
            }],
            lookup_mappings: vec![],
            mappings: vec![Mapping {
                id: "m".to_string(),
                name: String::new(),
                source_container_id: "src".to_string(),
                analytic_table_id: "t".to_string(),
                columns: vec![MappingColumn {
                    name: "out".to_string(),
                    expr: AstNode::Col {
                        name: "a".to_string(),
                    },
                }],
            }],
            analytic_tables: vec![AnalyticTable {
                id: "t".to_string(),
                name: "T".to_string(),
                schema: vec![ColumnSchema {
                    name: "out".to_string(),
                    type_: "string".to_string(),
                    nullable: Some(true),
                    assertions: None,
                }],
                partition_keys: vec![],
                dedup_keys: vec![],
            }],
            layout: HashMap::new(),
        };
        assert_eq!(validate(&cfg), Ok(()));
    }

    // -----------------------------------------------------------------------
    // Cross-entity column-reference checks
    // -----------------------------------------------------------------------

    #[test]
    fn mapping_column_not_in_analytic_table_is_reported() {
        // The analytic table schema is missing `mer`, so the mapping's `mer`
        // output column should be flagged as a dangling reference.
        let mut cfg = minimal_valid_config();
        cfg.analytic_tables[0].schema.retain(|c| c.name != "mer");
        let errs = validate(&cfg).unwrap_err();
        assert!(
            errs.iter().any(|e| matches!(
                e,
                ConfigError::DanglingReference { kind, target, .. }
                    if kind == "analytic_table_column" && target.ends_with(".mer")
            )),
            "expected analytic_table_column error, got {errs:?}"
        );
    }

    #[test]
    fn partition_key_must_name_schema_column() {
        let mut cfg = minimal_valid_config();
        cfg.analytic_tables[0].partition_keys = vec!["ghost".to_string()];
        let errs = validate(&cfg).unwrap_err();
        assert!(
            errs.iter().any(|e| matches!(
                e,
                ConfigError::DanglingReference { kind, target, .. }
                    if kind == "schema_column" && target == "ghost"
            )),
            "expected schema_column error, got {errs:?}"
        );
    }

    #[test]
    fn partition_keys_reject_floats_dupes_and_more_than_two() {
        let mut cfg = minimal_valid_config();
        cfg.analytic_tables[0].schema.push(ColumnSchema {
            name: "amt".to_string(),
            type_: "float64".to_string(),
            nullable: Some(true),
            assertions: None,
        });
        cfg.mappings[0].columns.push(MappingColumn {
            name: "amt".to_string(),
            expr: AstNode::Num { value: 1.0 },
        });
        cfg.analytic_tables[0].partition_keys =
            vec!["cat".into(), "cat".into(), "amt".into()];
        let errs = validate(&cfg).unwrap_err();
        let msgs: Vec<String> = errs.iter().map(|e| e.to_string()).collect();
        assert!(msgs.iter().any(|m| m.contains("at most 2")), "{msgs:?}");
        assert!(msgs.iter().any(|m| m.contains("duplicate partition key")), "{msgs:?}");
        assert!(msgs.iter().any(|m| m.contains("float-capable")), "{msgs:?}");

        // `number` is float-capable and equally ineligible.
        let mut cfg = minimal_valid_config();
        cfg.analytic_tables[0].schema.push(ColumnSchema {
            name: "n".to_string(),
            type_: "number".to_string(),
            nullable: Some(true),
            assertions: None,
        });
        cfg.mappings[0].columns.push(MappingColumn {
            name: "n".to_string(),
            expr: AstNode::Num { value: 1.0 },
        });
        cfg.analytic_tables[0].partition_keys = vec!["n".into()];
        let errs = validate(&cfg).unwrap_err();
        assert!(errs.iter().any(|e| e.to_string().contains("float-capable")), "{errs:?}");
    }

    #[test]
    fn dedup_key_must_name_schema_column_but_floats_allowed() {
        let mut cfg = minimal_valid_config();
        cfg.analytic_tables[0].dedup_keys = vec!["ghost".to_string()];
        let errs = validate(&cfg).unwrap_err();
        assert!(errs.iter().any(|e| matches!(
            e,
            ConfigError::DanglingReference { kind, target, .. }
                if kind == "schema_column" && target == "ghost"
        )));

        let mut cfg = minimal_valid_config();
        cfg.analytic_tables[0].schema.push(ColumnSchema {
            name: "amt".to_string(),
            type_: "float64".to_string(),
            nullable: Some(true),
            assertions: None,
        });
        cfg.mappings[0].columns.push(MappingColumn {
            name: "amt".to_string(),
            expr: AstNode::Num { value: 1.0 },
        });
        cfg.analytic_tables[0].dedup_keys = vec!["amt".to_string()];
        assert_eq!(validate(&cfg), Ok(()));
    }

    #[test]
    fn source_prefix_shape_is_validated() {
        for bad in ["/abs/", "no-slash", "a/../b/", ""] {
            let mut cfg = minimal_valid_config();
            cfg.source_containers[0].path_prefix = bad.to_string();
            assert!(validate(&cfg).is_err(), "prefix `{bad}` should fail");
        }
        let mut cfg = minimal_valid_config();
        cfg.source_containers[0].path_prefix = "banks/rbc/chequing/".to_string();
        assert_eq!(validate(&cfg), Ok(()));
    }

    #[test]
    fn nested_source_prefixes_rejected() {
        let mut cfg = minimal_valid_config();
        let mut second = cfg.source_containers[0].clone();
        second.id = "s2".to_string();
        second.name = "S2".to_string();
        second.path_prefix = format!("{}nested/", cfg.source_containers[0].path_prefix);
        cfg.source_containers.push(second);
        let errs = validate(&cfg).unwrap_err();
        assert!(
            errs.iter().any(|e| e.to_string().contains("must not overlap")),
            "{errs:?}"
        );
    }

    #[test]
    fn col_ref_not_in_source_schema_is_reported() {
        // Source container only has column `a`; rewrite a mapping expression
        // to reference `nope` instead.
        let mut cfg = minimal_valid_config();
        cfg.mappings[0].columns[0].expr = AstNode::Col {
            name: "nope".to_string(),
        };
        let errs = validate(&cfg).unwrap_err();
        assert!(
            errs.iter().any(|e| matches!(
                e,
                ConfigError::DanglingReference { kind, target, .. }
                    if kind == "source_column" && target.ends_with(".nope")
            )),
            "expected source_column error, got {errs:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Error message content checks (Display rendering must carry context)
    // -----------------------------------------------------------------------

    #[test]
    fn error_message_contains_offending_identifier() {
        // ConfigError variants embed the offending id/path in their Display
        // output (via thiserror). One representative case guards that the
        // context isn't dropped from the message; that each error variant is
        // raised for the right condition is covered by the validation tests
        // above (dangling references, missing fields, etc.).
        let mut cfg = minimal_valid_config();
        cfg.source_containers.push(cfg.source_containers[0].clone());
        let errs = validate(&cfg).unwrap_err();
        let err = errs
            .iter()
            .find(|e| matches!(e, ConfigError::DuplicateId { .. }))
            .expect("expected a DuplicateId error");
        assert!(
            err.to_string().contains("src"),
            "Display `{err}` should contain the duplicated id `src`"
        );
    }

    // -----------------------------------------------------------------------
    // Property tests: validator rejects mutated configs
    // -----------------------------------------------------------------------

    use proptest::prelude::*;

    #[derive(Debug, Clone, Copy)]
    enum Mutation {
        DropSourceId,
        DuplicateSourceId,
        DropMappingId,
        BreakMappingSourceRef,
        BreakMappingTableRef,
        BreakLookupRef,
    }

    fn arb_mutation() -> impl Strategy<Value = Mutation> {
        prop_oneof![
            Just(Mutation::DropSourceId),
            Just(Mutation::DuplicateSourceId),
            Just(Mutation::DropMappingId),
            Just(Mutation::BreakMappingSourceRef),
            Just(Mutation::BreakMappingTableRef),
            Just(Mutation::BreakLookupRef),
        ]
    }

    /// Generate a small valid `PipelineConfig` whose references all resolve.
    fn arb_valid_pipeline_config() -> impl Strategy<Value = PipelineConfig> {
        (1usize..=3, 0usize..=2, 1usize..=3, 1usize..=3).prop_flat_map(
            |(n_sources, n_lookups, n_tables, n_mappings)| {
                let expr_tag = prop_oneof![
                    Just(0u8), // Col
                    Just(1u8), // Str
                    Just(2u8), // Num
                    Just(3u8), // LookupRef (only valid when n_lookups > 0)
                ];
                (
                    proptest::collection::vec(0usize..n_sources, n_mappings),
                    proptest::collection::vec(0usize..n_tables, n_mappings),
                    proptest::collection::vec(expr_tag, n_mappings),
                    proptest::collection::vec(0usize..n_lookups.max(1), n_mappings),
                )
                    .prop_map(move |(src_idxs, tbl_idxs, tags, lk_idxs)| {
                        let source_containers: Vec<SourceContainer> = (0..n_sources)
                            .map(|i| SourceContainer {
                                id: format!("s{i}"),
                                name: format!("S{i}"),
                                path_prefix: format!("raw/s{i}/"),
                                schema: vec![ColumnSchema {
                                    name: "c0".to_string(),
                                    type_: "string".to_string(),
                                    nullable: None,
                                    assertions: None,
                                }],
                            })
                            .collect();

                        let lookup_mappings: Vec<LookupMapping> = (0..n_lookups)
                            .map(|i| LookupMapping {
                                id: format!("l{i}"),
                                name: Some(format!("L{i}")),
                                match_: Some("keyword_substring".to_string()),
                                case_insensitive: Some(true),
                                rows: vec![LookupRow {
                                    input_patterns: vec!["x".to_string()],
                                    output: "Y".to_string(),
                                    priority: 0,
                                }],
                                children: vec![],
                                catch_all: None,
                            })
                            .collect();

                        let analytic_tables: Vec<AnalyticTable> = (0..n_tables)
                            .map(|i| AnalyticTable {
                                id: format!("t{i}"),
                                name: format!("T{i}"),
                                schema: vec![ColumnSchema {
                                    name: "out0".to_string(),
                                    type_: "string".to_string(),
                                    nullable: Some(true),
                                    assertions: None,
                                }],
                                partition_keys: vec![],
                                dedup_keys: vec![],
                            })
                            .collect();

                        let mappings: Vec<Mapping> = (0..n_mappings)
                            .map(|i| {
                                let expr = match tags[i] {
                                    0 => AstNode::Col {
                                        name: "c0".to_string(),
                                    },
                                    1 => AstNode::Str {
                                        value: "x".to_string(),
                                    },
                                    2 => AstNode::Num { value: 1.0 },
                                    _ if n_lookups > 0 => AstNode::LookupRef {
                                        lookup_id: format!("l{}", lk_idxs[i] % n_lookups),
                                        input: Box::new(AstNode::Col {
                                            name: "c0".to_string(),
                                        }),
                                    },
                                    // Fallback when no lookups exist.
                                    _ => AstNode::Col {
                                        name: "c0".to_string(),
                                    },
                                };
                                Mapping {
                                                id: format!("m{i}"),
                                                name: String::new(),
                                                source_container_id: format!("s{}", src_idxs[i]),
                                                analytic_table_id: format!("t{}", tbl_idxs[i]),
                                                                                columns: vec![MappingColumn {
                                                    name: "out0".to_string(),
                                                    expr,
                                                }],
                                            }
                            })
                            .collect();

                        PipelineConfig {
                            version: 1,
                            source_containers,
                            lookup_mappings,
                            mappings,
                            analytic_tables,
                            layout: HashMap::new(),
                        }
                    })
            },
        )
    }

    /// Apply `mutation` to `cfg`. Returns `None` when the mutation's
    /// precondition isn't met (e.g. `DuplicateSourceId` on a single-source
    /// config), so the property test can skip that case instead of panicking.
    fn apply_mutation(mut cfg: PipelineConfig, mutation: Mutation) -> Option<PipelineConfig> {
        match mutation {
            Mutation::DropSourceId => {
                let s = cfg.source_containers.first_mut()?;
                s.id = String::new();
            }
            Mutation::DuplicateSourceId => {
                if cfg.source_containers.len() < 2 {
                    return None;
                }
                let first_id = cfg.source_containers[0].id.clone();
                cfg.source_containers[1].id = first_id;
            }
            Mutation::DropMappingId => {
                let m = cfg.mappings.first_mut()?;
                m.id = String::new();
            }
            Mutation::BreakMappingSourceRef => {
                let m = cfg.mappings.first_mut()?;
                m.source_container_id = "__does_not_exist__".to_string();
            }
            Mutation::BreakMappingTableRef => {
                let m = cfg.mappings.first_mut()?;
                m.analytic_table_id = "__does_not_exist__".to_string();
            }
            Mutation::BreakLookupRef => {
                let m = cfg.mappings.first_mut()?;
                let col = m.columns.first_mut()?;
                col.expr = AstNode::LookupRef {
                    lookup_id: "__does_not_exist__".to_string(),
                    input: Box::new(AstNode::Col {
                        name: "c0".to_string(),
                    }),
                };
            }
        }
        Some(cfg)
    }

    proptest! {
        // Mutated configs are always rejected by validate().
        #[test]
        fn mutated_config_is_rejected(
            cfg in arb_valid_pipeline_config(),
            mutation in arb_mutation(),
        ) {
            prop_assume!(validate(&cfg).is_ok());

            let Some(mutated) = apply_mutation(cfg, mutation) else {
                return Ok(());
            };

            let result = validate(&mutated);
            prop_assert!(result.is_err(), "validator accepted a mutated config");
            let errs = result.unwrap_err();
            prop_assert!(!errs.is_empty(), "error list should be non-empty");
            for err in &errs {
                prop_assert!(
                    !err.to_string().is_empty(),
                    "every error should have a non-empty Display message"
                );
            }
        }
    }
}
