//! Shared proptest generators for [`AstNode`] and `Pipeline_Config` values.
//!
//! Exposed under `#[cfg(any(test, feature = "test-support"))]` so unit tests
//! in this crate and integration tests (built with `--features test-support`)
//! can pull the same generators. All generators are intentionally bounded so
//! shrinking stays fast.

use std::collections::HashMap;

use proptest::collection::vec;
use proptest::prelude::*;

use crate::ast::{AstNode, CastType};
use crate::config::{
    AnalyticTable, ColumnSchema, LayoutPosition, LookupMapping, LookupRow, Mapping, MappingColumn,
    PartitionBy, PipelineConfig, SourceContainer,
};

/// ASCII-lowercase identifier: starts with a letter, 1..=8 chars total.
fn arb_id() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,7}".prop_map(|s| s)
}

/// Non-empty ASCII alphanumeric string, 1..=12 chars.
fn arb_name() -> impl Strategy<Value = String> {
    "[A-Za-z0-9]{1,12}".prop_map(|s| s)
}

/// Short path prefix like `raw/foo/`.
fn arb_path_prefix() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9/_-]{0,15}".prop_map(|s| s)
}

/// One of the supported logical column types.
fn arb_column_type() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("string".to_string()),
        Just("number".to_string()),
        Just("int64".to_string()),
        Just("float64".to_string()),
        Just("date".to_string()),
        Just("bool".to_string()),
    ]
}

/// A single [`ColumnSchema`] with a short name and a random logical type.
fn arb_column_schema() -> impl Strategy<Value = ColumnSchema> {
    (arb_name(), arb_column_type(), any::<Option<bool>>()).prop_map(|(name, type_, nullable)| {
        ColumnSchema {
            name,
            type_,
            nullable,
            assertions: None,
        }
    })
}

/// One of the four [`CastType`] targets, uniformly chosen.
fn arb_cast_type() -> impl Strategy<Value = CastType> {
    prop_oneof![
        Just(CastType::Int64),
        Just(CastType::Float64),
        Just(CastType::String),
        Just(CastType::Date),
    ]
}

/// Recursive generator for [`AstNode`].
///
/// Bounds: `depth = 6`, `desired_size = 32`, `expected_branch_size = 5`.
/// Leaves cover `Col`, `Str`, `Num`, `Bool`, `Null`. Recursive variants cover
/// arithmetic, string ops, comparisons, `Concat`, `Substring`, `If`,
/// `ParseDate`, `LookupRef`, and `Cast`.
pub fn arb_ast_node() -> impl Strategy<Value = AstNode> {
    // `f64` literals must survive JSON round-trip under `PartialEq`. Exclude
    // `NaN` (NaN != NaN breaks equality) and ±∞ (serde_json emits `null` for
    // non-finite floats, breaking the round-trip). The flag set below covers
    // every finite `f64`.
    let finite_f64 = proptest::num::f64::POSITIVE
        | proptest::num::f64::NEGATIVE
        | proptest::num::f64::NORMAL
        | proptest::num::f64::SUBNORMAL
        | proptest::num::f64::ZERO;

    let leaf = prop_oneof![
        arb_name().prop_map(|name| AstNode::Col { name }),
        ".*".prop_map(|value: String| AstNode::Str { value }),
        finite_f64.prop_map(|value| AstNode::Num { value }),
        any::<bool>().prop_map(|value| AstNode::Bool { value }),
        Just(AstNode::Null),
    ];

    leaf.prop_recursive(
        6,  // max recursion depth
        32, // desired total size
        5,  // expected branching factor
        |inner| {
            prop_oneof![
                // Arithmetic
                (inner.clone(), inner.clone()).prop_map(|(l, r)| AstNode::Add {
                    left: Box::new(l),
                    right: Box::new(r),
                }),
                (inner.clone(), inner.clone()).prop_map(|(l, r)| AstNode::Sub {
                    left: Box::new(l),
                    right: Box::new(r),
                }),
                (inner.clone(), inner.clone()).prop_map(|(l, r)| AstNode::Mul {
                    left: Box::new(l),
                    right: Box::new(r),
                }),
                (inner.clone(), inner.clone()).prop_map(|(l, r)| AstNode::Div {
                    left: Box::new(l),
                    right: Box::new(r),
                }),
                // String ops
                (".*", vec(inner.clone(), 0..5))
                    .prop_map(|(sep, args): (String, Vec<AstNode>)| AstNode::Concat { sep, args }),
                inner.clone().prop_map(|i| AstNode::Upper {
                    input: Box::new(i),
                }),
                inner.clone().prop_map(|i| AstNode::Lower {
                    input: Box::new(i),
                }),
                inner.clone().prop_map(|i| AstNode::Trim {
                    input: Box::new(i),
                }),
                (inner.clone(), any::<i64>(), any::<Option<i64>>()).prop_map(
                    |(input, start, length)| AstNode::Substring {
                        input: Box::new(input),
                        start,
                        length,
                    }
                ),
                // Comparisons
                (inner.clone(), inner.clone()).prop_map(|(l, r)| AstNode::Eq {
                    left: Box::new(l),
                    right: Box::new(r),
                }),
                (inner.clone(), inner.clone()).prop_map(|(l, r)| AstNode::Ne {
                    left: Box::new(l),
                    right: Box::new(r),
                }),
                (inner.clone(), inner.clone()).prop_map(|(l, r)| AstNode::Gt {
                    left: Box::new(l),
                    right: Box::new(r),
                }),
                (inner.clone(), inner.clone()).prop_map(|(l, r)| AstNode::Lt {
                    left: Box::new(l),
                    right: Box::new(r),
                }),
                (inner.clone(), inner.clone()).prop_map(|(l, r)| AstNode::Ge {
                    left: Box::new(l),
                    right: Box::new(r),
                }),
                (inner.clone(), inner.clone()).prop_map(|(l, r)| AstNode::Le {
                    left: Box::new(l),
                    right: Box::new(r),
                }),
                (inner.clone(), inner.clone()).prop_map(|(input, pattern)| AstNode::Contains {
                    input: Box::new(input),
                    pattern: Box::new(pattern),
                }),
                // Control flow
                (inner.clone(), inner.clone(), inner.clone()).prop_map(
                    |(cond, then, r#else)| AstNode::If {
                        cond: Box::new(cond),
                        then: Box::new(then),
                        r#else: Box::new(r#else),
                    }
                ),
                // Date and lookup
                (inner.clone(), "[%A-Za-z0-9_/-]{1,10}").prop_map(|(input, format)| {
                    AstNode::ParseDate {
                        input: Box::new(input),
                        format,
                    }
                }),
                (arb_id(), inner.clone()).prop_map(|(lookup_id, input)| AstNode::LookupRef {
                    lookup_id,
                    input: Box::new(input),
                }),
                // Cast
                (inner, arb_cast_type()).prop_map(|(input, to)| AstNode::Cast {
                    input: Box::new(input),
                    to,
                }),
            ]
        },
    )
}

/// Generator for [`SourceContainer`].
///
/// Non-empty ASCII id/name/path_prefix, schema of 1..=5 columns.
pub fn arb_source_container() -> impl Strategy<Value = SourceContainer> {
    (
        arb_id(),
        arb_name(),
        arb_path_prefix(),
        vec(arb_column_schema(), 1..=5),
    )
        .prop_map(|(id, name, path_prefix, schema)| SourceContainer {
            id,
            name,
            path_prefix,
            schema,
        })
}

/// Generator for a [`LookupRow`]: 1..=3 input_patterns.
fn arb_lookup_row() -> impl Strategy<Value = LookupRow> {
    (
        vec(arb_name(), 1..=3),
        arb_name(),
        any::<Option<String>>().prop_map(|o| o.map(|s| s.chars().take(8).collect::<String>())),
    )
        .prop_map(|(input_patterns, output, parent_output)| LookupRow {
            input_patterns,
            output,
            parent_output,
        })
}

/// Generator for [`LookupMapping`] (flat — no recursive children).
///
/// 1..=5 rows, each with 1..=3 patterns. `children` is always empty so this
/// generator stays bounded; validator tests can construct trees explicitly.
pub fn arb_lookup_mapping() -> impl Strategy<Value = LookupMapping> {
    (
        arb_id(),
        proptest::option::of(arb_name()),
        proptest::option::of(Just("keyword_substring".to_string())),
        any::<Option<bool>>(),
        vec(arb_lookup_row(), 1..=5),
        proptest::option::of(arb_name()),
    )
        .prop_map(
            |(id, name, match_, case_insensitive, rows, parent_output_column)| LookupMapping {
                id,
                name,
                match_,
                case_insensitive,
                rows,
                children: Vec::new(),
                parent_output_column,
                catch_all: None,
            },
        )
}

/// Generator for a single [`MappingColumn`] (name + fresh AST).
fn arb_mapping_column() -> impl Strategy<Value = MappingColumn> {
    (arb_name(), arb_ast_node()).prop_map(|(name, expr)| MappingColumn { name, expr })
}

/// Generator for [`PartitionBy`] with a `"month"` granularity.
fn arb_partition_by() -> impl Strategy<Value = PartitionBy> {
    (arb_name(), Just("month".to_string()))
        .prop_map(|(column, granularity)| PartitionBy { column, granularity })
}

/// Generator for [`Mapping`]: non-empty ids and 1..=5 columns.
pub fn arb_mapping() -> impl Strategy<Value = Mapping> {
    (
        arb_id(),
        arb_id(),
        arb_id(),
        proptest::option::of(arb_partition_by()),
        vec(arb_mapping_column(), 1..=5),
    )
        .prop_map(
            |(id, source_container_id, analytic_table_id, partition_by, columns)| Mapping {
                id,
                name: String::new(),
                source_container_id,
                analytic_table_id,
                partition_by,
                columns,
            },
        )
}

/// Generator for [`AnalyticTable`] used inside `arb_pipeline_config`.
fn arb_analytic_table() -> impl Strategy<Value = AnalyticTable> {
    (
        arb_id(),
        arb_name(),
        arb_path_prefix(),
        vec(arb_column_schema(), 1..=5),
    )
        .prop_map(|(id, name, output_prefix, schema)| AnalyticTable {
            id,
            name,
            output_prefix,
            schema,
        })
}

/// Generator for [`LayoutPosition`].
fn arb_layout_position() -> impl Strategy<Value = LayoutPosition> {
    (any::<f64>(), any::<f64>()).prop_map(|(x, y)| LayoutPosition { x, y })
}

/// Generator for [`PipelineConfig`].
///
/// `version = 1`, 1..=3 source_containers, 0..=3 lookup_mappings,
/// 1..=3 mappings, 1..=3 analytic_tables. References across collections are
/// NOT guaranteed to resolve — validator tests build valid configs explicitly.
pub fn arb_pipeline_config() -> impl Strategy<Value = PipelineConfig> {
    (
        vec(arb_source_container(), 1..=3),
        vec(arb_lookup_mapping(), 0..=3),
        vec(arb_mapping(), 1..=3),
        vec(arb_analytic_table(), 1..=3),
        vec((arb_id(), arb_layout_position()), 0..=4),
    )
        .prop_map(
            |(source_containers, lookup_mappings, mappings, analytic_tables, layout_pairs)| {
                let mut layout = HashMap::new();
                for (k, v) in layout_pairs {
                    layout.insert(k, v);
                }
                PipelineConfig {
                    version: 1,
                    source_containers,
                    lookup_mappings,
                    mappings,
                    analytic_tables,
                    layout,
                }
            },
        )
}
