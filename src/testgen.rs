//! Shared proptest generators for [`AstNode`] values.
//!
//! Exposed under `#[cfg(any(test, feature = "test-support"))]` so unit tests
//! in this crate and integration tests (built with `--features test-support`)
//! can pull the same generators. All generators are intentionally bounded so
//! shrinking stays fast.

use proptest::collection::vec;
use proptest::prelude::*;

use crate::ast::{AstNode, CastType};

/// ASCII-lowercase identifier: starts with a letter, 1..=8 chars total.
fn arb_id() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,7}".prop_map(|s| s)
}

/// Non-empty ASCII alphanumeric string, 1..=12 chars.
fn arb_name() -> impl Strategy<Value = String> {
    "[A-Za-z0-9]{1,12}".prop_map(|s| s)
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
                inner.clone().prop_map(|i| AstNode::Year { input: Box::new(i) }),
                inner.clone().prop_map(|i| AstNode::Month { input: Box::new(i) }),
                inner.clone().prop_map(|i| AstNode::Day { input: Box::new(i) }),
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
                vec(inner.clone(), 0..5)
                    .prop_map(|args| AstNode::Coalesce { args }),
                // Date and dimension
                (inner.clone(), "[%A-Za-z0-9_/-]{1,10}").prop_map(|(input, format)| {
                    AstNode::ParseDate {
                        input: Box::new(input),
                        format,
                    }
                }),
                (arb_id(), inner.clone()).prop_map(|(dim_id, input)| AstNode::DimRef {
                    dim_id,
                    value: None,
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
