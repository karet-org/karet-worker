//! AST node definitions for mapping expressions (AST-JSON).
//!
//! Every mapping output column in a `Pipeline_Config` is an [`AstNode`] tree
//! serialized to JSON with a `kind` discriminator.

use serde::{Deserialize, Serialize};

/// A node in the mapping expression AST.
///
/// Serialized as an internally-tagged JSON object: `{ "kind": "...", ... }`.
/// The `kind` tag uses snake_case variant names (e.g. `Add` -> `"add"`,
/// `ParseDate` -> `"parse_date"`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AstNode {
    // --- References and literals ---
    /// Column reference by name.
    Col { name: String },
    /// String literal.
    Str { value: String },
    /// Numeric literal (int or float).
    Num { value: f64 },
    /// Boolean literal.
    Bool { value: bool },
    /// Null literal.
    Null,

    // --- Arithmetic ---
    Add { left: Box<AstNode>, right: Box<AstNode> },
    Sub { left: Box<AstNode>, right: Box<AstNode> },
    Mul { left: Box<AstNode>, right: Box<AstNode> },
    Div { left: Box<AstNode>, right: Box<AstNode> },

    // --- String operations ---
    /// Concatenate `args` with `sep` as separator.
    Concat { sep: String, args: Vec<AstNode> },
    Upper { input: Box<AstNode> },
    Lower { input: Box<AstNode> },
    Trim { input: Box<AstNode> },
    Substring {
        input: Box<AstNode>,
        start: i64,
        length: Option<i64>,
    },

    // --- Comparisons ---
    Eq { left: Box<AstNode>, right: Box<AstNode> },
    Ne { left: Box<AstNode>, right: Box<AstNode> },
    Gt { left: Box<AstNode>, right: Box<AstNode> },
    Lt { left: Box<AstNode>, right: Box<AstNode> },
    Ge { left: Box<AstNode>, right: Box<AstNode> },
    Le { left: Box<AstNode>, right: Box<AstNode> },
    /// Substring test: `pattern` occurs inside `input`.
    Contains { input: Box<AstNode>, pattern: Box<AstNode> },

    // --- Control flow ---
    /// Conditional: `if cond then then-branch else else-branch`.
    ///
    /// `else` is a reserved keyword in Rust, so the field is written as
    /// `r#else` but still serializes to the JSON key `"else"`.
    If {
        cond: Box<AstNode>,
        then: Box<AstNode>,
        r#else: Box<AstNode>,
    },

    // --- Date and lookup ---
    /// Parse a string column into a date using a strftime format.
    ParseDate { input: Box<AstNode>, format: String },
    /// Reference into a `Lookup_Mapping` by dotted id (`parent.child`).
    LookupRef { lookup_id: String, input: Box<AstNode> },

    // --- Cast ---
    /// Explicit type cast.
    Cast { input: Box<AstNode>, to: CastType },
}

/// Target type for a [`AstNode::Cast`] node.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CastType {
    Int64,
    Float64,
    String,
    Date,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testgen::arb_ast_node;
    use proptest::prelude::*;

    proptest! {
        // AST JSON round-trip
        #[test]
        fn ast_json_round_trip(t in arb_ast_node()) {
            let s = serde_json::to_string(&t).expect("serialize");
            let t2: AstNode = serde_json::from_str(&s).expect("deserialize");
            prop_assert_eq!(t, t2);
        }
    }
}
