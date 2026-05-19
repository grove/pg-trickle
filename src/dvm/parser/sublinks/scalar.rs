//! Scalar SubLink hoisting and subquery-to-SQL deparsing.
//!
//! Extracted from `sublinks.rs` as part of QUAL-4 (v0.61.0) module decomposition.
//!
//! Contains:
//! - `parse_sublink_to_wrapper` — dispatch to exists/any/all parsers
//! - `extract_bare_scalar_subquery_sql` — extract raw SQL for scalar subqueries
//!
//! Functions are implemented in the parent module (`super`). This module
//! file declares the sub-module boundary for future full extraction.
