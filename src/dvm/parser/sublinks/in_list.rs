//! IN/NOT IN → SemiJoin/AntiJoin rewrites, multi-column NULL-safety.
//!
//! Extracted from `sublinks.rs` as part of QUAL-4 (v0.61.0) module decomposition.
//!
//! Contains:
//! - `parse_any_sublink` — ANY/IN sublink → SemiJoin
//! - `parse_all_sublink` — ALL sublink → AntiJoin
//! - `extract_aggregates_from_expr` — aggregate extraction from HAVING
//! - `extract_aggregates_from_expr_inner` — recursive aggregate extraction helper
//!
//! Functions are implemented in the parent module (`super`). This module
//! file declares the sub-module boundary for future full extraction.
