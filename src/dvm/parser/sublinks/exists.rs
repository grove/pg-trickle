//! EXISTS/NOT EXISTS → SemiJoin/AntiJoin rewrites.
//!
//! Extracted from `sublinks.rs` as part of QUAL-4 (v0.61.0) module decomposition.
//!
//! Contains:
//! - `parse_exists_sublink` — EXISTS/NOT EXISTS sublink parsing
//! - `collect_tree_source_aliases` — collect scan aliases from an OpTree
//! - `split_exists_correlation` — split EXISTS inner WHERE into corr + filter
//! - `try_extract_exists_corr_pair` — extract a single correlation predicate
//! - `qualify_inner_col_refs` — qualify unqualified column refs with alias
//!
//! Functions are implemented in the parent module (`super`). This module
//! file declares the sub-module boundary for future full extraction.
