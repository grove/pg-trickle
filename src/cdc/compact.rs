//! QUAL-3 (v0.60.0): Compaction helpers and result types.
//!
//! Pure-logic types and helpers extracted from `cdc.rs` as part of the QUAL-3
//! module decomposition.

/// COR-4: Result type for [`crate::cdc::compact_change_buffer`].
///
/// Distinguishes between "rows deleted", "skipped below threshold", and
/// "could not acquire advisory lock" — the last case is now observable
/// via the `pg_trickle_cdc_compact_contended_total` counter.
#[derive(Debug)]
pub enum CompactionResult {
    /// No compaction was attempted (buffer below threshold).
    BelowThreshold,
    /// Advisory lock was held by a concurrent refresh; compaction skipped.
    Contended,
    /// Compaction ran and deleted `n` rows (may be 0 if nothing to remove).
    Compacted(i64),
}
