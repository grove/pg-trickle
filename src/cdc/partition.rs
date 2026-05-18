//! QUAL-3 (v0.60.0): Buffer auto-promotion decision logic.
//!
//! Pure-logic functions extracted from `cdc.rs` as part of the QUAL-3
//! module decomposition.  All functions here are free of SPI calls and
//! can be unit-tested without a PostgreSQL backend.

/// PERF-2: Pure inner decision function — should an unpartitioned buffer be promoted?
///
/// Returns `true` only when ALL conditions are satisfied:
/// - `already_partitioned` is `false`
/// - `mode` is `"auto"` (the GUC value for `pg_trickle.buffer_partitioning`)
/// - `threshold` > 0
/// - `pending_count` > `threshold`
///
/// Extracted from `should_promote_to_partitioned` so the logic can be
/// exercised in unit tests without reading GUCs from PostgreSQL.
pub(super) fn should_promote_inner(
    pending_count: i64,
    already_partitioned: bool,
    mode: &str,
    threshold: i64,
) -> bool {
    if already_partitioned {
        return false;
    }
    if mode != "auto" {
        return false;
    }
    if threshold <= 0 {
        return false;
    }
    pending_count > threshold
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── PERF-2: should_promote_inner tests ─────────────────────────────────

    #[test]
    fn test_should_promote_to_partitioned_false_for_regular() {
        // relkind = 'r' (regular table): not already partitioned, but below threshold
        assert!(!should_promote_inner(999_999, true, "auto", 100_000));
    }

    #[test]
    fn test_should_promote_to_partitioned_true_for_partitioned() {
        // pending_count above threshold in "auto" mode → promote
        assert!(should_promote_inner(100_001, false, "auto", 100_000));
    }

    #[test]
    fn test_promote_below_threshold_returns_false() {
        assert!(!should_promote_inner(50_000, false, "auto", 100_000));
    }

    #[test]
    fn test_promote_at_threshold_returns_false() {
        assert!(!should_promote_inner(100_000, false, "auto", 100_000));
    }

    #[test]
    fn test_promote_above_threshold_off_mode() {
        assert!(!should_promote_inner(100_001, false, "off", 100_000));
    }

    #[test]
    fn test_promote_above_threshold_on_mode() {
        // "on" is not a valid mode for auto-promotion — only "auto" triggers it.
        assert!(!should_promote_inner(100_001, false, "on", 100_000));
    }

    #[test]
    fn test_promote_zero_pending_returns_false() {
        assert!(!should_promote_inner(0, false, "auto", 100_000));
    }

    #[test]
    fn test_promote_negative_pending_returns_false() {
        assert!(!should_promote_inner(-1, false, "auto", 100_000));
    }

    #[test]
    fn test_promote_zero_threshold_returns_false() {
        assert!(!should_promote_inner(999_999, false, "auto", 0));
    }

    #[test]
    fn test_promote_negative_threshold_returns_false() {
        assert!(!should_promote_inner(999_999, false, "auto", -1));
    }
}
