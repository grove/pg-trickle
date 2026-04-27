//! Cost module: predictive cost model helpers for the scheduler.
//!
//! Contains pure functions for computing refresh cost quotas and thresholds.

/// C3-1: Compute the effective per-database worker quota for this dispatch tick.
///
/// When `per_db_quota == 0` (disabled), falls back to `max_concurrent_refreshes`
/// (the legacy per-coordinator cap, not cluster-aware).
///
/// When `per_db_quota > 0`, the base entitlement is `per_db_quota`. If the
/// cluster has spare capacity (active workers < 80% of `max_cluster`), the
/// effective quota is increased to `per_db_quota * 3 / 2` to absorb a burst
/// without wasting idle cluster resources. The burst is reclaimed automatically
/// within 1 scheduler cycle once global load rises.
///
/// Pure logic — extracted for unit-testability.
pub fn compute_per_db_quota(
    per_db_quota: i32,
    max_concurrent_refreshes: i32,
    max_cluster: u32,
    current_active: u32,
) -> u32 {
    if per_db_quota <= 0 {
        // C3-1 disabled — fall back to per-coordinator cap (legacy).
        return max_concurrent_refreshes.max(1) as u32;
    }
    let base = per_db_quota.max(1) as u32;
    // Burst threshold: 80% of cluster capacity.
    let burst_threshold = ((max_cluster as f64) * 0.8).ceil() as u32;
    if current_active < burst_threshold {
        // Spare capacity — allow up to 150% of base quota.
        (base * 3 / 2).max(base + 1)
    } else {
        base
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_per_db_quota_disabled() {
        // When quota is 0, falls back to max_concurrent_refreshes.
        assert_eq!(compute_per_db_quota(0, 4, 10, 0), 4);
        assert_eq!(compute_per_db_quota(-1, 4, 10, 0), 4);
        // Minimum 1 even when max_concurrent_refreshes is 0
        assert_eq!(compute_per_db_quota(0, 0, 10, 0), 1);
    }

    #[test]
    fn test_compute_per_db_quota_burst_under_80_percent() {
        // Under burst threshold (80% of 10 = 8), quota is 150% of base.
        // base = 4, 150% = 6
        assert_eq!(compute_per_db_quota(4, 2, 10, 5), 6);
    }

    #[test]
    fn test_compute_per_db_quota_at_burst_threshold() {
        // At or over burst threshold, use base quota.
        // 80% of 10 = 8, current_active=8 → at threshold → base quota
        assert_eq!(compute_per_db_quota(4, 2, 10, 8), 4);
        assert_eq!(compute_per_db_quota(4, 2, 10, 10), 4);
    }

    #[test]
    fn test_compute_per_db_quota_base_one() {
        // base=1, burst: max(1*3/2=1, 1+1=2) = 2
        assert_eq!(compute_per_db_quota(1, 2, 10, 0), 2);
    }
}
