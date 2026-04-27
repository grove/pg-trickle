//! Tier module: SLA refresh tier definitions.
//!
//! Contains the `RefreshTier` enum used for tiered scheduling — controlling
//! the effective schedule multiplier for each stream table.

/// G-7: Refresh tier for tiered scheduling.
///
/// Controls the effective schedule multiplier when `pg_trickle.tiered_scheduling`
/// is enabled. User-assignable via `ALTER STREAM TABLE ... SET (tier = 'warm')`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RefreshTier {
    /// Refresh at configured schedule (1× multiplier). Default.
    #[default]
    Hot,
    /// Refresh at 2× configured schedule.
    Warm,
    /// Refresh at 10× configured schedule.
    Cold,
    /// Skip refresh entirely (manually promoted back to Hot/Warm/Cold).
    Frozen,
}

impl RefreshTier {
    /// Parse from SQL string. Falls back to `Hot` for unknown values.
    pub fn from_sql_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "hot" => RefreshTier::Hot,
            "warm" => RefreshTier::Warm,
            "cold" => RefreshTier::Cold,
            "frozen" => RefreshTier::Frozen,
            _ => RefreshTier::Hot,
        }
    }

    /// SQL representation.
    pub fn as_str(self) -> &'static str {
        match self {
            RefreshTier::Hot => "hot",
            RefreshTier::Warm => "warm",
            RefreshTier::Cold => "cold",
            RefreshTier::Frozen => "frozen",
        }
    }

    /// Schedule multiplier for this tier.
    ///
    /// Returns `None` for Frozen (skip entirely).
    pub fn schedule_multiplier(self) -> Option<f64> {
        match self {
            RefreshTier::Hot => Some(1.0),
            RefreshTier::Warm => Some(2.0),
            RefreshTier::Cold => Some(10.0),
            RefreshTier::Frozen => None,
        }
    }

    /// Validate a tier string. Returns `true` if recognized.
    pub fn is_valid_str(s: &str) -> bool {
        matches!(
            s.to_lowercase().as_str(),
            "hot" | "warm" | "cold" | "frozen"
        )
    }
}

impl std::fmt::Display for RefreshTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_refresh_tier_schedule_multiplier() {
        assert_eq!(RefreshTier::Hot.schedule_multiplier(), Some(1.0));
        assert_eq!(RefreshTier::Warm.schedule_multiplier(), Some(2.0));
        assert_eq!(RefreshTier::Cold.schedule_multiplier(), Some(10.0));
        assert_eq!(RefreshTier::Frozen.schedule_multiplier(), None);
    }

    #[test]
    fn test_refresh_tier_is_valid_str() {
        assert!(RefreshTier::is_valid_str("hot"));
        assert!(RefreshTier::is_valid_str("WARM"));
        assert!(RefreshTier::is_valid_str("Cold"));
        assert!(RefreshTier::is_valid_str("FROZEN"));
        assert!(!RefreshTier::is_valid_str(""));
        assert!(!RefreshTier::is_valid_str("invalid"));
    }

    #[test]
    fn test_refresh_tier_display() {
        assert_eq!(format!("{}", RefreshTier::Hot), "hot");
        assert_eq!(format!("{}", RefreshTier::Frozen), "frozen");
    }

    #[test]
    fn test_refresh_tier_default() {
        assert_eq!(RefreshTier::default(), RefreshTier::Hot);
    }

    #[test]
    fn test_refresh_tier_roundtrip() {
        for tier in [
            RefreshTier::Hot,
            RefreshTier::Warm,
            RefreshTier::Cold,
            RefreshTier::Frozen,
        ] {
            assert_eq!(RefreshTier::from_sql_str(tier.as_str()), tier);
        }
    }
}
