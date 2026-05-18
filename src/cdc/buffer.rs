//! QUAL-3 (v0.60.0): Change-buffer naming helpers.
//!
//! Pure-logic naming helpers extracted from `cdc.rs` as part of the QUAL-3
//! module decomposition.  These functions derive buffer table names from source
//! OIDs or stable names.

// ── Pure naming logic ────────────────────────────────────────────────────────

/// Derive the OID-based buffer base name (no schema, no SPI lookup).
///
/// This is the fallback naming scheme used when no stable name is recorded in
/// `pgt_change_tracking`.  The full `buffer_base_name_for_oid` function in
/// `cdc/mod.rs` tries the stable name first, then calls this helper.
pub(super) fn buffer_name_for_oid_fallback(oid: u32) -> String {
    format!("changes_{oid}")
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── buffer naming tests ───────────────────────────────────────────────

    #[test]
    fn test_buffer_base_name_for_oid() {
        // OID 12345 → "changes_12345" (OID-based fallback, no DB needed)
        assert_eq!(buffer_name_for_oid_fallback(12345), "changes_12345");
    }

    #[test]
    fn test_buffer_name_for_oid_fallback_zero() {
        assert_eq!(buffer_name_for_oid_fallback(0), "changes_0");
    }

    #[test]
    fn test_buffer_name_for_oid_fallback_max() {
        assert_eq!(
            buffer_name_for_oid_fallback(u32::MAX),
            format!("changes_{}", u32::MAX)
        );
    }
}
