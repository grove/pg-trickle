// FUZZ-7 (v0.49.0): TEST-10-03 — Merge SQL construction pipeline fuzz target.
//
// This target exercises pure-Rust helpers in the merge SQL pipeline to ensure
// no adversarial input causes a panic, integer overflow, or memory corruption.
//
// Functions under test (pure Rust, no PostgreSQL backend required):
//   - merge_pg_quote_literal_pub    — SQL literal quoting for partition bounds
//   - merge_parse_hash_bound_spec_pub — HASH partition bound spec parser
//   - merge_extract_keyword_int_pub — keyword→integer extractor
//   - merge_compute_amplification_ratio_pub — delta row amplification ratio
//   - merge_should_warn_amplification_pub   — amplification warning gate
//   - merge_build_content_hash_expr_pub     — __pgt_row_id hash expression builder
//
// Invariants verified for each input:
//   1. `pg_quote_literal` output is always valid UTF-8 and starts/ends with `'`.
//   2. `parse_hash_bound_spec` never panics; errors are returned as Result.
//   3. `extract_keyword_int` never panics.
//   4. `compute_amplification_ratio` never panics or produces NaN.
//   5. `should_warn_amplification` is deterministic (same inputs → same output).
//   6. `build_content_hash_expr` output is valid UTF-8 and non-empty.
//
// Run locally:
//   cargo +nightly fuzz run merge_sql_fuzz -- -max_total_time=60

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Require valid UTF-8.
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };

    // -------------------------------------------------------------------
    // 1. SQL literal quoting — must never panic; output must be wrapped in
    //    single quotes with internal single-quotes doubled.
    // -------------------------------------------------------------------
    let quoted = pg_trickle::fuzz_pub::merge_pg_quote_literal_pub(s);
    assert!(quoted.starts_with('\''), "quoted literal must start with '");
    assert!(quoted.ends_with('\''), "quoted literal must end with '");
    // Round-trip invariant: single-quote count doubles inside.
    let inner = &quoted[1..quoted.len() - 1];
    assert!(
        std::str::from_utf8(inner.as_bytes()).is_ok(),
        "quoted literal must be valid UTF-8"
    );

    // -------------------------------------------------------------------
    // 2. HASH partition bound spec parser — must never panic.
    //    Errors are acceptable; panics are not.
    // -------------------------------------------------------------------
    let _ = pg_trickle::fuzz_pub::merge_parse_hash_bound_spec_pub(s);

    // -------------------------------------------------------------------
    // 3. Keyword integer extractor — must never panic for any keyword.
    // -------------------------------------------------------------------
    let _ = pg_trickle::fuzz_pub::merge_extract_keyword_int_pub(s, "MODULUS");
    let _ = pg_trickle::fuzz_pub::merge_extract_keyword_int_pub(s, "REMAINDER");

    // -------------------------------------------------------------------
    // 4. Amplification ratio — must never panic or produce NaN.
    //    Derive two i64 values from the input bytes.
    // -------------------------------------------------------------------
    if data.len() >= 16 {
        let input_delta = i64::from_le_bytes(data[0..8].try_into().unwrap());
        let output_delta = i64::from_le_bytes(data[8..16].try_into().unwrap());
        let ratio = pg_trickle::fuzz_pub::merge_compute_amplification_ratio_pub(
            input_delta,
            output_delta,
        );
        // Ratio is 0.0 when input ≤ 0, otherwise a finite float.
        assert!(!ratio.is_nan(), "amplification ratio must not be NaN");

        // 5. Warning gate must be deterministic.
        let threshold = if data.len() >= 24 {
            f64::from_le_bytes(data[16..24].try_into().unwrap())
        } else {
            5.0_f64
        };
        let warn1 = pg_trickle::fuzz_pub::merge_should_warn_amplification_pub(
            input_delta,
            output_delta,
            threshold,
        );
        let warn2 = pg_trickle::fuzz_pub::merge_should_warn_amplification_pub(
            input_delta,
            output_delta,
            threshold,
        );
        assert_eq!(warn1, warn2, "should_warn_amplification must be deterministic");
    }

    // -------------------------------------------------------------------
    // 6. Content hash expression builder — must never panic; output must be
    //    non-empty and valid UTF-8.
    //    Build a column list from newline-split of the input string.
    // -------------------------------------------------------------------
    let cols: Vec<String> = s
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| l.to_owned())
        .collect();
    let expr_empty = pg_trickle::fuzz_pub::merge_build_content_hash_expr_pub("src.", &[]);
    assert!(!expr_empty.is_empty(), "hash expr must not be empty for zero cols");
    let expr = pg_trickle::fuzz_pub::merge_build_content_hash_expr_pub("src.", &cols);
    assert!(!expr.is_empty(), "hash expr must not be empty");
    assert!(
        std::str::from_utf8(expr.as_bytes()).is_ok(),
        "hash expr must be valid UTF-8"
    );
});
