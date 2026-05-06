// FUZZ-8 (v0.49.0): TEST-10-03 — Row identity tracking fuzz target.
//
// This target exercises the `RowIdSchema` compatibility and pipeline
// verification logic in `src/dvm/row_id.rs` to ensure that no combination
// of schema variants causes a panic.
//
// Functions under test (pure Rust, no PostgreSQL backend required):
//   - RowIdSchema::is_compatible_with — pairwise schema compatibility check
//   - RowIdSchema::verify_pipeline    — sequential pipeline validation
//
// Strategy: bytes 0..1 choose the downstream schema variant; bytes 1..2
// choose the upstream variant; remaining bytes supply column name data.
// All seven variants are exercised.
//
// Invariants verified:
//   1. `is_compatible_with` never panics for any pair of variants.
//   2. `is_compatible_with` is reflexive: a schema is always compatible
//      with itself (for Any, Derived, PassThrough).
//   3. `verify_pipeline` never panics for any slice of schemas.
//   4. An empty pipeline always returns `Ok(())`.
//   5. A singleton pipeline always returns `Ok(())`.
//
// Run locally:
//   cargo +nightly fuzz run row_id_fuzz -- -max_total_time=60

#![no_main]

use libfuzzer_sys::fuzz_target;
use pg_trickle::dvm::row_id::RowIdSchema;

/// Build a `RowIdSchema` variant from a discriminant byte and optional column data.
fn make_schema(discriminant: u8, col_a: &str, col_b: &str, table: &str) -> RowIdSchema {
    match discriminant % 7 {
        0 => RowIdSchema::PrimaryKey {
            table: table.to_owned(),
            columns: vec![col_a.to_owned()],
        },
        1 => RowIdSchema::AllColumns {
            columns: vec![col_a.to_owned(), col_b.to_owned()],
        },
        2 => RowIdSchema::GroupByKey {
            columns: vec![col_a.to_owned()],
        },
        3 => RowIdSchema::Combined {
            left: Box::new(RowIdSchema::PrimaryKey {
                table: table.to_owned(),
                columns: vec![col_a.to_owned()],
            }),
            right: Box::new(RowIdSchema::GroupByKey {
                columns: vec![col_b.to_owned()],
            }),
        },
        4 => RowIdSchema::PassThrough,
        5 => RowIdSchema::Any,
        _ => RowIdSchema::Derived,
    }
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    // Derive string tokens from the input (valid UTF-8 or fallback to empty).
    let s = std::str::from_utf8(data).unwrap_or("");
    let mut parts = s.splitn(4, '\n');
    let col_a = parts.next().unwrap_or("id");
    let col_b = parts.next().unwrap_or("region");
    let table = parts.next().unwrap_or("public.t");

    let disc_down = data[0];
    let disc_up = data[data.len() / 2];

    let downstream = make_schema(disc_down, col_a, col_b, table);
    let upstream = make_schema(disc_up, col_a, col_b, table);

    // -------------------------------------------------------------------
    // 1. is_compatible_with must never panic.
    // -------------------------------------------------------------------
    let _ = downstream.is_compatible_with(&upstream);
    let _ = upstream.is_compatible_with(&downstream);

    // -------------------------------------------------------------------
    // 2. Reflexivity: Any, Derived, PassThrough are always compatible.
    // -------------------------------------------------------------------
    assert!(
        RowIdSchema::Any.is_compatible_with(&downstream),
        "Any must be compatible with everything"
    );
    assert!(
        RowIdSchema::Derived.is_compatible_with(&downstream),
        "Derived must be compatible with everything"
    );
    assert!(
        RowIdSchema::PassThrough.is_compatible_with(&downstream),
        "PassThrough must be compatible with everything"
    );

    // -------------------------------------------------------------------
    // 3. verify_pipeline must never panic for any sequence of schemas.
    // -------------------------------------------------------------------
    let pipeline: Vec<RowIdSchema> = data
        .iter()
        .take(8) // Limit pipeline length for performance.
        .map(|&b| make_schema(b, col_a, col_b, table))
        .collect();
    let _ = RowIdSchema::verify_pipeline(&pipeline);

    // -------------------------------------------------------------------
    // 4. Empty pipeline always succeeds.
    // -------------------------------------------------------------------
    assert!(
        RowIdSchema::verify_pipeline(&[]).is_ok(),
        "empty pipeline must succeed"
    );

    // -------------------------------------------------------------------
    // 5. Singleton pipeline always succeeds.
    // -------------------------------------------------------------------
    let singleton = make_schema(disc_down, col_a, col_b, table);
    assert!(
        RowIdSchema::verify_pipeline(&[singleton]).is_ok(),
        "singleton pipeline must succeed"
    );
});
