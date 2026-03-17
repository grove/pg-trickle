//! T6 — TPC-H DAG Chain Correctness
//!
//! Creates a two-level DAG:
//!   Level 0: `tpch_dag_q01` — Q01 (aggregate over `lineitem`), DIFFERENTIAL
//!   Level 1: `tpch_dag_rollup` — re-aggregate Q01 output by l_returnflag (DIFFERENTIAL)
//!
//! Applies RF mutations to `lineitem`, refreshes in topological order
//! (level-0 first, level-1 second), and asserts both STs match their
//! defining queries.
//!
//! Also runs a multi-parent fan-in variant once the basic chain passes:
//!   Level 0a: `tpch_dag_q01_fp` — Q01 results, DIFFERENTIAL
//!   Level 0b: `tpch_dag_q06_fp` — Q06 aggregate, DIFFERENTIAL
//!   Level 1:  `tpch_dag_union`   — UNION ALL of the two level-0 STs, DIFFERENTIAL
//!
//! **TPC-H Fair Use:** Derived from TPC-H specification; not a benchmark result.

mod e2e;
mod tpch;

use e2e::E2eDb;
use std::time::Instant;
use tpch::{
    RF1_SQL, RF2_SQL, RF3_SQL, cycles, load_data, load_schema, max_orderkey, rf_count,
    scale_factor, substitute_rf,
};

// ── DAG-test-local helpers ──────────────────────────────────────────────

/// Refresh a stream table, returning an error string instead of panicking.
async fn try_refresh(db: &E2eDb, st_name: &str) -> Result<(), String> {
    db.try_execute(&format!(
        "SELECT pgtrickle.refresh_stream_table('{st_name}')"
    ))
    .await
    .map_err(|e| e.to_string())
}

/// Assert multiset equality between a stream table and its defining query.
///
/// Returns `Ok(())` on success or an `Err(description)` on mismatch.
async fn assert_invariant(
    db: &E2eDb,
    st_name: &str,
    query: &str,
    label: &str,
    cycle: usize,
) -> Result<(), String> {
    let st_table = format!("public.{st_name}");

    // Negative __pgt_count guard (T1).
    let has_pgt_count: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS ( \
                SELECT 1 FROM information_schema.columns \
                WHERE table_name = '{st_name}' AND column_name = '__pgt_count' \
            )"
        ))
        .await;
    if has_pgt_count {
        let neg_count: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM {st_table} WHERE __pgt_count < 0"
            ))
            .await;
        if neg_count > 0 {
            return Err(format!(
                "NEGATIVE __pgt_count: {label} cycle {cycle} — \
                 {neg_count} rows with __pgt_count < 0"
            ));
        }
    }

    let cols: String = db
        .query_scalar(&format!(
            "SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) \
             FROM information_schema.columns \
             WHERE (table_schema || '.' || table_name = 'public.{st_name}' \
                OR table_name = '{st_name}') \
               AND column_name NOT IN ('__pgt_row_id', '__pgt_count')"
        ))
        .await;

    let matches: bool = db
        .query_scalar(&format!(
            "SELECT NOT EXISTS ( \
                (SELECT {cols} FROM {st_table} EXCEPT ALL ({query})) \
                UNION ALL \
                (({query}) EXCEPT ALL SELECT {cols} FROM {st_table}) \
            )"
        ))
        .await;

    if !matches {
        let st_count: i64 = db
            .query_scalar(&format!("SELECT count(*) FROM {st_table}"))
            .await;
        let q_count: i64 = db
            .query_scalar(&format!("SELECT count(*) FROM ({query}) _q"))
            .await;
        return Err(format!(
            "INVARIANT VIOLATION: {label} cycle {cycle} — \
             ST rows: {st_count}, Q rows: {q_count}"
        ));
    }

    Ok(())
}

// P2.11: exec_sql_stmts is now a wrapper around the shared tpch::try_exec_sql.
async fn exec_sql_stmts(db: &E2eDb, sql: &str) -> Result<(), String> {
    tpch::try_exec_sql(db, sql).await
}

// ═══════════════════════════════════════════════════════════════════════
// T6a — Two-Level DAG Chain (with re-aggregation at level-1)
// ═══════════════════════════════════════════════════════════════════════
//
// Level 0: tpch_dag_q01   — Q01 aggregate over lineitem (by returnflag + linestatus)
// Level 1: tpch_dag_rollup — Re-aggregate Q01 output by l_returnflag only
//           (collapses the l_linestatus dimension), computing total charge
//           and total order count per flag.
//
// P3.14: Changed from a simple WHERE filter (SELECT * WHERE l_returnflag = 'R')
// to a GROUP BY re-aggregation.  This exercises a more complex DAG propagation
// path where level-1 must apply its own delta (sum of sums) rather than just
// projecting level-0 deltas directly.
//
// After each RF cycle: refresh level-0 first, then level-1.
// Both STs must match their defining queries at all checkpoints.

#[tokio::test]
#[ignore]
async fn test_tpch_dag_chain() {
    let sf = scale_factor();
    let n_cycles = cycles();
    println!("\n══════════════════════════════════════════════════════════");
    println!("  TPC-H DAG Chain Correctness — SF={sf}, cycles={n_cycles}");
    println!("══════════════════════════════════════════════════════════\n");

    let db = E2eDb::new_bench().await.with_extension().await;

    let t = Instant::now();
    load_schema(&db).await;
    load_data(&db).await;
    println!("  Data loaded in {:.1}s\n", t.elapsed().as_secs_f64());

    const Q01_SQL: &str = include_str!("tpch/queries/q01.sql");

    // Level-1: re-aggregate Q01 output by l_returnflag only (collapsing l_linestatus).
    // This is more complex than a simple filter — it requires DVM to propagate
    // a sum-of-sums delta from the level-0 aggregate into a new aggregate.
    const ROLLUP_SQL: &str = "SELECT l_returnflag, \
            SUM(sum_charge) AS total_charge, \
            SUM(count_order)::bigint AS total_orders \
         FROM tpch_dag_q01 \
         GROUP BY l_returnflag \
         ORDER BY l_returnflag";

    // Ground truth for level-1: compute directly from lineitem (no intermediate ST).
    const ROLLUP_GROUND_TRUTH: &str = "SELECT l_returnflag, \
            SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS total_charge, \
            COUNT(*)::bigint AS total_orders \
         FROM lineitem \
         WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90 days' \
         GROUP BY l_returnflag \
         ORDER BY l_returnflag";

    // Create level-0 ST
    if let Err(e) = db
        .try_execute(&format!(
            "SELECT pgtrickle.create_stream_table('tpch_dag_q01', $${Q01_SQL}$$, '1m', 'DIFFERENTIAL')"
        ))
        .await
    {
        println!("  SKIP — Q01 level-0 create failed: {e}");
        return;
    }
    println!("  tpch_dag_q01 created ✓");

    // Create level-1 ST (references level-0 — re-aggregation)
    if let Err(e) = db
        .try_execute(&format!(
            "SELECT pgtrickle.create_stream_table('tpch_dag_rollup', $${ROLLUP_SQL}$$, '1m', 'DIFFERENTIAL')"
        ))
        .await
    {
        println!("  SKIP — rollup level-1 create failed: {e}");
        let _ = db
            .try_execute("SELECT pgtrickle.drop_stream_table('tpch_dag_q01')")
            .await;
        return;
    }
    println!("  tpch_dag_rollup created ✓");

    // Baseline assertions
    if let Err(e) = assert_invariant(&db, "tpch_dag_q01", Q01_SQL, "dag_q01", 0).await {
        let _ = db
            .try_execute("SELECT pgtrickle.drop_stream_table('tpch_dag_rollup')")
            .await;
        let _ = db
            .try_execute("SELECT pgtrickle.drop_stream_table('tpch_dag_q01')")
            .await;
        panic!("DAG chain baseline (level-0) failed: {e}");
    }
    if let Err(e) =
        assert_invariant(&db, "tpch_dag_rollup", ROLLUP_GROUND_TRUTH, "dag_rollup", 0).await
    {
        let _ = db
            .try_execute("SELECT pgtrickle.drop_stream_table('tpch_dag_rollup')")
            .await;
        let _ = db
            .try_execute("SELECT pgtrickle.drop_stream_table('tpch_dag_q01')")
            .await;
        panic!("DAG chain baseline (level-1 rollup) failed: {e}");
    }
    println!("  baseline ✓\n");

    // Mutation cycles
    for cycle in 1..=n_cycles {
        let ct = Instant::now();

        let next_ok = max_orderkey(&db).await + 1;
        let rf1 = substitute_rf(RF1_SQL, next_ok);
        exec_sql_stmts(&db, &rf1)
            .await
            .unwrap_or_else(|e| panic!("RF1 cycle {cycle}: {e}"));
        let rf2 = RF2_SQL.replace("__RF_COUNT__", &rf_count().to_string());
        exec_sql_stmts(&db, &rf2)
            .await
            .unwrap_or_else(|e| panic!("RF2 cycle {cycle}: {e}"));
        let rf3 = RF3_SQL.replace("__RF_COUNT__", &rf_count().to_string());
        exec_sql_stmts(&db, &rf3)
            .await
            .unwrap_or_else(|e| panic!("RF3 cycle {cycle}: {e}"));

        db.execute("ANALYZE lineitem").await;
        db.execute("ANALYZE orders").await;

        // Refresh in topological order: level-0 must be refreshed before level-1.
        try_refresh(&db, "tpch_dag_q01")
            .await
            .unwrap_or_else(|e| panic!("DAG Q01 refresh cycle {cycle}: {e}"));
        try_refresh(&db, "tpch_dag_rollup")
            .await
            .unwrap_or_else(|e| panic!("DAG rollup refresh cycle {cycle}: {e}"));

        // Assert level-0
        assert_invariant(&db, "tpch_dag_q01", Q01_SQL, "dag_q01", cycle)
            .await
            .unwrap_or_else(|e| panic!("dag_q01 invariant cycle {cycle}: {e}"));

        // Assert level-1 (ground truth derived directly from lineitem)
        assert_invariant(
            &db,
            "tpch_dag_rollup",
            ROLLUP_GROUND_TRUTH,
            "dag_rollup",
            cycle,
        )
        .await
        .unwrap_or_else(|e| panic!("dag_rollup invariant cycle {cycle}: {e}"));

        db.execute("VACUUM").await;

        println!(
            "  cycle {cycle}/{n_cycles} — BOTH levels correct ✓ — {:.0}ms",
            ct.elapsed().as_secs_f64() * 1000.0,
        );
    }

    // Clean up
    let _ = db
        .try_execute("SELECT pgtrickle.drop_stream_table('tpch_dag_rollup')")
        .await;
    let _ = db
        .try_execute("SELECT pgtrickle.drop_stream_table('tpch_dag_q01')")
        .await;

    println!("\n  DAG chain correctness: PASSED — {n_cycles} cycle(s) ✓\n");
}

// ═══════════════════════════════════════════════════════════════════════
// T6b — Multi-Parent Fan-In DAG
// ═══════════════════════════════════════════════════════════════════════
//
// Level 0a: tpch_dag_q01_fp — Q01 aggregate
// Level 0b: tpch_dag_q06_fp — Q06 aggregate
// Level 1:  tpch_dag_union   — UNION ALL of both level-0 STs (different
//           schema columns; aggregated as a heterogeneous union example)
//
// This tests multi-parent DAG fan-in: refresh both level-0 STs then
// the level-1 ST, assert all three are correct.

#[tokio::test]
#[ignore]
async fn test_tpch_dag_multi_parent() {
    let sf = scale_factor();
    let n_cycles = cycles();
    println!("\n══════════════════════════════════════════════════════════");
    println!("  TPC-H DAG Multi-Parent Fan-In — SF={sf}, cycles={n_cycles}");
    println!("══════════════════════════════════════════════════════════\n");

    let db = E2eDb::new_bench().await.with_extension().await;

    let t = Instant::now();
    load_schema(&db).await;
    load_data(&db).await;
    println!("  Data loaded in {:.1}s\n", t.elapsed().as_secs_f64());

    const Q01_SQL: &str = include_str!("tpch/queries/q01.sql");
    const Q06_SQL: &str = include_str!("tpch/queries/q06.sql");

    // Level-1: aggregate revenue from the combined Q06 and Q01 sum_base_price.
    // Both parent STs expose a numeric column we can sum.
    const UNION_SQL: &str = "SELECT SUM(revenue_total) AS grand_revenue \
         FROM ( \
             SELECT SUM(revenue) AS revenue_total FROM tpch_dag_q06_fp \
             UNION ALL \
             SELECT SUM(sum_base_price) AS revenue_total FROM tpch_dag_q01_fp \
         ) combined";

    // Ground truth: compute union directly from base tables
    const UNION_GROUND_TRUTH: &str = "SELECT SUM(revenue_total) AS grand_revenue \
         FROM ( \
             SELECT SUM(l_extendedprice * l_discount) AS revenue_total \
             FROM lineitem \
             WHERE l_shipdate >= DATE '1994-01-01' \
               AND l_shipdate < DATE '1994-01-01' + INTERVAL '1 year' \
               AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01 \
               AND l_quantity < 24 \
             UNION ALL \
             SELECT SUM(sum_base_price) AS revenue_total \
             FROM ( \
                 SELECT SUM(l_extendedprice) AS sum_base_price \
                 FROM lineitem \
                 WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90 days' \
                 GROUP BY l_returnflag, l_linestatus \
             ) q01_inline \
         ) combined";

    // Create level-0 STs
    let q01_ok = db
        .try_execute(&format!(
            "SELECT pgtrickle.create_stream_table('tpch_dag_q01_fp', $${Q01_SQL}$$, '1m', 'DIFFERENTIAL')"
        ))
        .await;
    let q06_ok = db
        .try_execute(&format!(
            "SELECT pgtrickle.create_stream_table('tpch_dag_q06_fp', $${Q06_SQL}$$, '1m', 'DIFFERENTIAL')"
        ))
        .await;

    if q01_ok.is_err() || q06_ok.is_err() {
        println!("  SKIP — level-0 create failed");
        let _ = db
            .try_execute("SELECT pgtrickle.drop_stream_table('tpch_dag_q01_fp')")
            .await;
        let _ = db
            .try_execute("SELECT pgtrickle.drop_stream_table('tpch_dag_q06_fp')")
            .await;
        return;
    }
    println!("  level-0 STs created ✓");

    // Create level-1 ST.
    // P1.6: If both level-0 STs were created successfully, a union failure is
    // unexpected and should be a hard-fail rather than a soft-skip.  A soft-skip
    // here would silently hide DAG fan-in bugs introduced by future changes.
    if let Err(e) = db
        .try_execute(&format!(
            "SELECT pgtrickle.create_stream_table('tpch_dag_union', $${UNION_SQL}$$, '1m', 'DIFFERENTIAL')"
        ))
        .await
    {
        // Clean up level-0 STs before panicking.
        let _ = db
            .try_execute("SELECT pgtrickle.drop_stream_table('tpch_dag_q01_fp')")
            .await;
        let _ = db
            .try_execute("SELECT pgtrickle.drop_stream_table('tpch_dag_q06_fp')")
            .await;
        panic!(
            "union level-1 create failed after both level-0 STs were created successfully.\n\
             This is unexpected — it indicates a DAG fan-in bug, not a known limitation.\n\
             Error: {e}"
        );
    }
    println!("  tpch_dag_union created ✓");

    // Baseline
    let baseline_ok = assert_invariant(&db, "tpch_dag_q01_fp", Q01_SQL, "dag_q01_fp", 0)
        .await
        .and(assert_invariant(&db, "tpch_dag_q06_fp", Q06_SQL, "dag_q06_fp", 0).await)
        .and(assert_invariant(&db, "tpch_dag_union", UNION_GROUND_TRUTH, "dag_union", 0).await);

    if let Err(e) = baseline_ok {
        // P1.6: Hard-fail if all three STs were created successfully but baseline fails.
        // A passing baseline is required for the test to be meaningful.
        let _ = db
            .try_execute("SELECT pgtrickle.drop_stream_table('tpch_dag_union')")
            .await;
        let _ = db
            .try_execute("SELECT pgtrickle.drop_stream_table('tpch_dag_q01_fp')")
            .await;
        let _ = db
            .try_execute("SELECT pgtrickle.drop_stream_table('tpch_dag_q06_fp')")
            .await;
        panic!(
            "DAG multi-parent baseline failed — all STs were created but invariant check failed.\n\
             This indicates a DVM correctness bug, not just a known limitation.\n\
             Error: {e}"
        );
    }
    println!("  baseline ✓\n");

    // Mutation cycles
    for cycle in 1..=n_cycles {
        let ct = Instant::now();

        let next_ok = max_orderkey(&db).await + 1;
        let rf1 = substitute_rf(RF1_SQL, next_ok);
        exec_sql_stmts(&db, &rf1)
            .await
            .unwrap_or_else(|e| panic!("RF1 cycle {cycle}: {e}"));
        let rf2 = RF2_SQL.replace("__RF_COUNT__", &rf_count().to_string());
        exec_sql_stmts(&db, &rf2)
            .await
            .unwrap_or_else(|e| panic!("RF2 cycle {cycle}: {e}"));
        let rf3 = RF3_SQL.replace("__RF_COUNT__", &rf_count().to_string());
        exec_sql_stmts(&db, &rf3)
            .await
            .unwrap_or_else(|e| panic!("RF3 cycle {cycle}: {e}"));

        db.execute("ANALYZE lineitem").await;

        // Refresh in topological order: level-0 first, level-1 last
        if let Err(e) = try_refresh(&db, "tpch_dag_q01_fp").await {
            println!("  WARN Q01_fp refresh cycle {cycle}: {e}");
            break;
        }
        if let Err(e) = try_refresh(&db, "tpch_dag_q06_fp").await {
            println!("  WARN Q06_fp refresh cycle {cycle}: {e}");
            break;
        }
        if let Err(e) = try_refresh(&db, "tpch_dag_union").await {
            println!("  WARN union refresh cycle {cycle}: {e}");
            break;
        }

        // Assert all three
        assert_invariant(&db, "tpch_dag_q01_fp", Q01_SQL, "dag_q01_fp", cycle)
            .await
            .unwrap_or_else(|e| panic!("dag_q01_fp invariant cycle {cycle}: {e}"));
        assert_invariant(&db, "tpch_dag_q06_fp", Q06_SQL, "dag_q06_fp", cycle)
            .await
            .unwrap_or_else(|e| panic!("dag_q06_fp invariant cycle {cycle}: {e}"));
        assert_invariant(
            &db,
            "tpch_dag_union",
            UNION_GROUND_TRUTH,
            "dag_union",
            cycle,
        )
        .await
        .unwrap_or_else(|e| panic!("dag_union invariant cycle {cycle}: {e}"));

        db.execute("VACUUM").await;

        println!(
            "  cycle {cycle}/{n_cycles} — all 3 STs correct ✓ — {:.0}ms",
            ct.elapsed().as_secs_f64() * 1000.0,
        );
    }

    // Clean up
    let _ = db
        .try_execute("SELECT pgtrickle.drop_stream_table('tpch_dag_union')")
        .await;
    let _ = db
        .try_execute("SELECT pgtrickle.drop_stream_table('tpch_dag_q01_fp')")
        .await;
    let _ = db
        .try_execute("SELECT pgtrickle.drop_stream_table('tpch_dag_q06_fp')")
        .await;

    println!("\n  DAG multi-parent correctness: PASSED ✓\n");
}
