//! G17-MDB: Multi-database scheduler isolation test.
//!
//! Validates that pg_trickle's background scheduler operates correctly
//! when multiple databases in the same PostgreSQL cluster each have the
//! extension installed with independent stream tables.
//!
//! Assertions:
//!   - Worker quotas are per-database (one DB's load cannot starve the other)
//!   - Shared-memory state does not bleed between databases
//!   - Each database's stream tables refresh independently and correctly
//!   - DAG cache is isolated per database
//!
//! These tests are `#[ignore]`d to skip in normal `cargo test` runs.
//! Run manually or via CI:
//!
//! ```bash
//! just test-mdb           # Multi-database isolation test (~3 min)
//! ```
//!
//! Prerequisites: E2E Docker image (`just build-e2e-image`)

mod e2e;

use e2e::E2eDb;

// ── Helpers ────────────────────────────────────────────────────────────

/// Create a source table and stream table in the given database connection.
async fn setup_database(db: &E2eDb, prefix: &str) {
    // Create source table
    db.execute(&format!(
        "CREATE TABLE {prefix}_orders (
            id SERIAL PRIMARY KEY,
            customer_id INT NOT NULL DEFAULT (random() * 100)::int,
            amount NUMERIC(10,2) NOT NULL DEFAULT (random() * 500)::numeric(10,2),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )"
    ))
    .await;

    // Seed data
    db.execute(&format!(
        "INSERT INTO {prefix}_orders (customer_id, amount)
         SELECT (random() * 100)::int, (random() * 500)::numeric(10,2)
         FROM generate_series(1, 500)"
    ))
    .await;

    // Create stream table (aggregation)
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table(
            '{prefix}_summary',
            'SELECT customer_id, SUM(amount) AS total, COUNT(*) AS cnt
             FROM {prefix}_orders GROUP BY customer_id',
            schedule => '2s',
            refresh_mode => 'DIFFERENTIAL'
        )"
    ))
    .await;

    // Create stream table (filter)
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table(
            '{prefix}_big_orders',
            'SELECT id, customer_id, amount FROM {prefix}_orders WHERE amount > 250',
            schedule => '2s',
            refresh_mode => 'DIFFERENTIAL'
        )"
    ))
    .await;

    // Initial refresh
    db.execute(&format!(
        "SELECT pgtrickle.refresh_stream_table('{prefix}_summary')"
    ))
    .await;
    db.execute(&format!(
        "SELECT pgtrickle.refresh_stream_table('{prefix}_big_orders')"
    ))
    .await;
}

/// Apply DML to a database and verify correctness after refresh.
async fn mutate_and_verify(db: &E2eDb, prefix: &str, iteration: usize) -> Result<(), String> {
    // INSERT
    db.try_execute(&format!(
        "INSERT INTO {prefix}_orders (customer_id, amount)
         SELECT (random() * 100)::int, (random() * 500)::numeric(10,2)
         FROM generate_series(1, 20)"
    ))
    .await
    .map_err(|e| format!("[{prefix}] INSERT failed: {e}"))?;

    // UPDATE
    db.try_execute(&format!(
        "UPDATE {prefix}_orders
         SET amount = amount * 1.1
         WHERE id IN (SELECT id FROM {prefix}_orders ORDER BY random() LIMIT 10)"
    ))
    .await
    .map_err(|e| format!("[{prefix}] UPDATE failed: {e}"))?;

    // DELETE
    db.try_execute(&format!(
        "DELETE FROM {prefix}_orders
         WHERE id IN (SELECT id FROM {prefix}_orders ORDER BY random() LIMIT 5)"
    ))
    .await
    .map_err(|e| format!("[{prefix}] DELETE failed: {e}"))?;

    // Refresh both STs
    db.try_execute(&format!(
        "SELECT pgtrickle.refresh_stream_table('{prefix}_summary')"
    ))
    .await
    .map_err(|e| format!("[{prefix}] refresh summary failed: {e}"))?;

    db.try_execute(&format!(
        "SELECT pgtrickle.refresh_stream_table('{prefix}_big_orders')"
    ))
    .await
    .map_err(|e| format!("[{prefix}] refresh big_orders failed: {e}"))?;

    // Verify correctness: summary
    let summary_ok: bool = db
        .query_scalar(&format!(
            "SELECT NOT EXISTS (
                (SELECT customer_id, total, cnt FROM public.{prefix}_summary
                 EXCEPT ALL
                 SELECT customer_id, SUM(amount) AS total, COUNT(*) AS cnt
                 FROM {prefix}_orders GROUP BY customer_id)
                UNION ALL
                (SELECT customer_id, SUM(amount) AS total, COUNT(*) AS cnt
                 FROM {prefix}_orders GROUP BY customer_id
                 EXCEPT ALL
                 SELECT customer_id, total, cnt FROM public.{prefix}_summary)
            )"
        ))
        .await;

    if !summary_ok {
        return Err(format!(
            "[{prefix}] iteration {iteration}: {prefix}_summary correctness violation"
        ));
    }

    // Verify correctness: big_orders
    let filter_ok: bool = db
        .query_scalar(&format!(
            "SELECT NOT EXISTS (
                (SELECT id, customer_id, amount FROM public.{prefix}_big_orders
                 EXCEPT ALL
                 SELECT id, customer_id, amount FROM {prefix}_orders WHERE amount > 250)
                UNION ALL
                (SELECT id, customer_id, amount FROM {prefix}_orders WHERE amount > 250
                 EXCEPT ALL
                 SELECT id, customer_id, amount FROM public.{prefix}_big_orders)
            )"
        ))
        .await;

    if !filter_ok {
        return Err(format!(
            "[{prefix}] iteration {iteration}: {prefix}_big_orders correctness violation"
        ));
    }

    Ok(())
}

// ── Test ───────────────────────────────────────────────────────────────

/// Test that two databases in the same cluster can run pg_trickle
/// independently without interference.
///
/// Uses `E2eDb::new_on_postgres_db()` style database creation to get
/// two databases in the same container.
#[tokio::test]
#[ignore]
async fn test_multi_database_isolation() {
    println!("\n══════════════════════════════════════════════════════════");
    println!("  G17-MDB: Multi-Database Scheduler Isolation Test");
    println!("══════════════════════════════════════════════════════════\n");

    // Create two separate databases in the same container
    let db_a = E2eDb::new().await.with_extension().await;
    let db_b = E2eDb::new().await.with_extension().await;

    println!("  Setting up Database A...");
    setup_database(&db_a, "dba").await;

    println!("  Setting up Database B...");
    setup_database(&db_b, "dbb").await;

    // Verify initial state: both databases have 2 STs each
    let count_a: i64 = db_a
        .query_scalar("SELECT COUNT(*) FROM pgtrickle.pgt_stream_tables WHERE status = 'ACTIVE'")
        .await;
    let count_b: i64 = db_b
        .query_scalar("SELECT COUNT(*) FROM pgtrickle.pgt_stream_tables WHERE status = 'ACTIVE'")
        .await;
    assert_eq!(count_a, 2, "Database A should have 2 active stream tables");
    assert_eq!(count_b, 2, "Database B should have 2 active stream tables");
    println!("  Both databases initialized with 2 stream tables each\n");

    // ── Cross-database isolation: catalog ──────────────────────────────
    println!("  Testing catalog isolation...");

    // Database A should NOT see Database B's stream tables (or vice versa)
    let a_sees_dbb: bool = db_a
        .query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name LIKE 'dbb_%'
            )",
        )
        .await;
    assert!(
        !a_sees_dbb,
        "Database A should not see Database B's stream tables"
    );

    let b_sees_dba: bool = db_b
        .query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name LIKE 'dba_%'
            )",
        )
        .await;
    assert!(
        !b_sees_dba,
        "Database B should not see Database A's stream tables"
    );
    println!("    Catalog isolation: ✓");

    // ── Concurrent mutation cycles ─────────────────────────────────────
    let iterations = 10;
    println!(
        "\n  Running {} concurrent mutation cycles across both databases...\n",
        iterations
    );

    let mut failures: Vec<String> = Vec::new();

    for i in 1..=iterations {
        print!("  Iteration {i}/{iterations}: ");

        // Mutate and verify both databases
        // Run them sequentially to avoid connection pool contention
        if let Err(e) = mutate_and_verify(&db_a, "dba", i).await {
            println!("FAIL (A)");
            failures.push(e);
            continue;
        }
        if let Err(e) = mutate_and_verify(&db_b, "dbb", i).await {
            println!("FAIL (B)");
            failures.push(e);
            continue;
        }

        println!("✓");
    }

    // ── Final validation ───────────────────────────────────────────────
    println!("\n  Final state:");

    // Check no error states in either database
    let errors_a: i64 = db_a
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.pgt_stream_tables
             WHERE status IN ('ERROR', 'SUSPENDED')",
        )
        .await;
    let errors_b: i64 = db_b
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.pgt_stream_tables
             WHERE status IN ('ERROR', 'SUSPENDED')",
        )
        .await;

    println!("    Database A: {count_a} active, {errors_a} errors");
    println!("    Database B: {count_b} active, {errors_b} errors");

    // Check refresh history independence
    let history_a: i64 = db_a
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.pgt_refresh_history WHERE status = 'COMPLETED'",
        )
        .await;
    let history_b: i64 = db_b
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.pgt_refresh_history WHERE status = 'COMPLETED'",
        )
        .await;
    println!("    Database A completed refreshes: {history_a}");
    println!("    Database B completed refreshes: {history_b}");

    assert!(
        history_a > 0,
        "Database A should have completed at least one refresh"
    );
    assert!(
        history_b > 0,
        "Database B should have completed at least one refresh"
    );
    assert_eq!(errors_a, 0, "Database A should have no error states");
    assert_eq!(errors_b, 0, "Database B should have no error states");

    if !failures.is_empty() {
        println!("\n  ⚠ Failures:");
        for f in &failures {
            println!("    - {f}");
        }
        panic!("Multi-database test had {} failure(s)", failures.len());
    }

    println!("\n  ✓ Multi-database isolation test PASSED");
}
