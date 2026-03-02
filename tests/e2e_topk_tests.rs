//! E2E tests for TopK (ORDER BY … LIMIT N) stream tables.
//!
//! Validates that queries with a top-level ORDER BY + LIMIT are accepted,
//! correctly materialized, and refreshed via the scoped-recomputation
//! (MERGE-based) path.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ── Creation ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_create_basic() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_src (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_src VALUES (1,10),(2,50),(3,30),(4,40),(5,20)")
        .await;

    db.create_st(
        "topk_basic",
        "SELECT id, score FROM topk_src ORDER BY score DESC LIMIT 3",
        "1m",
        "FULL",
    )
    .await;

    // Should have exactly 3 rows — the top 3 by score
    assert_eq!(db.count("public.topk_basic").await, 3);

    // Verify the correct rows are present (scores 50, 40, 30)
    let top_score: i32 = db
        .query_scalar("SELECT score FROM public.topk_basic ORDER BY score DESC LIMIT 1")
        .await;
    assert_eq!(top_score, 50);

    let min_score: i32 = db
        .query_scalar("SELECT score FROM public.topk_basic ORDER BY score ASC LIMIT 1")
        .await;
    assert_eq!(min_score, 30);
}

#[tokio::test]
async fn test_topk_create_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_diff_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO topk_diff_src VALUES (1,100),(2,200),(3,300)")
        .await;

    // TopK tables should be accepted even with DIFFERENTIAL mode
    db.create_st(
        "topk_diff",
        "SELECT id, val FROM topk_diff_src ORDER BY val DESC LIMIT 2",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.topk_diff").await, 2);
}

// ── Catalog ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_catalog_fields_populated() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_cat (id INT PRIMARY KEY, rank INT)")
        .await;
    db.execute("INSERT INTO topk_cat VALUES (1,1),(2,2),(3,3)")
        .await;

    db.create_st(
        "topk_cat_st",
        "SELECT id, rank FROM topk_cat ORDER BY rank ASC LIMIT 2",
        "1m",
        "FULL",
    )
    .await;

    let topk_limit: i32 = db
        .query_scalar(
            "SELECT topk_limit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'topk_cat_st'",
        )
        .await;
    assert_eq!(topk_limit, 2, "topk_limit should be stored in catalog");

    let topk_order_by: String = db
        .query_scalar(
            "SELECT topk_order_by FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'topk_cat_st'",
        )
        .await;
    assert!(
        topk_order_by.to_lowercase().contains("rank"),
        "topk_order_by should contain 'rank', got: {}",
        topk_order_by
    );
}

#[tokio::test]
async fn test_topk_monitoring_view_shows_is_topk() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_mon (id INT PRIMARY KEY, v INT)")
        .await;
    db.execute("INSERT INTO topk_mon VALUES (1,1),(2,2)").await;

    db.create_st(
        "topk_mon_st",
        "SELECT id, v FROM topk_mon ORDER BY v DESC LIMIT 1",
        "1m",
        "FULL",
    )
    .await;

    let is_topk: bool = db
        .query_scalar(
            "SELECT is_topk FROM pgtrickle.stream_tables_info WHERE pgt_name = 'topk_mon_st'",
        )
        .await;
    assert!(is_topk, "is_topk should be true in monitoring view");
}

// ── Refresh — Inserts ──────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_refresh_new_row_enters_top_n() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_ins (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_ins VALUES (1,10),(2,20),(3,30)")
        .await;

    db.create_st(
        "topk_ins_st",
        "SELECT id, score FROM topk_ins ORDER BY score DESC LIMIT 2",
        "1m",
        "FULL",
    )
    .await;
    assert_eq!(db.count("public.topk_ins_st").await, 2);

    // Insert a row with a higher score that should enter the top 2
    db.execute("INSERT INTO topk_ins VALUES (4, 50)").await;
    db.refresh_st("topk_ins_st").await;

    assert_eq!(db.count("public.topk_ins_st").await, 2);

    // The new row (score=50) should be present, old bottom (score=20) should be gone
    let max_score: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_ins_st")
        .await;
    assert_eq!(max_score, 50);

    let min_score: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_ins_st")
        .await;
    assert_eq!(min_score, 30, "Bottom of top-2 should now be 30");
}

#[tokio::test]
async fn test_topk_refresh_new_row_below_cutoff() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_below (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_below VALUES (1,100),(2,200),(3,300)")
        .await;

    db.create_st(
        "topk_below_st",
        "SELECT id, score FROM topk_below ORDER BY score DESC LIMIT 2",
        "1m",
        "FULL",
    )
    .await;

    // Insert a row below the top-2 cutoff
    db.execute("INSERT INTO topk_below VALUES (4, 50)").await;
    db.refresh_st("topk_below_st").await;

    assert_eq!(db.count("public.topk_below_st").await, 2);

    // Top-2 should be unchanged (300, 200)
    let min_score: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_below_st")
        .await;
    assert_eq!(min_score, 200, "Top-2 should be unchanged");
}

// ── Refresh — Updates ──────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_refresh_update_changes_ranking() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_upd (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_upd VALUES (1,10),(2,20),(3,30)")
        .await;

    db.create_st(
        "topk_upd_st",
        "SELECT id, score FROM topk_upd ORDER BY score DESC LIMIT 2",
        "1m",
        "FULL",
    )
    .await;

    // Update the lowest-scoring row to become the highest
    db.execute("UPDATE topk_upd SET score = 100 WHERE id = 1")
        .await;
    db.refresh_st("topk_upd_st").await;

    assert_eq!(db.count("public.topk_upd_st").await, 2);

    let max_score: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_upd_st")
        .await;
    assert_eq!(max_score, 100, "Updated row should now be top");

    let min_score: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_upd_st")
        .await;
    assert_eq!(min_score, 30, "Second place should be 30");
}

// ── Refresh — Deletes ──────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_refresh_delete_top_row() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_del (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_del VALUES (1,10),(2,20),(3,30),(4,40)")
        .await;

    db.create_st(
        "topk_del_st",
        "SELECT id, score FROM topk_del ORDER BY score DESC LIMIT 3",
        "1m",
        "FULL",
    )
    .await;
    assert_eq!(db.count("public.topk_del_st").await, 3);

    // Delete the highest-scoring row
    db.execute("DELETE FROM topk_del WHERE id = 4").await;
    db.refresh_st("topk_del_st").await;

    assert_eq!(db.count("public.topk_del_st").await, 3);

    let max_score: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_del_st")
        .await;
    assert_eq!(max_score, 30, "After deleting 40, new top should be 30");

    // Row with score=10 should now be in the top 3
    let min_score: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_del_st")
        .await;
    assert_eq!(min_score, 10, "Row with score 10 should now be in top 3");
}

// ── Refresh — Fewer rows than LIMIT ────────────────────────────────────

#[tokio::test]
async fn test_topk_fewer_rows_than_limit() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_few (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO topk_few VALUES (1,10)").await;

    db.create_st(
        "topk_few_st",
        "SELECT id, val FROM topk_few ORDER BY val DESC LIMIT 5",
        "1m",
        "FULL",
    )
    .await;

    // Only 1 row exists, LIMIT 5 — should have 1 row
    assert_eq!(db.count("public.topk_few_st").await, 1);

    db.execute("INSERT INTO topk_few VALUES (2,20),(3,30)")
        .await;
    db.refresh_st("topk_few_st").await;

    assert_eq!(db.count("public.topk_few_st").await, 3);
}

// ── Refresh — Multiple refreshes ───────────────────────────────────────

#[tokio::test]
async fn test_topk_multiple_refreshes() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_multi (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_multi VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
        .await;

    db.create_st(
        "topk_multi_st",
        "SELECT id, score FROM topk_multi ORDER BY score DESC LIMIT 3",
        "1m",
        "FULL",
    )
    .await;
    assert_eq!(db.count("public.topk_multi_st").await, 3);

    // Refresh 1: insert new top row
    db.execute("INSERT INTO topk_multi VALUES (6, 100)").await;
    db.refresh_st("topk_multi_st").await;
    assert_eq!(db.count("public.topk_multi_st").await, 3);
    let max1: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_multi_st")
        .await;
    assert_eq!(max1, 100);

    // Refresh 2: delete the new top row
    db.execute("DELETE FROM topk_multi WHERE id = 6").await;
    db.refresh_st("topk_multi_st").await;
    assert_eq!(db.count("public.topk_multi_st").await, 3);
    let max2: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_multi_st")
        .await;
    assert_eq!(max2, 50);
}

// ── TopK with JOIN ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_with_join() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_orders (id INT PRIMARY KEY, customer_id INT, amount NUMERIC)")
        .await;
    db.execute("CREATE TABLE topk_customers (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("INSERT INTO topk_customers VALUES (1,'Alice'),(2,'Bob'),(3,'Carol')")
        .await;
    db.execute("INSERT INTO topk_orders VALUES (1,1,100),(2,2,200),(3,3,300),(4,1,400),(5,2,500)")
        .await;

    db.create_st(
        "topk_join_st",
        "SELECT o.id, c.name, o.amount FROM topk_orders o JOIN topk_customers c ON o.customer_id = c.id ORDER BY o.amount DESC LIMIT 3",
        "1m",
        "FULL",
    )
    .await;

    assert_eq!(db.count("public.topk_join_st").await, 3);

    let top_amount: f64 = db
        .query_scalar("SELECT amount::float8 FROM public.topk_join_st ORDER BY amount DESC LIMIT 1")
        .await;
    assert!((top_amount - 500.0).abs() < 0.01);
}

// ── TopK with aggregate ────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_with_aggregate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_sales (id INT PRIMARY KEY, product TEXT, revenue INT)")
        .await;
    db.execute(
        "INSERT INTO topk_sales VALUES (1,'A',100),(2,'B',200),(3,'A',150),(4,'C',300),(5,'B',50)",
    )
    .await;

    db.create_st(
        "topk_agg_st",
        "SELECT product, SUM(revenue) as total FROM topk_sales GROUP BY product ORDER BY total DESC LIMIT 2",
        "1m",
        "FULL",
    )
    .await;

    assert_eq!(db.count("public.topk_agg_st").await, 2);

    // C=300, A=250, B=250 — top 2 should be C and one of A/B
    let top_total: i64 = db
        .query_scalar("SELECT MAX(total) FROM public.topk_agg_st")
        .await;
    assert_eq!(top_total, 300);
}

// ── Drop ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_drop_stream_table() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_drop_src (id INT PRIMARY KEY, v INT)")
        .await;
    db.execute("INSERT INTO topk_drop_src VALUES (1,1),(2,2)")
        .await;

    db.create_st(
        "topk_drop_st",
        "SELECT id, v FROM topk_drop_src ORDER BY v DESC LIMIT 1",
        "1m",
        "FULL",
    )
    .await;

    db.drop_st("topk_drop_st").await;

    let exists = db.table_exists("public", "topk_drop_st").await;
    assert!(!exists, "TopK stream table should be dropped");
}
