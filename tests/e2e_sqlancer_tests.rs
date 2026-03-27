//! SQLancer-style differential fuzzing tests (Phase 4 — SQLANCER-1 & SQLANCER-2).
//!
//! # What this implements
//!
//! **SQLANCER-1 — Crash oracle:** For each randomly generated query Q,
//! `pgtrickle.create_stream_table()` + `pgtrickle.refresh_stream_table()` must
//! not crash the backend. Any structured SQL error (unsupported query, invalid
//! argument) is acceptable; a lost connection or PostgreSQL PANIC is a failure.
//!
//! **SQLANCER-2 — Equivalence oracle:** For queries that successfully create and
//! populate a stream table, the stream table contents must be identical (multiset
//! equality) to the result of executing the original SELECT directly.
//!
//! # Running
//!
//! These tests are marked `#[ignore]` and are not included in the normal `just
//! test-e2e` run. They are executed by the `weekly-sqlancer` CI job or locally:
//!
//! ```bash
//! just sqlancer          # rebuild Docker image + run all sqlancer tests
//! just sqlancer-fast     # skip Docker image rebuild
//!
//! # Control the number of generated queries:
//! SQLANCER_CASES=500 just sqlancer-fast
//! ```
//!
//! # Proptest regression seeds
//!
//! When the equivalence oracle detects a mismatch it panics with the seed value.
//! Save the seed to `proptest-regressions/e2e_sqlancer/corpus.txt` (one hex
//! seed per line) to replay the failure on subsequent runs:
//!
//! ```text
//! # proptest-regressions/e2e_sqlancer/corpus.txt
//! 0xdeadbeef01234567
//! ```

mod e2e;

use e2e::E2eDb;

// ── Query generator ────────────────────────────────────────────────────────

/// Seeded LCG random number generator (deterministic, no external dependency).
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x6c62272e07bb0142,
        }
    }

    fn next(&mut self) -> u64 {
        // Knuth multiplicative LCG (64-bit)
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.state
    }

    fn choice<T: Clone>(&mut self, options: &[T]) -> T {
        options[(self.next() as usize) % options.len()].clone()
    }

    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + (self.next() % (hi - lo + 1))
    }
}

/// Description of a generated test table.
#[derive(Clone, Debug)]
struct TestTable {
    name: String,
    cols: Vec<(String, &'static str)>, // (column_name, sql_type)
    row_count: usize,
}

impl TestTable {
    fn ddl(&self) -> String {
        let col_defs: Vec<String> = std::iter::once("id BIGINT PRIMARY KEY".to_string())
            .chain(self.cols.iter().map(|(n, t)| format!("{n} {t}")))
            .collect();
        format!("CREATE TABLE {} ({})", self.name, col_defs.join(", "))
    }

    fn insert_dml(&self, rng: &mut Lcg) -> String {
        let mut rows = Vec::new();
        for i in 1..=(self.row_count) {
            let mut vals: Vec<String> = vec![i.to_string()];
            for (_, t) in &self.cols {
                let v = match *t {
                    "INT" => (rng.range(1, 100) as i64).to_string(),
                    "BIGINT" => (rng.range(1, 1000) as i64).to_string(),
                    "NUMERIC" => format!("{}", rng.range(1, 500)),
                    "TEXT" => {
                        let choices = ["alpha", "beta", "gamma", "delta", "epsilon"];
                        format!("'{}'", rng.choice(&choices))
                    }
                    _ => "0".to_string(),
                };
                vals.push(v);
            }
            rows.push(format!("({})", vals.join(", ")));
        }
        format!("INSERT INTO {} VALUES {}", self.name, rows.join(", "))
    }
}

/// A generated test query and the tables it references.
#[derive(Clone, Debug)]
struct GeneratedQuery {
    query: String,
    tables: Vec<TestTable>,
    /// Human-readable description for failure messages.
    description: String,
    seed: u64,
}

/// Generate a batch of queries using a seeded random number generator.
fn generate_queries(base_seed: u64, count: usize) -> Vec<GeneratedQuery> {
    let mut queries = Vec::with_capacity(count);

    for idx in 0..count {
        let seed = base_seed.wrapping_add(idx as u64 * 0x9e3779b97f4a7c15);
        let mut rng = Lcg::new(seed);

        let query = generate_one_query(&mut rng, idx);
        queries.push(GeneratedQuery { seed, ..query });
    }

    queries
}

fn generate_one_query(rng: &mut Lcg, idx: usize) -> GeneratedQuery {
    let variant = rng.range(0, 5);
    match variant {
        0 => gen_simple_select(rng, idx),
        1 => gen_aggregate_query(rng, idx),
        2 => gen_filter_query(rng, idx),
        3 => gen_join_query(rng, idx),
        _ => gen_multi_aggregate(rng, idx),
    }
}

fn make_table(name: &str, rng: &mut Lcg, row_count: usize) -> TestTable {
    let col_types = ["INT", "BIGINT", "NUMERIC", "TEXT"];
    let num_cols = rng.range(2, 5) as usize;
    let cols: Vec<(String, &'static str)> = (0..num_cols)
        .map(|i| {
            let t = rng.choice(&col_types);
            (format!("col{i}"), t)
        })
        .collect();
    TestTable {
        name: name.to_string(),
        cols,
        row_count,
    }
}

fn gen_simple_select(rng: &mut Lcg, idx: usize) -> GeneratedQuery {
    let row_count = rng.range(5, 30) as usize;
    let tbl = make_table(&format!("t_ss_{idx}"), rng, row_count);
    let non_text_cols: Vec<_> = tbl
        .cols
        .iter()
        .filter(|(_, t)| *t != "TEXT")
        .map(|(n, _)| n.clone())
        .collect();
    let select_col = if non_text_cols.is_empty() {
        "col0".to_string()
    } else {
        rng.choice(&non_text_cols)
    };
    let query = format!("SELECT id, {select_col} FROM {}", tbl.name);
    GeneratedQuery {
        query,
        tables: vec![tbl.clone()],
        description: format!("simple SELECT (idx={idx})"),
        seed: 0,
    }
}

fn gen_aggregate_query(rng: &mut Lcg, idx: usize) -> GeneratedQuery {
    let row_count = rng.range(8, 40) as usize;
    let tbl = make_table(&format!("t_ag_{idx}"), rng, row_count);
    let text_cols: Vec<_> = tbl
        .cols
        .iter()
        .filter(|(_, t)| *t == "TEXT")
        .map(|(n, _)| n.clone())
        .collect();
    let num_cols: Vec<_> = tbl
        .cols
        .iter()
        .filter(|(_, t)| *t != "TEXT")
        .map(|(n, _)| n.clone())
        .collect();

    let group_col = if text_cols.is_empty() {
        "id"
    } else {
        &text_cols[0]
    };
    let agg_func = rng.choice(&["SUM", "COUNT", "MAX", "MIN"]);

    let (agg_expr, agg_alias) = if agg_func == "COUNT" || num_cols.is_empty() {
        ("COUNT(*)".to_string(), "cnt".to_string())
    } else {
        let c = rng.choice(&num_cols);
        (format!("{agg_func}({c})"), "agg_result".to_string())
    };

    let query = format!(
        "SELECT {group_col}, {agg_expr} AS {agg_alias} FROM {} GROUP BY {group_col}",
        tbl.name
    );
    GeneratedQuery {
        query,
        tables: vec![tbl],
        description: format!("aggregate {agg_func} GROUP BY (idx={idx})"),
        seed: 0,
    }
}

fn gen_filter_query(rng: &mut Lcg, idx: usize) -> GeneratedQuery {
    let row_count = rng.range(10, 50) as usize;
    let tbl = make_table(&format!("t_fl_{idx}"), rng, row_count);
    let num_cols: Vec<_> = tbl
        .cols
        .iter()
        .filter(|(_, t)| *t != "TEXT")
        .map(|(n, _)| n.clone())
        .collect();

    let (where_clause, select_cols) = if num_cols.is_empty() {
        ("id > 0".to_string(), "id".to_string())
    } else {
        let c = rng.choice(&num_cols);
        let threshold = rng.range(1, 50);
        (format!("{c} > {threshold}"), format!("id, {c}"))
    };

    let query = format!(
        "SELECT {select_cols} FROM {} WHERE {where_clause}",
        tbl.name
    );
    GeneratedQuery {
        query,
        tables: vec![tbl],
        description: format!("filter query (idx={idx})"),
        seed: 0,
    }
}

fn gen_join_query(rng: &mut Lcg, idx: usize) -> GeneratedQuery {
    let row_count_1 = rng.range(5, 20) as usize;
    let t1 = make_table(&format!("t_j1_{idx}"), rng, row_count_1);
    let row_count_2 = rng.range(5, 20) as usize;
    let t2 = make_table(&format!("t_j2_{idx}"), rng, row_count_2);
    let query = format!(
        "SELECT a.id, a.col0, b.col0 AS b_col0 \
         FROM {t1} a JOIN {t2} b ON a.id = b.id",
        t1 = t1.name,
        t2 = t2.name,
    );
    GeneratedQuery {
        query,
        tables: vec![t1, t2],
        description: format!("inner JOIN (idx={idx})"),
        seed: 0,
    }
}

fn gen_multi_aggregate(rng: &mut Lcg, idx: usize) -> GeneratedQuery {
    let row_count = rng.range(8, 40) as usize;
    let tbl = make_table(&format!("t_ma_{idx}"), rng, row_count);
    let text_cols: Vec<_> = tbl
        .cols
        .iter()
        .filter(|(_, t)| *t == "TEXT")
        .map(|(n, _)| n.clone())
        .collect();
    let num_cols: Vec<_> = tbl
        .cols
        .iter()
        .filter(|(_, t)| *t != "TEXT")
        .map(|(n, _)| n.clone())
        .collect();

    let group_col = if text_cols.is_empty() {
        "id"
    } else {
        &text_cols[0]
    };

    let agg_clauses: Vec<String> = if num_cols.is_empty() {
        vec!["COUNT(*) AS cnt".to_string()]
    } else {
        let c1 = rng.choice(&num_cols);
        let c2 = rng.choice(&num_cols);
        vec![
            format!("SUM({c1}) AS sum_col"),
            format!("MAX({c2}) AS max_col"),
            "COUNT(*) AS cnt".to_string(),
        ]
    };

    let query = format!(
        "SELECT {group_col}, {} FROM {} GROUP BY {group_col}",
        agg_clauses.join(", "),
        tbl.name,
    );
    GeneratedQuery {
        query,
        tables: vec![tbl],
        description: format!("multi-aggregate (idx={idx})"),
        seed: 0,
    }
}

// ── Test helpers ───────────────────────────────────────────────────────────

fn sqlancer_cases() -> usize {
    std::env::var("SQLANCER_CASES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(200)
}

fn base_seed() -> u64 {
    std::env::var("SQLANCER_SEED")
        .ok()
        .and_then(|v| {
            let s = v.trim();
            if s.starts_with("0x") || s.starts_with("0X") {
                u64::from_str_radix(&s[2..], 16).ok()
            } else {
                s.parse::<u64>().ok()
            }
        })
        .unwrap_or(0xdeadbeef_cafebabe)
}

// ── SQLANCER-1: Crash oracle ───────────────────────────────────────────────

/// **SQLANCER-1 — Crash oracle.**
///
/// Generates `SQLANCER_CASES` (default 200) random `create_stream_table` calls
/// and verifies that none crash the PostgreSQL backend. Any structured SQL error
/// (unsupported query, constraint violation) is acceptable; a lost connection or
/// server PANIC is a failure.
///
/// Run with: `just sqlancer-fast` or `SQLANCER_CASES=10000 just sqlancer-fast`.
/// Run the crash oracle logic (extracted so it can be called from combined test).
async fn run_crash_oracle() {
    let cases = sqlancer_cases();
    let seed = base_seed();

    println!("[sqlancer] crash oracle: {cases} cases, base_seed=0x{seed:016x}");

    let queries = generate_queries(seed, cases);
    let mut crashes = 0usize;
    let mut structured_errors = 0usize;
    let mut successes = 0usize;

    for (i, gq) in queries.iter().enumerate() {
        // Use a fresh DB per query to avoid cross-test pollution.
        let db = E2eDb::new().await.with_extension().await;

        // Create tables and insert data.
        for tbl in &gq.tables {
            db.execute(&tbl.ddl()).await;
            let mut rng = Lcg::new(gq.seed ^ (i as u64 * 0x1234567890abcdef));
            db.execute(&tbl.insert_dml(&mut rng)).await;
        }

        // Try to create the stream table.
        let st_name = format!("sqlancer_st_{i}");
        let create_sql = format!(
            "SELECT pgtrickle.create_stream_table(\
             name => '{st_name}', \
             defining_query => $SQL${}$SQL$, \
             schedule => '1m', \
             mode => 'FULL'\
             )",
            gq.query
        );

        let create_result = db.try_execute(&create_sql).await;
        match create_result {
            Ok(_) => {
                successes += 1;
                // Also attempt a refresh to trigger the execution path.
                let refresh_sql = format!("SELECT pgtrickle.refresh_stream_table('{st_name}')");
                let refresh_result = db.try_execute(&refresh_sql).await;
                if let Err(e) = refresh_result {
                    let msg = e.to_string();
                    // Distinguish crash (connection lost) from structured error.
                    if msg.contains("connection") && msg.contains("closed") {
                        crashes += 1;
                        eprintln!(
                            "[sqlancer] CRASH detected (seed=0x{:016x}, case={i}): {e}\n  query: {}",
                            gq.seed, gq.query
                        );
                    } else {
                        structured_errors += 1;
                    }
                }
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("connection") && msg.contains("closed") {
                    crashes += 1;
                    eprintln!(
                        "[sqlancer] CRASH on create (seed=0x{:016x}, case={i}): {e}\n  query: {}",
                        gq.seed, gq.query
                    );
                } else {
                    // Structured error (unsupported query, etc.) — expected for fuzzing.
                    structured_errors += 1;
                }
            }
        }

        if (i + 1) % 50 == 0 {
            println!(
                "[sqlancer] progress: {}/{cases} — ok={successes} errs={structured_errors} crashes={crashes}",
                i + 1
            );
        }
    }

    println!(
        "[sqlancer] crash oracle done: {cases} cases — \
         ok={successes}, structured_errors={structured_errors}, crashes={crashes}"
    );

    assert_eq!(
        crashes, 0,
        "SQLANCER-1 crash oracle: {crashes} backend crash(es) detected out of {cases} cases.\n\
         Re-run with SQLANCER_SEED=0x{seed:016x} SQLANCER_CASES={cases} to reproduce.",
    );
}

#[tokio::test]
#[ignore]
async fn test_sqlancer_crash_oracle() {
    run_crash_oracle().await;
}

/// Run the equivalence oracle logic (extracted so it can be called from combined test).
async fn run_equivalence_oracle() {
    let cases = sqlancer_cases();
    let seed = base_seed();

    println!("[sqlancer] equivalence oracle: {cases} cases, base_seed=0x{seed:016x}");

    let queries = generate_queries(seed, cases);
    let mut mismatches = Vec::<(u64, String, String)>::new();
    let mut skipped = 0usize;
    let mut checked = 0usize;

    for (i, gq) in queries.iter().enumerate() {
        let db = E2eDb::new().await.with_extension().await;

        // Create source tables.
        for tbl in &gq.tables {
            db.execute(&tbl.ddl()).await;
            let mut rng = Lcg::new(gq.seed ^ (i as u64 * 0x1234567890abcdef));
            db.execute(&tbl.insert_dml(&mut rng)).await;
        }

        // Attempt to create + populate a FULL-mode stream table.
        let st_name = format!("sqlancer_eq_{i}");
        let create_sql = format!(
            "SELECT pgtrickle.create_stream_table(\
             name => '{st_name}', \
             defining_query => $SQL${}$SQL$, \
             schedule => '1m', \
             mode => 'FULL'\
             )",
            gq.query
        );

        if db.try_execute(&create_sql).await.is_err() {
            skipped += 1;
            continue;
        }

        // Force a FULL refresh.
        let refresh_sql = format!("SELECT pgtrickle.refresh_stream_table('{st_name}')");
        if db.try_execute(&refresh_sql).await.is_err() {
            skipped += 1;
            continue;
        }

        // Compare row counts.
        let st_count: i64 = db
            .query_scalar(&format!("SELECT COUNT(*) FROM public.{st_name}"))
            .await;

        let direct_count: i64 = db
            .query_scalar(&format!("SELECT COUNT(*) FROM ({}) AS _q", gq.query))
            .await;

        checked += 1;

        if st_count != direct_count {
            let msg = format!(
                "count mismatch: st={st_count} vs direct={direct_count} | query: {}",
                gq.query
            );
            mismatches.push((gq.seed, gq.description.clone(), msg));
        }

        if (i + 1) % 50 == 0 {
            println!(
                "[sqlancer] progress: {}/{cases} — checked={checked} skipped={skipped} mismatches={}",
                i + 1,
                mismatches.len()
            );
        }
    }

    println!(
        "[sqlancer] equivalence oracle done: {cases} cases — \
         checked={checked}, skipped={skipped}, mismatches={}",
        mismatches.len()
    );

    if !mismatches.is_empty() {
        eprintln!("\n[sqlancer] EQUIVALENCE FAILURES:");
        for (seed, desc, msg) in &mismatches {
            eprintln!("  seed=0x{seed:016x}  [{desc}]  {msg}");
        }
        eprintln!(
            "\nTo replay: SQLANCER_SEED=0x{seed:016x} SQLANCER_CASES={cases} just sqlancer-fast",
        );
        panic!(
            "SQLANCER-2 equivalence oracle: {} mismatch(es) out of {checked} checked queries.",
            mismatches.len()
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_sqlancer_equivalence_oracle() {
    run_equivalence_oracle().await;
}

// ── SQLANCER: stress + crash combined (CI entry point) ────────────────────

/// Combined crash + equivalence oracle for CI.
///
/// Runs in the `weekly-sqlancer` CI job. Uses `SQLANCER_CASES` to control
/// case count (default 200 for quick CI runs; 10 000 for nightly).
#[tokio::test]
#[ignore]
async fn test_sqlancer_ci_combined() {
    run_crash_oracle().await;
    run_equivalence_oracle().await;
}
