# PLAN_POLARS.md — Polars Synergy & Integration Plan for pg_trickle

Date: 2026-04-20
Status: RESEARCH

---

## 1. Executive Summary

[Polars](https://pola.rs/) is a blazingly fast DataFrame library written in
Rust with bindings for Python, R, and Node.js. It features a lazy query engine
with automatic query optimization, Apache Arrow–based columnar memory, streaming
execution for larger-than-memory datasets, and native PostgreSQL connectivity.

pg_trickle and Polars occupy **adjacent layers** of the modern data stack:
pg_trickle keeps materialized views incrementally fresh *inside* PostgreSQL,
while Polars provides high-performance analytical computation *outside* the
database in application-side data pipelines. Together, they can deliver a
**real-time analytical stack** where pg_trickle guarantees low-latency
freshness of pre-computed results in PostgreSQL, and Polars enables
high-throughput client-side analytics over those results without taxing the
database.

This document catalogues: (1) a technical comparison, (2) synergy scenarios,
(3) concrete integration opportunities, (4) shared Rust/Arrow foundations,
(5) design patterns worth borrowing, and (6) a phased action plan.

---

## 2. Technical Comparison

| Dimension | Polars | pg_trickle |
|---|---|---|
| **Core problem** | High-performance DataFrame analytics in application code | Keeping materialized views fresh incrementally inside PostgreSQL |
| **Language** | Rust core, Python/R/Node.js bindings | Rust (pgrx), runs inside PostgreSQL |
| **Execution model** | Lazy query plan → optimized → collect/stream | SQL trigger/WAL CDC → delta query → merge into storage table |
| **Query language** | Expression API + SQL interface (`SQLContext`) | Standard PostgreSQL SQL |
| **Memory model** | Apache Arrow columnar buffers (can stream out-of-core) | PostgreSQL shared buffers / heap storage |
| **Parallelism** | Automatic multi-core via work-stealing thread pool | PostgreSQL single-backend per refresh (parallel workers via PG planner) |
| **Streaming** | Lazy API with `engine="streaming"` for larger-than-memory | Streaming via row-level CDC; change buffers processed in batches |
| **Optimization** | Predicate pushdown, projection pushdown, common subplan elimination, join ordering, cardinality estimation | DBSP-derived delta queries, net-effect optimization, monotonicity analysis |
| **Data formats** | CSV, Parquet, JSON, IPC, databases, cloud storage, Hive | PostgreSQL heap tables (source + storage) |
| **Plugin system** | Expression plugins (Rust) + IO plugins (Python/Rust) | PostgreSQL extension model (pgrx) |
| **Join types** | Inner, left, right, full, semi, anti, cross, asof, non-equi | Inner, left, right, full outer (differential rules for each) |
| **Aggregation** | Full expression-based agg with group_by | GROUP BY with delta maintenance (auxiliary count tracking) |
| **Window functions** | `over()` partition/order expressions | Window function differentiation (multi-PARTITION BY rewrite) |
| **Arrow support** | Native Arrow memory layout; PyCapsule Interface; zero-copy export/import | N/A (PostgreSQL heap format); potential via `pg_arrow` or COPY BINARY |
| **GPU support** | NVIDIA cuDF integration (open beta) | N/A |
| **Deployment** | pip install / cargo add | CREATE EXTENSION pg_trickle |
| **License** | MIT | Apache 2.0 |

---

## 3. Synergy Scenarios

### 3.1. Real-Time Dashboard Pipeline

**Pattern:** pg_trickle maintains fresh aggregate tables inside PostgreSQL;
Polars reads those tables for client-side visualization and ad-hoc analysis.

```
┌──────────────────────────────────┐
│   OLTP Application               │
│   INSERT / UPDATE / DELETE        │
└────────┬─────────────────────────┘
         │
         ▼
┌──────────────────────────────────┐
│   PostgreSQL + pg_trickle        │
│   Source tables → CDC →           │
│   Stream tables (fresh aggs)     │
└────────┬─────────────────────────┘
         │  pl.read_database_uri()
         ▼
┌──────────────────────────────────┐
│   Polars (Python / Rust)         │
│   LazyFrame → join → transform → │
│   visualize / export / ML        │
└──────────────────────────────────┘
```

**Why this works:** pg_trickle's differential refresh keeps the summary tables
current with sub-second overhead (7–42× faster than REFRESH MATERIALIZED VIEW).
Polars reads those pre-computed results via `pl.read_database_uri()` using
ConnectorX or ADBC — both support zero-copy Arrow ingestion from PostgreSQL.
The application-side Polars pipeline doesn't need to re-aggregate raw data;
it works with already-fresh summaries.

**User benefit:** Dashboards get sub-second data freshness without running
expensive queries against live OLTP tables.

### 3.2. Hybrid Analytics: SQL Aggregation + DataFrame Post-Processing

Many analytics workflows require two stages:

1. **Aggregate** — GROUP BY, JOIN, window functions over large tables
2. **Post-process** — pivot, transpose, custom scoring, ML feature engineering

pg_trickle handles stage 1 incrementally inside PostgreSQL (where the data
lives). Polars handles stage 2 with its rich expression API, which excels at
operations that are awkward or impossible in SQL:

- Pivoting/unpivoting
- Rolling/expanding windows with arbitrary functions
- Expression expansion (`pl.col("col_a", "col_b") * 0.95`)
- User-defined functions without GIL contention
- List/struct column manipulation
- Fold operations across columns

```python
import polars as pl

# Stage 1 — pg_trickle keeps this table fresh incrementally
uri = "postgresql://user:pass@host/db"
df = pl.read_database_uri(
    "SELECT * FROM pgtrickle_sales_by_region",
    uri=uri,
    engine="adbc",  # zero-copy Arrow
)

# Stage 2 — Polars post-processing
result = (
    df.with_columns(
        growth=pl.col("revenue") / pl.col("revenue").shift(1).over("region") - 1,
        rank=pl.col("revenue").rank(descending=True).over("quarter"),
    )
    .filter(pl.col("rank") <= 10)
    .pivot(on="quarter", index="region", values="revenue")
)
```

### 3.3. Change Data Export: pg_trickle CDC → Polars Streaming

pg_trickle's change buffer tables (`pgtrickle_changes.changes_<oid>`) contain
row-level deltas with action types (I/U/D) and typed column data. These buffers
are standard PostgreSQL tables that Polars can read directly:

```python
# Read the change buffer for a specific source table
changes = pl.read_database_uri(
    """
    SELECT pgt_action, pgt_lsn, new_customer_id, new_amount, new_ts
    FROM pgtrickle_changes.changes_16384
    WHERE pgt_lsn > '0/1A000000'
    ORDER BY pgt_lsn
    """,
    uri=uri,
)

# Compute analytics over the change stream itself
inserts_per_minute = (
    changes.filter(pl.col("pgt_action") == "I")
    .with_columns(pl.col("new_ts").dt.truncate("1m").alias("minute"))
    .group_by("minute")
    .agg(pl.len().alias("insert_count"), pl.col("new_amount").sum())
)
```

**Use case:** Event-sourcing analytics, change auditing, real-time anomaly
detection over the change stream itself — computed in Polars' fast columnar
engine rather than in PostgreSQL.

### 3.4. Polars as a pg_trickle Testing / Benchmarking Harness

Polars' `read_database_uri` + lazy evaluation makes it an excellent tool for:

- **Correctness testing** — Read stream table contents and source tables into
  Polars DataFrames, re-compute the expected result in Polars, compare.
- **Performance benchmarking** — Time the read of stream table results, track
  refresh history via `pgtrickle.pgt_refresh_history`, produce benchmark
  DataFrames/charts.
- **Data quality monitoring** — Periodically read stream table + source table,
  compute diffs in Polars, alert on discrepancies.

```python
# Correctness check: compare stream table vs full recomputation
st_result = pl.read_database_uri("SELECT * FROM my_stream_table", uri)
expected = pl.read_database_uri("SELECT ... (defining query) ...", uri)

assert st_result.sort("id").equals(expected.sort("id")), "Drift detected!"
```

### 3.5. Polars Cloud / On-Premises + pg_trickle

Polars offers [Polars Cloud](https://docs.pola.rs/polars-cloud/) (distributed
query execution) and [Polars On-Premises](https://docs.pola.rs/polars-on-premises/)
(Kubernetes / bare-metal deployment). In these environments:

- pg_trickle maintains fresh summary tables in PostgreSQL
- Polars Cloud reads those summaries as sources for distributed computation
- Polars' distributed engine can parallelize post-processing across workers

This positions pg_trickle as the **near-data computation layer** and Polars as
the **scale-out analytics layer**.

---

## 4. Shared Technical Foundations

### 4.1. Rust + Arrow Ecosystem

Both projects are written in Rust and operate in the Apache Arrow ecosystem:

| Foundation | Polars | pg_trickle |
|---|---|---|
| Language | Rust | Rust (pgrx) |
| Arrow dependency | `arrow2` / Polars own buffers (Arrow-compatible) | N/A today; could use `arrow` crate for export |
| Data transfer | Arrow IPC, PyCapsule Interface, C Data Interface | PostgreSQL COPY BINARY, SPI |
| Crate ecosystem | `polars`, `pyo3-polars`, `connectorx` | `pgrx`, `pg_sys` |

**Opportunity:** A future `pgtrickle_arrow` module could expose stream table
deltas or snapshots in Arrow IPC format, enabling true zero-copy data transfer
to Polars without going through PostgreSQL's text/binary protocol. This would
be especially impactful for large-scale reads.

### 4.2. Query Optimization Parallels

Both systems implement query optimization, though at different levels:

| Optimization | Polars | pg_trickle |
|---|---|---|
| Predicate pushdown | Into scan nodes (CSV, Parquet, DB) | Into CDC buffer queries (LSN filtering) |
| Projection pushdown | Only read needed columns | Column-level lineage tracks `columns_used` per source |
| Common subplan elimination | Cache shared subtrees | DAG-based: shared source tables refreshed once |
| Expression simplification | Constant folding, cheaper alternatives | Monotonicity analysis, volatility classification |
| Join ordering | Cardinality-based heuristics | Topological sort of dependency DAG |

Both projects could exchange ideas on cardinality estimation techniques and
join ordering heuristics.

### 4.3. SQL Context Compatibility

Polars' `SQLContext` aims to follow **PostgreSQL syntax** and function behavior:

> "Where possible, Polars aims to follow PostgreSQL syntax definitions and
> function behaviour."

This means SQL written for pg_trickle stream tables is likely to work
unmodified in Polars' `SQLContext`. Users can prototype analytical queries
locally in Polars' SQL interface, then deploy them as stream table definitions:

```python
# Prototype in Polars
ctx = pl.SQLContext(orders=orders_df, customers=customers_df)
result = ctx.execute("""
    SELECT c.region, sum(o.amount) as total
    FROM orders o JOIN customers c ON o.cust_id = c.id
    GROUP BY c.region
""")

# Deploy to pg_trickle
# SELECT pgtrickle.create_stream_table('regional_totals', $$ ... same SQL ... $$)
```

---

## 5. Design Patterns Worth Borrowing

### 5.1. From Polars → pg_trickle

| Pattern | Description | Applicability to pg_trickle |
|---|---|---|
| **Streaming execution** | Process data in batches without full materialization | Could apply to FULL refresh: stream source query results in batches rather than materializing entire result set before diffing |
| **Expression plugins** | Rust-native UDFs compiled as shared libraries, no Python GIL | Concept for user-defined delta rules: let users register custom differentiation logic for domain-specific functions |
| **IO plugins** | Register custom data sources with projection/predicate pushdown | Could inspire a plugin API for custom CDC sources (e.g., Kafka topics, S3 change feeds) |
| **Lazy query plan visualization** | `show_graph()` renders the physical query plan | pg_trickle's `explain_stream_table()` could render the DVM operator tree as a graph (Mermaid/DOT) |
| **Multiplexing sinks** | Single LazyFrame writes to multiple outputs simultaneously | A stream table refresh could simultaneously update the storage table AND write deltas to an Arrow export channel |
| **Cardinality estimation** | Estimates branch sizes for optimal group_by strategy | Could improve pg_trickle's choice between FULL vs DIFFERENTIAL refresh based on estimated delta size |

### 5.2. From pg_trickle → Polars

| Pattern | Description | Applicability to Polars |
|---|---|---|
| **Incremental view maintenance** | Only recompute what changed | Polars could benefit from IVM semantics for repeatedly-evaluated LazyFrames over changing data sources |
| **Demand-driven scheduling** | Downstream freshness requirements propagate upstream | Polars Cloud's distributed engine could use similar DAG-based scheduling for multi-step pipelines |
| **Hybrid CDC** | Auto-transition from simple to optimized capture | Polars IO plugins could auto-select between full scan and incremental read based on source capabilities |
| **DBSP delta derivation** | Formal algebraic rules for differential operators | Could inform Polars' future incremental computation features (e.g., incremental `group_by` over appended data) |

---

## 6. Concrete Integration Opportunities

### 6.1. `polars-pgtrickle` — Polars IO Plugin (Medium-Term)

A dedicated Polars IO plugin that understands pg_trickle metadata:

```python
import polars as pl
import polars_pgtrickle as pgt

# Scan a stream table — automatically reads from the optimal source
lf = pgt.scan_stream_table("regional_totals", uri=uri)

# Scan changes since last read (incremental)
lf_delta = pgt.scan_stream_table_changes(
    "regional_totals",
    since_lsn="0/1A000000",
    uri=uri,
)

# Get stream table metadata
status = pgt.get_stream_table_status(uri=uri)
```

**Implementation:** Rust-based IO plugin using `pyo3-polars` and `connectorx`
/ `tokio-postgres`. The plugin would:
1. Query `pgtrickle.pgt_stream_tables` for metadata
2. Read from the storage table with projection pushdown
3. Optionally read from change buffers for delta-aware pipelines
4. Leverage Arrow-native transfer via ADBC

### 6.2. Arrow Export from pg_trickle (Long-Term)

Add an optional `pgtrickle_arrow` module to pg_trickle that:
- Exposes stream table snapshots as Arrow IPC streams via a custom COPY handler or FDW
- Exposes change buffer deltas as Arrow record batches
- Enables zero-copy transfer to any Arrow-compatible consumer (Polars, DuckDB, DataFusion, etc.)

```sql
-- Hypothetical future syntax
COPY (SELECT * FROM my_stream_table) TO PROGRAM 'arrow_ipc' WITH (FORMAT arrow);
```

### 6.3. Polars-Based Correctness Testing Framework

A pytest-based framework using Polars as the reference engine:

```python
@pytest.mark.parametrize("stream_table", get_all_stream_tables())
def test_stream_table_correctness(stream_table, pg_conn):
    """Compare stream table contents against Polars recomputation."""
    st = pl.read_database_uri(f"SELECT * FROM {stream_table.name}", pg_conn)
    expected = pl.read_database_uri(stream_table.defining_query, pg_conn)

    # Sort both by primary key columns
    pk_cols = stream_table.pk_columns
    diff = st.sort(pk_cols).frame_equal(expected.sort(pk_cols))
    assert diff, f"Drift in {stream_table.name}"
```

### 6.4. Monitoring Dashboard with Polars + Altair/Plotly

Polars' built-in visualization support (Altair, hvPlot, Plotly) combined with
pg_trickle's monitoring views:

```python
import polars as pl

# Read refresh history
history = pl.read_database_uri(
    "SELECT * FROM pgtrickle.pgt_refresh_history ORDER BY start_time DESC LIMIT 1000",
    uri=uri,
)

# Compute refresh latency trends
chart = (
    history.with_columns(
        latency_ms=(pl.col("end_time") - pl.col("start_time")).dt.total_milliseconds(),
    )
    .plot.line(x="start_time", y="latency_ms", color="action")
    .properties(title="pg_trickle Refresh Latency Over Time", width=800)
)
```

### 6.5. Documentation: "Using pg_trickle with Polars" Tutorial

A tutorial in `docs/tutorials/` covering:
1. Setting up pg_trickle stream tables for a sample schema
2. Reading stream tables into Polars DataFrames
3. Building an analytical pipeline in Polars over fresh data
4. Monitoring refresh health from Polars
5. Incremental change consumption patterns

---

## 7. Competitive Positioning

### 7.1. vs. DuckDB + pg_trickle

DuckDB is another popular analytical engine. The Polars advantage:

| | Polars | DuckDB |
|---|---|---|
| PG connectivity | ConnectorX (zero-copy Arrow), ADBC | `postgres_scanner` extension |
| Streaming | Native streaming engine | Limited streaming |
| Plugin system | Expression + IO plugins (Rust) | Extensions (C++) |
| Python integration | Native, no GIL for plugins | sqlite-style connection API |
| Arrow interop | PyCapsule Interface, zero-copy | Excellent (Arrow-native) |
| GPU support | cuDF integration | Not available |

Both are complementary with pg_trickle; Polars has the edge for Python-native
workflows, while DuckDB shines for SQL-centric analytics.

### 7.2. vs. Pandas + pg_trickle

| | Polars | Pandas |
|---|---|---|
| Performance | 10–100× faster (Rust, columnar, multi-threaded) | Single-threaded, row-oriented copy |
| Memory | Arrow-based, out-of-core streaming | NumPy-backed, everything in RAM |
| PG reading | ConnectorX/ADBC (zero-copy) | `read_sql` via SQLAlchemy (row-by-row copy) |
| API | Declarative expressions with optimization | Imperative, eager evaluation |

Polars is the clear modern choice for pg_trickle integration due to performance
and Arrow-native data transfer.

---

## 8. Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Polars SQL dialect diverges from PostgreSQL | Stream table defining queries may not run identically in Polars' SQLContext | Document dialect differences; focus integration on DataFrame API, not SQL layer |
| Arrow export from PostgreSQL has overhead | Binary protocol → Arrow conversion adds latency | Use ADBC (Arrow-native) or ConnectorX; long-term: native Arrow export from pg_trickle |
| Polars API instability (still pre-2.0 for some features) | Integration code may break on upgrades | Pin versions; write integration tests |
| pg_trickle change buffer schema is internal | Polars plugin reading change buffers may break on schema changes | Version the buffer schema; provide a stable CDC export API |
| Resource contention | Polars reading large tables while pg_trickle refreshes | Use read replicas; schedule heavy reads outside refresh windows |

---

## 9. Action Plan

### Phase 1 — Documentation & Patterns (v0.23.x)

- [ ] Write "Using pg_trickle with Polars" tutorial in `docs/tutorials/`
- [ ] Add Polars examples to `examples/` showing read patterns
- [ ] Document recommended ADBC/ConnectorX setup for zero-copy reads
- [ ] Add Polars-based correctness check to CI (experimental)

### Phase 2 — Python Package `polars-pgtrickle` (v0.24.x)

- [ ] Create `polars-pgtrickle` Python package (MIT licensed)
  - `scan_stream_table()` — lazy scan of stream table contents
  - `scan_changes()` — lazy scan of change buffers with LSN filtering
  - `get_status()` — read stream table metadata as DataFrame
  - `get_refresh_history()` — read refresh history as DataFrame
- [ ] Publish to PyPI
- [ ] Add integration tests using testcontainers

### Phase 3 — Arrow Export (v0.25.x+)

- [ ] Investigate `pg_arrow` / custom COPY handler for Arrow IPC export
- [ ] Prototype Arrow-format delta export from change buffers
- [ ] Benchmark Arrow export vs ADBC vs ConnectorX for various data sizes

### Phase 4 — Ecosystem Cross-Pollination (Ongoing)

- [ ] Contribute PostgreSQL-specific improvements to ConnectorX / ADBC drivers
- [ ] Explore Polars expression plugin for pg_trickle monitoring UDFs
- [ ] Track Polars' incremental computation features as they develop
- [ ] Consider Polars Cloud integration for distributed analytics over stream tables

---

## 10. References

- [Polars Documentation](https://docs.pola.rs/)
- [Polars GitHub Repository](https://github.com/pola-rs/polars)
- [Polars Lazy API — Optimizations](https://docs.pola.rs/user-guide/lazy/optimizations/)
- [Polars Streaming Execution](https://docs.pola.rs/user-guide/concepts/streaming/)
- [Polars SQL Interface](https://docs.pola.rs/user-guide/sql/intro/)
- [Polars Database IO](https://docs.pola.rs/user-guide/io/database/)
- [Polars Expression Plugins](https://docs.pola.rs/user-guide/plugins/expr_plugins/)
- [Polars IO Plugins](https://docs.pola.rs/user-guide/plugins/io_plugins/)
- [Polars Arrow Interop](https://docs.pola.rs/user-guide/misc/arrow/)
- [Polars Cloud](https://docs.pola.rs/polars-cloud/)
- [Polars On-Premises](https://docs.pola.rs/polars-on-premises/)
- [ConnectorX — Zero-copy PostgreSQL to Arrow](https://github.com/sfu-db/connector-x)
- [ADBC — Arrow Database Connectivity](https://arrow.apache.org/docs/format/ADBC.html)
- [pg_trickle Architecture](../docs/ARCHITECTURE.md)
- [pg_trickle ESSENCE](../../ESSENCE.md)
- [DBSP: Automatic Incremental View Maintenance (Budiu et al., 2022)](https://arxiv.org/abs/2203.16684)
