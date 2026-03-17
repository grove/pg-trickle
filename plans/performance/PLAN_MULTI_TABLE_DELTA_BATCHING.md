# PLAN: Multi-Table Delta Batching (B-3)

## Status: 🔴 NOT STARTED

**Goal:** `v0.9.0` (Milestone 3)
**Remaining to do:** 
- [ ] **B3-3:** Write property-based correctness proofs for simultaneous multi-source changes (diamond-flow scenarios) using Z-set weight aggregation (`SUM(weight)`) instead of `DISTINCT ON` deduplication.
- [ ] **B3-2:** Implement merged-delta generation logic in DVM parser using `GROUP BY __pgt_row_id, SUM(weight)`.
- [ ] **B3-1:** Implement intra-query delta branch pruning (skip `UNION ALL` branches entirely when a source has zero changes).

---

## Overview

When a stream table joins multiple tables (A, B, C) and multiple tables change in the same scheduler cycle, `pg_trickle` currently evaluates the DVM delta query using potentially separate passes or constructs unions with empty branches.

This plan outlines the steps to merge these passes into one optimized query plan, while mathematically preserving correctnesThis plan outlines the steps to merge these passes into one optimiz`DThis plan outlines the steps to merge tle A and Table B in the exact same transaction, and a stream tablThis plan outlines tle canonical row might be corrected twice (once from the ΔA path, once from the ΔB path). 

Previously, it was proposed to use `DISTINCT ON` for cross-delta deduplication. **This causes silent data corruption.** `DISTINCT ON (__pgt_row_id)` discards subsequent corrections that might offset each other.

Instead, we must use **Z-set weight aggregation**. We aggregate the combined deltas:
```sql
merged_delta AS (
  SELECT 'A' AS src, * FROM delta_A   UNION ALL
  SELECT 'B' AS src, * FROM delta_B
),
final_delta AS (
  SELECT __pgt_row_id, SUM(weight) as weight, <other_cols>
  FROM merged_delta
  GROUP BY __pgt_row_id
  HAVING SUM(weight) != 0
)
```

## Implementation Sequence

### 1. Property-based C### 1. Property-based C### 1. Property-based C### 1. Property-based C### 1. Property-based C### 1. Property-based C### 1. Property-based C### 1. Property-based C### 1. Property-based C### 1. Property-based C### 1. Property-based C### 1. Property-based C### 1. Property-based C### 1. Property-based C### 1. Property-based C### 1. Property-based C### 1. Property-based C### 1. Property-based C### 1. Property-bDel### 1. Property-based C### 1. Property-based C### 1. Properhe true multi-source delta engine in `src/dvm/diff.rs`.
*Why:* Generates the `UNION ALL` + `GROUP BY` logic dynamically.
*Where:* `DiffEngine::diff_node(*Where:* `DiffEngine::diff_node(*Where:* `DiffEngine::diff_node(*Where:* `DiffEnginmize the *Where:* `DiffEn ALL` statements. If 3 tables are in a join but on*Where:* `DiffEngine::di skip genera*in*Where:* `DiffEel*Where:* `DiffEngine::diff_node(*Where:* `DiffEngine::diftgreSQL planner time and execution latency.
*W*W*W*W*W*W*W*W*W*W*W*W*Wchecking `any_changes` per buffer.
