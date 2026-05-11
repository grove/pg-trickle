# Research: Custom SQL Syntax Options

This document surveys custom-syntax extensions considered for pg_trickle
(e.g. `CREATE STREAM TABLE`) and the tradeoffs against the
current function-based API (`pgtrickle.create_stream_table()`). It is
intended for contributors and language/parser research.

> **User documentation** on SQL functions is in [SQL Reference](../SQL_REFERENCE.md).

---

## Abstract

pg_trickle deliberately chose a **function-based API** (`pgtrickle.create_stream_table()`) over custom DDL syntax (`CREATE STREAM TABLE …`) for three reasons: PostgreSQL's `pg_catalog` has no extension point for new top-level statement types without patching the core parser; function calls are portable across every client library and ORMs without any driver-level changes; and they compose naturally with PL/pgSQL, transaction blocks, and conditional DDL patterns.

This research surveys the three realistic implementation routes — grammar patches, `ProcessUtility` hooks, and comment-driven DDL shims — and quantifies the upgrade maintenance burden of each against the stable, zero-patch approach of function calls. The findings strongly favour the current design for an extension targeting production deployments.

The document also catalogues prior art: `pg_partman`'s `run_maintenance()` model, `timescaledb`'s grammar extension (and its associated patch surface), and `pg_ivm`'s function-only API. For pg_trickle's scale and lifecycle goals, the function-based API remains the correct long-term choice. A lightweight `CREATE STREAM TABLE` compatibility shim delivered via a PL/pgSQL wrapper is noted as a viable opt-in convenience without any parser modifications.

---

{{#include ../../plans/sql/REPORT_CUSTOM_SQL_SYNTAX.md}}
