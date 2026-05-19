# DuckLake Community Outreach — v0.64.0

This document tracks the community engagement plan for the v0.64.0 DuckLake
ecosystem release. All activities are outbound communication and conference
submissions — no code changes required.

---

## C-1: Named-User Outreach

DuckLake v1.0 (released April 2026) lists the following production users on the
DuckLake homepage. Each represents a company whose workload directly benefits
from pg_trickle's incremental view maintenance. The pitch: pg_trickle gives
them millisecond-fresh metrics over their existing DuckLake deployment without
adding any new infrastructure.

| Company | Use Case | pg_trickle Angle | Priority |
|---------|----------|-----------------|----------|
| **PostHog** | Product analytics — counting events per user per funnel step | Funnel aggregations (COUNT, SUM) over DuckLake event tables; sub-second freshness without Flink | HIGH |
| **Windmill** | Workflow engine — exposes run statistics | Run counts and latency percentiles over workflow event history in DuckLake | HIGH |
| **locals.com** | Community platform — surfaces trending content | Rolling engagement scores (views, likes, shares) maintained incrementally over content event stream | HIGH |
| **Ascend.io** | Data pipeline orchestration | Pipeline-run metrics and SLA monitoring over execution logs stored in DuckLake | MEDIUM |
| **Sliplane** | Kubernetes infrastructure platform | Container metrics aggregated over DuckLake telemetry tables | MEDIUM |
| **Media Cluster Norway** | Media company analytics | Content performance metrics over DuckLake-stored engagement events | MEDIUM |
| **AlterTable** | Data consultancy | Reference architecture for DuckLake + pg_trickle stack | LOW |
| **Summation** | Data engineering tooling | Integration testing and demo environment | LOW |

### Outreach Script

Opening line for all contacts:

> DuckLake's v1.0 roadmap lists "materialized views and incremental maintenance"
> as a future feature. We already built it. It lives in the same PostgreSQL
> instance that hosts your DuckLake catalog, requires no additional infrastructure,
> and delivers sub-second refresh latency using a signed-multiset differential
> dataflow engine.
>
> We'd love to show you a five-minute demo. Can we schedule 15 minutes?

Attach links to:
1. Tutorial: [Real-Time Dashboards on Your Data Lake](../../blog/ducklake-real-time-dashboards.md)
2. Demo: [The Five-Second Funnel](../demos/ducklake-funnel/README.md)
3. Blog: [Why pg_trickle + DuckLake Is the Missing Piece for Lakehouse IVM](../../blog/ducklake-ivm-missing-piece.md)

### Contact Channels

- **PostHog:** Open a GitHub Discussion on `PostHog/posthog` + direct LinkedIn
  outreach to infrastructure team
- **Windmill:** Contact via https://windmill.dev/contact — mention DuckLake
  integration
- **locals.com:** Twitter/X + LinkedIn DM to engineering team
- **Ascend.io:** Contact form + LinkedIn DM to data infrastructure lead
- **Sliplane:** GitHub Discussions on `sliplane/sliplane`
- **Media Cluster Norway:** LinkedIn DM to CTO

---

## C-2: Conference Talk Submissions

### DuckCon (DuckDB conference, typically autumn)

**Talk title:** pg_trickle: The Incremental View Maintenance Engine DuckLake's
Roadmap Is Asking For

**Abstract:**

DuckLake stores its catalog in PostgreSQL. pg_trickle is a PostgreSQL extension
that maintains SQL query results incrementally using a differential dataflow
engine. Put them in the same database and something remarkable happens:
DuckLake's change feed is a perfect input to pg_trickle's engine, and
pg_trickle's stream tables are a perfect output for DuckLake's catalog. This
talk demonstrates the integration — from the signed-multiset algebra that makes
it work to the `docker compose up` demo that makes it tangible. We also discuss
the Phase 2 roadmap: a native `table_changes()` adapter that eliminates polling
entirely, and a DuckLake sink that publishes pg_trickle results directly to the
lake as Parquet.

**Target audience:** DuckDB/DuckLake users, data engineers, PostgreSQL users

**Duration:** 30 minutes + Q&A

**Submission deadline:** Track the DuckCon CFP at https://duckdb.org/duckcon

---

### PGConf EU (annual, typically October/November)

**Talk title:** Sub-second materialized views on your data lake with pg_trickle
+ DuckLake

**Abstract:**

DuckLake v1.0 stores its entire catalog in PostgreSQL. pg_trickle is a
PostgreSQL 18 extension that does incremental view maintenance from inside
the database. Together they eliminate the CDC-pipeline-plus-stream-processor
stack that every data lake IVM solution currently requires. This talk covers
the architecture, the algebraic foundation, and the production path from a
DuckLake PostgreSQL catalog to a live Grafana dashboard — with no Kafka, no
Flink, and no JVM in the picture.

**Target audience:** PostgreSQL DBAs, data engineers, backend developers

**Duration:** 45 minutes (standard PGConf EU slot)

**Submission:** https://www.postgresql.eu/events/pgconfeu/callforpapers/

---

### PGCon (Ottawa, annual)

**Talk title:** Differential Dataflow Inside PostgreSQL — How pg_trickle Does
What Flink Does Without Leaving the Database

**Abstract:**

A technical deep-dive into the differential dataflow engine at the core of
pg_trickle: how it represents changes as signed multisets, how it applies
algebraic inverse rules for SUM, COUNT, GROUP BY, and JOIN, and why DuckLake's
`table_changes()` output maps so cleanly to this model. For the systems
programming audience that wants to understand the algebra before trusting the
results.

**Target audience:** PostgreSQL core developers, Rust extension authors,
streaming systems engineers

**Duration:** 45 minutes

**Submission:** https://www.pgcon.org/cfp/

---

## C-3: DuckLake GitHub Community Engagement

### GitHub Discussion: Reference Architecture Request

Open a Discussion on the `duckdb/ducklake` repository:

**Title:** pg_trickle + DuckLake reference architecture — proposing to
co-author documentation

**Body:**

> We have built a working integration between pg_trickle (a PostgreSQL
> incremental view maintenance extension) and DuckLake that demonstrates the
> IVM use case called out in DuckLake's roadmap. The integration requires no
> new DuckLake features — it uses the existing PostgreSQL catalog tables and
> `table_changes()` API.
>
> We have published:
> - Three tutorials (real-time dashboards, modern data stack, monitoring)
> - Two blog posts (IVM gap analysis, `table_changes()` wire-level tour)
> - Two self-contained `docker compose up` demos
>
> We would like to propose co-authoring a "pg_trickle + DuckLake reference
> architecture" page on the DuckLake documentation site, and we are happy to
> contribute to DuckLake's own IVM documentation when that roadmap item
> progresses.
>
> Is there interest from the DuckLake team in a reference partnership?

**Links to attach:**
- https://github.com/trickle-labs/pg-trickle
- Tutorial links (once published to documentation site)
- Demo links

### Follow-up: DuckLake Discord / Slack

Post in the DuckDB Discord `#ducklake` channel (or equivalent) with a short
introduction and link to the tutorials. Offer to answer integration questions.

---

## Tracking

| Item | Status | Owner | Target Date |
|------|--------|-------|-------------|
| PostHog outreach | Pending | — | One week after v0.64.0 release |
| Windmill outreach | Pending | — | One week after v0.64.0 release |
| locals.com outreach | Pending | — | Two weeks after v0.64.0 release |
| DuckCon CFP submission | Pending | — | Track DuckCon dates |
| PGConf EU CFP submission | Pending | — | Track PGConf EU CFP |
| PGCon CFP submission | Pending | — | Track PGCon CFP |
| GitHub Discussion on duckdb/ducklake | Pending | — | One week after v0.64.0 release |
| DuckDB Discord post | Pending | — | Day of v0.64.0 release |
