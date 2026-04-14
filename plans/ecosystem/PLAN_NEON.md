# PLAN_NEON — pg_trickle on Self-Hosted Neon

**Status:** Research / Planning  
**Created:** 2026-04-14  
**Priority:** Medium  
**Depends on:** Neon open-source stack, pg_trickle Docker images  

---

## 1  Executive Summary

Neon is an open-source serverless PostgreSQL platform that separates compute
from storage. The compute nodes run PostgreSQL; the storage layer (pageserver +
safekeepers) replaces the local data directory. This document details how to
deploy pg_trickle on a **self-hosted** Neon cluster — both for development/POC
via `cargo neon` (neon_local) and for production via Docker Compose or Helm.

**Key finding:** Self-hosted Neon gives full control over
`shared_preload_libraries` and `postgresql.conf`, so pg_trickle — including
background workers, shared memory, and automatic scheduling — works fully. The
compute-storage separation introduces some operational considerations but no
fundamental incompatibilities.

---

## 2  Neon Architecture (Relevant to pg_trickle)

### 2.1  Components

| Component | Role | Deployment |
|---|---|---|
| **Pageserver** | Serves pages on demand from object storage. Replaces local disk for PostgreSQL data. | StatefulSet or standalone process |
| **Safekeeper** (×3) | Accepts WAL from compute, stores it durably until pageserver processes it. | StatefulSet (quorum of 3) |
| **Storage Broker** | Fanout pub/sub for timeline updates between pageserver and safekeepers. | Deployment |
| **Storage Controller** | Manages tenant placement across pageservers. Optional for single-node setups. | Deployment |
| **Compute Node** | Stateless PostgreSQL instance managed by `compute_ctl`. Launched on demand. | Pod / process |
| **Proxy** (optional) | Connection routing, pooling, SNI-based routing. | Deployment |

### 2.2  Compute Node Lifecycle

`compute_ctl` is the entrypoint for every compute container. On each start it:

1. Receives a **compute spec** as JSON — contains roles, databases, extensions,
   and (crucially) `postgresql.conf` settings including
   `shared_preload_libraries`.
2. Reinitializes `PGDATA` from a pageserver base-backup.
3. Writes `postgresql.conf` from the spec.
4. Starts `postgres`.
5. Applies role/database/extension changes.
6. Waits for `postmaster` to exit.

**Implication:** pg_trickle's `.so` must be present in the compute image, and
`shared_preload_libraries` must include `pg_trickle` in the compute spec.

### 2.3  Stateless Compute Considerations

| Concern | Impact on pg_trickle | Mitigation |
|---|---|---|
| Fresh `PGDATA` on every start | Background worker launcher re-discovers databases from catalog on boot. Shmem structures are re-initialized. | pg_trickle already handles this via `RECONCILE_EPOCH` and catalog-based recovery. |
| Compute can be suspended (scale-to-zero) | Scheduler stops; change buffers accumulate. | On wake, the launcher resumes scheduling. First refresh may be slower due to accumulated changes. |
| Compute can be migrated to another node | SHM state lost, workers restarted. | Same as restart — catalog-driven recovery. No data loss. |
| Branching creates a copy-on-write fork | Stream tables + change buffers are forked in a consistent state. CDC triggers fire independently on each branch. | Works correctly. Each branch maintains independent stream tables. |
| Read replicas are read-only | Cannot run refreshes or execute DML. | Stream tables are queryable on replicas. Use `pooler_compatibility_mode` if connecting via a pooler. |

---

## 3  Deployment Paths

### 3.1  Local Development with `cargo neon` (neon_local)

The fastest way to test pg_trickle on Neon locally. Requires building both
Neon and pg_trickle from source.

#### Prerequisites

- Neon source built (`make -j$(nproc) -s` in the neon repo)
- pg_trickle compiled with `cargo pgrx package --pg-config <neon-pg_config>`
  targeting Neon's patched PostgreSQL

#### Steps

```bash
# 1. Build Neon (uses its own patched PostgreSQL fork)
git clone --recursive https://github.com/neondatabase/neon.git
cd neon
make -j$(nproc) -s

# 2. Build pg_trickle against Neon's PostgreSQL (requires matching PG version)
#    Neon's pg_config is at: pg_install/v17/bin/pg_config (or v18 if available)
cd /path/to/pg-trickle
cargo pgrx package --pg-config /path/to/neon/pg_install/v17/bin/pg_config

# 3. Copy extension files into Neon's PostgreSQL installation
cp target/release/pg_trickle-pg17/usr/share/postgresql/17/extension/pg_trickle* \
   /path/to/neon/pg_install/v17/share/extension/
cp target/release/pg_trickle-pg17/usr/lib/postgresql/17/lib/pg_trickle* \
   /path/to/neon/pg_install/v17/lib/

# 4. Initialize and start Neon
cd /path/to/neon
cargo neon init
cargo neon start
cargo neon tenant create --set-default
cargo neon endpoint create main
cargo neon endpoint start main

# 5. Enable pg_trickle (need to restart with shared_preload_libraries)
#    Edit .neon/endpoints/main/postgresql.conf:
#      shared_preload_libraries = 'neon,pg_trickle'
#    Then restart:
cargo neon endpoint stop main
cargo neon endpoint start main

# 6. Connect and create the extension
psql -p 55432 -h 127.0.0.1 -U cloud_admin postgres
# => CREATE EXTENSION pg_trickle;
```

**Limitation:** Neon currently ships PostgreSQL 14–17 in its forks. pg_trickle
targets PostgreSQL 18. Until Neon's `vendor/postgres-v18` is available, this
path requires either (a) downporting pg_trickle to PG 17, or (b) waiting for
Neon to add PG 18 support.

### 3.2  Docker Compose (Integration Testing)

Build a custom compute image that includes pg_trickle, then run it with
Neon's docker-compose setup.

#### Custom Compute Dockerfile

```dockerfile
# Dockerfile.neon-compute-pgtrickle
# Extends Neon's compute image with pg_trickle pre-installed.

# ── Stage 1: Build pg_trickle ────────────────────────────────────────────────
FROM neondatabase/compute-node-v17:latest AS builder

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl build-essential pkg-config libssl-dev libclang-dev clang \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

# Must match pg_trickle's Cargo.toml pgrx version
RUN cargo install --locked cargo-pgrx --version 0.17.0
RUN cargo pgrx init --pg17 /usr/local/pgsql/bin/pg_config

WORKDIR /build
COPY . .
RUN cargo pgrx package --pg-config /usr/local/pgsql/bin/pg_config

# ── Stage 2: Final compute image ─────────────────────────────────────────────
FROM neondatabase/compute-node-v17:latest

USER root

# Copy extension artifacts
COPY --from=builder \
    /build/target/release/pg_trickle-pg17/usr/share/postgresql/17/extension/ \
    /usr/local/pgsql/share/extension/

COPY --from=builder \
    /build/target/release/pg_trickle-pg17/usr/lib/postgresql/17/lib/ \
    /usr/local/pgsql/lib/

USER postgres
ENTRYPOINT ["/usr/local/bin/compute_ctl"]
```

#### Docker Compose Overlay

Clone Neon's `docker-compose/` directory and replace the compute image:

```yaml
# docker-compose.override.yml
services:
  compute:
    image: neon-compute-pgtrickle:latest
    build:
      context: /path/to/pg-trickle
      dockerfile: Dockerfile.neon-compute-pgtrickle
```

The compute spec JSON must include pg_trickle in `shared_preload_libraries`.
In the `docker-compose` setup this is controlled by the `compute_wrapper`
scripts that generate the spec. Modify the wrapper to inject:

```json
{
  "postgresql_conf": [
    {
      "name": "shared_preload_libraries",
      "value": "neon,pg_trickle"
    }
  ]
}
```

### 3.3  Kubernetes with Helm Charts

Neon's official helm charts cover **storage-tier** services only:

| Chart | What it deploys |
|---|---|
| `neon-storage-broker` | Pub/sub broker for safekeeper↔pageserver communication |
| `neon-storage-controller` | Tenant placement manager, manages pageserver fleet |
| `neon-proxy` | Connection proxy with SNI routing |
| `neon-pg-sni-router` | SNI-based PostgreSQL routing |
| `neon-endpoint-storage` | Endpoint storage service |
| `neon-storage-scrubber` | Storage garbage collection |

**There is no Helm chart for compute nodes or pageservers.** The README
explicitly states: *"the charts do not give you a fully working Neon-like
system: they are just for individual services."* Compute nodes and pageservers
must be deployed separately.

#### Recommended Kubernetes Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Kubernetes Cluster                   │
│                                                         │
│  ┌─────────────┐  ┌──────────────────┐  ┌───────────┐ │
│  │  Pageserver  │  │  Safekeeper (×3) │  │  Storage   │ │
│  │  StatefulSet │  │  StatefulSet     │  │  Broker    │ │
│  │  (1+ pods)   │  │                  │  │  (Helm)    │ │
│  └──────────────┘  └──────────────────┘  └───────────┘ │
│                                                         │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Compute Node (pg_trickle-enabled)               │   │
│  │  ┌──────────────────────────────────────────┐    │   │
│  │  │ compute_ctl → postgres                   │    │   │
│  │  │   shared_preload_libraries = 'neon,      │    │   │
│  │  │                             pg_trickle'  │    │   │
│  │  │   CREATE EXTENSION pg_trickle;           │    │   │
│  │  └──────────────────────────────────────────┘    │   │
│  │  Image: neon-compute + pg_trickle artifacts      │   │
│  └──────────────────────────────────────────────────┘   │
│                                                         │
│  ┌──────────────┐        ┌───────────────┐              │
│  │  Storage     │        │  Neon Proxy   │              │
│  │  Controller  │        │  (Helm)       │              │
│  │  (Helm)      │        │               │              │
│  └──────────────┘        └───────────────┘              │
└─────────────────────────────────────────────────────────┘
```

#### Deployment Steps

```bash
# 1. Add Neon Helm repo
helm repo add neondatabase https://neondatabase.github.io/helm-charts

# 2. Deploy storage tier
helm install neon-storage-broker neondatabase/neon-storage-broker \
  --set settings.sentryEnvironment=development

helm install neon-storage-controller neondatabase/neon-storage-controller \
  --set image.tag=latest \
  --set settings.databaseUrl=<controller-db-url>

# 3. Deploy pageserver and safekeepers manually (no Helm chart)
#    Use Neon's container images directly as StatefulSets.
#    See: https://github.com/neondatabase/neon/blob/main/docker-compose/docker-compose.yml

# 4. Build and deploy custom compute image with pg_trickle
#    (See Dockerfile in section 3.2)

# 5. Create compute endpoint via storage controller API
#    The compute spec must include shared_preload_libraries = 'neon,pg_trickle'
```

---

## 4  PostgreSQL Version Compatibility

| Neon PG version | Status | Notes |
|---|---|---|
| v14 | ❌ | pg_trickle requires PG 18+ |
| v15 | ❌ | pg_trickle requires PG 18+ |
| v16 | ❌ | pg_trickle requires PG 18+ |
| v17 | ❌ | pg_trickle requires PG 18+ |
| v18 | ⏳ Pending | Neon has not yet added PG 18 to its fork. Once `vendor/postgres-v18` exists, pg_trickle can be built against it. |

**This is the primary blocker.** Neon's patched PostgreSQL fork
(`neondatabase/postgres`) currently covers v14–v17. PostgreSQL 18 support will
need to land in their fork before pg_trickle can run on self-hosted Neon.

### Workarounds

1. **Wait for Neon PG 18 support** — Neon typically adds new major versions
   within months of PostgreSQL release. PG 18 is expected to be GA in late
   2025 / early 2026, so Neon support should be imminent.

2. **Build against vanilla PG 18 with Neon patches** — Neon's
   `compute-node.Dockerfile` shows how they build their patched PG.
   Theoretically, one could build pg_trickle against vanilla PG 18 and mount it
   into a Neon compute container, but the Neon-specific ABI changes (custom
   extensions, WAL format) would likely cause crashes. Not recommended.

3. **pgrx `unsafe-postgres` feature** — Neon's own compute Dockerfile
   extensively uses `pgrx`'s `unsafe-postgres` feature when building pgrx-based
   extensions against their fork. pg_trickle may need the same treatment if
   Neon's PG 18 fork changes the ABI name. Add to `Cargo.toml`:
   ```toml
   pgrx = { version = "0.17.0", features = ["unsafe-postgres"] }
   ```

---

## 5  Feature Compatibility Matrix

| pg_trickle Feature | Self-hosted Neon | Notes |
|---|---|---|
| `CREATE/ALTER/DROP STREAM TABLE` | ✅ | Standard SQL functions |
| Trigger-based CDC | ✅ | Standard AFTER triggers with transition tables |
| Differential refresh | ✅ | Pure SQL rewriting |
| Full refresh | ✅ | Standard REFRESH MATERIALIZED VIEW equivalent |
| Background workers | ✅ | Requires `shared_preload_libraries` — configurable on self-hosted |
| Shared memory (shmem) | ✅ | Allocated on compute start |
| Automatic scheduling | ✅ | Workers start on compute boot |
| DDL event triggers | ✅ | Standard PostgreSQL event triggers |
| WAL-based CDC | ⚠️ | Neon uses its own WAL pathway. `wal_level=logical` may behave differently. Default to `pg_trickle.cdc_mode = 'trigger'`. |
| `pooler_compatibility_mode` | ✅ | Neon proxy + PgBouncer are supported |
| Monitoring (Prometheus) | ✅ | pg_trickle metrics compatible with Neon's sql_exporter |
| DAG-based dependency tracking | ✅ | Catalog tables survive pageserver storage |
| Change buffer tables | ✅ | Standard heap tables, stored by pageserver |
| `pg_cron` integration | ✅ | Available in Neon's compute image by default |

---

## 6  Neon-Specific Operational Considerations

### 6.1  WAL and CDC

Neon's WAL goes to safekeepers, not local disk. The trigger-based CDC mode
writes change buffers as regular DML, which flows through the standard WAL path
to safekeepers → pageserver. This is fully compatible.

WAL-based CDC (logical replication slots) is more complex on Neon because:
- Neon manages WAL retention differently (safekeeper-based)
- Replication slots interact with Neon's WAL GC
- `pg_logical_slot_get_changes()` works but slot lifecycle must account for
  compute restarts

**Recommendation:** Use `pg_trickle.cdc_mode = 'trigger'` on Neon until
WAL-based CDC is explicitly tested.

### 6.2  Scale-to-Zero Recovery

When a Neon compute suspends and later wakes:

1. `compute_ctl` reinitializes `PGDATA` from pageserver
2. PostgreSQL starts with `shared_preload_libraries = 'neon,pg_trickle'`
3. pg_trickle's `_PG_init()` runs, detects `shared_preload_libraries` context
4. Background workers register: launcher → per-database scheduler → workers
5. Launcher discovers databases from `pg_database`, spawns schedulers
6. Schedulers read `pgtrickle.pgt_stream_tables` catalog, resume scheduling
7. First refresh picks up all accumulated changes in `pgtrickle_changes.*`

**Expected impact:** A latency spike on the first refresh after wake-up,
proportional to the volume of changes that accumulated while suspended. Steady
state resumes within one scheduler cycle.

### 6.3  Branching

When a Neon branch is created from a parent:

- All `pgtrickle.*` catalog tables are copied (snapshot consistent)
- All `pgtrickle_changes.changes_*` buffer tables are copied
- CDC triggers are present on the branch (they're part of the catalog)
- The branch's stream tables are immediately consistent

Changes made on the branch will be captured by CDC triggers independently.
Refreshing stream tables on the branch works as expected. There is no
cross-branch interference.

### 6.4  Read Replicas

Neon read replicas are read-only PostgreSQL connections served from the
pageserver at a specific LSN. pg_trickle stream tables (which are materialized
views under the hood) are readable on replicas. However:

- `pgtrickle.refresh_stream_table()` will fail (read-only transaction)
- Background workers do not run on replicas
- Replica data may lag the primary by the replication delay

---

## 7  Comparison: Neon Cloud vs. Self-Hosted Neon

| Capability | Neon Cloud (managed) | Self-Hosted Neon |
|---|---|---|
| Install pg_trickle `.so` | ❌ Scale plan + custom ext request | ✅ Build into compute image |
| `shared_preload_libraries` | ❌ Curated allowlist only | ✅ Full control |
| Background workers | ❌ Blocked | ✅ Works |
| Shared memory | ❌ Blocked | ✅ Works |
| Automatic scheduling | ❌ Manual refresh only (use `pg_cron`) | ✅ Built-in scheduler |
| WAL-based CDC | ⚠️ Logical replication available, slot-limited | ⚠️ Same Neon WAL path, use trigger CDC |
| DDL event triggers | ⚠️ May be restricted | ✅ Full support |
| Scale to zero | ✅ Native | Requires external orchestration |
| Branching | ✅ Native | ✅ Via `cargo neon` or storage controller API |

---

## 8  Comparison: Self-Hosted Neon vs. CloudNativePG

pg_trickle already has CNPG support (see `PLAN_CLOUDNATIVEPG.md`). Here's how
self-hosted Neon compares:

| Aspect | CloudNativePG | Self-hosted Neon |
|---|---|---|
| **Extension delivery** | Image Volume Extensions (scratch image) | Build into compute container |
| **Storage** | Local PVCs | Disaggregated (pageserver + object store) |
| **Branching** | Not supported | Copy-on-write branches |
| **Scale to zero** | Not native (manual annotations) | Native (compute suspend/resume) |
| **HA** | Primary + replicas with automated failover | Safekeepers provide WAL durability; compute is stateless |
| **Complexity** | Lower — standard K8s Operator | Higher — multiple storage services to manage |
| **Maturity** | Production-ready (GA) | Beta / experimental for self-hosted |

**Recommendation:** Use CloudNativePG for production deployments unless you
specifically need Neon's branching or scale-to-zero capabilities.

---

## 9  Testing Plan

### 9.1  Smoke Test (manual)

```bash
# 1. Start self-hosted Neon (cargo neon or docker-compose)
# 2. Enable pg_trickle
CREATE EXTENSION pg_trickle;

# 3. Create a source table and stream table
CREATE TABLE orders (id serial PRIMARY KEY, amount numeric, ts timestamptz DEFAULT now());
CREATE STREAM TABLE order_totals AS SELECT sum(amount) AS total FROM orders;

# 4. Insert data and observe CDC + refresh
INSERT INTO orders (amount) SELECT random() * 100 FROM generate_series(1, 1000);
SELECT pgtrickle.refresh_stream_table('order_totals');
SELECT * FROM order_totals;

# 5. Test branch isolation
--   Create a new branch, verify stream tables are consistent
--   Insert data on the branch, refresh, verify independence
```

### 9.2  Automated Tests (future)

If Neon support becomes a priority, add to the E2E test suite:

- `tests/e2e_neon_tests.rs` — requires a Neon docker-compose fixture
- Test compute restart recovery (stop/start endpoint, verify scheduler resumes)
- Test branch isolation (create branch, verify independent CDC)
- Test accumulated changes after pause (stop compute, insert via another
  connection?, resume, verify catch-up refresh)

---

## 10  Action Items

| # | Task | Priority | Depends on |
|---|---|---|---|
| 1 | Monitor Neon PG 18 support (`vendor/postgres-v18` in neon repo) | High | Neon team |
| 2 | Test pg_trickle build against Neon's PG fork when v18 lands | High | Item 1 |
| 3 | Create `Dockerfile.neon-compute` for custom compute image | Medium | Item 2 |
| 4 | Test trigger-based CDC end-to-end on self-hosted Neon | Medium | Item 3 |
| 5 | Test compute restart recovery (scheduler resumes correctly) | Medium | Item 3 |
| 6 | Test WAL-based CDC on Neon (replication slots via safekeepers) | Low | Item 4 |
| 7 | Test branch isolation for stream tables | Low | Item 3 |
| 8 | Document deployment guide for self-hosted Neon users | Low | Items 3–7 |
| 9 | Investigate Neon Cloud custom extension onboarding (Scale plan) | Low | Business decision |

---

## 11  References

- Neon GitHub: https://github.com/neondatabase/neon
- Neon Helm Charts: https://github.com/neondatabase/helm-charts
- Neon `compute-node.Dockerfile`:
  `https://github.com/neondatabase/neon/blob/main/compute/compute-node.Dockerfile`
- Neon `compute_ctl` README:
  `https://github.com/neondatabase/neon/tree/main/compute_tools`
- Neon `cargo neon` (neon_local):
  `https://github.com/neondatabase/neon/tree/main/control_plane`
- Neon extension support: https://neon.com/docs/extensions/pg-extensions
- pg_trickle CNPG plan: `plans/ecosystem/PLAN_CLOUDNATIVEPG.md`
- pg_trickle CNPG Dockerfile: `cnpg/Dockerfile.ext`
- pg_trickle GHCR Dockerfile: `Dockerfile.ghcr`
