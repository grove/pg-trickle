# Outbox Relay CLI — Implementation Plan

> **Status:** Implementation Plan (DRAFT)
> **Created:** 2026-04-18
> **Category:** Tooling — Outbox Relay
> **Related:** [PLAN_TRANSACTIONAL_OUTBOX_HELPER.md](../patterns/PLAN_TRANSACTIONAL_OUTBOX_HELPER.md) ·
> [ROADMAP v0.23.0](../../ROADMAP.md#v0230--transactional-inbox--outbox-patterns)

---

## Table of Contents

- [Executive Summary](#executive-summary)
- [Goals & Non-Goals](#goals--non-goals)
- [Architecture Overview](#architecture-overview)
- [Part A — Core Framework](#part-a--core-framework)
  - [A.1 Crate Structure](#a1-crate-structure)
  - [A.2 CLI Interface](#a2-cli-interface)
  - [A.3 Configuration](#a3-configuration)
  - [A.4 Outbox Poller](#a4-outbox-poller)
  - [A.5 Payload Handling (Inline + Claim-Check)](#a5-payload-handling-inline--claim-check)
  - [A.6 Consumer Group Integration](#a6-consumer-group-integration)
  - [A.7 Graceful Shutdown & Signal Handling](#a7-graceful-shutdown--signal-handling)
  - [A.8 Observability](#a8-observability)
  - [A.9 Error Handling & Retries](#a9-error-handling--retries)
- [Part B — Sink Backends](#part-b--sink-backends)
  - [B.1 Sink Trait](#b1-sink-trait)
  - [B.2 NATS JetStream](#b2-nats-jetstream)
  - [B.3 HTTP Webhook](#b3-http-webhook)
  - [B.4 Apache Kafka](#b4-apache-kafka)
  - [B.5 stdout / File](#b5-stdout--file)
  - [B.6 Redis Streams](#b6-redis-streams)
  - [B.7 Amazon SQS](#b7-amazon-sqs)
  - [B.8 PostgreSQL (Inbox)](#b8-postgresql-inbox)
  - [B.9 RabbitMQ (AMQP 0-9-1)](#b9-rabbitmq-amqp-0-9-1)
- [Part C — Testing Strategy](#part-c--testing-strategy)
- [Part D — Documentation & Distribution](#part-d--documentation--distribution)
- [Part E — Implementation Roadmap](#part-e--implementation-roadmap)
- [Open Questions](#open-questions)

---

## Executive Summary

`pgtrickle-relay` is a standalone Rust CLI binary that polls pg-trickle
outbox tables and forwards deltas to popular messaging systems and
destinations. It ships as a separate crate in the pg-trickle workspace
(`pgtrickle-relay/`), alongside the existing `pgtrickle-tui` crate.

The relay handles both inline and claim-check payloads, integrates with
pg-trickle consumer groups for multi-instance coordination, and supports
graceful shutdown with at-least-once delivery guarantees. Sink backends are
implemented behind a trait so adding new destinations is straightforward.

**Primary use-case:** Operators enable an outbox on a stream table, point
`pgtrickle-relay` at it, and deltas flow to Kafka / NATS / webhooks / etc.
without writing any relay code.

---

## Goals & Non-Goals

### Goals

- **Zero custom code required** to relay outbox events to a supported sink.
- **Correct handling of all payload modes:** inline, claim-check, and
  full-refresh fallback — including the combined `full_refresh` +
  `claim_check` case.
- **At-least-once delivery** via consumer groups + broker-side dedup keys.
- **Multi-instance safe** — multiple relay instances share a consumer group.
- **Observable** — structured logging, Prometheus metrics endpoint, health
  check endpoint.
- **Configurable via file, env vars, and CLI flags** — 12-factor friendly.
- **Small binary, fast startup** — suitable for sidecar containers.

### Non-Goals

- The relay is **not a message broker**. It is a poll-and-forward bridge.
- **Exactly-once delivery is not guaranteed by the relay alone**. Exactly-once
  is achieved by composition (relay dedup keys + broker dedup + inbox
  `ON CONFLICT DO NOTHING`), per the pg-trickle outbox design.
- **Complex routing / transformation / filtering** beyond subject/topic
  mapping is not in scope. Users needing ETL should compose the relay with
  downstream stream processors.
- **No embedded PostgreSQL client library** — uses `tokio-postgres` (same as
  `pgtrickle-tui`).

---

## Architecture Overview

```
                 ┌─────────────────────────────┐
                 │        PostgreSQL            │
                 │                              │
                 │  pgtrickle.outbox_<st>       │
                 │  pgtrickle.outbox_delta_     │
                 │    rows_<st>                 │
                 │  pgtrickle.poll_outbox()     │
                 │  pgtrickle.commit_offset()   │
                 └──────────┬──────────────────┘
                            │ tokio-postgres
                            ▼
                 ┌──────────────────────┐
                 │   pgtrickle-relay    │
                 │                      │
                 │  ┌────────────────┐  │
                 │  │  Outbox Poller │  │
                 │  └──────┬─────────┘  │
                 │         │            │
                 │  ┌──────▼─────────┐  │
                 │  │ Payload Decoder│  │
                 │  │ (inline/claim) │  │
                 │  └──────┬─────────┘  │
                 │         │            │
                 │  ┌──────▼─────────┐  │
                 │  │  Sink Backend  │  │
                 │  │  (trait impl)  │  │
                 │  └──────┬─────────┘  │
                 │         │            │
                 └─────────┼────────────┘
                           │
            ┌──────────────┼──────────────────┐
            ▼              ▼                  ▼
      ┌──────────┐  ┌───────────┐   ┌──────────────┐
      │   NATS   │  │   Kafka   │   │ HTTP Webhook  │
      │ JetStream│  │           │   │               │
      └──────────┘  └───────────┘   └──────────────┘
            ▼              ▼                  ▼
      ┌──────────┐  ┌───────────┐   ┌──────────────┐
      │  Redis   │  │    SQS    │   │   stdout /   │
      │ Streams  │  │           │   │   file       │
      └──────────┘  └───────────┘   └──────────────┘
            ▼              ▼
      ┌──────────┐  ┌───────────┐
      │ RabbitMQ │  │ PostgreSQL│
      │  (AMQP)  │  │  (Inbox)  │
      └──────────┘  └───────────┘
```

---

## Part A — Core Framework

### A.1 Crate Structure

A new workspace member at `pgtrickle-relay/`:

```
pgtrickle-relay/
├── Cargo.toml
├── src/
│   ├── main.rs           # Entry point, CLI parsing, signal handling
│   ├── cli.rs            # clap derive definitions
│   ├── config.rs         # TOML + env + CLI merging
│   ├── error.rs          # RelayError enum
│   ├── poller.rs         # Outbox polling loop (simple + consumer group)
│   ├── payload.rs        # Inline / claim-check / full-refresh decoder
│   ├── metrics.rs        # Prometheus metrics + health endpoint
│   ├── sink/
│   │   ├── mod.rs        # Sink trait definition
│   │   ├── nats.rs       # NATS JetStream backend
│   │   ├── webhook.rs    # HTTP webhook backend
│   │   ├── kafka.rs      # Apache Kafka backend
│   │   ├── stdout.rs     # stdout / file backend
│   │   ├── redis.rs      # Redis Streams backend
│   │   ├── sqs.rs        # Amazon SQS backend
│   │   ├── pg_inbox.rs   # PostgreSQL inbox backend
│   │   └── rabbitmq.rs   # RabbitMQ AMQP backend
│   └── transforms.rs     # Subject/topic templating, key extraction
└── tests/
    ├── common/
    │   └── mod.rs         # Test helpers, Testcontainers setup
    ├── poller_tests.rs    # Poller integration tests
    ├── payload_tests.rs   # Payload decode unit tests
    ├── nats_tests.rs      # NATS E2E tests
    ├── webhook_tests.rs   # Webhook E2E tests
    └── kafka_tests.rs     # Kafka E2E tests
```

#### Cargo.toml — Feature Flags

Each sink backend is gated behind a Cargo feature so users compile only
what they need. A default feature set covers the most common sinks.

```toml
[package]
name = "pgtrickle-relay"
version = "0.23.0"
edition = "2024"

[[bin]]
name = "pgtrickle-relay"
path = "src/main.rs"

[features]
default = ["nats", "webhook", "kafka", "stdout"]
nats     = ["dep:async-nats"]
webhook  = ["dep:reqwest"]
kafka    = ["dep:rdkafka"]
stdout   = []                       # no extra deps
redis    = ["dep:redis"]
sqs      = ["dep:aws-sdk-sqs"]
pg-inbox = []                       # uses tokio-postgres (already a dep)
rabbitmq = ["dep:lapin"]

[dependencies]
# Core
clap = { version = "4", features = ["derive", "env"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal", "time", "sync"] }
tokio-postgres = "0.7"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "1.1"
thiserror = "2"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
chrono = { version = "0.4", features = ["serde"] }
uuid = { version = "1", features = ["v4", "serde"] }

# Metrics / health
axum = "0.8"
prometheus = "0.14"

# Sinks (optional)
async-nats = { version = "0.40", optional = true }
reqwest = { version = "0.12", features = ["json"], optional = true }
rdkafka = { version = "0.37", features = ["cmake-build"], optional = true }
redis = { version = "0.28", features = ["tokio-comp", "streams"], optional = true }
aws-sdk-sqs = { version = "1", optional = true }
lapin = { version = "2", optional = true }
```

### A.2 CLI Interface

```
pgtrickle-relay [OPTIONS] --sink <SINK>

OPTIONS:
  -c, --config <FILE>           Path to TOML config file
      --pg-url <URL>            PostgreSQL connection string [env: PG_URL]
      --outbox <NAME>           Stream table name to relay [env: RELAY_OUTBOX]
      --sink <SINK>             Sink backend: nats, webhook, kafka, stdout,
                                redis, sqs, pg-inbox, rabbitmq [env: RELAY_SINK]
      --group <NAME>            Consumer group name (enables group mode)
                                [env: RELAY_GROUP]
      --consumer-id <ID>        Consumer ID within group (default: hostname)
                                [env: RELAY_CONSUMER_ID]
      --batch-size <N>          Rows per poll (default: 100) [env: RELAY_BATCH_SIZE]
      --poll-interval <MS>      Milliseconds between empty polls (default: 1000)
                                [env: RELAY_POLL_INTERVAL_MS]
      --visibility-seconds <N>  Lease visibility timeout (default: 30)
                                [env: RELAY_VISIBILITY_SECONDS]
      --metrics-addr <ADDR>     Prometheus metrics + health endpoint
                                (default: 0.0.0.0:9090) [env: RELAY_METRICS_ADDR]
      --log-format <FMT>        Log format: text, json (default: text)
                                [env: RELAY_LOG_FORMAT]
      --log-level <LEVEL>       Log level (default: info) [env: RELAY_LOG_LEVEL]
  -V, --version                 Print version
  -h, --help                    Print help

SINK-SPECIFIC OPTIONS (passed via --sink-opt KEY=VALUE or config file):
  See documentation for each sink backend.
```

#### Subcommands

```
pgtrickle-relay relay [OPTIONS]     # Main relay loop (default if no subcommand)
pgtrickle-relay validate [OPTIONS]  # Validate config, test connections, exit
pgtrickle-relay schema [OPTIONS]    # Print JSON schema of the outbox payload
pgtrickle-relay completion <SHELL>  # Generate shell completions
```

### A.3 Configuration

Configuration is resolved in priority order (highest wins):

1. CLI flags
2. Environment variables
3. TOML config file
4. Built-in defaults

#### TOML Config File

```toml
[postgres]
url = "postgres://user:password@localhost/mydb"

[relay]
outbox = "order_events"
sink = "nats"
group = "order-publisher"
consumer_id = "relay-1"       # default: hostname
batch_size = 100
poll_interval_ms = 1000
visibility_seconds = 30

[metrics]
addr = "0.0.0.0:9090"
enabled = true

[logging]
format = "json"     # text | json
level = "info"

# Subject/topic template — available variables:
#   {stream_table}, {event_type}, {outbox_id}, {refresh_id}
[routing]
subject_template = "pgtrickle.{stream_table}"
# Optional per-event-type override:
# [routing.overrides]
# "order.created" = "orders.created"
# "order.shipped" = "orders.shipped"

# Sink-specific configuration
[sink.nats]
url = "nats://localhost:4222"
# See B.2 for full options

[sink.webhook]
url = "https://api.example.com/webhook"
# See B.3 for full options

[sink.kafka]
brokers = "localhost:9092"
topic = "pgtrickle-events"
# See B.4 for full options
```

### A.4 Outbox Poller

The poller is the core loop. It operates in two modes:

#### Simple Mode (no consumer group)

Used when `--group` is not specified. Tracks offset locally in memory
(lost on restart — consumers re-read from latest or a configured position).

```rust
async fn poll_simple(
    db: &Client,
    outbox_name: &str,
    batch_size: i64,
    sink: &dyn Sink,
) -> Result<(), RelayError> {
    let mut last_offset: i64 = 0; // or query MAX(id) on startup

    loop {
        let rows = db.query(
            &format!(
                "SELECT id, payload FROM pgtrickle.{} WHERE id > $1 ORDER BY id LIMIT $2",
                outbox_name  // validated at startup against catalog
            ),
            &[&last_offset, &batch_size],
        ).await?;

        if rows.is_empty() {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        for row in &rows {
            let id: i64 = row.get("id");
            let payload: serde_json::Value = row.get("payload");
            let events = decode_payload(&payload, db, outbox_name, id).await?;
            sink.publish(events).await?;
            last_offset = id;
        }
    }
}
```

#### Consumer Group Mode

Used when `--group` is specified. Delegates coordination to pg-trickle's
built-in consumer group SQL functions.

```rust
async fn poll_group(
    db: &Client,
    group: &str,
    consumer_id: &str,
    batch_size: i32,
    visibility_seconds: i32,
    sink: &dyn Sink,
) -> Result<(), RelayError> {
    loop {
        let rows = db.query(
            "SELECT * FROM pgtrickle.poll_outbox($1, $2, $3, $4)",
            &[&group, &consumer_id, &batch_size, &visibility_seconds],
        ).await?;

        if rows.is_empty() {
            // Heartbeat even when idle
            db.execute(
                "SELECT pgtrickle.consumer_heartbeat($1, $2)",
                &[&group, &consumer_id],
            ).await?;
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        let mut last_id: i64 = 0;
        for row in &rows {
            let id: i64 = row.get("id");
            let payload: serde_json::Value = row.get("payload");
            let events = decode_payload(&payload, db, &outbox_name, id).await?;
            sink.publish(events).await?;
            last_id = id;
        }

        // Commit offset + heartbeat
        db.execute(
            "SELECT pgtrickle.commit_offset($1, $2, $3)",
            &[&group, &consumer_id, &last_id],
        ).await?;
    }
}
```

#### Lease Renewal for Slow Sinks

If a batch takes longer to publish than `visibility_seconds`, the poller
spawns a background task that periodically calls `extend_lease()`:

```rust
let lease_guard = spawn_lease_renewer(db, group, consumer_id, visibility_seconds);
// ... process batch ...
lease_guard.cancel(); // stop renewing after commit
```

The renewer calls `extend_lease()` at `visibility_seconds / 2` intervals.

### A.5 Payload Handling (Inline + Claim-Check)

The payload decoder handles all four modes:

| `claim_check` | `full_refresh` | Path |
|----------------|----------------|------|
| `false`        | `false`        | Inline differential — rows in `payload.inserted` / `payload.deleted` |
| `false`        | `true`         | Inline full refresh — all current rows in `payload.inserted`, apply upsert semantics |
| `true`         | `false`        | Claim-check differential — cursor-fetch from `outbox_delta_rows_<st>` |
| `true`         | `true`         | Claim-check full refresh — cursor-fetch + upsert semantics |

```rust
#[derive(Debug)]
struct OutboxBatch {
    outbox_id: i64,
    refresh_id: Uuid,
    is_full_refresh: bool,
    inserted: Vec<serde_json::Value>,
    deleted: Vec<serde_json::Value>,
}

async fn decode_payload(
    payload: &serde_json::Value,
    db: &Client,
    outbox_name: &str,
    outbox_id: i64,
) -> Result<OutboxBatch, RelayError> {
    let v = payload["v"].as_i64().unwrap_or(0);
    if v != 1 {
        return Err(RelayError::UnsupportedPayloadVersion(v));
    }

    let is_full_refresh = payload.get("full_refresh")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let is_claim_check = payload.get("claim_check")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if is_claim_check {
        // Cursor-fetch from companion table in bounded batches
        let (inserted, deleted) = fetch_claim_check_rows(
            db, outbox_name, outbox_id
        ).await?;

        // Signal consumption complete
        db.execute(
            "SELECT pgtrickle.outbox_rows_consumed($1, $2)",
            &[&outbox_name, &outbox_id],
        ).await?;

        Ok(OutboxBatch {
            outbox_id,
            refresh_id: parse_uuid(payload),
            is_full_refresh,
            inserted,
            deleted,
        })
    } else {
        // Inline — rows are in the payload itself
        Ok(OutboxBatch {
            outbox_id,
            refresh_id: parse_uuid(payload),
            is_full_refresh,
            inserted: extract_array(payload, "inserted"),
            deleted: extract_array(payload, "deleted"),
        })
    }
}

async fn fetch_claim_check_rows(
    db: &Client,
    outbox_name: &str,
    outbox_id: i64,
) -> Result<(Vec<Value>, Vec<Value>), RelayError> {
    // Use a server-side cursor for bounded memory
    let delta_table = format!("pgtrickle.outbox_delta_rows_{}", outbox_name);
    let rows = db.query(
        &format!(
            "SELECT op, payload FROM {} WHERE outbox_id = $1 ORDER BY row_num",
            delta_table  // validated at startup
        ),
        &[&outbox_id],
    ).await?;

    let mut inserted = Vec::new();
    let mut deleted = Vec::new();
    for row in rows {
        let op: &str = row.get("op");
        let payload: serde_json::Value = row.get("payload");
        match op {
            "I" => inserted.push(payload),
            "D" => deleted.push(payload),
            _ => tracing::warn!(op, "unknown delta op"),
        }
    }
    Ok((inserted, deleted))
}
```

### A.6 Consumer Group Integration

When `--group` is specified, the relay:

1. **Startup:** Calls `create_consumer_group()` (idempotent) to ensure the
   group exists.
2. **Poll loop:** Uses `poll_outbox()` + `commit_offset()` as described in A.4.
3. **Heartbeat:** Sends `consumer_heartbeat()` every 10 seconds via a
   background task, independent of the poll loop.
4. **Lease renewal:** Calls `extend_lease()` if batch processing takes
   longer than `visibility_seconds / 2`.
5. **Shutdown:** On SIGTERM/SIGINT, finishes the current batch, commits the
   offset, then exits.

### A.7 Graceful Shutdown & Signal Handling

```rust
let shutdown = tokio::signal::ctrl_c();
let sigterm = async {
    tokio::signal::unix::signal(SignalKind::terminate())
        .expect("register SIGTERM")
        .recv()
        .await;
};

tokio::select! {
    _ = relay_loop => {},
    _ = shutdown => {
        tracing::info!("SIGINT received, finishing current batch...");
    }
    _ = sigterm => {
        tracing::info!("SIGTERM received, finishing current batch...");
    }
}
// Drain in-flight batch, commit offset, close connections
```

The shutdown sequence:

1. Stop polling for new batches.
2. Wait for the in-flight batch to finish publishing (up to a configurable
   `shutdown_timeout_seconds`, default 30).
3. Commit the offset for the completed batch.
4. Close sink and database connections.
5. Exit with code 0.

If the in-flight batch doesn't complete within the timeout, the relay exits
with code 1. The uncommitted batch will be re-polled by another instance
after the visibility timeout expires.

### A.8 Observability

#### Structured Logging

- `tracing` + `tracing-subscriber` with `env-filter`.
- `--log-format json` for structured JSON logs (default in container mode).
- Key span fields: `outbox`, `group`, `consumer_id`, `outbox_id`, `sink`.

#### Prometheus Metrics

Exposed via an HTTP endpoint (default `:9090/metrics`):

| Metric | Type | Description |
|--------|------|-------------|
| `relay_polls_total` | counter | Total poll attempts |
| `relay_rows_published_total` | counter | Rows published to sink (label: `sink`, `outbox`) |
| `relay_batches_committed_total` | counter | Batches committed |
| `relay_batch_publish_duration_seconds` | histogram | Time to publish a batch |
| `relay_poll_duration_seconds` | histogram | Time for a poll round-trip |
| `relay_claim_check_fetches_total` | counter | Claim-check cursor fetches |
| `relay_full_refresh_batches_total` | counter | Full-refresh batches received |
| `relay_errors_total` | counter | Errors (label: `kind`) |
| `relay_last_committed_offset` | gauge | Last committed outbox offset |
| `relay_consumer_lag_rows` | gauge | Rows behind latest (group mode only) |
| `relay_sink_connected` | gauge | 1 if sink connection is healthy |

#### Health Endpoint

`GET :9090/health` returns:

```json
{
  "status": "healthy",
  "postgres": "connected",
  "sink": "connected",
  "last_poll_at": "2026-04-18T12:00:00Z",
  "last_commit_at": "2026-04-18T12:00:00Z",
  "lag_rows": 42
}
```

Returns HTTP 200 if healthy, 503 if degraded (no poll in last 60 s or sink
disconnected). Suitable for Kubernetes liveness/readiness probes.

### A.9 Error Handling & Retries

| Error class | Behaviour |
|-------------|-----------|
| PostgreSQL connection lost | Reconnect with exponential backoff (1 s → 2 s → 4 s → … → 60 s cap). |
| Sink connection lost | Reconnect with exponential backoff. Pause polling until reconnected. |
| Sink publish failure (transient) | Retry the batch up to 3 times with backoff. Do not commit offset. |
| Sink publish failure (permanent, e.g. 400) | Log error, skip the event, emit `relay_errors_total{kind="sink_permanent"}`. Advance offset to avoid poison-pill blocking. |
| Payload decode error | Log error, skip the event. Emit `relay_errors_total{kind="decode"}`. |
| Claim-check fetch failure | Retry up to 3 times. If permanent, log + skip + emit error metric. |

All retries use jittered exponential backoff to avoid thundering herds.

---

## Part B — Sink Backends

### B.1 Sink Trait

```rust
#[async_trait]
pub trait Sink: Send + Sync {
    /// Human-readable sink name for logs and metrics.
    fn name(&self) -> &str;

    /// Establish connection to the sink. Called on startup and reconnect.
    async fn connect(&mut self) -> Result<(), RelayError>;

    /// Publish a batch of events. The sink must handle dedup keys.
    /// Returns the number of events successfully published.
    async fn publish(&self, batch: &OutboxBatch, routing: &Routing) -> Result<usize, RelayError>;

    /// Check if the sink connection is healthy.
    async fn is_healthy(&self) -> bool;

    /// Graceful close.
    async fn close(&mut self) -> Result<(), RelayError>;
}
```

Each event published to a sink carries:

```rust
struct SinkEvent {
    /// Dedup key: "{outbox}:{outbox_id}:{row_index}"
    dedup_key: String,
    /// Resolved subject/topic from the routing template
    subject: String,
    /// The row payload as JSON
    payload: serde_json::Value,
    /// Operation: "insert" or "delete"
    op: &'static str,
    /// Whether this batch is a full-refresh snapshot
    is_full_refresh: bool,
    /// pg-trickle metadata
    outbox_id: i64,
    refresh_id: Uuid,
}
```

### B.2 NATS JetStream

**Crate:** `async-nats`

```toml
[sink.nats]
url = "nats://localhost:4222"
# credentials_file = "/etc/nats/creds"
# tls_ca = "/etc/nats/ca.pem"
subject_template = "pgtrickle.{stream_table}.{op}"
# Publish options:
# ack_timeout_ms = 5000
# max_in_flight = 256          # concurrent unacked publishes
```

**Dedup:** Uses `Nats-Msg-Id` header set to `{outbox}:{outbox_id}:{row_index}`.
JetStream deduplicates within the configured dedup window.

**Full-refresh handling:** Publishes with header `Pgtrickle-Full-Refresh: true`
so consumers can detect snapshot events.

### B.3 HTTP Webhook

**Crate:** `reqwest`

```toml
[sink.webhook]
url = "https://api.example.com/events"
method = "POST"                             # POST (default) or PUT
# headers:
#   Authorization = "Bearer token123"
#   X-Custom = "value"
# timeout_ms = 10000
# batch_mode = true                         # send entire batch as one POST
# retry_on_status = [429, 500, 502, 503, 504]
# tls_ca = "/etc/ssl/custom-ca.pem"
```

**Batch mode (default: true):** Sends the entire `OutboxBatch` as a single
POST body:

```json
{
  "outbox_id": 42,
  "refresh_id": "abc-123",
  "full_refresh": false,
  "inserted": [...],
  "deleted": [...]
}
```

**Per-event mode (`batch_mode = false`):** One HTTP request per event row.
Slower but compatible with endpoints that expect single-event payloads.

**Dedup:** Sends `Idempotency-Key` header with the dedup key.

**Full-refresh handling:** Sends `X-Pgtrickle-Full-Refresh: true` header.

### B.4 Apache Kafka

**Crate:** `rdkafka` (librdkafka wrapper)

```toml
[sink.kafka]
brokers = "localhost:9092"
topic = "pgtrickle-events"
# topic_template = "pgtrickle.{stream_table}"
# key_template = "{op}:{outbox_id}"          # Kafka record key
# acks = "all"                               # all | 1 | 0
# compression = "lz4"                        # none | gzip | snappy | lz4 | zstd
# linger_ms = 5
# batch_num_messages = 1000
# security_protocol = "SASL_SSL"
# sasl_mechanism = "PLAIN"
# sasl_username = "user"
# sasl_password = "password"
# Additional librdkafka config:
# [sink.kafka.rdkafka]
# "message.max.bytes" = "10485760"
```

**Dedup:** Uses the dedup key as the Kafka record key + enables idempotent
producer (`enable.idempotence = true`).

**Full-refresh:** Sets Kafka header `pgtrickle-full-refresh: true`.

### B.5 stdout / File

No external dependencies.

```toml
[sink.stdout]
format = "jsonl"            # jsonl (default) | json_pretty | csv
# output = "/var/log/relay/events.jsonl"   # file path; omit for stdout
# rotate_bytes = 104857600  # 100 MiB log rotation (file mode only)
```

Useful for:
- Debugging and development
- Piping to `jq`, `tee`, log collectors
- Integration testing

### B.6 Redis Streams

**Crate:** `redis` with `streams` feature

```toml
[sink.redis]
url = "redis://localhost:6379"
stream_key = "pgtrickle:{stream_table}"
# maxlen = 100000             # MAXLEN ~ approximate trimming
# password = "secret"
# tls = false
```

**Dedup:** Uses the dedup key as the Redis Stream entry ID field
(`pgt_dedup_key`). Consumers use consumer groups on the Redis side for
exactly-once processing.

### B.7 Amazon SQS

**Crate:** `aws-sdk-sqs`

```toml
[sink.sqs]
queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"
# region = "us-east-1"       # default: from env/config
# message_group_id = "pgtrickle"   # for FIFO queues
# batch_send = true          # use SendMessageBatch (up to 10 per call)
```

**Dedup:** For FIFO queues, uses `MessageDeduplicationId` set to the dedup
key. For standard queues, dedup is the consumer's responsibility.

### B.8 PostgreSQL (Inbox)

Uses `tokio-postgres` (already a dependency).

```toml
[sink.pg-inbox]
url = "postgres://user:password@other-host/other_db"
inbox_table = "my_inbox"
# schema = "public"
# on_conflict = "DO NOTHING"   # default: idempotent via event_id PK
```

Inserts events into a pg-trickle inbox (or any table with compatible
schema) on a different PostgreSQL instance. Enables cross-database /
cross-service event propagation using pg-trickle on both sides.

```sql
INSERT INTO my_inbox (event_id, event_type, payload, received_at)
VALUES ($1, $2, $3, now())
ON CONFLICT (event_id) DO NOTHING;
```

### B.9 RabbitMQ (AMQP 0-9-1)

**Crate:** `lapin`

```toml
[sink.rabbitmq]
url = "amqp://guest:guest@localhost:5672"
exchange = "pgtrickle"
routing_key_template = "{stream_table}.{op}"
# exchange_type = "topic"     # topic | direct | fanout
# declare_exchange = true     # auto-declare on connect
# mandatory = true            # require at least one queue binding
# tls = false
```

**Dedup:** Sets `message-id` AMQP property to the dedup key. Consumer-side
dedup required (RabbitMQ does not deduplicate natively).

---

## Part C — Testing Strategy

### C.1 Unit Tests

- Payload decoder: all 4 modes (inline, claim-check, full-refresh, combined).
- Config merging: CLI > env > TOML > defaults.
- Subject/topic templating with variables.
- Dedup key generation.
- Retry backoff calculation.

### C.2 Integration Tests (Testcontainers)

Each test spins up PostgreSQL + the target sink via Testcontainers:

| Test | Containers | Validates |
|------|-----------|-----------|
| NATS relay E2E | postgres + nats | Poll → publish → JetStream consume; verify dedup |
| Kafka relay E2E | postgres + kafka (redpanda) | Poll → produce → consume; verify partition key |
| Webhook relay E2E | postgres + wiremock | Poll → POST → verify request body + headers |
| Redis relay E2E | postgres + redis | Poll → XADD → XRANGE; verify stream entries |
| PG inbox relay E2E | postgres (source) + postgres (target) | Poll → INSERT → verify inbox rows |
| Consumer group E2E | postgres + nats + 2 relay instances | Verify no duplicates, crash recovery |
| Claim-check E2E | postgres + nats | Large delta → cursor fetch → publish all rows |
| Full-refresh E2E | postgres + nats | AUTO fallback → full-refresh header set |

### C.3 Benchmark

- Throughput: relay 100K inline outbox rows to NATS; measure events/sec.
- Throughput: relay 100K claim-check outbox rows; measure events/sec.
- Latency: p50/p95/p99 poll-to-publish for a single event.
- Memory: verify bounded memory during claim-check fetch (no full delta
  buffering).

---

## Part D — Documentation & Distribution

### D.1 Documentation

| Document | Content |
|----------|---------|
| `pgtrickle-relay/README.md` | Quick start, installation, basic usage |
| `docs/RELAY.md` | Comprehensive guide: all sinks, configuration reference, deployment patterns |
| `docs/SQL_REFERENCE.md` | Updated with relay-related SQL functions |
| `docs/PATTERNS.md` | "Outbox Relay" section with worked examples per sink |

### D.2 Distribution

| Channel | Artifact |
|---------|----------|
| GitHub Releases | Pre-built binaries (Linux amd64/arm64, macOS amd64/arm64) |
| Docker Hub | `grove/pgtrickle-relay:0.23.0` — minimal distroless image |
| Cargo | `cargo install pgtrickle-relay` |
| Homebrew | `brew install grove/tap/pgtrickle-relay` |

#### Docker Image

```dockerfile
FROM rust:1.85-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --release --bin pgtrickle-relay \
    --features default

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /src/target/release/pgtrickle-relay /usr/local/bin/
ENTRYPOINT ["pgtrickle-relay"]
```

#### Kubernetes Sidecar Pattern

```yaml
# Runs alongside the application pod
containers:
  - name: app
    image: myapp:latest
  - name: relay
    image: grove/pgtrickle-relay:0.23.0
    env:
      - name: PG_URL
        valueFrom:
          secretKeyRef:
            name: pg-credentials
            key: url
      - name: RELAY_OUTBOX
        value: order_events
      - name: RELAY_SINK
        value: nats
      - name: RELAY_GROUP
        value: order-publisher
    ports:
      - name: metrics
        containerPort: 9090
    livenessProbe:
      httpGet:
        path: /health
        port: 9090
    readinessProbe:
      httpGet:
        path: /health
        port: 9090
```

---

## Part E — Implementation Roadmap

### Phase 1 — Core + Tier 1 Sinks (10 days)

| Item | Description | Effort |
|------|-------------|--------|
| RELAY-1 | Crate scaffold, CLI parsing, config loading, error types | 1d |
| RELAY-2 | Outbox poller (simple mode + consumer group mode) | 2d |
| RELAY-3 | Payload decoder (inline + claim-check + full-refresh) | 1d |
| RELAY-4 | Sink trait + stdout/file backend | 0.5d |
| RELAY-5 | NATS JetStream backend | 1d |
| RELAY-6 | HTTP webhook backend | 1d |
| RELAY-7 | Apache Kafka backend | 1.5d |
| RELAY-8 | Metrics endpoint + health check | 1d |
| RELAY-9 | Signal handling, graceful shutdown, lease renewal | 1d |

### Phase 2 — Tier 2 Sinks (5 days)

| Item | Description | Effort |
|------|-------------|--------|
| RELAY-10 | Redis Streams backend | 1d |
| RELAY-11 | Amazon SQS backend | 1d |
| RELAY-12 | PostgreSQL inbox backend | 1d |
| RELAY-13 | RabbitMQ AMQP backend | 1d |
| RELAY-14 | Subject/topic routing templates, key extraction | 1d |

### Phase 3 — Testing & Polish (5 days)

| Item | Description | Effort |
|------|-------------|--------|
| RELAY-15 | Unit tests (payload, config, routing, retries) | 1d |
| RELAY-16 | Integration tests (NATS, Kafka, webhook, Redis, PG inbox) | 2d |
| RELAY-17 | Consumer group E2E (multi-instance, crash recovery) | 1d |
| RELAY-18 | Benchmarks (throughput, latency, memory) | 0.5d |
| RELAY-19 | Documentation (README, docs/RELAY.md, PATTERNS.md) | 0.5d |

### Phase 4 — Distribution (2 days)

| Item | Description | Effort |
|------|-------------|--------|
| RELAY-20 | Dockerfile + GitHub Actions CI for binary builds | 1d |
| RELAY-21 | Release automation (GitHub Releases, Docker Hub, Homebrew) | 1d |

> **Total: ~22 days solo / ~14 days with two developers**
> (Phase 1 is serial; Phases 2–4 can be parallelised with the outbox
> implementation in v0.23.0.)

### Dependencies

- **Requires v0.23.0 outbox (Part A)** — the relay polls `pgtrickle.outbox_<st>`
  and reads claim-check rows from `pgtrickle.outbox_delta_rows_<st>`.
- **Consumer group mode requires v0.23.0 Part B** — `poll_outbox()`,
  `commit_offset()`, `consumer_heartbeat()`, `extend_lease()`.
- **Can be developed in parallel** with the outbox implementation: mock the
  outbox table schema in integration tests until the real extension is ready.

---

## Open Questions

| # | Question | Options | Recommendation |
|---|----------|---------|----------------|
| 1 | Should the relay support multiple outboxes in one process? | (a) One outbox per process (simpler, Kubernetes-native scaling), (b) Multi-outbox config (fewer processes) | **(a)** — one outbox per process. Scale via replicas. Simpler operationally. |
| 2 | Should we support custom transforms (e.g. JMESPath, JSONata)? | (a) No transforms in v0.23.0, (b) JMESPath filter | **(a)** — out of scope. The stream table query itself is the transform layer. |
| 3 | Dead-letter queue for the relay? | (a) Skip poison events + log, (b) DLQ table in PostgreSQL | **(a)** for v0.23.0. Log + metric is sufficient. DLQ can be added later. |
| 4 | Should `pgtrickle-relay` be a workspace member or a separate repo? | (a) Workspace member (shared CI, version lock), (b) Separate repo | **(a)** — workspace member alongside `pgtrickle-tui`. Shared version, single release. |
| 5 | Should NATS Micro integration be included for service discovery? | (a) Yes, (b) No | **(b)** for initial release. Can be added if demand exists. |
| 6 | Google Cloud Pub/Sub as a sink? | (a) v0.23.0, (b) Post-v0.23.0 | **(b)** — add post-launch based on demand. |
| 7 | Azure Service Bus as a sink? | (a) v0.23.0, (b) Post-v0.23.0 | **(b)** — add post-launch based on demand. |
