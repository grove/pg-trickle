# Transactional Outbox Pattern with pg_trickle

> **Status:** Research Report  
> **Created:** 2026-04-17  
> **Category:** Integration Pattern

---

## Table of Contents

- [Executive Summary](#executive-summary)
- [What Is the Transactional Outbox Pattern?](#what-is-the-transactional-outbox-pattern)
- [The Problem It Solves](#the-problem-it-solves)
- [How pg_trickle Enables the Transactional Outbox](#how-pg_trickle-enables-the-transactional-outbox)
  - [Architecture Overview](#architecture-overview)
  - [Approach 1: Stream Table as the Outbox View](#approach-1-stream-table-as-the-outbox-view)
  - [Approach 2: CDC Buffer as the Outbox](#approach-2-cdc-buffer-as-the-outbox)
  - [Approach 3: IMMEDIATE Mode for Zero-Lag Outbox](#approach-3-immediate-mode-for-zero-lag-outbox)
- [Message Relay Strategies](#message-relay-strategies)
  - [Strategy A: Polling Publisher via Stream Table](#strategy-a-polling-publisher-via-stream-table)
  - [Strategy B: LISTEN/NOTIFY + Stream Table](#strategy-b-listennotify--stream-table)
  - [Strategy C: Transaction Log Tailing via WAL CDC](#strategy-c-transaction-log-tailing-via-wal-cdc)
- [Worked Example: Order Service Outbox](#worked-example-order-service-outbox)
- [Complementary PostgreSQL Extensions](#complementary-postgresql-extensions)
  - [pgmq — PostgreSQL Message Queue](#pgmq--postgresql-message-queue)
  - [pg_amqp — AMQP Publishing from PostgreSQL](#pg_amqp--amqp-publishing-from-postgresql)
  - [pgflow — Durable Workflow Engine](#pgflow--durable-workflow-engine)
  - [Debezium — External CDC Platform](#debezium--external-cdc-platform)
- [Potential pg_trickle Extensions](#potential-pg_trickle-extensions)
  - [Extension 1: Outbox Table Helper](#extension-1-outbox-table-helper)
  - [Extension 2: Message Relay Background Worker](#extension-2-message-relay-background-worker)
  - [Extension 3: pgmq Integration](#extension-3-pgmq-integration)
  - [Extension 4: Webhook Dispatcher](#extension-4-webhook-dispatcher)
- [Design Considerations](#design-considerations)
- [Comparison with Traditional Approaches](#comparison-with-traditional-approaches)
- [References](#references)

---

## Executive Summary

The **Transactional Outbox Pattern** guarantees that domain events (messages)
are published if and only if the originating database transaction commits. It
decouples services in a microservice architecture while preventing data
inconsistency caused by partial failures between the database and a message
broker.

pg_trickle is a natural fit for implementing this pattern because:

1. **CDC triggers already capture every DML change** atomically within the
   source transaction — no additional outbox trigger is needed.
2. **Stream tables can materialize the "pending events" view** with sub-second
   latency via DIFFERENTIAL refresh.
3. **IMMEDIATE mode** can provide zero-lag outbox materialization within the
   same transaction.
4. **LISTEN/NOTIFY integration** (`pg_trickle_alert`, `pgtrickle_wake`) enables
   push-based relay without polling overhead.
5. **WAL-based CDC** can act as a built-in transaction log tailer, avoiding
   external tools like Debezium for simple topologies.

---

## What Is the Transactional Outbox Pattern?

In a microservice architecture, a service often needs to:

1. Update its own database (e.g., create an order).
2. Publish an event to a message broker (e.g., `OrderCreated`).

These two operations must be **atomic** — either both succeed or neither does.
Using a distributed transaction (2PC) across the database and broker is
fragile, slow, and often unsupported.

The **Transactional Outbox** solves this by:

1. Writing the business entity **and** the event into the **same database
   transaction** (the event goes into an "outbox" table).
2. A separate **message relay** process reads the outbox and publishes events
   to the external broker.
3. Events are published **at least once** — consumers must be idempotent.

```
┌─────────────────────────────────────────────────────────┐
│                    PostgreSQL                            │
│                                                         │
│  BEGIN;                                                 │
│    INSERT INTO orders (...) VALUES (...);               │
│    INSERT INTO outbox_events (event_type, payload, ...) │
│      VALUES ('OrderCreated', '{"order_id": 42}');       │
│  COMMIT;                                                │
│                                                         │
│  ┌──────────────────┐    ┌──────────────────────────┐   │
│  │  outbox_events   │───→│ Message Relay (worker)   │───┼──→ Kafka / RabbitMQ / pgmq
│  │  (pending rows)  │    │ polls or tails changes   │   │
│  └──────────────────┘    └──────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

---

## The Problem It Solves

| Failure Scenario                        | Without Outbox             | With Outbox                  |
|-----------------------------------------|----------------------------|------------------------------|
| DB commits, broker publish fails        | Event lost forever         | Relay retries from outbox    |
| Service crashes after DB commit         | Event lost (was in memory) | Event persisted in outbox    |
| DB commits, service crashes before ack  | Possible duplicate         | Relay re-publishes (idempotent) |
| Broker down for maintenance             | Events lost during outage  | Events queue in outbox table |

**Key guarantees:**

- **No event loss:** If the transaction commits, the event is durably stored.
- **At-least-once delivery:** The relay may publish duplicates, but never drops.
- **Ordering preserved:** Events from a single aggregate are ordered by their
  database sequence (outbox PK or LSN).

---

## How pg_trickle Enables the Transactional Outbox

### Architecture Overview

pg_trickle's CDC infrastructure already captures row-level changes into buffer
tables (`pgtrickle_changes.changes_<oid>`) within the same transaction as the
source DML. This means **the CDC buffer is itself a transactional outbox** —
the only missing piece is the relay to an external broker.

```
┌──────────────────────────────────────────────────────────────────┐
│                         PostgreSQL                                │
│                                                                   │
│  ┌──────────────┐  CDC trigger   ┌──────────────────────────┐    │
│  │ orders       │ ──────────────→│ pgtrickle_changes.       │    │
│  │ (base table) │  (same txn)    │   changes_<oid>          │    │
│  └──────────────┘                └────────────┬─────────────┘    │
│                                               │                   │
│  ┌──────────────┐                             │ DIFFERENTIAL      │
│  │ outbox_events│  (optional,                 │ refresh           │
│  │ (explicit)   │   user-managed)             ↓                   │
│  └──────┬───────┘                ┌──────────────────────────┐    │
│         │ CDC trigger            │ pending_outbox_events    │    │
│         ↓                        │ (stream table)           │    │
│  ┌──────────────────────────┐    └────────────┬─────────────┘    │
│  │ pgtrickle_changes.       │                 │                   │
│  │   changes_<oid>          │                 │ NOTIFY             │
│  └──────────────────────────┘                 ↓                   │
│                                  ┌──────────────────────────┐    │
│                                  │ Message Relay             │    │
│                                  │ (app worker or pg worker) │───┼──→ Kafka / pgmq
│                                  └──────────────────────────┘    │
└──────────────────────────────────────────────────────────────────┘
```

### Approach 1: Stream Table as the Outbox View

The simplest approach — use an explicit outbox table and a pg_trickle stream
table to materialize the "pending events" view. The stream table gives you a
fast, pre-filtered, always-fresh view of unpublished events.

```sql
-- Step 1: Create the outbox table
CREATE TABLE outbox_events (
    event_id     BIGSERIAL PRIMARY KEY,
    event_type   TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ          -- NULL = not yet published
);

CREATE INDEX idx_outbox_pending ON outbox_events (event_id)
    WHERE published_at IS NULL;

-- Step 2: Create a stream table for pending events
SELECT pgtrickle.create_stream_table(
    'pending_outbox_events',
    $$SELECT event_id, event_type, aggregate_id, aggregate_type, payload, created_at
      FROM outbox_events
      WHERE published_at IS NULL$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);

-- Step 3: Application writes business entity + outbox in one transaction
BEGIN;
  INSERT INTO orders (user_id, total, status)
      VALUES (123, 99.95, 'confirmed');
  INSERT INTO outbox_events (event_type, aggregate_id, aggregate_type, payload)
      VALUES ('OrderCreated', currval('orders_id_seq')::text, 'Order',
              jsonb_build_object('user_id', 123, 'total', 99.95, 'status', 'confirmed'));
COMMIT;

-- Step 4: Relay reads from the stream table (always fresh within ~1s)
-- Application-side pseudocode:
--   rows = SELECT * FROM pending_outbox_events ORDER BY event_id;
--   for each row:
--     publish(row) to Kafka/RabbitMQ
--     UPDATE outbox_events SET published_at = now() WHERE event_id = row.event_id;
```

**Advantages:**
- Stream table is pre-filtered (only pending events), no full-table scan.
- DIFFERENTIAL refresh means only changed rows are recomputed.
- `pg_trickle_alert` channel notifies when the stream table refreshes.
- Multiple relays can read the stream table (but need coordination — see below).

**When to use:** Standard outbox with moderate throughput (< 10K events/sec).

### Approach 2: CDC Buffer as the Outbox

For higher throughput, skip the explicit outbox table entirely. pg_trickle's
CDC triggers already capture every INSERT/UPDATE/DELETE into typed buffer
tables. Your relay can read directly from the CDC buffers.

```sql
-- The application just writes to the business table
BEGIN;
  INSERT INTO orders (user_id, total, status)
      VALUES (123, 99.95, 'confirmed');
COMMIT;

-- pg_trickle's CDC trigger has already captured this INSERT in:
--   pgtrickle_changes.changes_<orders_oid>
-- with columns: change_id, lsn, action='I', new_user_id, new_total, new_status, ...

-- Create a stream table that transforms CDC changes into domain events
SELECT pgtrickle.create_stream_table(
    'order_events',
    $$SELECT o.id AS aggregate_id,
             CASE
               WHEN o.status = 'confirmed' THEN 'OrderCreated'
               WHEN o.status = 'shipped'   THEN 'OrderShipped'
               WHEN o.status = 'cancelled' THEN 'OrderCancelled'
             END AS event_type,
             jsonb_build_object(
               'order_id', o.id,
               'user_id', o.user_id,
               'total', o.total,
               'status', o.status
             ) AS payload
      FROM orders o$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);
```

**Advantages:**
- No explicit outbox table — zero additional write overhead.
- CDC captures happen within the same transaction automatically.
- Delta-based: only changed rows flow through the stream table.

**When to use:** When the business table itself is the source of events and
you don't need custom event shaping beyond what a SQL query can express.

### Approach 3: IMMEDIATE Mode for Zero-Lag Outbox

For scenarios requiring **sub-millisecond event freshness**, IMMEDIATE mode
updates the stream table within the same transaction as the source write.

```sql
-- Create the outbox with IMMEDIATE refresh
SELECT pgtrickle.create_stream_table(
    'realtime_outbox',
    $$SELECT event_id, event_type, aggregate_id, payload, created_at
      FROM outbox_events
      WHERE published_at IS NULL$$,
    refresh_mode => 'IMMEDIATE'
);

-- Now, within the same transaction:
BEGIN;
  INSERT INTO orders (...) VALUES (...);
  INSERT INTO outbox_events (...) VALUES (...);
  -- At COMMIT time, realtime_outbox is already updated
COMMIT;

-- A LISTEN-based relay sees the change immediately
LISTEN pg_trickle_alert;
-- Payload: {"event":"refresh_completed","pgt_name":"realtime_outbox",...}
```

**Advantages:**
- Zero lag between business write and outbox materialization.
- Relay can use LISTEN/NOTIFY for push-based publishing.

**Trade-offs:**
- Adds latency to the source transaction (statement-level trigger overhead).
- Best for low-to-moderate write throughput (< 1K writes/sec).

---

## Message Relay Strategies

The outbox pattern requires a **message relay** — a process that reads pending
events and publishes them to an external broker. pg_trickle supports three
relay strategies.

### Strategy A: Polling Publisher via Stream Table

The relay polls the stream table at regular intervals.

```python
import psycopg2
import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='kafka:9092')
conn = psycopg2.connect("postgresql://localhost/mydb")

while True:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT event_id, event_type, aggregate_id, payload
            FROM pending_outbox_events
            ORDER BY event_id
            LIMIT 100
        """)
        rows = cur.fetchall()

        for event_id, event_type, aggregate_id, payload in rows:
            producer.send(
                topic=event_type,
                key=aggregate_id.encode(),
                value=json.dumps(payload).encode(),
                headers=[('event_id', str(event_id).encode())]
            )
            cur.execute(
                "UPDATE outbox_events SET published_at = now() WHERE event_id = %s",
                (event_id,)
            )
        conn.commit()

    time.sleep(0.5)  # or match the stream table schedule
```

**Throughput:** Moderate. Limited by polling interval.  
**Latency:** ~1–2s (stream table schedule + polling interval).

### Strategy B: LISTEN/NOTIFY + Stream Table

Push-based relay using PostgreSQL's built-in notification system.

```python
import psycopg2
import psycopg2.extensions
import select

conn = psycopg2.connect("postgresql://localhost/mydb")
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

cur = conn.cursor()
cur.execute("LISTEN pg_trickle_alert;")

while True:
    if select.select([conn], [], [], 5.0) != ([], [], []):
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            payload = json.loads(notify.payload)

            if (payload.get('event') == 'refresh_completed' and
                payload.get('pgt_name') == 'pending_outbox_events'):
                # Fetch and publish pending events
                publish_pending_events()
```

**Throughput:** High. Wakes immediately on refresh.  
**Latency:** ~15ms (event-driven wake) + stream table schedule.

### Strategy C: Transaction Log Tailing via WAL CDC

When pg_trickle transitions a source table to WAL-based CDC, the logical
replication slot already captures changes in commit order. An external tool or
custom relay can tail this stream.

This is conceptually identical to using Debezium, but pg_trickle manages the
replication slot lifecycle automatically.

**When to use:** Very high throughput (> 10K events/sec) where polling overhead
is unacceptable and you need strict commit-order guarantees.

---

## Worked Example: Order Service Outbox

A complete example showing an e-commerce order service with pg_trickle-powered
outbox.

```sql
-- === Schema ===

CREATE TABLE customers (
    id    SERIAL PRIMARY KEY,
    name  TEXT NOT NULL,
    email TEXT NOT NULL
);

CREATE TABLE orders (
    id          SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(id),
    total       NUMERIC(10,2) NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE outbox_events (
    event_id       BIGSERIAL PRIMARY KEY,
    event_type     TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    payload        JSONB NOT NULL,
    metadata       JSONB DEFAULT '{}',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);

-- === Trigger to auto-populate outbox on order changes ===

CREATE OR REPLACE FUNCTION fn_order_outbox() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO outbox_events (event_type, aggregate_type, aggregate_id, payload)
        VALUES (
            'OrderCreated', 'Order', NEW.id::text,
            jsonb_build_object(
                'order_id', NEW.id,
                'customer_id', NEW.customer_id,
                'total', NEW.total,
                'status', NEW.status
            )
        );
    ELSIF TG_OP = 'UPDATE' AND OLD.status <> NEW.status THEN
        INSERT INTO outbox_events (event_type, aggregate_type, aggregate_id, payload)
        VALUES (
            'OrderStatusChanged', 'Order', NEW.id::text,
            jsonb_build_object(
                'order_id', NEW.id,
                'old_status', OLD.status,
                'new_status', NEW.status
            )
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_order_outbox
    AFTER INSERT OR UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION fn_order_outbox();

-- === pg_trickle Stream Tables ===

-- Pending events for the relay to publish
SELECT pgtrickle.create_stream_table(
    'pending_outbox_events',
    $$SELECT event_id, event_type, aggregate_type, aggregate_id,
             payload, metadata, created_at
      FROM outbox_events
      WHERE published_at IS NULL
      ORDER BY event_id$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);

-- Published event audit log (for monitoring dashboards)
SELECT pgtrickle.create_stream_table(
    'outbox_stats',
    $$SELECT event_type,
             COUNT(*) AS total_events,
             COUNT(*) FILTER (WHERE published_at IS NULL) AS pending,
             COUNT(*) FILTER (WHERE published_at IS NOT NULL) AS published,
             MAX(created_at) FILTER (WHERE published_at IS NULL) AS oldest_pending
      FROM outbox_events
      GROUP BY event_type$$,
    schedule => '10s',
    refresh_mode => 'DIFFERENTIAL'
);

-- === Monitoring ===

-- Alert when outbox is backing up
-- pg_trickle_alert channel will emit stale_data events if the stream table
-- falls behind schedule, giving you a built-in health signal.
SELECT pgtrickle.get_staleness('pending_outbox_events');
```

### Competing Consumers with `FOR UPDATE SKIP LOCKED`

For multiple relay instances processing the same outbox:

```sql
-- Each relay instance runs:
BEGIN;
  SELECT event_id, event_type, aggregate_id, payload
  FROM outbox_events
  WHERE published_at IS NULL
  ORDER BY event_id
  LIMIT 50
  FOR UPDATE SKIP LOCKED;

  -- Publish batch to broker...

  UPDATE outbox_events
  SET published_at = now()
  WHERE event_id = ANY($published_ids);
COMMIT;
```

This provides safe concurrent processing without duplicate publishing.

---

## Complementary PostgreSQL Extensions

### pgmq — PostgreSQL Message Queue

[pgmq](https://github.com/pgmq/pgmq) (4.8K+ stars) is a lightweight
PostgreSQL extension that implements an SQS-like message queue entirely within
PostgreSQL.

**How it works with pg_trickle:**

```sql
CREATE EXTENSION pgmq;

-- Create a queue for order events
SELECT pgmq.create('order_events');

-- Use a trigger on the pg_trickle stream table (or outbox table)
-- to enqueue messages automatically
CREATE OR REPLACE FUNCTION fn_enqueue_outbox() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pgmq.send(
        'order_events',
        jsonb_build_object(
            'event_id', NEW.event_id,
            'event_type', NEW.event_type,
            'aggregate_id', NEW.aggregate_id,
            'payload', NEW.payload
        )
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Consumers read from the pgmq queue
SELECT * FROM pgmq.read('order_events', vt => 30, qty => 10);

-- After processing, delete or archive
SELECT pgmq.delete('order_events', msg_id => 1);
```

**Advantages of pgmq + pg_trickle:**
- Both run inside PostgreSQL — no external broker needed.
- pgmq provides visibility timeouts, exactly-once delivery semantics, and
  message archival.
- pg_trickle provides the real-time materialized view layer on top.
- Combined: write to business table → CDC → stream table → pgmq queue →
  consumer.

**Use case:** Small-to-medium deployments that want to avoid operating Kafka or
RabbitMQ.

### pg_amqp — AMQP Publishing from PostgreSQL

[pg_amqp](https://github.com/omniti-labs/pg_amqp) allows publishing messages
to an AMQP broker (RabbitMQ) directly from PostgreSQL triggers or functions.

```sql
-- Configure the broker connection
INSERT INTO amqp.broker (host, port, vhost, username, password)
VALUES ('rabbitmq', 5672, '/', 'guest', 'guest');

-- Publish from a trigger on the outbox table
CREATE OR REPLACE FUNCTION fn_publish_to_amqp() RETURNS TRIGGER AS $$
BEGIN
    PERFORM amqp.publish(1, 'events', NEW.event_type, NEW.payload::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

**Trade-off:** Publishing happens within the transaction — if the broker is
slow or down, it blocks the transaction. Better suited as a relay step after
the outbox commit.

### pgflow — Durable Workflow Engine

[pgflow](https://pgflow.dev/) builds durable, multi-step workflows on top of
pgmq. It can orchestrate complex event processing pipelines with retry logic,
timeouts, and DAG-based step dependencies.

**Synergy with pg_trickle:** Use pg_trickle stream tables as the "sensing"
layer that detects changes, and pgflow as the "acting" layer that orchestrates
side effects.

### Debezium — External CDC Platform

[Debezium](https://debezium.io/) is the industry-standard CDC platform for
tailing database WALs and publishing changes to Kafka. When pg_trickle
transitions to WAL-based CDC, the logical replication slot it creates is
conceptually similar to what Debezium uses.

**When to choose Debezium over pg_trickle-native relay:**
- You need Kafka Connect ecosystem integration.
- You need to capture changes from multiple databases into a central event bus.
- You need schema registry integration (Avro/Protobuf serialization).

**When pg_trickle-native is sufficient:**
- Single-database topology.
- Events are consumed by services that can query PostgreSQL directly.
- You want to avoid operating an additional infrastructure component.

---

## Potential pg_trickle Extensions

### Extension 1: Outbox Table Helper

A convenience function that creates the outbox table, trigger, and stream
table in one call.

```sql
-- Proposed API
SELECT pgtrickle.create_outbox(
    source_table => 'orders',
    outbox_name  => 'order_outbox',
    event_types  => ARRAY['OrderCreated', 'OrderUpdated', 'OrderDeleted'],
    schedule     => '1s'
);

-- This would create:
-- 1. outbox_events_<source> table with standard schema
-- 2. AFTER INSERT/UPDATE/DELETE trigger on source_table
-- 3. Stream table: pending_<outbox_name> (WHERE published_at IS NULL)
-- 4. Index on (event_id) WHERE published_at IS NULL
```

**Effort:** Medium. Mostly SQL generation and metadata management.

### Extension 2: Message Relay Background Worker

A pg_trickle background worker that reads from the outbox stream table and
publishes to an external system via a configurable connector.

```sql
-- Proposed GUC configuration
SET pg_trickle.outbox_relay_enabled = true;
SET pg_trickle.outbox_relay_target = 'pgmq';  -- or 'http', 'kafka'
SET pg_trickle.outbox_relay_queue = 'order_events';
SET pg_trickle.outbox_relay_batch_size = 100;
SET pg_trickle.outbox_relay_interval_ms = 500;
```

**Effort:** High. Requires connector framework, error handling, and
backpressure management.

### Extension 3: pgmq Integration

First-class integration with pgmq so that stream table refreshes can
automatically enqueue changed rows into a pgmq queue.

```sql
-- Proposed API
SELECT pgtrickle.create_stream_table(
    'order_notifications',
    $$SELECT id, status, total FROM orders WHERE status = 'confirmed'$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL',
    on_change => 'pgmq:order_events'  -- auto-enqueue deltas
);
```

**Effort:** Medium. Hook into the refresh completion path to call
`pgmq.send_batch()` with the delta rows.

### Extension 4: Webhook Dispatcher

A built-in HTTP webhook dispatcher that posts delta rows to an external URL
after each refresh.

```sql
-- Proposed API
SELECT pgtrickle.create_stream_table(
    'order_webhooks',
    $$SELECT id, status, total FROM orders$$,
    schedule => '5s',
    refresh_mode => 'DIFFERENTIAL',
    on_change_webhook => 'https://api.example.com/webhooks/orders'
);
```

**Effort:** High. Requires HTTP client, retry logic, authentication, and
circuit breaker patterns. Consider using pg_net extension as the HTTP layer.

---

## Design Considerations

### Ordering Guarantees

| Scope | Guarantee |
|-------|-----------|
| Single aggregate (same `aggregate_id`) | Ordered by `event_id` (monotonic PK) |
| Across aggregates | No global ordering; each aggregate ordered independently |
| Across source tables | Independent refresh schedules; no cross-table ordering |
| Within a diamond dependency group | Atomic refresh group ensures consistency |

### Idempotency

The outbox pattern provides **at-least-once** delivery. Consumers MUST handle
duplicates. Recommended strategies:

1. **Event ID deduplication:** Store processed `event_id` values in a
   `processed_events` table and check before processing.
2. **Idempotent operations:** Design state mutations so that applying the same
   event twice has no additional effect (e.g., `UPDATE ... SET status = 'x'`
   is naturally idempotent).
3. **Version vectors:** Include an `expected_version` in the event payload and
   reject stale events.

### Garbage Collection

Published events should be cleaned up to prevent outbox table bloat:

```sql
-- Option A: Delete published events older than 7 days
DELETE FROM outbox_events
WHERE published_at IS NOT NULL
  AND published_at < now() - INTERVAL '7 days';

-- Option B: Partition the outbox table by month
CREATE TABLE outbox_events (
    event_id BIGSERIAL,
    ...
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
) PARTITION BY RANGE (created_at);
```

### Backpressure

When the relay falls behind:

1. `pg_trickle_alert` emits `stale_data` events when the stream table
   staleness exceeds 2× its schedule.
2. Monitor `pending_outbox_events` row count via the `outbox_stats` stream
   table.
3. Adjust `schedule` or increase relay parallelism.
4. pg_trickle's adaptive refresh can automatically switch to FULL refresh if
   the change ratio exceeds `differential_max_change_ratio`.

### Failure Modes

| Failure | Impact | Recovery |
|---------|--------|----------|
| Relay crashes after publish, before marking published | Duplicate event published | Consumer deduplicates via event_id |
| PostgreSQL crashes after COMMIT | Events durable in WAL | Normal crash recovery; outbox intact |
| Stream table refresh fails | Relay reads stale data | pg_trickle retries with backoff; alert emitted |
| Broker unreachable | Relay retries | Events queue in outbox; monitor staleness |

---

## Comparison with Traditional Approaches

| Aspect | Traditional Outbox | pg_trickle Outbox | Debezium CDC |
|--------|--------------------|-------------------|--------------|
| Additional write overhead | 1 extra INSERT per event | Zero (CDC trigger is automatic) | Zero (WAL tail) |
| Relay mechanism | Custom polling worker | Stream table + NOTIFY | Kafka Connect |
| Latency | Depends on poll interval | ~1s (DIFFERENTIAL) or ~0ms (IMMEDIATE) | ~1-5s |
| Ordering | PK-ordered | PK-ordered + LSN-ordered | LSN-ordered |
| Infrastructure | PostgreSQL only | PostgreSQL only | PostgreSQL + Kafka + Debezium |
| Monitoring | Custom | Built-in (staleness, alerts, stats) | Kafka + Debezium metrics |
| Competing consumers | FOR UPDATE SKIP LOCKED | FOR UPDATE SKIP LOCKED or pgmq | Kafka consumer groups |
| Garbage collection | Manual DELETE/partition | Manual or partitioned | Kafka retention |

---

## References

- Chris Richardson, [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- Microsoft, [Implement the Transactional Outbox Pattern](https://learn.microsoft.com/en-us/azure/architecture/best-practices/transactional-outbox-cosmos)
- [pgmq — PostgreSQL Message Queue](https://github.com/pgmq/pgmq)
- [Debezium CDC Platform](https://debezium.io/)
- Krzysztof Atłasik, [Microservices 101: Transactional Outbox and Inbox](https://softwaremill.com/microservices-101/)
- pg_trickle [ARCHITECTURE.md](../../docs/ARCHITECTURE.md), [PATTERNS.md](../../docs/PATTERNS.md), [SQL_REFERENCE.md](../../docs/SQL_REFERENCE.md)
