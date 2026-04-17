# Transactional Inbox Pattern with pg_trickle

> **Status:** Research Report  
> **Created:** 2026-04-17  
> **Category:** Integration Pattern  
> **Related:** [PLAN_TRANSACTIONAL_OUTBOX.md](PLAN_TRANSACTIONAL_OUTBOX.md)

---

## Table of Contents

- [Executive Summary](#executive-summary)
- [What Is the Transactional Inbox Pattern?](#what-is-the-transactional-inbox-pattern)
- [The Problem It Solves](#the-problem-it-solves)
- [How pg_trickle Enables the Transactional Inbox](#how-pg_trickle-enables-the-transactional-inbox)
  - [Architecture Overview](#architecture-overview)
  - [Approach 1: Inbox Table + Stream Table for Processing Queue](#approach-1-inbox-table--stream-table-for-processing-queue)
  - [Approach 2: Inbox with Deduplication and Ordering](#approach-2-inbox-with-deduplication-and-ordering)
  - [Approach 3: IMMEDIATE Mode for Synchronous Inbox Processing](#approach-3-immediate-mode-for-synchronous-inbox-processing)
  - [Approach 4: Multi-Source Inbox Aggregation](#approach-4-multi-source-inbox-aggregation)
- [Processing Strategies](#processing-strategies)
  - [Strategy A: Background Worker Polling](#strategy-a-background-worker-polling)
  - [Strategy B: Event-Driven Processing](#strategy-b-event-driven-processing)
  - [Strategy C: Competing Workers with SKIP LOCKED](#strategy-c-competing-workers-with-skip-locked)
- [Worked Example: Payment Service Inbox](#worked-example-payment-service-inbox)
- [Complementary PostgreSQL Extensions](#complementary-postgresql-extensions)
  - [pgmq — As the Inbox Transport](#pgmq--as-the-inbox-transport)
  - [pg_cron — Scheduled Inbox Cleanup](#pg_cron--scheduled-inbox-cleanup)
  - [pg_partman — Inbox Table Partitioning](#pg_partman--inbox-table-partitioning)
  - [pgflow — Durable Inbox Processing Workflows](#pgflow--durable-inbox-processing-workflows)
  - [NATS / JetStream — As the Inbox Transport Layer](#nats--jetstream--as-the-inbox-transport-layer)
- [Potential pg_trickle Extensions](#potential-pg_trickle-extensions)
  - [Extension 1: Inbox Table Helper](#extension-1-inbox-table-helper)
  - [Extension 2: Deduplication Stream Table](#extension-2-deduplication-stream-table)
  - [Extension 3: Dead Letter Queue Stream Table](#extension-3-dead-letter-queue-stream-table)
  - [Extension 4: Inbox Health Dashboard](#extension-4-inbox-health-dashboard)
- [Design Considerations](#design-considerations)
- [Schema Evolution & Message Versioning](#schema-evolution--message-versioning)
- [Observability & Distributed Tracing](#observability--distributed-tracing)
- [Testing Strategies](#testing-strategies)
- [Multi-Tenancy](#multi-tenancy)
- [Cost Analysis](#cost-analysis)
- [Security Considerations](#security-considerations)
- [Combining Outbox and Inbox Patterns](#combining-outbox-and-inbox-patterns)
- [Comparison with Traditional Approaches](#comparison-with-traditional-approaches)
- [References](#references)

---

## Executive Summary

The **Transactional Inbox Pattern** ensures reliable, idempotent processing of
inbound messages from external systems. When a service receives a message (from
a broker, webhook, or API call), it first persists the message to an "inbox"
table within a database transaction, then acknowledges receipt. A separate
background process picks up messages from the inbox and processes them.

pg_trickle enhances this pattern by:

1. **Materializing the processing queue** as a stream table — the "unprocessed
   messages" view is always fresh and pre-filtered via DIFFERENTIAL refresh.
2. **Providing built-in deduplication** through `DISTINCT ON` stream tables
   that can eliminate duplicate message deliveries.
3. **Detecting ordering gaps** via stream tables that surface missing sequence
   numbers or out-of-order arrivals.
4. **Monitoring inbox health** through built-in staleness alerts, refresh
   statistics, and the `pg_trickle_alert` notification channel.
5. **Enabling multi-source aggregation** where messages from different upstream
   services are unified into a single processing pipeline via stream tables.

---

## What Is the Transactional Inbox Pattern?

In a microservice architecture, a service receives messages from external
systems (brokers, other services, webhooks). The challenge is ensuring:

1. **No message loss:** Every message is eventually processed.
2. **No duplicate processing:** Messages delivered more than once (at-least-once
   delivery) don't cause duplicate side effects.
3. **Ordered processing:** Messages for the same entity are processed in order.
4. **Resilience:** Processing failures don't lose the message.

The **Transactional Inbox** solves this by:

1. **Persisting the message** to an "inbox" table as the first step.
2. **Acknowledging the message** to the broker/sender only after the inbox
   write succeeds.
3. A **background processor** reads from the inbox, executes business logic,
   and marks messages as processed.
4. **Deduplication** happens at the inbox level using the message's unique ID.

```
External System                  PostgreSQL
     │                               │
     │  message (event_id=abc)       │
     ├──────────────────────────────→│
     │                               │ BEGIN;
     │                               │   INSERT INTO inbox_events (event_id, ...)
     │                               │     ON CONFLICT (event_id) DO NOTHING;
     │                ACK            │ COMMIT;
     │←──────────────────────────────┤
     │                               │
     │                               │ ┌──────────────────────┐
     │                               │ │ Background Processor │
     │                               │ │ reads inbox, applies │
     │                               │ │ business logic, marks│
     │                               │ │ as processed         │
     │                               │ └──────────────────────┘
```

---

## The Problem It Solves

| Failure Scenario                         | Without Inbox                    | With Inbox                       |
|------------------------------------------|----------------------------------|----------------------------------|
| Service crashes after receiving message  | Message lost (acked but not saved) | Never acked — broker redelivers |
| Service crashes during processing        | Partial state + message lost     | Message still in inbox; retry    |
| Duplicate delivery (broker retry)        | Duplicate side effects           | Deduplicated by event_id PK     |
| Processing takes too long (timeout)      | Broker redelivers → concurrent processing | Inbox deduplicates; single processor |
| Out-of-order delivery                    | Inconsistent state               | Inbox + ordering by sequence ID  |
| Burst of messages                        | Overwhelms processing capacity   | Messages queue in inbox; backpressure |

**Key guarantees:**

- **Exactly-once processing:** Deduplication at the inbox level + idempotent
  processing ensures each logical message is processed exactly once.
- **Durability:** Once the inbox write commits, the message survives crashes.
- **Decoupled ingestion and processing:** The receiver is fast (just INSERT +
  ACK); processing happens asynchronously at whatever pace the system can
  sustain.

---

## How pg_trickle Enables the Transactional Inbox

### Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                           PostgreSQL                                  │
│                                                                       │
│  External messages arrive via consumer worker (Kafka, RabbitMQ, HTTP) │
│                         │                                             │
│                         ▼                                             │
│  ┌──────────────────────────────────────────┐                        │
│  │ inbox_events                              │                        │
│  │ (event_id PK, event_type, source,         │                        │
│  │  payload, received_at, processed_at,      │                        │
│  │  error, retry_count)                      │                        │
│  └────────────────────┬─────────────────────┘                        │
│                       │ CDC trigger (automatic)                       │
│                       ▼                                               │
│  ┌──────────────────────────────────────────┐                        │
│  │ pending_inbox     (stream table)          │  DIFFERENTIAL, 1s     │
│  │ WHERE processed_at IS NULL                │                        │
│  │   AND retry_count < max_retries           │                        │
│  └────────────────────┬─────────────────────┘                        │
│                       │                                               │
│        ┌──────────────┼──────────────┐                                │
│        ▼              ▼              ▼                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────────┐                       │
│  │ Worker 1 │  │ Worker 2 │  │ dead_letter  │  (stream table)       │
│  │ (process)│  │ (process)│  │ WHERE retries│  DIFFERENTIAL, 30s    │
│  └──────────┘  └──────────┘  │ >= max       │                       │
│                              └──────────────┘                        │
│                                                                       │
│  ┌──────────────────────────────────────────┐                        │
│  │ inbox_stats       (stream table)          │  DIFFERENTIAL, 10s    │
│  │ COUNT(*) pending, processed, failed       │                        │
│  │ GROUP BY event_type, source               │                        │
│  └──────────────────────────────────────────┘                        │
└──────────────────────────────────────────────────────────────────────┘
```

### Approach 1: Inbox Table + Stream Table for Processing Queue

The core pattern: an inbox table for durable storage + a stream table for the
"ready to process" queue.

```sql
-- Step 1: Create the inbox table
CREATE TABLE inbox_events (
    event_id     TEXT PRIMARY KEY,        -- globally unique (from sender)
    event_type   TEXT NOT NULL,
    source       TEXT NOT NULL,           -- originating service/system
    payload      JSONB NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ,            -- NULL = not yet processed
    error        TEXT,                    -- last processing error
    retry_count  INT NOT NULL DEFAULT 0
);

CREATE INDEX idx_inbox_pending ON inbox_events (received_at)
    WHERE processed_at IS NULL;

-- Step 2: Stream table for unprocessed messages
SELECT pgtrickle.create_stream_table(
    'pending_inbox',
    $$SELECT event_id, event_type, source, payload, received_at, retry_count
      FROM inbox_events
      WHERE processed_at IS NULL
        AND retry_count < 5
      ORDER BY received_at$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);

-- Step 3: Consumer writes to inbox (idempotent via ON CONFLICT)
-- Application-side pseudocode:
--   msg = kafka_consumer.poll()
--   INSERT INTO inbox_events (event_id, event_type, source, payload)
--     VALUES (msg.id, msg.type, msg.source, msg.payload)
--     ON CONFLICT (event_id) DO NOTHING;
--   kafka_consumer.ack(msg)

-- Step 4: Processor reads from stream table
-- Application-side pseudocode:
--   rows = SELECT * FROM pending_inbox LIMIT 50;
--   for each row:
--     try:
--       process(row)
--       UPDATE inbox_events SET processed_at = now() WHERE event_id = row.event_id;
--     catch:
--       UPDATE inbox_events SET error = err_msg, retry_count = retry_count + 1
--         WHERE event_id = row.event_id;
```

**Advantages:**
- Deduplication via `ON CONFLICT (event_id) DO NOTHING`.
- Stream table provides a fast, pre-filtered view of pending work.
- DIFFERENTIAL refresh only recomputes when inbox_events changes.
- Built-in monitoring via `pg_trickle_alert`.

### Approach 2: Inbox with Deduplication and Ordering

For scenarios where messages arrive out of order and must be processed
sequentially per entity.

```sql
-- Inbox with sequence tracking
CREATE TABLE inbox_events (
    event_id      TEXT PRIMARY KEY,
    event_type    TEXT NOT NULL,
    aggregate_id  TEXT NOT NULL,         -- entity this message belongs to
    sequence_num  BIGINT NOT NULL,       -- monotonic per aggregate
    source        TEXT NOT NULL,
    payload       JSONB NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at  TIMESTAMPTZ,
    error         TEXT,
    retry_count   INT NOT NULL DEFAULT 0,
    UNIQUE (aggregate_id, sequence_num)  -- enforce ordering uniqueness
);

-- Stream table: next message to process per aggregate (in-order)
SELECT pgtrickle.create_stream_table(
    'inbox_next_to_process',
    $$WITH last_processed AS (
        SELECT aggregate_id, COALESCE(MAX(sequence_num), 0) AS last_seq
        FROM inbox_events
        WHERE processed_at IS NOT NULL
        GROUP BY aggregate_id
      ),
      next_event AS (
        SELECT DISTINCT ON (i.aggregate_id)
               i.event_id, i.event_type, i.aggregate_id, i.sequence_num,
               i.payload, i.received_at
        FROM inbox_events i
        LEFT JOIN last_processed lp ON lp.aggregate_id = i.aggregate_id
        WHERE i.processed_at IS NULL
          AND i.retry_count < 5
          AND i.sequence_num = COALESCE(lp.last_seq, 0) + 1
        ORDER BY i.aggregate_id, i.sequence_num
      )
      SELECT * FROM next_event$$,
    schedule => '2s',
    refresh_mode => 'DIFFERENTIAL'
);

-- Stream table: detect gaps in message sequences
SELECT pgtrickle.create_stream_table(
    'inbox_missing_sequences',
    $$WITH expected AS (
        SELECT aggregate_id,
               generate_series(
                 MIN(sequence_num),
                 MAX(sequence_num)
               ) AS expected_seq
        FROM inbox_events
        GROUP BY aggregate_id
      )
      SELECT e.aggregate_id, e.expected_seq AS missing_sequence
      FROM expected e
      LEFT JOIN inbox_events i
        ON i.aggregate_id = e.aggregate_id
        AND i.sequence_num = e.expected_seq
      WHERE i.event_id IS NULL$$,
    schedule => '30s',
    refresh_mode => 'DIFFERENTIAL'
);
```

**Advantages:**
- Guarantees in-order processing per aggregate.
- Detects missing messages so gaps can be investigated.
- DIFFERENTIAL refresh keeps the "next to process" view cheap to maintain.

### Approach 3: IMMEDIATE Mode for Synchronous Inbox Processing

For low-latency scenarios where you want to start processing the moment a
message arrives.

```sql
-- IMMEDIATE mode updates the stream table within the ingestion transaction
SELECT pgtrickle.create_stream_table(
    'realtime_inbox',
    $$SELECT event_id, event_type, payload, received_at
      FROM inbox_events
      WHERE processed_at IS NULL$$,
    refresh_mode => 'IMMEDIATE'
);

-- Combined with LISTEN/NOTIFY for instant processing triggers
LISTEN pg_trickle_alert;
-- When a message is ingested, the stream table updates within the same txn,
-- and the alert fires, waking the processor immediately.
```

**Trade-off:** Adds latency to the message ingestion path. Use only when
processing latency is more critical than ingestion throughput.

### Approach 4: Multi-Source Inbox Aggregation

Unify messages from multiple upstream services into a single processing
pipeline.

```sql
-- Separate inbox tables per source (for isolation)
CREATE TABLE inbox_from_orders (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ DEFAULT now(),
    processed_at TIMESTAMPTZ
);

CREATE TABLE inbox_from_payments (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ DEFAULT now(),
    processed_at TIMESTAMPTZ
);

-- Unified stream table aggregating all sources
SELECT pgtrickle.create_stream_table(
    'unified_pending_inbox',
    $$SELECT event_id, event_type, 'orders' AS source, payload, received_at
        FROM inbox_from_orders WHERE processed_at IS NULL
      UNION ALL
      SELECT event_id, event_type, 'payments' AS source, payload, received_at
        FROM inbox_from_payments WHERE processed_at IS NULL$$,
    schedule => '2s',
    refresh_mode => 'DIFFERENTIAL'
);
```

**Advantages:**
- Single processing pipeline for all inbound events.
- pg_trickle's CDC captures changes from all source tables independently.
- UNION ALL is fully supported by the DVM engine with delta propagation.

---

## Processing Strategies

### Strategy A: Background Worker Polling

The processor periodically reads from the inbox stream table.

```python
import psycopg2
import json
import traceback

conn = psycopg2.connect("postgresql://localhost/mydb")

while True:
    with conn.cursor() as cur:
        # Read from the materialized stream table (fast, pre-filtered)
        cur.execute("""
            SELECT event_id, event_type, payload
            FROM pending_inbox
            ORDER BY received_at
            LIMIT 50
        """)
        rows = cur.fetchall()

        for event_id, event_type, payload in rows:
            try:
                process_event(event_type, payload)
                cur.execute(
                    "UPDATE inbox_events SET processed_at = now() WHERE event_id = %s",
                    (event_id,)
                )
            except Exception as e:
                cur.execute("""
                    UPDATE inbox_events
                    SET error = %s, retry_count = retry_count + 1
                    WHERE event_id = %s
                """, (str(e)[:500], event_id))

        conn.commit()

    time.sleep(0.5)
```

**Throughput:** Moderate. Tunable via batch size and poll interval.

### Strategy B: Event-Driven Processing

Use `pg_trickle_alert` notifications to trigger processing immediately.

```python
import psycopg2
import psycopg2.extensions
import select
import json

conn = psycopg2.connect("postgresql://localhost/mydb")
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

cur = conn.cursor()
cur.execute("LISTEN pg_trickle_alert;")

while True:
    if select.select([conn], [], [], 10.0) != ([], [], []):
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            payload = json.loads(notify.payload)

            if (payload.get('event') == 'refresh_completed' and
                payload.get('pgt_name') == 'pending_inbox'):

                rows_changed = payload.get('rows_inserted', 0)
                if rows_changed > 0:
                    process_pending_inbox()
```

**Latency:** ~15ms (event-driven wake) + stream table schedule.

### Strategy C: Competing Workers with SKIP LOCKED

Multiple worker instances safely process from the same inbox without
duplicating work.

```sql
-- Each worker runs:
BEGIN;
  -- Lock and fetch a batch of unprocessed messages
  SELECT event_id, event_type, payload
  FROM inbox_events
  WHERE processed_at IS NULL
    AND retry_count < 5
  ORDER BY received_at
  LIMIT 20
  FOR UPDATE SKIP LOCKED;

  -- Process each message...

  -- Mark batch as processed
  UPDATE inbox_events
  SET processed_at = now()
  WHERE event_id = ANY($processed_ids);
COMMIT;
```

**Note:** This strategy reads directly from the inbox table (not the stream
table) because `FOR UPDATE` requires a base table. The stream table still
serves as the monitoring/stats layer.

---

## Worked Example: Payment Service Inbox

A payment service receives `OrderCreated` events and must create payment
intents for each order.

```sql
-- === Schema ===

CREATE TABLE payment_inbox (
    event_id     TEXT PRIMARY KEY,
    event_type   TEXT NOT NULL,
    source       TEXT NOT NULL DEFAULT 'order-service',
    payload      JSONB NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ,
    error        TEXT,
    retry_count  INT NOT NULL DEFAULT 0
);

CREATE TABLE payment_intents (
    id          SERIAL PRIMARY KEY,
    order_id    INT NOT NULL UNIQUE,     -- idempotency key
    customer_id INT NOT NULL,
    amount      NUMERIC(10,2) NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- === pg_trickle Stream Tables ===

-- Pending messages ready for processing
SELECT pgtrickle.create_stream_table(
    'pending_payments',
    $$SELECT event_id, event_type, payload, received_at, retry_count
      FROM payment_inbox
      WHERE processed_at IS NULL
        AND retry_count < 5$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);

-- Dead letter queue: messages that exceeded retry limit
SELECT pgtrickle.create_stream_table(
    'payment_dead_letters',
    $$SELECT event_id, event_type, payload, error, retry_count, received_at
      FROM payment_inbox
      WHERE processed_at IS NULL
        AND retry_count >= 5$$,
    schedule => '30s',
    refresh_mode => 'DIFFERENTIAL'
);

-- Processing statistics dashboard
SELECT pgtrickle.create_stream_table(
    'payment_inbox_stats',
    $$SELECT
        event_type,
        COUNT(*) FILTER (WHERE processed_at IS NULL AND retry_count < 5) AS pending,
        COUNT(*) FILTER (WHERE processed_at IS NOT NULL) AS processed,
        COUNT(*) FILTER (WHERE retry_count >= 5) AS dead_letter,
        AVG(EXTRACT(EPOCH FROM (processed_at - received_at)))
            FILTER (WHERE processed_at IS NOT NULL) AS avg_processing_time_sec,
        MAX(received_at) FILTER (WHERE processed_at IS NULL) AS oldest_pending
      FROM payment_inbox
      GROUP BY event_type$$,
    schedule => '10s',
    refresh_mode => 'DIFFERENTIAL'
);

-- === Ingestion (from Kafka consumer) ===

-- The consumer worker does:
--   msg = consumer.poll()
--   INSERT INTO payment_inbox (event_id, event_type, source, payload)
--     VALUES (msg.id, msg.headers['event_type'], msg.headers['source'], msg.value)
--     ON CONFLICT (event_id) DO NOTHING;
--   consumer.commit()

-- === Processing Function ===

CREATE OR REPLACE FUNCTION process_payment_inbox_event(
    p_event_id TEXT,
    p_payload JSONB
) RETURNS VOID AS $$
BEGIN
    -- Create payment intent (idempotent via UNIQUE on order_id)
    INSERT INTO payment_intents (order_id, customer_id, amount)
    VALUES (
        (p_payload->>'order_id')::int,
        (p_payload->>'customer_id')::int,
        (p_payload->>'total')::numeric
    )
    ON CONFLICT (order_id) DO NOTHING;

    -- Mark inbox event as processed
    UPDATE payment_inbox
    SET processed_at = now()
    WHERE event_id = p_event_id;
END;
$$ LANGUAGE plpgsql;

-- === Monitoring ===

-- Check inbox health
SELECT * FROM payment_inbox_stats;

-- Get staleness
SELECT pgtrickle.get_staleness('pending_payments');

-- Listen for alerts
LISTEN pg_trickle_alert;
-- Emits: {"event":"stale_data","pgt_name":"pending_payments","staleness_seconds":15}
```

---

## Complementary PostgreSQL Extensions

### pgmq — As the Inbox Transport

[pgmq](https://github.com/pgmq/pgmq) can serve as the inbox itself, replacing
the custom inbox table with a pgmq queue. This gives you visibility timeouts,
exactly-once semantics, and built-in archival.

```sql
CREATE EXTENSION pgmq;

-- Create inbox queue
SELECT pgmq.create('payment_inbox');

-- Ingest: enqueue message (from external consumer)
SELECT pgmq.send('payment_inbox',
    '{"event_id": "abc-123", "event_type": "OrderCreated", "payload": {...}}'::jsonb
);

-- Process: read with visibility timeout
SELECT * FROM pgmq.read('payment_inbox', vt => 60, qty => 10);

-- After processing, delete
SELECT pgmq.delete('payment_inbox', msg_id => 42);

-- pg_trickle stream table on top of pgmq queue table for monitoring
SELECT pgtrickle.create_stream_table(
    'pgmq_inbox_depth',
    $$SELECT COUNT(*) AS queue_depth,
             MIN(enqueued_at) AS oldest_message,
             MAX(read_ct) AS max_read_count
      FROM pgmq.q_payment_inbox
      WHERE vt <= now()$$,
    schedule => '5s',
    refresh_mode => 'DIFFERENTIAL'
);
```

**Advantages of pgmq as inbox:**
- Visibility timeout prevents duplicate concurrent processing.
- Built-in `read_ct` tracks how many times a message was read (retry counter).
- Archive table provides audit trail.
- pg_trickle stream table adds real-time monitoring dashboards on top.

### pg_cron — Scheduled Inbox Cleanup

[pg_cron](https://github.com/citusdata/pg_cron) can automate garbage
collection of processed inbox messages.

```sql
-- Clean up processed messages older than 30 days
SELECT cron.schedule(
    'inbox_cleanup',
    '0 3 * * *',  -- daily at 3 AM
    $$DELETE FROM payment_inbox
      WHERE processed_at IS NOT NULL
        AND processed_at < now() - INTERVAL '30 days'$$
);

-- Retry dead letter messages (reset retry count) weekly
SELECT cron.schedule(
    'inbox_retry_dead_letters',
    '0 6 * * 1',  -- Monday at 6 AM
    $$UPDATE payment_inbox
      SET retry_count = 0, error = NULL
      WHERE processed_at IS NULL
        AND retry_count >= 5
        AND received_at > now() - INTERVAL '7 days'$$
);
```

### pg_partman — Inbox Table Partitioning

For high-volume inboxes, [pg_partman](https://github.com/pgpartman/pg_partman)
manages time-based partitioning automatically.

```sql
CREATE TABLE payment_inbox (
    event_id     TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    source       TEXT NOT NULL,
    payload      JSONB NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ,
    error        TEXT,
    retry_count  INT NOT NULL DEFAULT 0,
    PRIMARY KEY (event_id, received_at)
) PARTITION BY RANGE (received_at);

-- pg_partman manages partition creation/retention
SELECT partman.create_parent(
    p_parent_table  => 'public.payment_inbox',
    p_control       => 'received_at',
    p_interval      => 'daily',
    p_premake       => 7
);

-- Old partitions can be detached/dropped cheaply (vs DELETE)
```

### pgflow — Durable Inbox Processing Workflows

[pgflow](https://pgflow.dev/) can orchestrate complex processing logic
triggered by inbox events — multi-step workflows with retry, timeout, and
compensation logic.

**Synergy:** pg_trickle detects new inbox messages via stream tables; pgflow
orchestrates the processing workflow (e.g., validate → enrich → store →
notify).

### NATS / JetStream — As the Inbox Transport Layer

[NATS](https://nats.io/) with its persistent streaming layer **JetStream**
can serve as the transport that delivers messages into the PostgreSQL inbox
table. NATS is a CNCF incubating project — a single ~10 MB binary providing
pub/sub, request/reply, and durable streaming with sub-millisecond latency.

**Key JetStream features relevant to the inbox pattern:**

| Feature | Benefit for Inbox |
|---------|-------------------|
| Durable consumers | Resume from last ack after consumer restart |
| Exactly-once delivery | Double-ack protocol prevents duplicate delivery to consumer |
| Per-subject ordering | Messages for the same entity arrive in order |
| Redelivery with backoff | Unacked messages redeliver with configurable backoff |
| Work queue mode | Automatic load balancing across multiple inbox writers |
| Dead letter (`MaxDeliver`) | Messages exceeding max delivery attempts are surfaced |
| Wildcard subscriptions | `payments.>` receives all payment-related events |

**Architecture: NATS JetStream → PostgreSQL Inbox → pg_trickle:**

```
┌────────────────────────────────────────────────────────────────────────┐
│  Upstream Services                                                     │
│  (Order Service, Shipping Service, ...)                                │
│       │                                                                │
│       │ nats.js_publish('payments.order_created', payload)             │
│       ▼                                                                │
│  ┌──────────────────────────────────────┐                              │
│  │ NATS JetStream                       │                              │
│  │ Stream: PAYMENTS                     │                              │
│  │ Subjects: payments.>                 │                              │
│  │ Retention: WorkQueue                 │                              │
│  └──────────────┬───────────────────────┘                              │
│                 │ Pull consumer (durable)                               │
│                 ▼                                                       │
│  ┌──────────────────────────────────────┐                              │
│  │ Inbox Writer (application worker)    │                              │
│  │ 1. msg = consumer.fetch(batch=50)    │                              │
│  │ 2. INSERT INTO inbox ON CONFLICT     │                              │
│  │    DO NOTHING                        │                              │
│  │ 3. msg.ack()                         │                              │
│  └──────────────┬───────────────────────┘                              │
│                 │                                                       │
│                 ▼                                                       │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ PostgreSQL                                                       │  │
│  │                                                                  │  │
│  │  inbox_events table                                              │  │
│  │       │ CDC trigger (automatic)                                  │  │
│  │       ▼                                                          │  │
│  │  pending_inbox (stream table, DIFFERENTIAL, 1s)                  │  │
│  │       │                                                          │  │
│  │       ▼                                                          │  │
│  │  Processing workers                                              │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

**Inbox writer consuming from NATS JetStream:**

```python
import nats
from nats.js import JetStreamContext
import psycopg2
import json
import asyncio

async def inbox_writer():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    # Create a durable pull consumer for the PAYMENTS stream
    consumer = await js.pull_subscribe(
        "payments.>",
        durable="payment-inbox-writer",
        stream="PAYMENTS",
    )

    conn = psycopg2.connect("postgresql://localhost/mydb")

    while True:
        try:
            messages = await consumer.fetch(batch=50, timeout=5)
        except nats.errors.TimeoutError:
            continue

        with conn.cursor() as cur:
            for msg in messages:
                payload = json.loads(msg.data)
                event_id = msg.headers.get("Nats-Msg-Id", msg.reply)

                cur.execute("""
                    INSERT INTO inbox_events (event_id, event_type, source, payload)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                """, (
                    event_id,
                    msg.subject.split(".")[-1],   # e.g. 'order_created'
                    msg.headers.get("Source", "unknown"),
                    json.dumps(payload),
                ))

                await msg.ack()  # Ack AFTER successful INSERT

            conn.commit()
```

**Double-ack exactly-once flow:**

1. NATS delivers message to the inbox writer (visibility timeout starts).
2. Writer INSERTs into `inbox_events` with `ON CONFLICT DO NOTHING`.
3. Writer calls `msg.ack()` — NATS marks the message as consumed.
4. If the writer crashes between step 2 and 3, NATS redelivers. The
   `ON CONFLICT` clause deduplicates, so no duplicate processing occurs.
5. pg_trickle's stream table materializes the new pending event within ~1s.
6. Processing workers pick up the event from the stream table.

**NATS vs. Kafka/RabbitMQ as inbox transport:**

| Factor | Kafka | RabbitMQ | NATS JetStream |
|--------|-------|----------|----------------|
| Publish latency | ~5-10ms | ~2-5ms | <1ms |
| Operational complexity | High (ZooKeeper/KRaft, brokers) | Medium (Erlang cluster) | Low (single binary) |
| Binary size | ~500 MB+ | ~250 MB+ | ~10 MB |
| Built-in deduplication | Manual (idempotent producer) | Manual | `Nats-Msg-Id` header |
| Consumer groups | Built-in (partition-based) | Queue-based | Work queue mode |
| Dead letter handling | Manual DLQ topic | Built-in DLX | `MaxDeliver` + advisory |
| Ordering | Per-partition | Per-queue | Per-subject |
| Edge deployment | Impractical | Possible | Designed for edge |

**When to choose NATS for the inbox transport:**
- Low-ops environments where running Kafka is too heavy.
- Edge or IoT deployments with constrained resources.
- Sub-millisecond message delivery latency is important.
- You want built-in deduplication and redelivery without custom logic.
- You already use NATS for service-to-service communication.

**When Kafka is still preferred:**
- Existing Kafka ecosystem (Connect, Schema Registry, ksqlDB).
- Multi-datacenter replication with MirrorMaker.
- Very high throughput (millions of messages/sec) with long retention.

---

## Potential pg_trickle Extensions

### Extension 1: Inbox Table Helper

A convenience function to create the inbox table with best-practice schema,
indexes, and stream tables.

```sql
-- Proposed API
SELECT pgtrickle.create_inbox(
    inbox_name    => 'payment_inbox',
    max_retries   => 5,
    schedule      => '1s',
    with_dead_letter => true,
    with_stats    => true
);

-- Creates:
-- 1. payment_inbox table with standard schema
-- 2. pending_payment_inbox stream table (WHERE processed_at IS NULL)
-- 3. payment_inbox_dead_letters stream table (WHERE retry_count >= max_retries)
-- 4. payment_inbox_stats stream table (aggregate counts/latencies)
-- 5. Partial indexes on pending rows
```

**Effort:** Medium. SQL generation + metadata tracking.

### Extension 2: Deduplication Stream Table

A specialized stream table that tracks which event IDs have been seen, enabling
cross-service deduplication.

```sql
-- Proposed: automatic dedup tracking
SELECT pgtrickle.create_stream_table(
    'inbox_dedup_log',
    $$SELECT DISTINCT event_id, MIN(received_at) AS first_seen,
             COUNT(*) AS delivery_count
      FROM inbox_events
      GROUP BY event_id
      HAVING COUNT(*) > 1$$,
    schedule => '30s',
    refresh_mode => 'DIFFERENTIAL'
);

-- This surfaces duplicate deliveries for monitoring/alerting
```

### Extension 3: Dead Letter Queue Stream Table

Automatic DLQ materialization with alerting when messages fail repeatedly.

```sql
-- The dead_letter stream table emits pg_trickle_alert when new rows appear
-- Proposed enhancement: configurable alert thresholds
SELECT pgtrickle.create_stream_table(
    'inbox_dlq',
    $$SELECT event_id, event_type, error, retry_count, received_at
      FROM inbox_events
      WHERE processed_at IS NULL AND retry_count >= 5$$,
    schedule => '10s',
    refresh_mode => 'DIFFERENTIAL'
    -- alert_on_new_rows => true  (proposed feature)
);
```

### Extension 4: Inbox Health Dashboard

A composite monitoring view that combines all inbox metrics.

```sql
-- Proposed: built-in health check function
SELECT pgtrickle.inbox_health('payment_inbox');

-- Returns:
-- {
--   "pending_count": 42,
--   "dead_letter_count": 3,
--   "avg_processing_time_ms": 120,
--   "oldest_pending_age_sec": 5.2,
--   "throughput_per_sec": 85.3,
--   "duplicate_rate_pct": 0.02,
--   "health_status": "healthy"
-- }
```

---

## Design Considerations

### Idempotency

The inbox pattern's primary purpose is enabling idempotent processing.
Recommended strategies:

| Strategy | How | When to Use |
|----------|-----|-------------|
| **Primary key dedup** | `ON CONFLICT (event_id) DO NOTHING` | Always — first line of defense |
| **Idempotency key in business table** | `UNIQUE (order_id)` on `payment_intents` | When processing creates new entities |
| **Processed events log** | Separate `processed_events` table checked before processing | When side effects can't be made naturally idempotent |
| **Version checking** | Compare `sequence_num` before applying state change | When message ordering matters |

### Ordering Guarantees

| Scope | Strategy |
|-------|----------|
| Single aggregate | `ORDER BY sequence_num` per `aggregate_id` |
| Cross-aggregate | No ordering guarantee (each aggregate independent) |
| Gap detection | Stream table surfacing missing `sequence_num` values |
| Hold-and-wait | Only process `sequence_num = last_processed + 1` |

### Retry and Backoff

```sql
-- Exponential backoff via retry_count
-- Only process messages where enough time has elapsed since last attempt
SELECT pgtrickle.create_stream_table(
    'inbox_ready_for_retry',
    $$SELECT event_id, event_type, payload, retry_count
      FROM inbox_events
      WHERE processed_at IS NULL
        AND retry_count < 5
        AND (retry_count = 0 OR
             received_at + (INTERVAL '1 second' * POWER(2, retry_count))
               < now())$$,
    schedule => '5s',
    refresh_mode => 'DIFFERENTIAL'
);
```

### Garbage Collection

| Strategy | Mechanism | Overhead |
|----------|-----------|----------|
| DELETE old rows | `DELETE WHERE processed_at < now() - '30 days'` | Table bloat, vacuum overhead |
| Partitioning | `DETACH PARTITION` + `DROP TABLE` | Near-zero (instant partition drop) |
| Archive table | Move to `inbox_archive` after processing | Extra INSERT during processing |
| TTL (with pg_cron) | Scheduled DELETE job | Predictable, manageable |

### Monitoring and Alerting

pg_trickle provides built-in monitoring that maps naturally to inbox health:

| Metric | pg_trickle Feature |
|--------|--------------------|
| Pending queue depth | `SELECT COUNT(*) FROM pending_inbox` (stream table) |
| Processing latency | `inbox_stats` stream table `avg_processing_time_sec` |
| Dead letter count | `dead_letters` stream table row count |
| Oldest pending age | `pgtrickle.get_staleness('pending_inbox')` |
| Queue backing up | `pg_trickle_alert` → `stale_data` event |
| Refresh failures | `pg_trickle_alert` → `refresh_failed` event |
| Throughput | `pgtrickle.st_refresh_stats` → rows_inserted/deleted per refresh |

---

## Schema Evolution & Message Versioning

As upstream services evolve, the shape of inbound messages changes. A robust
inbox implementation must accept multiple schema versions gracefully.

### Versioning Strategies

| Strategy | Description | Pros | Cons |
|----------|-------------|------|------|
| **Version field in inbox row** | `schema_version INTEGER` column | Filter per version in stream table | Extra column |
| **Event type suffix** | `OrderCreated.v2` in `event_type` | Easy routing; clear in DLQ | Type proliferation |
| **Version in JSONB payload** | `payload->>'schema_version'` | No schema change required | Consumers must parse payload |
| **Schema registry** | External Confluent / Apicurio lookup | Centralised governance | Additional dependency |

### Backward Compatibility Best Practices

- **Add** optional payload fields freely — existing processors ignore unknown keys.
- **Never remove** required fields without a grace period and version bump.
- **Document** every schema version in a companion `inbox_schema_versions` table.

```sql
CREATE TABLE inbox_schema_versions (
    event_type      TEXT NOT NULL,
    schema_version  INTEGER NOT NULL,
    introduced_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    deprecated_at   TIMESTAMPTZ,
    json_schema     JSONB,  -- optional: store JSON Schema for validation
    PRIMARY KEY (event_type, schema_version)
);
```

### Version-Aware Processing Stream Tables

```sql
-- Route v1 events to a processor that handles the old schema
SELECT pgtrickle.create_stream_table(
    'inbox_pending_v1',
    $$SELECT event_id, event_type, payload, received_at
      FROM inbox_events
      WHERE processed_at IS NULL
        AND (payload->>'schema_version')::int = 1$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);

-- Route v2+ events to a processor that handles the new schema
SELECT pgtrickle.create_stream_table(
    'inbox_pending_v2',
    $$SELECT event_id, event_type, payload, received_at
      FROM inbox_events
      WHERE processed_at IS NULL
        AND (payload->>'schema_version')::int >= 2$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);
```

### Rolling Schema Migration

1. **Deploy** inbox writer that accepts both old and new schema.
2. **Deploy** processors that handle both versions.
3. **Drain** old-version `inbox_pending_v1` to zero.
4. **Retire** the v1 processing path once the upstream producer is fully migrated.

---

## Observability & Distributed Tracing

End-to-end visibility across broker → inbox writer → processor is essential for
diagnosing duplicate delivery, ordering gaps, and processing failures.

### Correlation IDs

Propagate trace context from the inbound message into the inbox row:

```sql
ALTER TABLE inbox_events
    ADD COLUMN trace_id UUID,
    ADD COLUMN span_id  UUID;

-- Store NATS/Kafka trace headers when writing to inbox
-- (set by the inbox writer process before INSERT)
```

```python
async def inbox_writer(msg):
    headers = msg.headers or {}
    await pg_conn.execute(
        """INSERT INTO inbox_events
               (event_id, event_type, aggregate_id, payload, trace_id)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (event_id) DO NOTHING""",
        msg_id, event_type, aggregate_id, payload,
        headers.get("X-Trace-Id"),  # propagate distributed trace
    )
```

### Monitoring Dashboard Stream Table

```sql
SELECT pgtrickle.create_stream_table(
    'inbox_observability',
    $$SELECT
        event_type,
        COUNT(*) FILTER (WHERE processed_at IS NULL)         AS pending_count,
        COUNT(*) FILTER (WHERE processed_at IS NOT NULL)     AS processed_count,
        COUNT(*) FILTER (WHERE retry_count >= 5)             AS dlq_count,
        AVG(EXTRACT(EPOCH FROM (processed_at - received_at)))
            FILTER (WHERE processed_at IS NOT NULL)          AS avg_processing_latency_sec,
        MAX(EXTRACT(EPOCH FROM (now() - received_at)))
            FILTER (WHERE processed_at IS NULL
              AND retry_count < 5)                           AS max_pending_age_sec
      FROM inbox_events
      GROUP BY event_type$$,
    schedule => '10s',
    refresh_mode => 'DIFFERENTIAL'
);
```

### pg_trickle Alert Integration

```python
async def monitor_inbox_pipeline(dsn: str):
    conn = await asyncpg.connect(dsn)

    async def handle_alert(conn, pid, channel, payload):
        alert = json.loads(payload)
        match alert["type"]:
            case "stale_data":
                if "pending_inbox" in alert["stream_table"]:
                    sentry.capture_message(
                        f"Inbox queue backing up: {alert['staleness_seconds']}s"
                    )
            case "refresh_failed":
                pagerduty.trigger(f"Inbox stream table failed: {alert['detail']}")

    await conn.add_listener("pg_trickle_alert", handle_alert)
    await asyncio.sleep(float("inf"))
```

### OpenTelemetry Spans in the Processor

```python
from opentelemetry import trace

tracer = trace.get_tracer("inbox-processor")

async def process_event(event: dict):
    with tracer.start_as_current_span(
        "inbox.process",
        context=trace_context_from_id(event.get("trace_id")),
        attributes={
            "event.id":      event["event_id"],
            "event.type":    event["event_type"],
            "aggregate.id":  str(event["aggregate_id"]),
            "retry.count":   event["retry_count"],
        }
    ) as span:
        await apply_business_logic(event)
        span.set_attribute("processing.outcome", "success")
```

### Key Metrics to Track

| Metric | How to Measure | Alert Threshold |
|--------|---------------|------------------|
| **Inbox depth** | `pending_count` in `inbox_observability` | > 1 000 events |
| **Processing latency p99** | `avg_processing_latency_sec` | > 60 s |
| **DLQ depth** | `dlq_count` or `dead_letters` stream table | > 0 |
| **Oldest pending message** | `max_pending_age_sec` | > 5 min |
| **Stream table refresh failure** | `pg_trickle_alert` → `refresh_failed` | Built-in |
| **Ordering gaps** | `inbox_ordering_gaps` stream table row count | Any gap > 30 s |

---

## Testing Strategies

Testing the transactional inbox requires verifying idempotency, ordering, and
processor recovery under failure.

### Unit Tests — Deduplication

Verify that duplicate messages are silently dropped:

```sql
-- pgTAP: second insert of same event_id is a no-op
INSERT INTO inbox_events (event_id, event_type, payload)
    VALUES ('evt-001', 'PaymentReceived', '{"amount": 100}');

INSERT INTO inbox_events (event_id, event_type, payload)
    VALUES ('evt-001', 'PaymentReceived', '{"amount": 100}')
    ON CONFLICT (event_id) DO NOTHING;

SELECT is(
    (SELECT COUNT(*)::int FROM inbox_events WHERE event_id = 'evt-001'),
    1,
    'Duplicate event_id silently dropped by ON CONFLICT'
);
```

### Integration Tests — Stream Table Refresh

```rust
#[tokio::test]
async fn test_pending_inbox_reflects_unprocessed_events() {
    let ctx = TestContext::new().await;

    ctx.execute("INSERT INTO inbox_events (event_id, event_type, payload)
                 VALUES ('e1', 'PaymentReceived', '{\"amount\": 50}')").await;

    ctx.execute("SELECT pgtrickle.refresh('pending_inbox')").await;

    let count: i64 = ctx.query_one(
        "SELECT COUNT(*) FROM pending_inbox WHERE event_type = 'PaymentReceived'"
    ).await;
    assert_eq!(count, 1);

    // Mark as processed — must disappear from stream table on next refresh
    ctx.execute("UPDATE inbox_events SET processed_at = now() WHERE event_id = 'e1'").await;
    ctx.execute("SELECT pgtrickle.refresh('pending_inbox')").await;

    let count_after: i64 = ctx.query_one(
        "SELECT COUNT(*) FROM pending_inbox"
    ).await;
    assert_eq!(count_after, 0, "Processed event must leave the stream table");
}
```

### End-to-End Tests — Inbox Writer + Processor

```python
@pytest.mark.asyncio
async def test_message_processed_exactly_once(nats_client, pg_conn):
    """Message published to NATS must be processed exactly once in PostgreSQL."""
    # Publish the same event twice (simulates at-least-once broker delivery)
    payload = json.dumps({"amount": 75.00, "order_id": "ord-1"})
    for _ in range(2):
        await js.publish(
            "payments.received",
            payload.encode(),
            headers={"Nats-Msg-Id": "evt-dedup-001"},
        )

    await asyncio.sleep(3.0)  # allow inbox writer and processor to run

    processed = await pg_conn.fetchval(
        "SELECT COUNT(*) FROM inbox_events WHERE event_id = 'evt-dedup-001'"
    )
    assert processed == 1, "Duplicate delivery must result in exactly one inbox row"
```

### Chaos Tests — Processor Failure & Retry

```python
@pytest.mark.asyncio
async def test_failed_event_retried_with_backoff(pg_conn, processor):
    """Events that fail processing are retried according to backoff schedule."""
    # Insert an event that will fail on first attempt
    await pg_conn.execute(
        "INSERT INTO inbox_events (event_id, event_type, payload)"
        " VALUES ('fail-evt', 'PaymentReceived', '{\"amount\": -1}')"  # invalid
    )

    await processor.run_once()  # first attempt — should fail and increment retry_count

    retry_count = await pg_conn.fetchval(
        "SELECT retry_count FROM inbox_events WHERE event_id = 'fail-evt'"
    )
    assert retry_count == 1, "retry_count must increment after failed processing"

    error_msg = await pg_conn.fetchval(
        "SELECT last_error FROM inbox_events WHERE event_id = 'fail-evt'"
    )
    assert error_msg is not None, "last_error must be populated on failure"
```

---

## Multi-Tenancy

In multi-tenant SaaS applications, the inbox pipeline must isolate messages per
tenant and prevent cross-tenant processing.

### Tenant-Per-Row Isolation

```sql
-- Add tenant_id to inbox table
ALTER TABLE inbox_events ADD COLUMN tenant_id UUID NOT NULL;

-- Row-Level Security to prevent cross-tenant access
ALTER TABLE inbox_events ENABLE ROW LEVEL SECURITY;
CREATE POLICY inbox_tenant_isolation ON inbox_events
    USING (tenant_id = current_setting('app.tenant_id')::UUID);

-- Tenant-aware pending stream table (processor sets app.tenant_id per connection)
SELECT pgtrickle.create_stream_table(
    'pending_inbox_tenant_scoped',
    $$SELECT event_id, tenant_id, event_type, payload, retry_count
      FROM inbox_events
      WHERE processed_at IS NULL
        AND retry_count < 5$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);
```

### Tenant-Per-Schema Isolation

```sql
CREATE SCHEMA tenant_abc;
CREATE TABLE tenant_abc.inbox_events (LIKE public.inbox_events INCLUDING ALL);

SELECT pgtrickle.create_stream_table(
    'tenant_abc.pending_inbox',
    $$SELECT * FROM tenant_abc.inbox_events
      WHERE processed_at IS NULL AND retry_count < 5$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);
```

### Multi-Tenant Inbox Writer

```python
async def inbox_writer(msg):
    headers = msg.headers or {}
    tenant_id = headers.get("X-Tenant-Id") or extract_tenant_from_subject(msg.subject)

    # Validate tenant before writing — never trust broker subjects alone
    if not is_valid_tenant_id(tenant_id):
        await msg.nak()  # reject; do not persist
        return

    await pg_conn.execute(
        """INSERT INTO inbox_events
               (event_id, tenant_id, event_type, aggregate_id, payload)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (event_id) DO NOTHING""",
        msg_id, tenant_id, event_type, aggregate_id, payload,
    )
    await msg.ack()
```

---

## Cost Analysis

### Storage Cost

| Component | Typical Size per Message | Notes |
|-----------|--------------------------|-------|
| `inbox_events` row | ~200–600 bytes | JSONB payload dominates |
| `processed_events` dedup log | ~50–100 bytes | event_id + timestamps only |
| Stream table | ~same as inbox rows | Materialized view |
| Index overhead | ~40–60% of table size | PK + event_type + processed_at indexes |

**Example:** 500 000 messages/day × 400 bytes = ~200 MB/day. With 14-day
retention: ~2.8 GB. With monthly partitioning, expired months drop instantly.

### Refresh CPU Overhead

| Mode | CPU Impact | Latency | Use When |
|------|------------|---------|----------|
| DIFFERENTIAL | Low (only changed rows) | ~50–200 ms | Default for most inbox cases |
| FULL | High (full table scan) | seconds | Complex DISTINCT ON + aggregations |
| IMMEDIATE | Minimal (synchronous) | ~0 ms additional | Synchronous, transactional processing |

### Infrastructure Comparison

| Setup | Additional Infrastructure | Estimated Monthly Cost (small–medium) |
|-------|--------------------------|----------------------------------------|
| PostgreSQL + pgmq only | None | $0 additional |
| PostgreSQL + NATS | ~10 MB binary | ~$5–20/mo |
| PostgreSQL + Kafka | KRaft cluster + brokers | $50–500/mo (managed) |
| PostgreSQL + Debezium | Kafka + Kafka Connect | $100–1 000/mo (managed) |

> **Note:** For inbox workloads under 100 000 messages/sec, NATS JetStream
> with durable pull consumers is often the lowest-ops choice.

---

## Security Considerations

### Validating Inbound Messages

Never trust message payloads from external sources — validate before inserting
into the inbox:

```python
import jsonschema

ORDER_CREATED_SCHEMA_V2 = {
    "type": "object",
    "required": ["order_id", "amount", "customer_id"],
    "properties": {
        "order_id":    {"type": "string", "format": "uuid"},
        "amount":      {"type": "number", "minimum": 0},
        "customer_id": {"type": "string"},
    },
    "additionalProperties": True,  # tolerate forward-compatible additions
}

async def validated_inbox_writer(msg):
    payload = json.loads(msg.data)
    try:
        jsonschema.validate(payload, ORDER_CREATED_SCHEMA_V2)
    except jsonschema.ValidationError as e:
        # Write to DLQ instead of inbox; do NOT nak — prevents infinite redelivery
        await pg_conn.execute(
            "INSERT INTO inbox_dead_letters (event_id, reason, payload)"
            " VALUES ($1, $2, $3)",
            msg_id, str(e), json.dumps(payload),
        )
        await msg.ack()  # ack to prevent redelivery of permanently-invalid messages
        return

    await write_to_inbox(msg, payload)
```

### Payload Encryption for Sensitive Data

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Encrypt PII fields before storing in the inbox
INSERT INTO inbox_events (event_id, event_type, payload)
VALUES (
    $1,
    'PaymentReceived',
    jsonb_build_object(
        'order_id', $2,
        'amount',   $3,
        -- Encrypt cardholder data at rest; processor decrypts on access
        'card_pii', encode(
            pgp_sym_encrypt($4::bytea, current_setting('app.encryption_key')),
            'base64'
        )
    )
);
```

### Database-Level Access Controls

```sql
-- Dedicated role for the inbox writer — INSERT only, no SELECT
CREATE ROLE inbox_writer LOGIN PASSWORD '...';
GRANT USAGE ON SCHEMA public TO inbox_writer;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM inbox_writer;
GRANT INSERT ON inbox_events TO inbox_writer;

-- Dedicated role for the processor — SELECT + UPDATE only
CREATE ROLE inbox_processor LOGIN PASSWORD '...';
GRANT SELECT, UPDATE ON inbox_events TO inbox_processor;
GRANT SELECT ON pending_inbox TO inbox_processor;
```

### TLS and Broker Authentication

```python
import ssl, nats

# Always use TLS + credentials for broker connections in production
nc = await nats.connect(
    servers=["tls://nats.example.com:4222"],
    tls=ssl.create_default_context(cafile="/etc/ssl/nats-ca.pem"),
    tls_hostname="nats.example.com",
    user_credentials="/etc/nats/inbox-writer.creds",
)
```

### Immutable Processing Log

```sql
-- Record every processing attempt; never UPDATE or DELETE
CREATE TABLE inbox_processing_log (
    id           BIGSERIAL PRIMARY KEY,
    event_id     TEXT NOT NULL REFERENCES inbox_events(event_id),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    outcome      TEXT NOT NULL CHECK (outcome IN ('success', 'failure', 'duplicate')),
    error_detail TEXT,
    processor_id TEXT  -- hostname or pod name
);

ALTER TABLE inbox_processing_log ENABLE ROW LEVEL SECURITY;
CREATE POLICY log_insert_only ON inbox_processing_log
    FOR INSERT WITH CHECK (TRUE);
```

---

## Combining Outbox and Inbox Patterns

In a microservice architecture, services typically use BOTH patterns:

- **Outbox** for publishing events to downstream services.
- **Inbox** for consuming events from upstream services.

pg_trickle can power both sides:

```
┌─────────────────────────────────────────────────────────────────────┐
│                       Order Service                                  │
│                                                                      │
│  ┌──────────┐    ┌──────────────────┐    ┌────────────────────┐     │
│  │  orders   │───→│ outbox_events    │───→│ pending_outbox     │──── │──→ Kafka
│  │ (table)   │    │ (table + trigger)│    │ (stream table)     │     │
│  └──────────┘    └──────────────────┘    └────────────────────┘     │
│                                                                      │
│  Kafka ──→ ┌──────────────────┐    ┌────────────────────┐           │
│            │ inbox_events     │───→│ pending_inbox       │           │
│            │ (table)          │    │ (stream table)      │           │
│            └──────────────────┘    └────────────────────┘           │
│                                                                      │
│  Stream tables for monitoring:                                       │
│    - outbox_stats, inbox_stats, dead_letters                         │
└─────────────────────────────────────────────────────────────────────┘
```

```sql
-- Both patterns in one service, powered by pg_trickle:

-- OUTBOX: publish order events
SELECT pgtrickle.create_stream_table('pending_outbox',
    $$SELECT * FROM outbox_events WHERE published_at IS NULL$$,
    schedule => '1s', refresh_mode => 'DIFFERENTIAL');

-- INBOX: consume payment confirmation events
SELECT pgtrickle.create_stream_table('pending_inbox',
    $$SELECT * FROM inbox_events WHERE processed_at IS NULL AND retry_count < 5$$,
    schedule => '1s', refresh_mode => 'DIFFERENTIAL');

-- UNIFIED MONITORING: single stats view
SELECT pgtrickle.create_stream_table('message_health',
    $$SELECT 'outbox' AS direction, event_type,
             COUNT(*) FILTER (WHERE published_at IS NULL) AS pending
      FROM outbox_events GROUP BY event_type
      UNION ALL
      SELECT 'inbox' AS direction, event_type,
             COUNT(*) FILTER (WHERE processed_at IS NULL) AS pending
      FROM inbox_events GROUP BY event_type$$,
    schedule => '10s', refresh_mode => 'DIFFERENTIAL');
```

---

## Comparison with Traditional Approaches

| Aspect | Traditional Inbox | pg_trickle Inbox | pgmq Inbox | pg_trickle + NATS JetStream |
|--------|-------------------|-------------------|------------|-----------------------------|
| Processing queue view | Custom SQL query each time | Pre-materialized stream table | Built-in `pgmq.read()` | Stream table (post-ingest) |
| Deduplication | Manual `ON CONFLICT` | `ON CONFLICT` + `DISTINCT ON` stream table | Manual (by `msg_id`) | `Nats-Msg-Id` + `ON CONFLICT` |
| Ordering | Manual `ORDER BY` + gap tracking | Stream table with gap detection | FIFO within queue | Per-subject (JetStream) + stream table |
| Dead letter queue | Manual query | Materialized stream table with alerts | Manual (check `read_ct`) | `MaxDeliver` + DLQ stream table |
| Monitoring | Custom queries | Built-in staleness, alerts, stats | Basic (`read_ct`, queue depth) | pg_trickle alerts + NATS advisories |
| Competing consumers | `FOR UPDATE SKIP LOCKED` | `FOR UPDATE SKIP LOCKED` + stream table stats | Visibility timeout | JetStream work queues → inbox table |
| Retry backoff | Manual calculation | Stream table with backoff filter | Visibility timeout extension | NATS redelivery backoff + stream table |
| Infrastructure | PostgreSQL only | PostgreSQL only | PostgreSQL only | PostgreSQL + NATS (~10 MB binary) |
| Throughput overhead | Full-table scan per poll | DIFFERENTIAL (only changed rows) | Index scan per read | NATS push + DIFFERENTIAL |
| Latency | Poll interval | ~1s (DIFFERENTIAL) or ~0ms (IMMEDIATE) | ~0ms (direct read) | <1ms (NATS) + ~1s (stream table) |

---

## References

- Chris Richardson, [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
  (includes inbox discussion)
- Krzysztof Atłasik, [Microservices 101: Transactional Outbox and Inbox](https://softwaremill.com/microservices-101/)
- Wikipedia, [Inbox and Outbox Pattern](https://en.wikipedia.org/wiki/Inbox_and_outbox_pattern)
- [pgmq — PostgreSQL Message Queue](https://github.com/pgmq/pgmq)
- [pgflow — Durable Workflow Engine](https://pgflow.dev/)
- [pg_partman — Partition Management](https://github.com/pgpartman/pg_partman)
- [NATS.io — Cloud-Native Messaging](https://nats.io/)
- [NATS JetStream Documentation](https://docs.nats.io/nats-concepts/jetstream)
- pg_trickle [ARCHITECTURE.md](../../docs/ARCHITECTURE.md), [PATTERNS.md](../../docs/PATTERNS.md), [SQL_REFERENCE.md](../../docs/SQL_REFERENCE.md)
- [PLAN_TRANSACTIONAL_OUTBOX.md](PLAN_TRANSACTIONAL_OUTBOX.md) — companion document for the outbox pattern
