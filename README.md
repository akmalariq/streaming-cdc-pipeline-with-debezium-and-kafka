# Streaming CDC Pipeline with Debezium and Kafka

![CI](https://github.com/akmalariq/streaming-cdc-pipeline-with-debezium-and-kafka/actions/workflows/ci.yml/badge.svg)

A production-shaped change data capture pipeline: **Postgres logical decoding via Debezium -> Kafka -> a Python consumer -> an idempotent ClickHouse sink**, with batching, dead-letter routing, consumer-lag sampling, and structured run metrics.

Built to exercise the parts of streaming that actually break in production: offset handling, duplicate delivery, deletes, poison messages, and schema-shaped payloads.

## Architecture

```
Postgres (wal_level=logical)
      |  row-level changes (INSERT / UPDATE / DELETE)
      v
Debezium Postgres connector  -->  Kafka topics
      (Kafka Connect)             shopdb.public.customers
                                  shopdb.public.orders
                                  shopdb.public.order_items
                                         |
                                         v
                              Python consumer (confluent-kafka)
                              - decode Debezium envelope (op c/u/d/r)
                              - transform to the ClickHouse row shape
                              - batch by size or flush interval
                              - write to ClickHouse, then commit offsets
                              - poison messages --> cdc.dlq
                                         |
                                         v
                            ClickHouse ReplacingMergeTree
                            ordered by primary key, versioned by source ts
```

## Delivery guarantee (stated honestly)

**At-least-once delivery plus an idempotent merge, which behaves like effectively-once.**

- Offsets are committed **only after** the sink write succeeds, so a crash replays at most one batch.
- Replays are harmless because every row carries `_version` (the source change timestamp) and ClickHouse `ReplacingMergeTree(_version, is_deleted)` keeps the highest version per key.
- Deletes are soft: `is_deleted = 1` with the delete's version, so an older replay cannot resurrect a deleted row.
- This is **not** a distributed transaction and **not** exactly-once. That distinction is deliberate.

## Quickstart

```bash
make sync                # uv sync --dev
make up                  # postgres, redpanda, debezium connect, clickhouse (waits for health)
make register-connector  # POST the Debezium connector to Kafka Connect
make verify              # counts before any new events
```

### 1. Consume

```bash
make consume             # bounded drain of 50 messages, prints run metrics
```

### 2. Or produce simulated events (no source database needed)

```bash
make simulate            # emits realistic create/update/delete envelopes
make consume
```

### 3. Or exercise real CDC end to end

```bash
make psql
```

```sql
INSERT INTO customers (customer_id, name, email, city)
VALUES (99, 'Test User', 'test@example.com', 'Jakarta');
UPDATE customers SET city = 'Bandung' WHERE customer_id = 99;
DELETE FROM customers WHERE customer_id = 99;
```

Then:

```bash
make consume
make verify              # cdc.customers shows the row with is_deleted = 1
```

### 4. Tear down

```bash
make down                # stops containers and deletes volumes
```

## Configuration

All settings are environment variables, so the same code runs anywhere.

| Variable | Default | Purpose |
|---|---|---|
| `CDC_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka (Redpanda) bootstrap |
| `CDC_CONNECT_URL` | `http://localhost:8083` | Kafka Connect REST endpoint |
| `CDC_TOPIC_PREFIX` | `shopdb` | Topic prefix matching the connector `topic.prefix` |
| `CDC_TABLES` | `customers,orders,order_items` | Source tables to subscribe to |
| `CDC_DLQ_TOPIC` | `cdc.dlq` | Dead-letter topic for poison messages |
| `CDC_CONSUMER_GROUP` | `cdc-clickhouse-sink` | Consumer group id |
| `CDC_AUTO_OFFSET_RESET` | `earliest` | Where to start when no offset is stored |
| `CDC_BATCH_SIZE` | `200` | Rows buffered before a flush |
| `CDC_FLUSH_INTERVAL` | `5` | Seconds before flushing a partial batch |
| `CDC_POLL_TIMEOUT` | `1.0` | Kafka poll timeout |
| `CDC_METRICS_INTERVAL` | `10` | Seconds between lag samples |
| `CDC_IDLE_TIMEOUT` | `15` | Seconds without a message before a bounded run stops |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host |
| `CLICKHOUSE_PORT` | `8124` | ClickHouse HTTP port |
| `CLICKHOUSE_USER` | `cdc` | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | `cdc_pw` | ClickHouse password |
| `CLICKHOUSE_DATABASE` | `cdc` | ClickHouse database |
| `CDC_DATA_DIR` | `data` | Where run reports are written |

## Observability

- **Structured JSON logs** on every lifecycle event (subscribe, page parsed, batch flushed, decode failure, shutdown).
- **Run report** per consumer run: processed, written, decode errors, DLQ routed, max lag.
- **`cdc.consumer_metrics`** table in ClickHouse for querying throughput and lag over time.
- **Dead-letter topic** `cdc.dlq` carrying the original payload plus `x-error` and `x-origin-topic` headers.

## Tests and CI

```bash
make lint    # ruff
make test    # pytest, offline, no broker required
```

Tests run without Kafka or ClickHouse: the consumer is tested with an injected fake consumer, sink, and producer, which also asserts the write-then-commit ordering and the DLQ path.

## Orchestration

```bash
uv sync --extra dagster
uv run dagster dev -f orchestration/dagster_pipeline.py
```

`cdc_batch_drain` runs a bounded drain under a retry policy and records metrics; `cdc_sink_health` reports row and active-row counts per table.

## Scope and honest limits

- **Batch and streaming boundary:** the consumer is a throughput-oriented batch committer, not a per-record low-latency processor.
- **No exactly-once:** documented above. If you need transactional sink semantics, that is a different design.
- **Schema evolution:** the Debezium connector runs with `schemas.enable=false`. A schema registry and data contracts would be the next step.
- **Scale:** this is single-partition-friendly demo scale, not a tuned multi-broker cluster.
- Redpanda is used as the broker because it speaks the Kafka API and starts as a single container; the client code is standard Kafka.

## Next steps

- Schema registry plus data contracts (reject schema drift to the DLQ)
- Incremental snapshotting on a large table
- Prometheus metrics endpoint for lag and throughput
- Click: connect the sink to dbt for a gold layer
