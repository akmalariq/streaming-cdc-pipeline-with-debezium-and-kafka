# Study Notes: how this pipeline works, and how to break it on purpose

A guided walkthrough of the streaming CDC pipeline, followed by a lab where you deliberately
break it and watch the safety mechanisms work.

Read this with the repo open. Every claim here can be verified with a command.

---

## 1. The problem

Your OLTP database (Postgres) is the source of truth. A warehouse (ClickHouse) needs a copy for
analytics. The naive approach, a nightly `SELECT ... WHERE updated_at > yesterday`, fails three ways:

1. **Load**: full scans hurt the production database.
2. **Deletes are invisible**: a deleted row simply stops appearing, so the warehouse keeps it forever.
3. **Latency**: you only ever see yesterday's data.

**Change Data Capture (CDC)** reads the write-ahead log (WAL) instead of querying tables. Every
insert, update, and delete is already recorded there, in commit order, with before and after values.

The line that enables it in `infra/docker-compose.yml`:

```yaml
command: [postgres, -c, wal_level=logical, ...]
```

`wal_level=logical` makes Postgres publish row-level change data that a logical decoding plugin
(`pgoutput`) can stream to a client.

---

## 2. The five moving parts

```
Postgres (WAL)  ->  Debezium (decode)  ->  Kafka (log)  ->  Python consumer  ->  ClickHouse
   source            translator             buffer            transform          store
```

| Piece | Job | Why it cannot be skipped |
|---|---|---|
| Postgres | Writes change data to the WAL | It is the source of truth; we never poll it |
| Debezium | Decodes WAL, emits a standard JSON envelope | Handles replication slots, snapshots, and diffing |
| Kafka | Durable, ordered, replayable log | Decouples producer from consumer; events wait if the consumer is down |
| Consumer | Decode, transform, batch, write, commit | Where correctness lives |
| ClickHouse | Columnar analytics store | Merges duplicate writes cleanly (section 4) |

Two infra details to notice:

- **Healthchecks plus `depends_on: condition: service_healthy`**: Kafka Connect waits until Redpanda
  and Postgres are genuinely ready. `make up` uses `--wait`, so it returns only when healthy.
- **Redpanda as the broker**: same Kafka wire protocol, one container, no ZooKeeper or KRaft setup.
  The client code is standard `confluent-kafka`.

---

## 3. The Debezium envelope

```json
{
  "before": { "customer_id": 101, "city": "Jakarta", ... },
  "after":  { "customer_id": 101, "city": "Bandung", ... },
  "source": { "db": "shopdb", "schema": "public", "table": "customers", "ts_ms": 1789899890000 },
  "op": "u",
  "ts_ms": 1789899890123
}
```

| op | meaning | image that holds the data |
|---|---|---|
| `c` | create | `after` |
| `u` | update | `before` and `after` |
| `d` | delete | `before` only (`after` is null) |
| `r` | read (initial snapshot) | `after` |

`source.ts_ms` is the database change timestamp. It becomes the version number in section 4.

Connector settings that matter (`infra/debezium/postgres-connector.json`):

- `topic.prefix: shopdb` plus schema and table yields the topic `shopdb.public.customers`.
- `snapshot.mode: initial` snapshots existing rows as `op=r`, then streams live changes.
- `plugin.name: pgoutput` is Postgres's built-in logical decoding plugin.

---

## 4. The core idea: effectively-once

Kafka guarantees **at-least-once**. If the consumer crashes after writing but before recording its
position, the message is redelivered. Duplicates are inevitable. So we make duplicates harmless.

### (a) Write first, commit second

`src/cdc_pipeline/consumer.py`:

```python
def flush(self) -> int:
    written = 0
    for target, rows in self.buffers.items():
        if not rows:
            continue
        written += self.sink.write(target, rows)   # land the data
        rows.clear()
    if written:
        consumer = self._ensure_consumer()
        commit(asynchronous=False)                  # then record the position
```

Commit-then-write risks **data loss**. Write-then-commit risks **replay**. Replay is recoverable,
loss is not. `tests/test_consumer.py::test_flush_writes_before_committing_offsets` asserts the order
is exactly `["write", "commit"]`.

### (b) Version every row

`src/cdc_pipeline/transforms.py`:

```python
row["op"] = event.op
row["source_ts_ms"] = event.source_ts_ms
row["is_deleted"] = 1 if event.is_delete else 0
row["_version"] = event.source_ts_ms
```

### (c) Let the storage engine resolve duplicates

`infra/clickhouse/init.sql`:

```sql
ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY customer_id
```

`ReplacingMergeTree` keeps the row with the **highest `_version`** per key when parts merge. The
second argument (`is_deleted`) lets the winning row also mark a deletion.

Consequences:

- Replaying an old event is harmless: lower version, so it loses.
- Deletes need no physical `DELETE`: a soft flag with a newer version wins.
- Using **source** timestamps rather than ingestion time is what makes this safe, because ingestion
  order is not business order.

**Honest phrasing:** at-least-once delivery plus an idempotent merge, which behaves like
effectively-once. It is not a distributed transaction and not true exactly-once.

Because merges are lazy, `sink.count()` uses `FINAL` to force merge-on-read before counting.

---

## 5. The code path

- **`config.py`**: all settings come from environment variables. Topics are derived, not duplicated.
- **`events.py`**: validates the envelope. Rejects non-JSON, unknown `op`, missing `source.table`,
  and deletes without a `before` image. Bad data fails at the boundary.
- **`transforms.py`**: `SPECS` maps each table (target, primary key, columns, int columns, money
  columns). `to_row` chooses `before` for deletes and `after` otherwise, then coerces types.
- **`sink.py`**: explicit column ordering, insert, `count` with and without deletes, `OPTIMIZE`,
  metrics insert.
- **`consumer.py`**: the poll loop. Decode, transform, buffer, flush on batch size or interval,
  write then commit, route poison messages to the DLQ, sample lag.
- **`producer.py`**: a change-event simulator so the pipeline can be exercised without Postgres.
- **`cli.py`**: `simulate`, `consume`, `verify`, `register-connector`.

---

## 6. Lab: break it and fix it

Each lab is short, and each one teaches a specific guarantee. Run them in order.

### Lab 0: bring up the stack

```bash
make sync
make up
make register-connector
curl -s localhost:8083/connectors/shop-postgres-connector/status
```

Expect `"state":"RUNNING"` for both the connector and task 0.

### Lab 1: watch the log stream live

In one terminal:

```bash
docker exec -it cdc-redpanda rpk topic consume shopdb.public.customers --num 50
```

In another, change data:

```bash
docker exec -it cdc-postgres psql -U cdc_user -d shopdb \
  -c "INSERT INTO customers (customer_id, name, email, city) VALUES (500,'Watcher','w@example.com','Jakarta');" \
  -c "UPDATE customers SET city='Bandung' WHERE customer_id=500;" \
  -c "DELETE FROM customers WHERE customer_id=500;"
```

**Observe**: three envelopes with `op` of `c`, `u`, `d`. That is the raw material the consumer sees.

### Lab 2: prove the delete becomes a soft flag

```bash
docker exec -it cdc-postgres psql -U cdc_user -d shopdb \
  -c "INSERT INTO customers (customer_id, name, email, city) VALUES (501,'Ghost','g@example.com','Medan');"
sleep 3
make consume      # drains new events
```

Now query **without** collapsing merges:

```bash
docker exec cdc-clickhouse clickhouse-client --user cdc --password cdc_pw \
  --query "SELECT customer_id, city, op, is_deleted, _version FROM cdc.customers WHERE customer_id IN (501, 500) ORDER BY _version"
```

**Observe**: rows exist, and the delete (if any) shows `is_deleted = 1`. Then:

```bash
make verify       # uses FINAL, so merged state only
```

### Lab 3: kill the consumer mid-batch

This is the at-least-once lesson. Force one-row batches, then interrupt mid-drain:

```bash
CDC_BATCH_SIZE=1 uv run cdc consume --max-messages 100 --idle-timeout 30
```

Press `Ctrl+C` after it processes a few messages. Then check what ClickHouse holds:

```bash
make verify
```

Then drain again with a fresh group (which forces a full replay):

```bash
CDC_CONSUMER_GROUP=lab-replay uv run cdc consume --max-messages 100 --idle-timeout 8
make verify
```

**Observe**: the second `verify` shows the **same counts**. The interrupted batch was replayed, and
the versioned merge absorbed it. That is effectively-once in practice.

### Lab 4: send a poison message

```bash
docker exec cdc-redpanda rpk topic produce shopdb.public.customers --key '{"customer_id":999}' <<< 'this is not json'
CDC_CONSUMER_GROUP=lab-dlq uv run cdc consume --max-messages 50 --idle-timeout 8
```

**Observe**: the run logs `decode_failed`, `dlq_routed` increments, and the pipeline keeps going
instead of dying. Read the dead letter:

```bash
docker exec -it cdc-redpanda rpk topic consume cdc.dlq --num 1
```

### Lab 5: prove idempotency on purpose

```bash
make verify                        # note the counts
CDC_CONSUMER_GROUP=lab-idem-1 uv run cdc consume --max-messages 100 --idle-timeout 8
CDC_CONSUMER_GROUP=lab-idem-2 uv run cdc consume --max-messages 100 --idle-timeout 8
make verify                        # counts must be identical
```

Each new consumer group replays the entire log from the beginning. Duplicates are absorbed by design.

### Lab 6: decouple with the simulator

Stop the source from mattering:

```bash
make simulate                      # emits Debezium-shaped events directly to Kafka
make consume
make verify
```

**Observe**: data lands without Postgres or Debezium being involved. That is why Kafka sits in the
middle: the consumer depends on the log, not on the source.

### Lab 7: pause and resume the connector

```bash
curl -s -X PUT localhost:8083/connectors/shop-postgres-connector/pause
docker exec -it cdc-postgres psql -U cdc_user -d shopdb -c "UPDATE customers SET city='Surabaya' WHERE customer_id=1;"
curl -s -X PUT localhost:8083/connectors/shop-postgres-connector/resume
```

**Observe**: after resume, the event still arrives. The replication slot retains the position, so a
paused connector does not lose data.

### Lab 8: extend it with a new table

The real exercise. Add `products` end to end:

1. `infra/postgres/init.sql`: create the table.
2. `infra/debezium/postgres-connector.json`: add it to `table.include.list`.
3. `src/cdc_pipeline/transforms.py`: add an `EntitySpec` for it.
4. `infra/clickhouse/init.sql`: add a `ReplacingMergeTree` table.
5. `make down && make up && make register-connector`.
6. Insert a row, `make consume`, `make verify`.

If you can do Lab 8 unaided, you understand the whole pipeline.

---

## 7. The bug this pipeline shipped with (and why that matters)

**Symptom:** `cdc consume --max-messages 20` ran for five minutes and printed nothing.

**Diagnosis:**
1. Silence was misleading: `tail` buffers output until the process exits, so the real signal was
   "did not terminate", not "did nothing".
2. The defect was in the loop:

```python
while max_messages is None or self.metrics.processed < max_messages:
    message = consumer.poll(...)
```

With only 12 messages available and a target of 20, `poll()` returns `None` forever and `processed`
never reaches the target. An infinite loop with no data.

**Lesson:** any "wait for N items" loop needs a termination condition that does not depend on the
data arriving.

**Fix:** an idle timeout (`CDC_IDLE_TIMEOUT`, default 15s, `--idle-timeout` flag), plus a regression
test using a fake clock that advances one second per read so the test never actually waits.

---

## 8. Check your understanding

1. Why can an older replayed event never overwrite a newer row?
2. What breaks if you commit the offset before writing to the sink?
3. Why is a soft delete necessary instead of `DELETE FROM` in ClickHouse?
4. Why does `count()` use `FINAL`, and what would the number look like without it?
5. What did the replay prove, and what would break if we used ingestion timestamps?
6. What terminates a bounded consume run, and why is that needed?
7. Which single line in `docker-compose.yml` enables CDC, and why?

---

## 9. Where to take it next

- Schema registry and data contracts (reject schema drift to the dead-letter topic)
- Incremental snapshotting on a large table
- Prometheus metrics endpoint for lag and throughput
- A dbt gold layer on top of the ClickHouse sink
