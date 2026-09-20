CREATE DATABASE IF NOT EXISTS cdc;

CREATE TABLE IF NOT EXISTS cdc.customers
(
    customer_id   UInt64,
    name          String,
    email         String,
    city          String,
    updated_at    DateTime64(3),
    op            LowCardinality(String),
    source_ts_ms  UInt64,
    ingested_at   DateTime DEFAULT now(),
    is_deleted    UInt8 DEFAULT 0,
    _version      UInt64
)
ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS cdc.orders
(
    order_id      UInt64,
    customer_id   UInt64,
    status        LowCardinality(String),
    amount        Decimal(12, 2),
    updated_at    DateTime64(3),
    op            LowCardinality(String),
    source_ts_ms  UInt64,
    ingested_at   DateTime DEFAULT now(),
    is_deleted    UInt8 DEFAULT 0,
    _version      UInt64
)
ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS cdc.order_items
(
    order_item_id UInt64,
    order_id      UInt64,
    sku           String,
    quantity      Int32,
    unit_price    Decimal(12, 2),
    updated_at    DateTime64(3),
    op            LowCardinality(String),
    source_ts_ms  UInt64,
    ingested_at   DateTime DEFAULT now(),
    is_deleted    UInt8 DEFAULT 0,
    _version      UInt64
)
ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY order_item_id;

CREATE TABLE IF NOT EXISTS cdc.consumer_metrics
(
    run_id        String,
    recorded_at   DateTime DEFAULT now(),
    topic         String,
    partition     Int32,
    event_lag     Int64,
    processed     UInt64,
    written       UInt64,
    decode_errors UInt64,
    dlq_routed    UInt64
)
ENGINE = MergeTree
ORDER BY (run_id, recorded_at);
