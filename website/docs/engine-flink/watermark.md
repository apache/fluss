---
sidebar_label: "Watermark"
title: "Watermark"
sidebar_position: 9
---

# Watermark

Watermark is a fundamental concept in Apache Flink for event-time processing. It tracks the progress of event time and enables time-based operations like windowed aggregation. Fluss provides two ways to configure watermark generation for Flink streaming queries: **table-level definition** and **hint-based override**.

## Table-Level Watermark

You can define a watermark strategy directly in the table DDL using the standard Flink `WATERMARK FOR` clause. This watermark definition is stored as metadata and automatically applied whenever the table is read in streaming mode.

```sql title="Flink SQL"
CREATE TABLE orders (
  order_id BIGINT,
  amount DOUBLE,
  order_time TIMESTAMP(3),
  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
  'bucket.num' = '4',
  'bucket.key' = 'order_id'
);

-- The watermark is automatically applied when reading
SELECT
  TUMBLE_START(order_time, INTERVAL '1' HOUR) AS window_start,
  COUNT(*) AS order_count
FROM orders
GROUP BY TUMBLE(order_time, INTERVAL '1' HOUR);
```

The `WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND` clause declares `order_time` as the event-time attribute and allows up to 5 seconds of out-of-orderness.

## Hint-Based Watermark

SQL hints allow you to dynamically configure watermark at query time without modifying the table definition. This is useful when:

- You want to override the table-level watermark with different parameters
- You want to use a different column for watermark generation
- You want to disable watermark entirely for specific queries

### Override Watermark Column and Delay

Use `scan.watermark.column` and `scan.watermark.delay` to specify a custom watermark strategy via SQL hints:

```sql title="Flink SQL"
-- Use 'ts' column with 0 delay (overrides table-level watermark)
SELECT * FROM orders
/*+ OPTIONS('scan.watermark.column' = 'order_time', 'scan.watermark.delay' = '0s') */;
```

```sql title="Flink SQL"
-- Use a BIGINT column (epoch millis) for watermark generation
SELECT order_id, amount, event_time_millis FROM events
/*+ OPTIONS('scan.watermark.column' = 'event_time_millis', 'scan.watermark.delay' = '3s') */
GROUP BY ...;
```

:::note
The watermark column specified in the hint must be included in your query's SELECT clause (either directly or via aggregation). If the column is not referenced, Flink's projection pushdown will remove it from the source output, causing an error.
:::

### Disable Watermark

Use `scan.watermark.enabled` to disable watermark generation entirely. This is useful when an existing table has a `WATERMARK` clause defined but you want to disable it for specific queries (e.g., for debugging or batch-style processing):

```sql title="Flink SQL"
-- Disable watermark even though the table has a WATERMARK definition
SELECT * FROM orders
/*+ OPTIONS('scan.watermark.enabled' = 'false') */;
```

## Priority

The watermark resolution follows this priority order:

1. **Disabled** — If `scan.watermark.enabled = false` is set via hint, watermark is completely disabled (returns `noWatermarks()`).
2. **Hint column** — If `scan.watermark.column` is specified via hint, it overrides any table-level watermark definition.
3. **Table-level** — The `WATERMARK FOR` clause defined in the table DDL is used.
4. **None** — If no watermark is configured at any level, `noWatermarks()` is used.

## Watermark Options

| Option                  | Type     | Default | Description                                                                                                                                                                              |
|-------------------------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| scan.watermark.enabled  | Boolean  | true    | Whether to enable watermark generation. Set to `false` via SQL hints to disable watermark for tables that already have a table-level `WATERMARK` definition.                             |
| scan.watermark.column   | String   | (None)  | The column name used for event-time watermark generation. Overrides the table-level `WATERMARK` definition. The column must be `TIMESTAMP`, `TIMESTAMP_LTZ`, or `BIGINT` (epoch millis). |
| scan.watermark.delay    | Duration | 0s      | The maximum out-of-orderness allowed for event-time watermark. Used together with `scan.watermark.column`.                                                                               |

### Supported Column Types

The hint-based watermark supports the following column types:

| Column Type       | Behavior                                                |
|-------------------|---------------------------------------------------------|
| TIMESTAMP(p)      | Extracts milliseconds from the timestamp value          |
| TIMESTAMP_LTZ(p)  | Extracts milliseconds from the local-zoned timestamp    |
| BIGINT            | Interprets the value directly as epoch milliseconds     |

## Examples

### Windowed Aggregation with Table-Level Watermark

```sql title="Flink SQL"
-- Create table with watermark definition
CREATE TABLE sensor_data (
  sensor_id STRING,
  temperature DOUBLE,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH ('bucket.num' = '4', 'bucket.key' = 'sensor_id');

-- Tumbling window aggregation uses the table-level watermark
SELECT
  sensor_id,
  TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
  AVG(temperature) AS avg_temp
FROM sensor_data
GROUP BY sensor_id, TUMBLE(event_time, INTERVAL '1' MINUTE);
```

### Override with Hint for Lower Latency

```sql title="Flink SQL"
-- Table has 10-second delay, but we want lower latency for this query
SELECT
  sensor_id,
  TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
  AVG(temperature) AS avg_temp
FROM sensor_data
/*+ OPTIONS('scan.watermark.column' = 'event_time', 'scan.watermark.delay' = '0s') */
GROUP BY sensor_id, TUMBLE(event_time, INTERVAL '1' MINUTE);
```

### Using BIGINT Epoch Column

```sql title="Flink SQL"
-- Table with both TIMESTAMP and BIGINT time columns
CREATE TABLE events (
  id BIGINT,
  payload STRING,
  event_time TIMESTAMP(3),
  event_time_millis BIGINT,
  WATERMARK FOR event_time AS event_time - INTERVAL '10' HOUR
) WITH ('bucket.num' = '4', 'bucket.key' = 'id');

-- Use BIGINT column for watermark with 0 delay (overrides 10-hour table delay)
SELECT
  id, COUNT(payload), MIN(event_time_millis)
FROM events
/*+ OPTIONS('scan.watermark.column' = 'event_time_millis') */
GROUP BY id, TUMBLE(event_time, INTERVAL '1' HOUR);
```
