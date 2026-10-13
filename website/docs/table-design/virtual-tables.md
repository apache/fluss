---
sidebar_label: Virtual Tables
title: Virtual Tables
sidebar_position: 4
---

# Virtual Tables

Virtual tables in Fluss are system-generated tables that provide access to metadata, change data, and other system information without storing additional data. They are accessed by appending a suffix (e.g., `$changelog`) to the base table name.

Fluss supports the following virtual table types:

| Virtual Table | Suffix | Description                                               | Supported Tables |
|---------------|--------|-----------------------------------------------------------|------------------|
| [Changelog](#changelog-table) | `$changelog` | Provides access to the raw changelog stream with metadata | Primary Key Tables, Log Tables |
| [Binlog](#binlog-table) | `$binlog` | Provides binlog format with before/after metadata         | Primary Key Tables only |

## Flink Runtime Modes

Both `$changelog` and `$binlog` can be queried in Flink streaming or batch runtime.

| Runtime Mode | Behavior |
|--------------|----------|
| Streaming | Reads from the configured starting position and continues waiting for new changes. |
| Batch | Reads a bounded range and terminates after reaching the stopping offset of every bucket. |

For a batch scan, Fluss resolves the starting offset from `scan.startup.mode` and captures the latest offset of each bucket as its stopping offset when the bounded splits are initialized. The scan reads the range `[starting offset, stopping offset)`. Records appended to a bucket after its stopping offset is captured are not included in that batch query.

:::note
A batch scan does not wait for future changes. Therefore, `scan.startup.mode = 'latest'` is generally useful for streaming queries, but is typically not useful for historical replay in batch runtime.

Virtual tables currently read directly from the Fluss log and do not reconstruct historical changes from lake snapshots. The `full` and `earliest` modes start from the earliest log offset still available in Fluss. Batch scans always use the latest offset captured during split initialization as the stopping offset; a separate stopping offset cannot be configured.
:::

For example, the following query replays a historical part of a changelog and terminates as a bounded Flink job:

```sql title="Flink SQL"
SET 'execution.runtime-mode' = 'batch';

SELECT _change_type, _log_offset, order_id, amount
FROM orders$changelog
/*+ OPTIONS(
  'scan.startup.mode' = 'timestamp',
  'scan.startup.timestamp' = '1705312200000'
) */
WHERE _commit_timestamp < TO_TIMESTAMP_LTZ(1705398600000, 3)
ORDER BY _log_offset
LIMIT 100;
```

:::warning
On Flink 1.18, a predicate on `_commit_timestamp` (or on any other `TIMESTAMP_LTZ` column) returns wrong results when the session time zone is not UTC. Flink 1.18 rebuilds such a predicate using the session time zone instead of UTC after the filter is pushed into the source, which shifts the literal by the zone offset and typically makes the query return no rows. This is [FLINK-35318](https://issues.apache.org/jira/browse/FLINK-35318), fixed in Flink 1.19.2 and 1.20.0 and never backported to 1.18. On Flink 1.18, either avoid `TIMESTAMP_LTZ` predicates or set `table.local-time-zone` to `UTC`.
:::

## Changelog Table

The `$changelog` virtual table provides read-only access to the raw changelog stream of a table, allowing you to audit and process all data changes with their associated metadata.

### Accessing the Changelog

To access the changelog of a table, append `$changelog` to the table name:

```sql title="Flink SQL"
SELECT * FROM my_table$changelog;
```

### Schema

The changelog virtual table includes three metadata columns prepended to the original table columns:

| Column | Type | Description |
|--------|------|-------------|
| `_change_type` | STRING NOT NULL | The type of change operation (see [Change Types](#change-types)) |
| `_log_offset` | BIGINT NOT NULL | The offset position in the log |
| `_commit_timestamp` | TIMESTAMP_LTZ(3) NOT NULL | The timestamp when the change was committed |

Followed by all columns from the base table.

### Change Types

The `_change_type` column indicates the type of data modification:

#### Primary Key Tables

For Primary Key Tables, the following change types are supported:

| Change Type | Description |
|-------------|-------------|
| `insert` | A new row was inserted |
| `update_before` | The previous value of an updated row (retraction) |
| `update_after` | The new value of an updated row |
| `delete` | A row was deleted |

#### Log Tables

For Log Tables (append-only), only one change type is used:

| Change Type | Description |
|-------------|-------------|
| `insert` | A new row was inserted into the log |

### Examples

Consider a Primary Key Table tracking user orders:

```sql title="Flink SQL"
-- Create a primary key table
CREATE TABLE orders (
    order_id INT NOT NULL,
    customer_name STRING,
    amount DECIMAL(10, 2),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket.num' = '1');

-- Insert a record
INSERT INTO orders VALUES (1, 'Rhea', 100.00);

-- Update the record
INSERT INTO orders VALUES (1, 'Rhea', 150.00);

-- Delete the record
DELETE FROM orders WHERE order_id = 1;

-- Query the changelog
SELECT * FROM orders$changelog;
```

Output:

```
+---------------+-------------+---------------------+----------+---------------+---------+
| _change_type  | _log_offset | _commit_timestamp   | order_id | customer_name | amount  |
+---------------+-------------+---------------------+----------+---------------+---------+
| insert        |           0 | 2024-01-15 10:30:00 |        1 | Rhea          |  100.00 |
| update_before |           1 | 2024-01-15 10:35:00 |        1 | Rhea          |  100.00 |
| update_after  |           2 | 2024-01-15 10:35:00 |        1 | Rhea          |  150.00 |
| delete        |           3 | 2024-01-15 10:40:00 |        1 | Rhea          |  150.00 |
+---------------+-------------+---------------------+----------+---------------+---------+
```


### Startup Modes


| Mode | Description |
|------|-------------|
| `full` | Start reading from the beginning of the log (default) |
| `earliest` | Start reading from the beginning of the log |
| `latest` | Use the current end of the log as the starting position |
| `timestamp` | Start reading from a specific timestamp (milliseconds since epoch) |


The changelog table supports different startup modes to control where reading begins:

```sql title="Flink SQL"
-- Read from the beginning (default full mode)
SELECT * FROM orders$changelog;

-- Explicitly read from the earliest offset
SELECT * FROM orders$changelog /*+ OPTIONS('scan.startup.mode' = 'earliest') */;

-- Use the current log end as the starting position
SELECT * FROM orders$changelog /*+ OPTIONS('scan.startup.mode' = 'latest') */;

-- Read from a specific timestamp
SELECT * FROM orders$changelog /*+ OPTIONS('scan.startup.mode' = 'timestamp', 'scan.startup.timestamp' = '1705312200000') */;
```

### Limitations

- Projection, partition, and predicate pushdowns are not supported yet. This will be addressed in future releases.

## Binlog Table

The `$binlog` virtual table provides access to change data where each record contains both the before and after images of the row. This is useful for:

:::note
The `$binlog` virtual table is only available for **Primary Key Tables**.
:::

### Accessing the Binlog

To access the binlog of a Primary Key Table, append `$binlog` to the table name:

```sql title="Flink SQL"
SELECT * FROM my_pk_table$binlog;
```

### Schema

The binlog virtual table includes three metadata columns followed by nested `before` and `after` row structures:

| Column | Type | Description |
|--------|------|-------------|
| `_change_type` | STRING NOT NULL | The type of change operation: `insert`, `update`, or `delete` |
| `_log_offset` | BIGINT NOT NULL | The offset position in the log |
| `_commit_timestamp` | TIMESTAMP_LTZ(3) NOT NULL | The timestamp when the change was committed |
| `before` | ROW&lt;...&gt; | The row values before the change (NULL for inserts) |
| `after` | ROW&lt;...&gt; | The row values after the change (NULL for deletes) |

The `before` and `after` columns are nested ROW types containing all columns from the base table.

### Change Types

| Change Type | Description | `before` | `after` |
|-------------|-------------|----------|---------|
| `insert` | A new row was inserted | NULL | Contains new row values |
| `update` | A row was updated | Contains old row values | Contains new row values |
| `delete` | A row was deleted | Contains deleted row values | NULL |

### Examples

Consider the same Primary Key Table tracking user orders:

```sql title="Flink SQL"
-- Create a primary key table
CREATE TABLE orders (
    order_id INT NOT NULL,
    customer_name STRING,
    amount DECIMAL(10, 2),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket.num' = '1');

-- Insert a record
INSERT INTO orders VALUES (1, 'Rhea', 100.00);

-- Update the record
INSERT INTO orders VALUES (1, 'Rhea', 150.00);

-- Delete the record
DELETE FROM orders WHERE order_id = 1;

-- Query the binlog
SELECT * FROM orders$binlog;
```

Output:

```
+--------------+-------------+---------------------+----------------------+----------------------+
| _change_type | _log_offset | _commit_timestamp   | before               | after                |
+--------------+-------------+---------------------+----------------------+----------------------+
| insert       |           0 | 2024-01-15 10:30:00 | NULL                 | (1, Rhea, 100.00)    |
| update       |           2 | 2024-01-15 10:35:00 | (1, Rhea, 100.00)    | (1, Rhea, 150.00)    |
| delete       |           3 | 2024-01-15 10:40:00 | (1, Rhea, 150.00)    | NULL                 |
+--------------+-------------+---------------------+----------------------+----------------------+
```

#### Accessing Nested Fields

You can access individual fields from the `before` and `after` structures:

```sql title="Flink SQL"
SELECT
    _change_type,
    _commit_timestamp,
    `before`.name AS old_name,
    `after`.name AS new_name
FROM users$binlog
WHERE _change_type = 'update';
```

### Startup Modes


| Mode | Description |
|------|-------------|
| `full` | Start reading from the beginning of the log (default) |
| `earliest` | Start reading from the beginning of the log |
| `latest` | Use the current end of the log as the starting position |
| `timestamp` | Start reading from a specific timestamp (milliseconds since epoch) |


The binlog table supports different startup modes to control where reading begins:

```sql title="Flink SQL"
-- Read from the beginning (default full mode)
SELECT * FROM orders$binlog;

-- Explicitly read from the earliest offset
SELECT * FROM orders$binlog /*+ OPTIONS('scan.startup.mode' = 'earliest') */;

-- Use the current log end as the starting position
SELECT * FROM orders$binlog /*+ OPTIONS('scan.startup.mode' = 'latest') */;

-- Read from a specific timestamp
SELECT * FROM orders$binlog /*+ OPTIONS('scan.startup.mode' = 'timestamp', 'scan.startup.timestamp' = '1705312200000') */;
```


### Limitations

- Projection, partition, and predicate pushdowns are not supported yet. This will be addressed in future releases.
