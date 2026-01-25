---
sidebar_label: Virtual Tables
title: Virtual Tables
sidebar_position: 4
---

# Virtual Tables

Virtual tables in Fluss are system-generated tables that provide access to metadata, change data, and other system information without storing additional data. They are accessed by appending a suffix (e.g., `$changelog`) to the base table name.

Fluss supports the following virtual table types:

| Virtual Table | Suffix | Description |
|---------------|--------|-------------|
| [Changelog](#changelog-table) | `$changelog` | Provides access to the raw changelog stream with metadata |

More virtual table types will be added in future releases.

## Changelog Table

The `$changelog` virtual table provides read-only access to the raw changelog stream of a table, allowing you to audit and process all data changes with their associated metadata. This is useful for:

- **Change Data Capture (CDC)**: Track all inserts, updates, and deletes
- **Auditing**: Monitor data modifications with timestamps and offsets
- **Event Processing**: Build event-driven applications based on data changes
- **Data Replication**: Replicate changes to downstream systems

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
| `+I` | **Insert** - A new row was inserted |
| `-U` | **Update Before** - The previous value of an updated row (retraction) |
| `+U` | **Update After** - The new value of an updated row |
| `-D` | **Delete** - A row was deleted |

#### Log Tables

For Log Tables (append-only), only one change type is used:

| Change Type | Description |
|-------------|-------------|
| `+A` | **Append** - A new row was appended to the log |

### Examples

#### Primary Key Table Changelog

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
+----+--------------+-------------+---------------------+----------+---------------+---------+
| op | _change_type | _log_offset | _commit_timestamp   | order_id | customer_name | amount  |
+----+--------------+-------------+---------------------+----------+---------------+---------+
| +I | +I           |           0 | 2024-01-15 10:30:00 |        1 | Rhea          |  100.00 |
| +I | -U           |           1 | 2024-01-15 10:35:00 |        1 | Rhea          |  100.00 |
| +I | +U           |           2 | 2024-01-15 10:35:00 |        1 | Rhea          |  150.00 |
| +I | -D           |           3 | 2024-01-15 10:40:00 |        1 | Rhea          |  150.00 |
+----+--------------+-------------+---------------------+----------+---------------+---------+
```

:::note
The `op` column is Flink's row kind indicator. For changelog virtual tables, all rows are emitted as `+I` (insert) to the downstream, while the actual change type is captured in the `_change_type` column.
:::

#### Log Table Changelog

Consider a Log Table storing click events:

```sql title="Flink SQL"
-- Create a log table (no primary key)
CREATE TABLE click_events (
    event_id INT,
    user_id INT,
    event_type STRING
) WITH ('bucket.num' = '1');

-- Append events
INSERT INTO click_events VALUES (1, 101, 'click'), (2, 102, 'view');

-- Query the changelog
SELECT * FROM click_events$changelog;
```

Output:

```
+----+--------------+-------------+---------------------+----------+---------+------------+
| op | _change_type | _log_offset | _commit_timestamp   | event_id | user_id | event_type |
+----+--------------+-------------+---------------------+----------+---------+------------+
| +I | +A           |           0 | 2024-01-15 11:00:00 |        1 |     101 | click      |
| +I | +A           |           1 | 2024-01-15 11:00:00 |        2 |     102 | view       |
+----+--------------+-------------+---------------------+----------+---------+------------+
```

### Startup Modes

The changelog virtual table supports different startup modes to control where reading begins:

```sql title="Flink SQL"
-- Read from the beginning (default)
SELECT * FROM orders$changelog /*+ OPTIONS('scan.startup.mode' = 'earliest') */;

-- Read only new changes from now
SELECT * FROM orders$changelog /*+ OPTIONS('scan.startup.mode' = 'latest') */;

-- Read from a specific timestamp
SELECT * FROM orders$changelog /*+ OPTIONS('scan.startup.mode' = 'timestamp', 'scan.startup.timestamp' = '1705312200000') */;
```

| Mode | Description |
|------|-------------|
| `earliest` | Start reading from the beginning of the log |
| `latest` | Start reading from the current end of the log (only new changes) |
| `timestamp` | Start reading from a specific timestamp (milliseconds since epoch) |

### Limitations
- Projection & partition & predicate pushdowns are not supported yet. This will be addressed in future releases.
