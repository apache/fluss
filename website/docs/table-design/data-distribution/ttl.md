---
title: TTL
sidebar_position: 3
---

# TTL

Fluss supports TTL for data by setting the TTL attribute for tables with `'table.log.ttl' = '<duration>'` (default is 7 days). Fluss can periodically and automatically check for and clean up expired data in the table.

For log tables, this attribute indicates the expiration time of the log table data.
For primary key tables, this attribute indicates the expiration time of the changelog and does not represent the expiration time of the primary key table data. If you also want the data in the primary key table to expire automatically, please use [auto partitioning](partitioning.md#auto-partitioning).

When tiered storage is enabled, `table.log.local-ttl` can be used to control how long copied local log segments are retained. If it is not configured, it falls back to `table.log.ttl` for backward compatibility. A non-positive local TTL disables TTL-based local cleanup. When both TTLs are positive, the local TTL must be less than or equal to `table.log.ttl`.

## Row TTL for Primary Key Tables

Primary key tables can configure row-level TTL with `'table.kv.ttl' = '<duration>'`. The duration must be at least 1 millisecond. The option has no default value; if it is not configured, row-level TTL is disabled.

```sql title="Flink SQL"
CREATE TABLE pk_table
(
    id BIGINT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '4',
    'table.kv.ttl' = '7 d'
);
```

Row TTL is best-effort cleanup. A row becomes eligible for cleanup after the configured duration, but expired rows may still be visible until RocksDB compaction removes them. Fluss stores the TTL timestamp in the primary-key table value and uses a compaction filter to remove expired rows during RocksDB compaction.

Auto partitioning is the recommended expiration mechanism when data can be partitioned by time, because expiring whole partitions preserves changelog completeness. Row TTL is intended for cases that require per-row cleanup and can accept weaker changelog semantics.

Row TTL cleanup does not emit delete records. Downstream consumers of `$changelog` or `$binlog` will not receive delete changes when rows expire. After compaction removes an expired row, the next write for the same primary key is treated as an insert and does not emit an `UPDATE_BEFORE` for the expired value.

By default, row TTL uses processing time. To use event time, configure `table.kv.ttl.time-column` when creating the table:

```sql title="Flink SQL"
CREATE TABLE pk_table_with_event_time
(
    id BIGINT,
    event_time BIGINT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '4',
    'table.kv.ttl' = '7 d',
    'table.kv.ttl.time-column' = 'event_time'
);
```

The event-time column must be `BIGINT` epoch milliseconds, `TIMESTAMP`, or `TIMESTAMP_LTZ`. `TIMESTAMP` values are interpreted in the TabletServer's system time zone, so all TabletServers must use the same system time zone. Rows with null event-time values do not expire through row TTL.

Row TTL must be configured when creating the primary key table. Changing or disabling `table.kv.ttl`, or changing `table.kv.ttl.time-column`, with `ALTER TABLE ... SET` or `ALTER TABLE ... RESET` is not supported in this version.

Before creating a row-TTL primary key table, upgrade every server to Fluss 1.0+ so that all servers understand the tagged value layout used for row TTL. After a row-TTL table exists, downgrading to a version < 1.0 that does not understand that layout is not supported.

See [Flink Connector Options](/engine-flink/options.md#storage-options) for the complete row TTL option definitions.
