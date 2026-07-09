---
title: TTL
sidebar_position: 3
---

# TTL

Fluss supports TTL for data by setting the TTL attribute for tables with `'table.log.ttl' = '<duration>'` (default is 7 days). Fluss can periodically and automatically check for and clean up expired data in the table.

For log tables, this attribute indicates the expiration time of the log table data.
For primary key tables, this attribute indicates the expiration time of the changelog and does not represent the expiration time of the primary key table data.

## Row TTL for Primary Key Tables

Primary key tables can configure row-level TTL with `'table.row.ttl' = '<duration>'`. The option has no default value; if it is not configured, row-level TTL is disabled.

```sql title="Flink SQL"
CREATE TABLE pk_table
(
    id BIGINT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '4',
    'table.row.ttl' = '7 d'
);
```

Row TTL is best-effort cleanup. A row becomes eligible for cleanup after the configured duration, but expired rows may still be visible until RocksDB compaction removes them. Fluss stores the TTL timestamp in the primary-key table value and uses a compaction filter to remove expired rows during RocksDB compaction.

TTL cleanup does not emit delete records. Downstream consumers of `$changelog` or `$binlog` will not receive delete changes when rows expire due to TTL.

By default, row TTL uses processing time. To use event time, configure `table.row.ttl.time-column` when creating the table:

```sql title="Flink SQL"
CREATE TABLE pk_table_with_event_time
(
    id BIGINT,
    event_time BIGINT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '4',
    'table.row.ttl' = '7 d',
    'table.row.ttl.time-column' = 'event_time'
);
```

The event-time column must be `BIGINT` epoch milliseconds or `TIMESTAMP_LTZ`. Rows with null event-time values do not expire through row TTL.

Row TTL must be configured when creating the primary key table. Changing or disabling `table.row.ttl`, or changing `table.row.ttl.time-column`, with `ALTER TABLE ... SET` or `ALTER TABLE ... RESET` is not supported in this version.

See [Flink Connector Options](/engine-flink/options.md#storage-options) for the complete row TTL option definitions.
