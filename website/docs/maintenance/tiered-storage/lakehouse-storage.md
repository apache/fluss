---
title: "Lakehouse Storage"
sidebar_position: 3
---

# Lakehouse Storage

Fluss leverages well-known Lakehouse storage solutions like Apache Paimon, Apache Iceberg, and Lance as the tiered storage layer. The Tiering Service continuously tiers Fluss data to Lakehouse storage, where it can be read by Fluss clients in a streaming manner and accessed directly by external systems such as Flink, Spark, StarRocks, and others.

For deployment instructions, see [Deploying Streaming Lakehouse](../../install-deploy/deploying-streaming-lakehouse.md).

For architecture details, see [Tiering Service](../../streaming-lakehouse/tiering-service.md).

## Cluster Configuration

Lakehouse storage is configured in `server.yaml` using the `datalake.` prefix:

```yaml title="server.yaml"
datalake.format: paimon
datalake.paimon.metastore: filesystem
datalake.paimon.warehouse: /tmp/paimon
```

Fluss processes configurations by removing the `datalake.<format>.` prefix and uses the remaining configuration to create the data lake catalog.

For format-specific configuration, see:
- [Paimon](../../streaming-lakehouse/datalake-formats/paimon.md)
- [Iceberg](../../streaming-lakehouse/datalake-formats/iceberg.md)
- [Lance](../../streaming-lakehouse/datalake-formats/lance.md)

## Table-Level Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `table.datalake.enabled` | Boolean | false | Enable lakehouse storage for this table |
| `table.datalake.freshness` | Duration | 3min | Maximum lag between Fluss and data lake table |
| `table.datalake.format` | String | - | Data lake format (paimon, iceberg, lance). Inherits from cluster config |
| `table.datalake.auto-compaction` | Boolean | false | Auto-trigger compaction in the data lake |
| `table.datalake.auto-expire-snapshot` | Boolean | false | Auto-expire snapshots in the data lake |

Example:
```sql title="Flink SQL"
CREATE TABLE my_table (
    id BIGINT PRIMARY KEY NOT ENFORCED,
    name STRING
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '1min'
);
```

## Tiering Service Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `lake.tiering.auto-expire-snapshot` | Boolean | false | Auto-trigger snapshot expiration on commit, even if `table.datalake.auto-expire-snapshot` is false |

## Data Retention

When using lakehouse storage with Union Read:
- **Expired Fluss log data** (controlled by `table.log.ttl`) remains accessible via the lake if previously tiered
- **Cleaned-up partitions** (controlled by `table.auto-partition.num-retention`) remain accessible via the lake if previously tiered

This enables Fluss to store only recent data while the lake serves as a complete historical archive.
