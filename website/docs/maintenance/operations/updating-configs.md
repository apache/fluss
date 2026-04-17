---
title: Updating Configs
sidebar_position: 1
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Updating Configs

## Overview

Fluss allows you to update cluster or table configurations dynamically without requiring a cluster restart or table recreation. This section demonstrates how to modify and apply such configurations.

## Updating Cluster Configs

From Fluss version 0.8 onwards, some of the server configs can be updated without restarting the server.

Currently, the supported dynamically updatable server configurations include:
- `datalake.enabled`: Control whether the cluster is ready to create and manage lakehouse tables. When this option is explicitly configured to true, `datalake.format` must also be configured.
- `datalake.format`: Specify the lakehouse format, e.g., `paimon`, `iceberg`. When enabling lakehouse storage explicitly, use it together with `datalake.enabled = true`.
- Options with prefix `datalake.${datalake.format}`
- `kv.rocksdb.shared-rate-limiter.bytes-per-sec`: Control RocksDB flush and compaction write rate shared across all RocksDB instances on the TabletServer. The rate limiter is always enabled. Set to a lower value (e.g., 100MB) to limit the rate, or a very high value to effectively disable rate limiting.


You can update the configuration of a cluster with [Java client](#using-java-client) or [Flink SQL](#using-flink-sql).

<Tabs>
<TabItem value="java" label="Java Client" default>

```java
// Enable lakehouse storage with Paimon format
admin.alterClusterConfigs(
        Arrays.asList(
                new AlterConfig(DATALAKE_ENABLED.key(), "true", AlterConfigOpType.SET),
                new AlterConfig(DATALAKE_FORMAT.key(), "paimon", AlterConfigOpType.SET)));

// Disable lakehouse storage
admin.alterClusterConfigs(
        Collections.singletonList(
                new AlterConfig(DATALAKE_ENABLED.key(), "false", AlterConfigOpType.SET)));

// Set RocksDB shared rate limiter to 200MB/sec
admin.alterClusterConfigs(
        Collections.singletonList(
                new AlterConfig(KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(), "200MB", AlterConfigOpType.SET)));
```

The `AlterConfig` class contains three properties:
- `key`: The configuration key to be modified (e.g., `datalake.format`)
- `value`: The configuration value to be set (e.g., `paimon`)
- `opType`: The operation type, either `AlterConfigOpType.SET` or `AlterConfigOpType.DELETE`

</TabItem>
<TabItem value="flink-sql" label="Flink SQL">

```sql
-- Use the Fluss catalog (replace 'fluss_catalog' with your catalog name if different)
USE fluss_catalog;

-- Enable lakehouse storage with Paimon format
CALL sys.set_cluster_configs(
  config_pairs => 'datalake.enabled', 'true', 'datalake.format', 'paimon'
);

-- Disable lakehouse storage
CALL sys.set_cluster_configs(
  config_pairs => 'datalake.enabled', 'false'
);

-- Set RocksDB shared rate limiter to 200MB/sec
CALL sys.set_cluster_configs(
  config_pairs => 'kv.rocksdb.shared-rate-limiter.bytes-per-sec', '200MB'
);
```

See [Procedures](engine-flink/procedures.md#cluster-configuration-procedures) for detailed documentation on `set_cluster_configs`, `reset_cluster_configs`, and `get_cluster_configs` procedures.

</TabItem>
</Tabs>

## Updating Table Configs

The connector options on a table including [Storage Options](engine-flink/options.md#storage-options) can be updated dynamically by [ALTER TABLE ... SET](engine-flink/ddl.md#alter-table) statement. See the example below:

```sql
-- Enable lakehouse storage for the given table
ALTER TABLE my_table SET ('table.datalake.enabled' = 'true');
```
