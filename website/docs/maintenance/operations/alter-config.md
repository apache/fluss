---
title: Alter Configuration
sidebar_position: 4
---
# Alter Configuration
## Overview

Fluss provides ways to alter the configuration of a cluster or a table. You can change and apply the configuration without restarting Fluss server. In this section, we will show how to alter the configuration of a cluster or a table.

## Alter Cluster Configuration

Currently, you can only alter the configuration of a cluster with  [Java client](apis/java-client.md).

Currently, only `datalake.format` and options with prefix `datalake.${datalake.format}` can be altered. These options will also be served as table options when getting the table info.

The AlterConfig class contains three key properties:
* key: The configuration key to be modified (e.g., `datalake.format`)
* value: The configuration value to be set (e.g., "paimon")
* opType: The operation type, either AlterConfigOpType.SET or AlterConfigOpType.DELETE

To disable a cluster, you can use the following code:
```java
admin.alterClusterConfigs(
        Collections.singletonList(
                new AlterConfig(DATALAKE_FORMAT.key(), "paimon", AlterConfigOpType.SET)));
```

To disable a cluster, you can use the following code:
```java
admin.alterClusterConfigs(
        Collections.singletonList(
                new AlterConfig(DATALAKE_FORMAT.key(), "paimon", AlterConfigOpType.DELETE)));
```

Currently, only `datalake.format` and option with prefix `datalake.${datalake.format}` can be altered. And this options will also be served as table options when getting the table info.

## Alter Table Configuration

Storage Options can be altered by [Alter Table](engine-flink/ddl.md#alter-table). The limitations are as follows:
1. `bootstrap.servers`,`bucket.num` and `bucket.key` cannot be altered.
2. All the table options except `table.datalake.enabled` can be modified.
3. If lakehouse is already enabled for a table, options with lakehouse format prefixes (e.g., `paimon.*`) cannot be set again.