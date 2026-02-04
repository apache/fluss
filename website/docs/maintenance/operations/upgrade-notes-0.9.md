---
title: Upgrade Notes
sidebar_position: 4
---

# Upgrade Notes from v0.8 to v0.9

These upgrade notes discuss important aspects, such as configuration, behavior, or dependencies, that changed between Fluss 0.8 and Fluss 0.9. Please read these notes carefully if you are planning to upgrade your Fluss version to 0.9.

Previous versions can be found in the [archive of upgrade notes](upgrade-notes-archive.md).

## Adding Columns

Fluss storage format was designed with schema evolution protocols from day one. Therefore, tables created in v0.8 or earlier can immediately benefit from the `ADD COLUMN` feature after upgrading to v0.9 without dropping and re-creating table.
However, old clients (pre-v0.9) do not recognize the new schema versioning protocol. If an old client attempts to read data from a table that has undergone schema evolution, it may encounter decoding errors or data inaccessibility.
Therefore, it is crucial to ensure that all Fluss servers and all clients interacting with the Fluss cluster are upgraded to v0.9 before performing any schema modifications.

:::warning
Attempting to add columns while older clients (v0.8 or below) are still actively reading from the table will lead to Schema Inconsistency and may crash your streaming pipelines.
:::

## Primary Key Encoding Change

:::info
This section only applies to primary key tables created on clusters with `datalake.format` configured (e.g., Paimon). If your cluster does not have `datalake.format` configured, you can skip this section.
:::

### Prefix Lookup Issue in Older Versions

In versions **0.8 and earlier**, tables may encounter incorrect or incomplete results during prefix lookups when **both** of the following conditions are met:

1. The table is created on a cluster where `datalake.format` is configured (e.g., Paimon), resulting in the table property `table.datalake.format` being set.
2. The table uses a **bucket key that differs from its primary key** (pattern commonly used for Flink Delta Join).

Under these circumstances, Fluss incorrectly uses the datalake’s encoder (e.g., Paimon’s) to serialize the **primary key**.
However, this encoding does not support prefix-based scanning, leading to malformed or incomplete query results.

### What is Changed in the New Version

- **New tables (version 2)**: Tables created in v0.9+ use Fluss's default encoder for primary key encoding when bucket key differs from primary key. This ensures proper prefix lookup support.
- **Legacy tables (version 1)**: Tables created before v0.9 are treated as version 1 and continue using datalake's encoder (e.g., Paimon/Iceberg) for both primary key and bucket key encoding.

### How to Resolve This Issue After Upgrading

For legacy tables that require prefix lookup support:

1. **Recreate the table**: Drop and recreate the table after upgrading to v0.9.

2. **Upgrade client and connector**: Make sure to upgrade your Fluss client and Flink connector to v0.9 to use the new encoding format.

### Compatibility Note

After upgrading to **v0.9**, ensure that **all clients** interacting with the Fluss cluster are also upgraded to **v0.9 or later**.

Mixing client versions can cause compatibility issues. Specifically, primary key tables created on v0.9 clusters use **KV format version 2**, which older clients cannot recognize. This may result in exceptions during lookup or write operations.

## Paimon Integration

### Manual Paimon Bundle Dependency Required

Starting from Fluss 0.9, the `fluss-lake-paimon` JAR no longer bundles `paimon-bundle`. Previously, including `paimon-bundle` inside `fluss-lake-paimon` caused class conflicts during union reads when users had a different version of Paimon in their environment.

🔧 **Action Required**: Manually add a compatible `paimon-bundle` JAR to your Flink `lib` directory.

### Migration Steps

Choose a compatible version of `paimon-bundle` JAR from the [Apache Paimon Downloads](https://paimon.apache.org/docs/1.3/project/download/) page 
and place it in `$FLINK_HOME/lib/`.

### Tested Paimon Versions

| Use Case        | Required/Tested Versions                          |
|-----------------|---------------------------------------------------|
| Tiering Service | Paimon **1.3** (required)                         |
| Union Read      | Paimon 1.1, 1.2, 1.3 (tested and verified to work)|

## Deprecation / End of Support

### Configuration Options Deprecated

Several configuration options have been deprecated in Fluss 0.9 and replaced with a unified `server.io-pool.size` option. This change simplifies configuration management by consolidating IO thread pool settings across different components.

🔧 **Action Required**: Update your configuration files to use the new option.

#### Deprecated Options

The following options are deprecated and will be removed in a future version:

| Deprecated Option                     | Replacement           | Description                                                      |
|---------------------------------------|-----------------------|------------------------------------------------------------------|
| `coordinator.io-pool.size`            | `server.io-pool.size` | The size of the IO thread pool for coordinator server operations |
| `remote.log.data-transfer-thread-num` | `server.io-pool.size` | The number of threads for transferring remote log files          |
| `kv.snapshot.transfer-thread-num`     | `server.io-pool.size` | The number of threads for transferring KV snapshot files         |

#### Migration Steps

1. **Identify deprecated options in your configuration**:
   - Check your `server.yaml` configuration file for any of the deprecated options listed above

2. **Replace with the unified option**:
   - Remove the deprecated options from your configuration
   - Add or update `server.io-pool.size` with an appropriate value
   - The default value is `10`, which should work for most use cases

#### Benefits of the Change

- **Simplified Configuration**: One option instead of multiple options for IO thread pool management
- **Better Resource Management**: Unified thread pool allows better resource sharing across different IO operations
- **Consistent Behavior**: All IO operations (remote log, KV snapshot, etc.) now use the same thread pool configuration

