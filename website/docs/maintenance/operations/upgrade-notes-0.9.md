---
title: Upgrade Notes
sidebar_position: 4
---

# Upgrade Notes from v0.8 to v0.9

These upgrade notes discuss important aspects, such as configuration, behavior, or dependencies, that changed between Fluss 0.8 and Fluss 0.9. Please read these notes carefully if you are planning to upgrade your Fluss version to 0.9.

## Adding Columns

Fluss storage format was designed with schema evolution protocols from day one. Therefore, tables created in v0.8 or earlier can immediately benefit from the `ADD COLUMN` feature after upgrading to v0.9 without dropping and re-creating table.
However, old clients (pre-v0.9) do not recognize the new schema versioning protocol. If an old client attempts to read data from a table that has undergone schema evolution, it may encounter decoding errors or data inaccessibility.
Therefore, it is crucial to ensure that all Fluss servers and all clients interacting with the Fluss cluster are upgraded to v0.9 before performing any schema modifications.

:::warning
Attempting to add columns while older clients (v0.8 or below) are still actively reading from the table will lead to Schema Inconsistency and may crash your streaming pipelines.
:::

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

## Primary Key Encoding Change for Clusters with `datalake.format` Configured

:::info
This section only applies to primary key tables created on clusters with `datalake.format` configured (e.g., Paimon). If your cluster does not have `datalake.format` configured, you can skip this section.
:::

### What Changed

- **New tables (version 2)**: Tables created in v0.9+ use Fluss's default encoder for primary key encoding when bucket key differs from primary key. This ensures proper prefix lookup support.
- **Legacy tables (version 1)**: Tables created before v0.9 are treated as version 1 and continue using datalake's encoder (e.g., Paimon/Iceberg) for both primary key and bucket key encoding.

### Prefix Lookup Limitation with Legacy Tables

Legacy tables (version 1) may not support prefix lookup properly when:
1. The cluster has `datalake.format` configured as `paimon`
2. The table has a bucket key that differs from the primary key

:::warning
If you have legacy tables with bucket key different from primary key and need prefix lookup functionality, the prefix lookup results may be incorrect or incomplete.
:::

### How to Address This Issue

For legacy tables that require prefix lookup support:

1. **Recreate the table**: Drop and recreate the table after upgrading to v0.9.

2. **Upgrade client and connector**: Make sure to upgrade your Fluss client and Flink connector to v0.9 to use the new encoding format.
