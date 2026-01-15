---
sidebar_label: Procedures
title: Procedures
sidebar_position: 3
---

# Procedures

Fluss provides stored procedures to perform administrative and management operations through Flink SQL. All procedures are located in the `sys` namespace and can be invoked using the `CALL` statement.

## Available Procedures

You can list all available procedures using:

```sql title="Flink SQL"
SHOW PROCEDURES;
```

## Access Control Procedures

Fluss provides procedures to manage Access Control Lists (ACLs) for security and authorization. See the [Security](/security/overview.md) documentation for more details.

### add_acl

Add an ACL entry to grant permissions to a principal.

**Syntax:**

```sql
CALL [catalog_name.]sys.add_acl(
  resource => 'STRING',
  permission => 'STRING', 
  principal => 'STRING',
  operation => 'STRING',
  host => 'STRING'  -- optional, defaults to '*'
)
```

**Parameters:**

- `resource` (required): The resource to grant permissions on. Can be `'CLUSTER'` for cluster-level permissions or a specific resource name (e.g., database or table name).
- `permission` (required): The permission type to grant. Valid values are `'ALLOW'` or `'DENY'`.
- `principal` (required): The principal to grant permissions to, in the format `'Type:Name'` (e.g., `'User:Alice'`).
- `operation` (required): The operation type to grant. Valid values include `'READ'`, `'WRITE'`, `'CREATE'`, `'DELETE'`, `'ALTER'`, `'DESCRIBE'`, `'CLUSTER_ACTION'`, `'IDEMPOTENT_WRITE'`.
- `host` (optional): The host from which the principal can access the resource. Defaults to `'*'` (all hosts).

**Example:**

```sql title="Flink SQL"
-- Use the Fluss catalog (replace 'fluss_catalog' with your catalog name if different)
USE fluss_catalog;

-- Grant read permission to user Alice from any host
CALL sys.add_acl(
  resource => 'CLUSTER',
  permission => 'ALLOW',
  principal => 'User:Alice',
  operation => 'READ',
  host => '*'
);

-- Grant write permission to user Bob from a specific host
CALL sys.add_acl(
  resource => 'my_database.my_table',
  permission => 'ALLOW',
  principal => 'User:Bob',
  operation => 'WRITE',
  host => '192.168.1.100'
);
```

### drop_acl

Remove an ACL entry to revoke permissions.

**Syntax:**

```sql
CALL [catalog_name.]sys.drop_acl(
  resource => 'STRING',
  permission => 'STRING',
  principal => 'STRING', 
  operation => 'STRING',
  host => 'STRING'  -- optional, defaults to '*'
)
```

**Parameters:**

All parameters accept the same values as `add_acl`. You can use `'ANY'` as a wildcard value to match multiple entries for batch deletion.

**Example:**

```sql title="Flink SQL"
-- Use the Fluss catalog (replace 'fluss_catalog' with your catalog name if different)
USE fluss_catalog;

-- Remove a specific ACL entry
CALL sys.drop_acl(
  resource => 'CLUSTER',
  permission => 'ALLOW',
  principal => 'User:Alice',
  operation => 'READ',
  host => '*'
);

-- Remove all ACL entries for a specific user
CALL sys.drop_acl(
  resource => 'ANY',
  permission => 'ANY',
  principal => 'User:Alice',
  operation => 'ANY',
  host => 'ANY'
);
```

### list_acl

List ACL entries matching the specified filters.

**Syntax:**

```sql
CALL [catalog_name.]sys.list_acl(
  resource => 'STRING',
  permission => 'STRING',  -- optional, defaults to 'ANY'
  principal => 'STRING',   -- optional, defaults to 'ANY'
  operation => 'STRING',   -- optional, defaults to 'ANY'
  host => 'STRING'         -- optional, defaults to 'ANY'
)
```

**Parameters:**

All parameters accept the same values as `add_acl`. Use `'ANY'` as a wildcard to match all values for that parameter.

**Returns:** An array of strings, each representing an ACL entry in the format: `resource="...";permission="...";principal="...";operation="...";host="..."`

**Example:**

```sql title="Flink SQL"
-- Use the Fluss catalog (replace 'fluss_catalog' with your catalog name if different)
USE fluss_catalog;

-- List all ACL entries
CALL sys.list_acl(resource => 'ANY');

-- List all ACL entries for a specific user
CALL sys.list_acl(
  resource => 'ANY',
  principal => 'User:Alice'
);

-- List all read permissions
CALL sys.list_acl(
  resource => 'ANY',
  operation => 'READ'
);
```

## Cluster Configuration Procedures

Fluss provides procedures to dynamically manage cluster configurations without requiring a server restart.

### get_cluster_configs

Retrieve cluster configuration values.

**Syntax:**

```sql
-- Get multiple configurations
CALL [catalog_name.]sys.get_cluster_configs(config_keys => 'key1' [, 'key2', ...])

-- Get all cluster configurations
CALL [catalog_name.]sys.get_cluster_configs()
```

**Parameters:**

- `config_keys` (optional): The configuration keys to retrieve. If omitted, returns all cluster configurations.

**Returns:** A table with columns:
- `config_key`: The configuration key name
- `config_value`: The current value
- `config_source`: The source of the configuration (e.g., `DYNAMIC_CONFIG`, `STATIC_CONFIG`)

**Example:**

```sql title="Flink SQL"
-- Use the Fluss catalog (replace 'fluss_catalog' with your catalog name if different)
USE fluss_catalog;

-- Get a specific configuration
CALL sys.get_cluster_configs(
  config_keys => 'kv.rocksdb.shared-rate-limiter.bytes-per-sec'
);

-- Get multiple configuration
CALL sys.get_cluster_configs(
  config_keys => 'kv.rocksdb.shared-rate-limiter.bytes-per-sec', 'datalake.format'
);

-- Get all cluster configurations
CALL sys.get_cluster_configs();
```

### set_cluster_configs

Set cluster configurations dynamically.

**Syntax:**

```sql
-- Set configuration values
CALL [catalog_name.]sys.set_cluster_configs(
  config_pairs => 'key1', 'value1' [, 'key2', 'value2' ...]
)
```

**Parameters:**

- `config_pairs`(required): For key-value pairs in configuration items, the number of parameters must be even.

**Important Notes:**

- Changes are validated before being applied and persisted in ZooKeeper
- Changes are automatically applied to all servers (Coordinator and TabletServers)
- Changes survive server restarts
- Not all configurations support dynamic changes. The server will reject invalid modifications

**Example:**

```sql title="Flink SQL"
-- Use the Fluss catalog (replace 'fluss_catalog' with your catalog name if different)
USE fluss_catalog;

-- Set RocksDB rate limiter
CALL sys.set_cluster_configs(
  config_pairs => 'kv.rocksdb.shared-rate-limiter.bytes-per-sec', '200MB'
);

-- Set RocksDB rate limiter and datalake format
CALL sys.set_cluster_configs(
  config_pairs => 'kv.rocksdb.shared-rate-limiter.bytes-per-sec', '200MB', 'datalake.format','paimon'
);
```

### reset_cluster_configs

reset cluster configurations dynamically.

**Syntax:**

```sql
-- reset configuration values
CALL [catalog_name.]sys.reset_cluster_configs(config_keys => 'key1' [, 'key2', ...])
```

**Parameters:**

- `config_keys`(required): The configuration keys to reset.


**Example:**

```sql title="Flink SQL"
-- Use the Fluss catalog (replace 'fluss_catalog' with your catalog name if different)
USE fluss_catalog;

-- Reset a specific configuration
CALL sys.reset_cluster_configs(
  config_keys => 'kv.rocksdb.shared-rate-limiter.bytes-per-sec'
);

-- Reset RocksDB rate limiter and datalake format
CALL sys.reset_cluster_configs(
  config_keys => 'kv.rocksdb.shared-rate-limiter.bytes-per-sec', 'datalake.format'
);
```

## Rebalance Procedures

Fluss provides procedures to rebalance buckets across the cluster based on workload.
Rebalancing primarily occurs in the following scenarios: Offline existing tabletServers
from the cluster, adding new tabletServers to the cluster, and routine adjustments for load imbalance.

### add_server_tag

Add server tag to TabletServers in the cluster. For example, adding `tabletServer-0` with `PERMANENT_OFFLINE` tag
indicates that `tabletServer-0` is about to be permanently decommissioned, and during the next rebalance,
all buckets on this node need to be migrated away.

**Syntax:**

```sql
CALL [catalog_name.]sys.add_server_tag(
  tabletServers => 'STRING',
  serverTag => 'STRING'
)
```

**Parameters:**

- `tabletServers` (required): The TabletServer IDs to add tag to. Can be a single server ID (e.g., `'0'`) or multiple IDs separated by commas (e.g., `'0,1,2'`).
- `serverTag` (required): The tag to add to the TabletServers. Valid values are:
    - `'PERMANENT_OFFLINE'`: Indicates the TabletServer is permanently offline and will be decommissioned. All buckets on this server will be migrated during the next rebalance.
    - `'TEMPORARY_OFFLINE'`: Indicates the TabletServer is temporarily offline (e.g., for upgrading). Buckets may be temporarily migrated but can return after the server comes back online.

**Returns:** An array with a single element `'success'` if the operation completes successfully.

**Example:**

```sql title="Flink SQL"
-- Use the Fluss catalog (replace 'fluss_catalog' with your catalog name if different)
USE fluss_catalog;

-- Add PERMANENT_OFFLINE tag to a single TabletServer
CALL sys.add_server_tag('0', 'PERMANENT_OFFLINE');

-- Add TEMPORARY_OFFLINE tag to multiple TabletServers
CALL sys.add_server_tag('1,2,3', 'TEMPORARY_OFFLINE');
```

### remove_server_tag

Remove server tag from TabletServers in the cluster. This operation is typically used when a previously tagged TabletServer is ready to return to normal service, or to cancel a planned offline operation.

**Syntax:**

```sql
CALL [catalog_name.]sys.remove_server_tag(
  tabletServers => 'STRING',
  serverTag => 'STRING'
)
```

**Parameters:**

- `tabletServers` (required): The TabletServer IDs to remove tag from. Can be a single server ID (e.g., `'0'`) or multiple IDs separated by commas (e.g., `'0,1,2'`).
- `serverTag` (required): The tag to remove from the TabletServers. Valid values are:
    - `'PERMANENT_OFFLINE'`: Remove the permanent offline tag from the TabletServer.
    - `'TEMPORARY_OFFLINE'`: Remove the temporary offline tag from the TabletServer.

**Returns:** An array with a single element `'success'` if the operation completes successfully.

**Example:**

```sql title="Flink SQL"
-- Use the Fluss catalog (replace 'fluss_catalog' with your catalog name if different)
USE fluss_catalog;

-- Remove PERMANENT_OFFLINE tag from a single TabletServer
CALL sys.remove_server_tag('0', 'PERMANENT_OFFLINE');

-- Remove TEMPORARY_OFFLINE tag from multiple TabletServers
CALL sys.remove_server_tag('1,2,3', 'TEMPORARY_OFFLINE');
```

### rebalance

Trigger a rebalance operation to redistribute buckets across TabletServers in the cluster. This procedure helps balance workload based on specified goals, such as distributing replicas or leaders evenly across the cluster.

**Syntax:**

```sql
CALL [catalog_name.]sys.rebalance(
  priorityGoals => 'STRING'
)
```

**Parameters:**

- `priorityGoals` (required): The rebalance goals to achieve, specified as goal types. Can be a single goal (e.g., `'REPLICA_DISTRIBUTION'`) or multiple goals separated by commas (e.g., `'REPLICA_DISTRIBUTION,LEADER_DISTRIBUTION'`). Valid goal types are:
    - `'REPLICA_DISTRIBUTION'`: Generates replica movement tasks to ensure the number of replicas on each TabletServer is near balanced.
    - `'LEADER_DISTRIBUTION'`: Generates leadership movement and leader replica movement tasks to ensure the number of leader replicas on each TabletServer is near balanced.

**Returns:** An array with a single element containing the rebalance ID (e.g., `'rebalance-12345'`), which can be used to track or cancel the rebalance operation.

**Important Notes:**

- Multiple goals can be specified in priority order. The system will attempt to achieve goals in the order specified.
- Rebalance operations run asynchronously in the background. Use the returned rebalance ID to monitor progress.
- The rebalance operation respects server tags set by `add_server_tag`. For example, servers marked with `PERMANENT_OFFLINE` will have their buckets migrated away.

**Example:**

```sql title="Flink SQL"
-- Use the Fluss catalog (replace 'fluss_catalog' with your catalog name if different)
USE fluss_catalog;

-- Trigger rebalance with replica distribution goal
CALL sys.rebalance('REPLICA_DISTRIBUTION');

-- Trigger rebalance with multiple goals in priority order
CALL sys.rebalance('REPLICA_DISTRIBUTION,LEADER_DISTRIBUTION');
```

### list_rebalance

Query the progress and status of a rebalance operation. This procedure allows you to monitor ongoing or completed rebalance operations to track their progress and view detailed information about bucket movements.

**Syntax:**

```sql
-- List the most recent rebalance progress
CALL [catalog_name.]sys.list_rebalance()

-- List a specific rebalance progress by ID
CALL [catalog_name.]sys.list_rebalance(
  rebalanceId => 'STRING'
)
```

**Parameters:**

- `rebalanceId` (optional): The rebalance ID to query. If omitted, returns the progress of the most recent rebalance operation. The rebalance ID is returned when calling the `rebalance` procedure.

**Returns:** An array of strings containing:
- Rebalance ID: The unique identifier of the rebalance operation
- Rebalance total status: The overall status of the rebalance. Possible values are:
    - `NOT_STARTED`: The rebalance has been created but not yet started
    - `REBALANCING`: The rebalance is currently in progress
    - `COMPLETED`: The rebalance has successfully completed
    - `FAILED`: The rebalance has failed
    - `CANCELED`: The rebalance has been canceled
- Rebalance progress: The completion percentage (e.g., `75.5%`)
- Rebalance detail progress for bucket: Detailed progress information for each bucket being moved

If no rebalance is found, returns empty line.

**Example:**

```sql title="Flink SQL"
-- Use the Fluss catalog (replace 'fluss_catalog' with your catalog name if different)
USE fluss_catalog;

-- List the most recent rebalance progress
CALL sys.list_rebalance();

-- List a specific rebalance progress by ID
CALL sys.list_rebalance('rebalance-12345');
```

### cancel_rebalance

Cancel an ongoing rebalance operation. This procedure allows you to stop a rebalance that is in progress, which is useful when you need to halt bucket redistribution due to operational requirements or unexpected issues.

**Syntax:**

```sql
-- Cancel the most recent rebalance operation
CALL [catalog_name.]sys.cancel_rebalance()

-- Cancel a specific rebalance operation by ID
CALL [catalog_name.]sys.cancel_rebalance(
  rebalanceId => 'STRING'
)
```

**Parameters:**

- `rebalanceId` (optional): The rebalance ID to cancel. If omitted, cancels the most recent rebalance operation. The rebalance ID is returned when calling the `rebalance` procedure.

**Returns:** An array with a single element `'success'` if the operation completes successfully.

**Important Notes:**

- Only rebalance operations in `NOT_STARTED` or `REBALANCING` status can be canceled.
- Canceling a rebalance will stop bucket movements, but already completed bucket migrations will not be rolled back.
- After cancellation, the rebalance status will change to `CANCELED`.
- You can verify the cancellation by calling `list_rebalance` to check the status.

**Example:**

```sql title="Flink SQL"
-- Use the Fluss catalog (replace 'fluss_catalog' with your catalog name if different)
USE fluss_catalog;

-- Cancel the most recent rebalance operation
CALL sys.cancel_rebalance();

-- Cancel a specific rebalance operation by ID
CALL sys.cancel_rebalance('rebalance-12345');
```