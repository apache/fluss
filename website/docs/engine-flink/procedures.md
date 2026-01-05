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

### get_cluster_config

Retrieve cluster configuration values.

**Syntax:**

```sql
-- Get a specific configuration
CALL [catalog_name.]sys.get_cluster_config(config_key => 'STRING')

-- Get all cluster configurations
CALL [catalog_name.]sys.get_cluster_config()
```

**Parameters:**

- `config_key` (optional): The configuration key to retrieve. If omitted, returns all cluster configurations.

**Returns:** A table with columns:
- `config_key`: The configuration key name
- `config_value`: The current value
- `config_source`: The source of the configuration (e.g., `DYNAMIC_CONFIG`, `STATIC_CONFIG`)

**Example:**

```sql title="Flink SQL"
-- Use the Fluss catalog (replace 'fluss_catalog' with your catalog name if different)
USE fluss_catalog;

-- Get a specific configuration
CALL sys.get_cluster_config(
  config_key => 'kv.rocksdb.shared-rate-limiter.bytes-per-sec'
);

-- Get all cluster configurations
CALL sys.get_cluster_config();
```

### set_cluster_config

Set or delete a cluster configuration dynamically.

**Syntax:**

```sql
-- Set a configuration value
CALL [catalog_name.]sys.set_cluster_config(
  config_key => 'STRING',
  config_value => 'STRING'
)

-- Delete a configuration (reset to default)
CALL [catalog_name.]sys.set_cluster_config(config_key => 'STRING')
```

**Parameters:**

- `config_key` (required): The configuration key to modify.
- `config_value` (optional): The new value to set. If omitted or empty, the configuration is deleted (reset to default).

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
CALL sys.set_cluster_config(
  config_key => 'kv.rocksdb.shared-rate-limiter.bytes-per-sec',
  config_value => '200MB'
);

-- Set datalake format
CALL sys.set_cluster_config(
  config_key => 'datalake.format',
  config_value => 'paimon'
);

-- Delete a configuration (reset to default)
CALL sys.set_cluster_config(
  config_key => 'kv.rocksdb.shared-rate-limiter.bytes-per-sec'
);
```

