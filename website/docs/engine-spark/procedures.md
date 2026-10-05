---
sidebar_label: Procedures
title: Procedures
sidebar_position: 3
---

# Spark Procedures

Fluss provides stored procedures to perform administrative and management operations through Spark SQL. All procedures are located in the `sys` namespace and can be invoked using the `CALL` statement.

## Configuration

To enable Fluss procedures in Spark, you need to configure the Spark session extensions:

```scala
spark.conf.set("spark.sql.extensions", "org.apache.fluss.spark.FlussSparkSessionExtensions")
```

Or in `spark-defaults.conf`:

```properties
spark.sql.extensions=org.apache.fluss.spark.FlussSparkSessionExtensions
```

## Syntax

The general syntax for calling a procedure is:

```sql
CALL [catalog_name.]sys.procedure_name(
  parameter_name => 'value',
  another_parameter => 'value'
)
```

### Argument Passing

Procedures support two ways to pass arguments:

1. **Named Arguments** (recommended):
   ```sql
   CALL catalog.sys.procedure_name(parameter => 'value')
   ```

2. **Positional Arguments**:
   ```sql
   CALL catalog.sys.procedure_name('value')
   ```

Note: You cannot mix named and positional arguments in a single procedure call.

## Cluster Configuration Procedures

Fluss provides procedures to dynamically manage cluster configurations without requiring a server restart.

### get_cluster_configs

Retrieve cluster configuration values.

**Syntax:**

```sql
CALL [catalog_name.]sys.get_cluster_configs()
CALL [catalog_name.]sys.get_cluster_configs(config_keys => ARRAY('key1', 'key2'))
```

**Parameters:**

- `config_keys` (optional): Array of configuration keys to retrieve. If omitted, returns all cluster configurations.

**Returns:** A table with columns:

- `config_key`: The configuration key name
- `config_value`: The current value
- `config_source`: The source of the configuration (e.g., `DEFAULT`, `DYNAMIC`, `STATIC`)

**Example:**

```sql title="Spark SQL"
-- Use the Fluss catalog (replace 'fluss_catalog' with your catalog name if different)
USE fluss_catalog;

-- Get all cluster configurations
CALL sys.get_cluster_configs();

-- Get specific configurations
CALL sys.get_cluster_configs(config_keys => ARRAY('kv.rocksdb.shared-rate-limiter.bytes-per-sec', 'datalake.format'));
```

### set_cluster_configs

Set cluster configuration values dynamically. The changes are validated, persisted, and applied to all servers without requiring a restart.

**Syntax:**

```sql
CALL [catalog_name.]sys.set_cluster_configs(config_pairs => ARRAY('key1', 'value1', 'key2', 'value2'))
```

**Parameters:**

- `config_pairs` (required): Array of configuration key-value pairs. Keys and values must alternate (key1, value1, key2, value2, ...).

**Returns:** A table with columns:

- `config_key`: The configuration key that was set
- `config_value`: The value that was set
- `result`: A message indicating the result of the operation

**Example:**

```sql title="Spark SQL"
-- Set a single configuration
CALL sys.set_cluster_configs(config_pairs => ARRAY('kv.rocksdb.shared-rate-limiter.bytes-per-sec', '200MB'));

-- Set multiple configurations at once
CALL sys.set_cluster_configs(config_pairs => ARRAY('kv.rocksdb.shared-rate-limiter.bytes-per-sec', '200MB', 'datalake.format', 'paimon'));
```

:::note
Not all configurations support dynamic changes. The server will validate the change and reject it if the configuration cannot be modified dynamically or if the new value is invalid.
:::

### reset_cluster_configs

Reset cluster configurations to their default values. The changes are validated, persisted, and applied to all servers without requiring a restart.

**Syntax:**

```sql
CALL [catalog_name.]sys.reset_cluster_configs(config_keys => ARRAY('key1', 'key2'))
```

**Parameters:**

- `config_keys` (required): Array of configuration keys to reset to their default values.

**Returns:** A table with columns:

- `config_key`: The configuration key that was reset
- `result`: A message indicating the result of the operation

**Example:**

```sql title="Spark SQL"
-- Reset a single configuration
CALL sys.reset_cluster_configs(config_keys => ARRAY('kv.rocksdb.shared-rate-limiter.bytes-per-sec'));

-- Reset multiple configurations at once
CALL sys.reset_cluster_configs(config_keys => ARRAY('kv.rocksdb.shared-rate-limiter.bytes-per-sec', 'datalake.format'));
```

## Error Handling

Procedures will throw exceptions in the following cases:

- **Missing Required Parameters**: If a required parameter is not provided
- **Invalid Procedure Name**: If the specified procedure does not exist
- **Type Mismatch**: If a parameter value cannot be converted to the expected type
- **Permission Denied**: If the user does not have permission to perform the operation

## Implementation Notes

- Procedures are executed synchronously and return results immediately
- The `sys` namespace is reserved for system procedures
- Custom procedures can be added by implementing the `Procedure` interface

## See Also

- [Flink Procedures](../../engine-flink/procedures)
