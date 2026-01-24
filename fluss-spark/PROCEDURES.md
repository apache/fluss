# Fluss Spark Procedures

This document describes the stored procedures available in Fluss for Spark.

## Overview

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

## Available Procedures

Currently, no procedures are implemented in this PR. This section will be updated when procedures are added.

## Error Handling

Procedures will throw exceptions in the following cases:

- **Missing Required Parameters**: If a required parameter is not provided
- **Invalid Table Name**: If the specified table does not exist
- **Type Mismatch**: If a parameter value cannot be converted to the expected type
- **Permission Denied**: If the user does not have permission to perform the operation

## Examples

### Basic Usage

```scala
// Start Spark with Fluss extensions
val spark = SparkSession.builder()
  .config("spark.sql.extensions", "org.apache.fluss.spark.FlussSparkSessionExtensions")
  .config("spark.sql.catalog.fluss_catalog", "org.apache.fluss.spark.SparkCatalog")
  .config("spark.sql.catalog.fluss_catalog.bootstrap.servers", "localhost:9092")
  .getOrCreate()

// Create a table
spark.sql("""
  CREATE TABLE fluss_catalog.my_db.my_table (
    id INT,
    name STRING,
    age INT
  ) USING fluss
""")

// Procedures will be added here when implemented
```

## Implementation Notes

- Procedures are executed synchronously and return results immediately
- The `sys` namespace is reserved for system procedures
- Custom procedures can be added by implementing the `Procedure` interface

## See Also

- [Fluss Spark Connector Documentation](../spark-connector.md)
- [Fluss Admin API](../admin-api.md)
