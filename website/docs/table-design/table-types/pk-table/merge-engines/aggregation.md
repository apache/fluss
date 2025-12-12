---
sidebar_label: Aggregation
title: Aggregation Merge Engine
sidebar_position: 4
---

# Aggregation Merge Engine

## Overview

The **Aggregation Merge Engine** is designed for scenarios where users only care about aggregated results rather than individual records. It aggregates each value field with the latest data one by one under the same primary key according to the specified aggregate function.

Each field not part of the primary keys can be assigned an aggregate function using the `'table.merge-engine.aggregate.<field-name>'` property. If no function is specified for a field, it will use `last_non_null_value` aggregation as the default behavior.

This merge engine is useful for real-time aggregation scenarios such as:
- Computing running totals and statistics
- Maintaining counters and metrics
- Tracking maximum/minimum values over time
- Building real-time dashboards and analytics

## Configuration

To enable the aggregation merge engine, set the following table property:

```
'table.merge-engine' = 'aggregation'
```

Then specify the aggregate function for each non-primary key field:

```
'table.merge-engine.aggregate.<field-name>' = '<function-name>'
```

## API Usage

### Creating a Table with Aggregation

```java
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

// Create connection
Connection conn = Connection.create(config);
Admin admin = conn.getAdmin();

// Define schema
Schema schema = Schema.newBuilder()
    .column("product_id", DataTypes.BIGINT())
    .column("price", DataTypes.DOUBLE())
    .column("sales", DataTypes.BIGINT())
    .column("last_update_time", DataTypes.TIMESTAMP(3))
    .primaryKey("product_id")
    .build();

// Create table with aggregation merge engine
TableDescriptor tableDescriptor = TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.price", "max")
    .property("table.merge-engine.aggregate.sales", "sum")
    // last_update_time will use default 'last_non_null_value'
    .build();

TablePath tablePath = TablePath.of("my_database", "product_stats");
admin.createTable(tablePath, tableDescriptor, false).get();
```

### Writing Data

```java
// Get table
Table table = conn.getTable(tablePath);

// Create upsert writer
UpsertWriter writer = table.newUpsert().createWriter();

// Write data - these will be aggregated
writer.upsert(row(1L, 23.0, 15L, timestamp1));
writer.upsert(row(1L, 30.2, 20L, timestamp2)); // Same primary key - triggers aggregation

writer.flush();
```

**Result after aggregation:**
- `product_id`: 1
- `price`: 30.2 (max of 23.0 and 30.2)
- `sales`: 35 (sum of 15 and 20)
- `last_update_time`: timestamp2 (last non-null value)

## Supported Aggregate Functions

Fluss currently supports the following aggregate functions:

### sum

Aggregates values by computing the sum across multiple rows.

- **Supported Data Types**: TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL
- **Behavior**: Adds incoming values to the accumulator
- **Null Handling**: Null values are ignored

**Example:**
```java
TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.amount", "sum")
    .build();

// Input: (1, 100.50), (1, 200.75)
// Result: (1, 301.25)
```

### product

Computes the product of values across multiple rows.

- **Supported Data Types**: TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL
- **Behavior**: Multiplies incoming values with the accumulator
- **Null Handling**: Null values are ignored

**Example:**
```java
TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.discount_factor", "product")
    .build();

// Input: (1, 0.9), (1, 0.8)
// Result: (1, 0.72) -- 90% * 80% = 72%
```

### max

Identifies and retains the maximum value.

- **Supported Data Types**: CHAR, STRING, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ
- **Behavior**: Keeps the larger value between accumulator and incoming value
- **Null Handling**: Null values are ignored

**Example:**
```java
TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.max_temperature", "max")
    .property("table.merge-engine.aggregate.max_reading_time", "max")
    .build();

// Input: (1, 25.5, '2024-01-01 10:00:00'), (1, 28.3, '2024-01-01 11:00:00')
// Result: (1, 28.3, '2024-01-01 11:00:00')
```

### min

Identifies and retains the minimum value.

- **Supported Data Types**: CHAR, STRING, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ
- **Behavior**: Keeps the smaller value between accumulator and incoming value
- **Null Handling**: Null values are ignored

**Example:**
```java
TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.lowest_price", "min")
    .build();

// Input: (1, 99.99), (1, 79.99), (1, 89.99)
// Result: (1, 79.99)
```

### last_value

Replaces the previous value with the most recently received value.

- **Supported Data Types**: All data types
- **Behavior**: Always uses the latest incoming value
- **Null Handling**: Null values will overwrite previous values

**Example:**
```java
TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.status", "last_value")
    .property("table.merge-engine.aggregate.last_login", "last_value")
    .build();

// Input: (1, 'online', '2024-01-01 10:00:00'), (1, 'offline', '2024-01-01 11:00:00')
// Result: (1, 'offline', '2024-01-01 11:00:00')
```

### last_non_null_value

Replaces the previous value with the latest non-null value. This is the **default aggregate function** when no function is specified.

- **Supported Data Types**: All data types
- **Behavior**: Uses the latest incoming value only if it's not null
- **Null Handling**: Null values are ignored, previous value is retained

**Example:**
```java
TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.email", "last_non_null_value")
    .property("table.merge-engine.aggregate.phone", "last_non_null_value")
    .build();

// Input: (1, 'user@example.com', '123-456'), (1, null, '789-012')
// Result: (1, 'user@example.com', '789-012')
// Email remains 'user@example.com' because the second upsert had null email
```

### first_value

Retrieves and retains the first value seen for a field.

- **Supported Data Types**: All data types
- **Behavior**: Keeps the first received value, ignores all subsequent values
- **Null Handling**: Null values are retained if received first

**Example:**
```java
TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.first_purchase_date", "first_value")
    .property("table.merge-engine.aggregate.first_product", "first_value")
    .build();

// Input: (1, '2024-01-01', 'ProductA'), (1, '2024-02-01', 'ProductB')
// Result: (1, '2024-01-01', 'ProductA')
```

### first_non_null_value

Selects the first non-null value in a data set.

- **Supported Data Types**: All data types
- **Behavior**: Keeps the first received non-null value, ignores all subsequent values
- **Null Handling**: Null values are ignored until a non-null value is received

**Example:**
```java
TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.email", "first_non_null_value")
    .property("table.merge-engine.aggregate.verified_at", "first_non_null_value")
    .build();

// Input: (1, null, null), (1, 'user@example.com', '2024-01-01 10:00:00'), (1, 'other@example.com', '2024-01-02 10:00:00')
// Result: (1, 'user@example.com', '2024-01-01 10:00:00')
```

### listagg

Concatenates multiple string values into a single string with a delimiter.

- **Supported Data Types**: STRING, CHAR
- **Behavior**: Concatenates values using the specified delimiter
- **Null Handling**: Null values are skipped
- **Configuration**: Use `'table.merge-engine.aggregate.<field-name>.listagg-delimiter'` to specify a custom delimiter (default is comma `,`)

**Example:**
```java
TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.tags", "listagg")
    .property("table.merge-engine.aggregate.tags.listagg-delimiter", ";")
    .build();

// Input: (1, 'developer'), (1, 'java'), (1, 'flink')
// Result: (1, 'developer;java;flink')
```

### bool_and

Evaluates whether all boolean values in a set are true (logical AND).

- **Supported Data Types**: BOOLEAN
- **Behavior**: Returns true only if all values are true
- **Null Handling**: Null values are ignored

**Example:**
```java
TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.has_all_permissions", "bool_and")
    .build();

// Input: (1, true), (1, true), (1, false)
// Result: (1, false) -- Not all values are true
```

### bool_or

Checks if at least one boolean value in a set is true (logical OR).

- **Supported Data Types**: BOOLEAN
- **Behavior**: Returns true if any value is true
- **Null Handling**: Null values are ignored

**Example:**
```java
TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.has_any_alert", "bool_or")
    .build();

// Input: (1, false), (1, false), (1, true)
// Result: (1, true) -- At least one value is true
```

## Advanced Configuration

### Default Aggregate Function

You can set a default aggregate function for all non-primary key fields that don't have an explicitly specified function:

```java
TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.default-function", "last_value")
    .property("table.merge-engine.aggregate.col2", "sum")  // Override default for col2
    .build();
```

In this example:
- `col2` uses `sum` aggregation
- Other non-primary-key columns use `last_value` as the default

### Partial Update with Aggregation

The aggregation merge engine supports partial updates through the UpsertWriter API. When performing a partial update:

- **Target columns**: These columns will be aggregated according to their configured aggregate functions
- **Non-target columns**: These columns will retain their existing values from the old row

**Example:**

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("count1", DataTypes.BIGINT())
    .column("count2", DataTypes.BIGINT())
    .column("sum1", DataTypes.DOUBLE())
    .column("sum2", DataTypes.DOUBLE())
    .primaryKey("id")
    .build();

TableDescriptor tableDescriptor = TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.count1", "sum")
    .property("table.merge-engine.aggregate.count2", "sum")
    .property("table.merge-engine.aggregate.sum1", "sum")
    .property("table.merge-engine.aggregate.sum2", "sum")
    .build();

// Create partial update writer targeting only id, count1, and sum1
int[] targetColumns = new int[]{0, 1, 3}; // id, count1, sum1
UpsertWriter partialWriter = table.newUpsert()
    .withPartialUpdate(targetColumns)
    .createWriter();

// When writing:
// - count1 and sum1 will be aggregated with existing values
// - count2 and sum2 will remain unchanged
```

**Use cases for partial aggregation**:
1. **Independent metrics**: When different processes update different subsets of metrics for the same key
2. **Reduced data transfer**: Only send the columns that need to be updated
3. **Flexible pipelines**: Different data sources can contribute to different aggregated fields

### Delete Behavior

The aggregation merge engine provides limited support for delete operations. You can configure the behavior using the `'table.agg.remove-record-on-delete'` option:

```java
TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.agg.remove-record-on-delete", "true")  // Default is false
    .build();
```

**Configuration options**:
- **`'table.agg.remove-record-on-delete' = 'false'`** (default): Delete operations will cause an error
- **`'table.agg.remove-record-on-delete' = 'true'`**: Delete operations will remove the entire record from the table

:::note
**Current Limitation**: The aggregation merge engine does not support retraction semantics (e.g., subtracting from a sum, reverting a max). Delete operations can only remove the entire record or be rejected.

Future versions may support fine-grained retraction by enhancing the protocol to carry row data with delete operations.
:::

## Use Cases

### Real-Time Metrics Dashboard

```java
Schema schema = Schema.newBuilder()
    .column("product_id", DataTypes.BIGINT())
    .column("total_sales", DataTypes.BIGINT())
    .column("max_price", DataTypes.DOUBLE())
    .column("min_price", DataTypes.DOUBLE())
    .column("last_update", DataTypes.TIMESTAMP(3))
    .primaryKey("product_id")
    .build();

TableDescriptor tableDescriptor = TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.total_sales", "sum")
    .property("table.merge-engine.aggregate.max_price", "max")
    .property("table.merge-engine.aggregate.min_price", "min")
    .property("table.merge-engine.aggregate.last_update", "last_value")
    .build();
```

### User Session Tracking

```java
Schema schema = Schema.newBuilder()
    .column("user_id", DataTypes.BIGINT())
    .column("first_login", DataTypes.TIMESTAMP(3))
    .column("last_login", DataTypes.TIMESTAMP(3))
    .column("total_clicks", DataTypes.INT())
    .column("visited_pages", DataTypes.STRING())
    .primaryKey("user_id")
    .build();

TableDescriptor tableDescriptor = TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.first_login", "first_non_null_value")
    .property("table.merge-engine.aggregate.last_login", "last_value")
    .property("table.merge-engine.aggregate.total_clicks", "sum")
    .property("table.merge-engine.aggregate.visited_pages", "listagg")
    .property("table.merge-engine.aggregate.visited_pages.listagg-delimiter", ",")
    .build();
```

### IoT Sensor Data Aggregation

```java
Schema schema = Schema.newBuilder()
    .column("sensor_id", DataTypes.BIGINT())
    .column("max_temp", DataTypes.DOUBLE())
    .column("min_temp", DataTypes.DOUBLE())
    .column("avg_humidity", DataTypes.DOUBLE())
    .column("alert_triggered", DataTypes.BOOLEAN())
    .primaryKey("sensor_id")
    .build();

TableDescriptor tableDescriptor = TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.max_temp", "max")
    .property("table.merge-engine.aggregate.min_temp", "min")
    .property("table.merge-engine.aggregate.avg_humidity", "last_value")
    .property("table.merge-engine.aggregate.alert_triggered", "bool_or")
    .build();
```

## Performance Considerations

1. **Choose Appropriate Aggregate Functions**: Select functions that match your use case to avoid unnecessary computations
2. **Primary Key Design**: Use appropriate primary keys to ensure proper grouping of aggregated data
3. **Null Handling**: Be aware of how each function handles null values to avoid unexpected results
4. **Delete Handling**: If you need to handle delete operations, be aware that enabling `'table.agg.remove-record-on-delete' = 'true'` will remove entire records rather than retracting aggregated values

## Limitations

:::warning Critical Limitations
When using the `aggregation` merge engine, be aware of the following critical limitations:

### 1. No Transactional Guarantee After Compute Engine Failover

**The aggregation merge engine does not provide transactional guarantees when the upstream compute engine (e.g., Flink, Spark) experiences a failover or restart.** This is a fundamental limitation caused by the fact that Fluss primary key tables do not support transactional writes and rollback mechanisms.

**Root Cause:**
- Fluss primary key tables lack transaction support and rollback capabilities
- When records are written to Fluss, they are immediately visible and cannot be rolled back
- Even if the compute engine supports exactly-once semantics (like Flink), the absence of transaction support in Fluss prevents achieving end-to-end exactly-once guarantees

**What Happens During Failover:**
- When a compute engine restarts from a checkpoint/savepoint, it may replay records that were already successfully written and aggregated in Fluss
- Since Fluss cannot rollback these already-written records, each replayed record will be re-aggregated with the existing aggregated value, leading to **incorrect results**
- There is no mechanism to detect or prevent duplicate aggregation of replayed data

**Example of Incorrect Behavior:**

Suppose you have a table tracking total sales with a `sum` aggregate function:

```
Initial state: product_id=1, total_sales=100

1. Compute engine writes: product_id=1, sales=50
   → Fluss aggregates: total_sales = 100 + 50 = 150 ✓ (correct)

2. Compute engine checkpoints

3. Compute engine crashes and restarts from checkpoint

4. Compute engine replays the same record: product_id=1, sales=50
   → Fluss re-aggregates: total_sales = 150 + 50 = 200 ✗ (incorrect - should be 150)
```

**Impact:**
- Aggregate functions like `sum`, `product`, `listagg` will produce **incorrect cumulative results**
- Functions like `max`, `min` may produce incorrect results if replayed values differ from original values
- Functions like `last_value`, `first_value` may show inconsistent behavior

**Recommendations:**
- **Use aggregation merge engine only for at-least-once scenarios** where duplicate aggregation is acceptable or where you have application-level deduplication
- For exactly-once aggregation semantics, perform aggregation within the compute engine (e.g., using Flink's state) and sink only the final aggregated results using the `default` or `versioned` merge engine
- Alternatively, use idempotent aggregate functions (like `max` for monotonically increasing values) where replaying has minimal impact
- Monitor and validate aggregation correctness in production environments

:::note
Future versions may introduce transaction support or sequence number tracking to prevent duplicate aggregation after failover.
:::

### 2. No Retraction Support

The aggregation merge engine does not support retraction semantics (e.g., subtracting from a sum, reverting a max):
- Delete operations do not carry row values, preventing fine-grained retraction
- The system can only remove entire records (if `'table.agg.remove-record-on-delete' = 'true'`) or reject delete operations
- `UPDATE_BEFORE` messages from changelog streams must be filtered out by compute engines

### 3. Delete Operations

By default, delete operations will cause errors:
- You must set `'table.agg.remove-record-on-delete' = 'true'` if you need to handle delete operations
- This configuration will remove the entire aggregated record, not reverse individual aggregations

### 4. Data Type Restrictions

Each aggregate function supports specific data types (see function documentation above)
:::

## Integration with Compute Engines

When integrating the Aggregation Merge Engine with compute engines (such as Apache Flink, Apache Spark, etc.), there are important requirements and limitations that must be understood to ensure correct aggregation behavior.

:::warning Important
Due to the [lack of transactional guarantees after compute engine failover](#1-no-transactional-guarantee-after-compute-engine-failover), the aggregation merge engine **cannot guarantee exactly-once semantics** when used with compute engines that may experience failures. This limitation exists because Fluss primary key tables do not support transactional writes and rollback mechanisms. Even if the compute engine supports exactly-once semantics (like Flink), only at-least-once delivery guarantees can be achieved when writing to aggregation tables.
:::

### Requirements for Compute Engines

#### 1. UPDATE_BEFORE Message Filtering

**Requirement**: Compute engines must filter out or ignore `UPDATE_BEFORE` changelog messages when writing to aggregation tables.

**Rationale**:
- The aggregation merge engine does not support retraction semantics
- `UPDATE_BEFORE` messages cannot be used to reverse previous aggregations
- Only `INSERT` and `UPDATE_AFTER` messages should trigger aggregation operations

**Implementation Considerations**:
```
Changelog Stream Processing:
┌─────────────────┬──────────────────┬─────────────────────────┐
│ Message Type    │ Action           │ Sent to Fluss?          │
├─────────────────┼──────────────────┼─────────────────────────┤
│ INSERT          │ Aggregate        │ ✓ Yes (as upsert)       │
│ UPDATE_AFTER    │ Aggregate        │ ✓ Yes (as upsert)       │
│ UPDATE_BEFORE   │ Ignore/Filter    │ ✗ No (must be filtered) │
│ DELETE          │ Configurable     │ Optional (see config)   │
└─────────────────┴──────────────────┴─────────────────────────┘
```

**Impact of Not Filtering**:
- If `UPDATE_BEFORE` messages are sent as delete operations, they will cause errors (unless `table.agg.remove-record-on-delete=true`)
- Incorrect filtering may lead to unexpected record deletions
- Performance degradation due to unnecessary operations



### Integration Examples

For concrete implementation examples, see:
- [Flink Integration - Writing to Aggregation Tables](../../../../engine-flink/writes.md#writing-to-aggregation-tables)

## See Also

- [Default Merge Engine](./default.md)
- [FirstRow Merge Engine](./first-row.md)
- [Versioned Merge Engine](./versioned.md)
- [Primary Key Tables](../index.md)
- [Fluss Client API](../../../../apis/java-client.md)
