---
sidebar_label: Aggregation
title: Aggregation Merge Engine
sidebar_position: 5
---

# Aggregation Merge Engine

## Overview

The **Aggregation Merge Engine** is designed for scenarios where users only care about aggregated results rather than individual records. It aggregates each value field with the latest data one by one under the same primary key according to the specified aggregate function.

Each field not part of the primary keys can be assigned an aggregate function. The recommended way depends on the client you are working with:
- For **Flink SQL** or **Spark SQL**, use DDL and connector options (`'fields.<field-name>.agg'`)
- For **Java clients**, use the Schema API

If no function is specified for a field, it will use `last_value_ignore_nulls` aggregation as the default behavior.

When using `aggregation` merge engine, `UPDATE` and `DELETE` SQL statements are not supported.

This merge engine is useful for real-time aggregation scenarios such as:
- Computing running totals and statistics
- Maintaining counters and metrics
- Tracking maximum/minimum values over time
- Building real-time dashboards and analytics

## Configuration

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

To enable the aggregation merge engine, set the following table property:

```sql
CREATE TABLE product_stats (
    product_id BIGINT,
    price DOUBLE,
    sales BIGINT,
    last_update_time TIMESTAMP(3),
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.price.agg' = 'max',
    'fields.sales.agg' = 'sum'
    -- last_update_time defaults to 'last_value_ignore_nulls'
);
```

Specify the aggregate function for each non-primary key field using connector options:

```sql
'fields.<field-name>.agg' = '<function-name>'
```

For functions that require parameters (e.g., `listagg` with custom delimiter):

```sql
'fields.<field-name>.agg' = '<function-name>',
'fields.<field-name>.<function-name>.<param-name>' = '<param-value>'
```

</TabItem>
<TabItem value="java-client" label="Java Client">

To enable the aggregation merge engine, set the following table property:

```java
TableDescriptor tableDescriptor = TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();
```

Specify the aggregate function for each non-primary key field using the Schema API:

```java
Schema schema = Schema.newBuilder()
    .column("product_id", DataTypes.BIGINT())
    .column("price", DataTypes.DOUBLE(), AggFunctions.MAX())
    .column("sales", DataTypes.BIGINT(), AggFunctions.SUM())
    .column("last_update_time", DataTypes.TIMESTAMP(3))  // Defaults to LAST_VALUE_IGNORE_NULLS
    .primaryKey("product_id")
    .build();
```

</TabItem>
</Tabs>

## Usage Examples

<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

### Creating a Table with Aggregation

```sql
CREATE TABLE product_stats (
    product_id BIGINT,
    price DOUBLE,
    sales BIGINT,
    last_update_time TIMESTAMP(3),
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.price.agg' = 'max',
    'fields.sales.agg' = 'sum'
    -- last_update_time defaults to 'last_value_ignore_nulls'
);
```

### Writing Data

```sql
-- Insert data - these will be aggregated
INSERT INTO product_stats VALUES
    (1, 23.0, 15, TIMESTAMP '2024-01-01 10:00:00'),
    -- Same primary key - triggers aggregation
    (1, 30.2, 20, TIMESTAMP '2024-01-01 11:00:00');
```

### Querying Results

```sql
-- Point query by primary key
SELECT * FROM product_stats WHERE product_id = 1;
```

**Result after aggregation:**
```
+------------+-------+-------+---------------------+
| product_id | price | sales | last_update_time     |
+------------+-------+-------+---------------------+
|          1 |  30.2 |    35 | 2024-01-01 11:00:00 |
+------------+-------+-------+---------------------+
```

- `product_id`: 1
- `price`: 30.2 (max of 23.0 and 30.2)
- `sales`: 35 (sum of 15 and 20)
- `last_update_time`: 2024-01-01 11:00:00 (last non-null value)

</TabItem>
<TabItem value="java-client" label="Java Client">

### Creating a Table with Aggregation

```java
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.metadata.AggFunction;

// Create connection
Connection conn = Connection.create(config);
Admin admin = conn.getAdmin();

// Define schema with aggregation functions
Schema schema = Schema.newBuilder()
    .column("product_id", DataTypes.BIGINT())
    .column("price", DataTypes.DOUBLE(), AggFunctions.MAX())
    .column("sales", DataTypes.BIGINT(), AggFunctions.SUM())
    .column("last_update_time", DataTypes.TIMESTAMP(3))  // Defaults to LAST_VALUE_IGNORE_NULLS
    .primaryKey("product_id")
    .build();

// Create table with aggregation merge engine
TableDescriptor tableDescriptor = TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
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

</TabItem>
</Tabs>

## Supported Aggregate Functions

Fluss currently supports the following aggregate functions:

The Behavior and Null Handling entries below describe forward aggregation through `upsert(row)` or Flink INSERT / UPDATE_AFTER. Forward aggregation support does not imply retract support. Each function lists its native retract behavior; setting `fields.<column>.ignore-retract=true` preserves that field instead of applying a retract rule. See [Retract Validation and Paimon Compatibility](#retract-validation-and-paimon-compatibility) for the scope and timing of rejection.

### count

Counts non-null input values for each primary key.

- **Supported Data Type**: `BIGINT`, for both input and stored count
- **Behavior**: Each non-null input contributes one, regardless of its numeric value. The first non-null input is stored as `1`.
- **Null Handling**: Null inputs contribute zero. A null input initializes a `NULL` count to `0` and leaves an existing count unchanged.
- **Partial Updates**: Only updates targeting this column contribute to the count. An untargeted count column retains its previous value, or remains `NULL` on the first write for a key. A targeted column follows the behavior above, including initialization to `0` for a null input.

```sql
CREATE TABLE event_counts (
    id INT,
    cnt BIGINT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.cnt.agg' = 'count'
);

INSERT INTO event_counts VALUES (1, 100), (1, 0), (1, NULL);
-- The stored cnt for id = 1 is 2.
```

For the Java API, use `.column("cnt", DataTypes.BIGINT(), AggFunctions.COUNT())`.
To count every input row, provide a non-null value for this column in every row.
To accumulate counts already computed upstream, use `sum` instead. Native retraction is not implemented for `count`. See [Retract Validation and Paimon Compatibility](#retract-validation-and-paimon-compatibility) for validation timing and the field-level `ignore-retract` option.

When `'table.delete.behavior' = 'allow'`, a partial delete targeting the count column clears it to `NULL` rather than decrementing it. A subsequent targeted update starts the count at `1` for a non-null input or `0` for a null input. If the count column is not targeted by the delete, its value is retained; a count of `0` is non-null and therefore prevents the entire record from being removed when deleting other columns. See [Delete Behavior](#delete-behavior) for the full and partial delete rules.

### sum

Aggregates values by computing the sum across multiple rows.

- **Supported Data Types**: `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL`
- **Behavior**: Adds incoming values to the accumulator
- **Null Handling**: Null values are ignored
- **Retraction**: Supported: subtracts a non-null input; a null input leaves the accumulator unchanged. Retracting from a null accumulator produces the negative input. See [Field Rules](#field-rules) and [validation timing](#retract-validation-and-paimon-compatibility).

**Example:**

<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_sum (
    id BIGINT,
    amount DECIMAL(10, 2),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.amount.agg' = 'sum'
);

INSERT INTO test_sum VALUES
    (1, 100.50),
    (1, 200.75);

SELECT * FROM test_sum WHERE id = 1;
+------------+---------+
| id         | amount  |
+------------+---------+
|          1 | 301.25  |
+------------+---------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("amount", DataTypes.DECIMAL(10, 2), AggFunctions.SUM())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 100.50), (1, 200.75)
// Result: (1, 301.25)
```
</TabItem>
</Tabs>

### product

Computes the product of values across multiple rows.

- **Supported Data Types**: `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL`
- **Behavior**: Multiplies incoming values with the accumulator
- **Null Handling**: Null values are ignored
- **Retraction**: Divides a non-null accumulator by a non-null input. A null accumulator remains null; a null input preserves the accumulator. Integer division truncates, integral or decimal division by zero fails, and non-terminating decimal division fails. Decimal results use the field's scale and become null if their precision exceeds the field's precision. Floating-point division follows its Infinity/NaN rules. These rules follow Paimon 2.1.0; they do not reconstruct factors lost through zero or overflow. See [validation timing](#retract-validation-and-paimon-compatibility).

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_product (
    id BIGINT,
    discount_factor DOUBLE,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.discount_factor.agg' = 'product'
);

INSERT INTO test_product VALUES
    (1, 0.9),
    (1, 0.8);

SELECT * FROM test_product WHERE id = 1;
+------------+-----------------------+
| id         | discount_factor       |
+------------+-----------------------+
|          1 | 0.7200000000000001    |
+------------+-----------------------+
```

:::note
The result `0.7200000000000001` instead of `0.72` is expected behavior due to IEEE 754 double-precision floating-point arithmetic. If exact precision is required, consider using `DECIMAL` type instead of `DOUBLE`.
:::

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("discount_factor", DataTypes.DOUBLE(), AggFunctions.PRODUCT())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 0.9), (1, 0.8)
// Result: (1, 0.7200000000000001) -- due to IEEE 754 floating-point arithmetic
// Use DECIMAL type if exact precision is needed
```

</TabItem>
</Tabs>

### max

Identifies and retains the maximum value.

- **Supported Data Types**: `CHAR`, `STRING`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL`, `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMP_LTZ`
- **Behavior**: Keeps the larger value between accumulator and incoming value
- **Null Handling**: Null values are ignored
- **Retraction**: Not implemented; the stored maximum does not retain the other values needed to recompute a maximum after a contribution is removed. See [validation timing](#retract-validation-and-paimon-compatibility).

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_max (
    id BIGINT,
    temperature DOUBLE,
    reading_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.temperature.agg' = 'max',
    'fields.reading_time.agg' = 'max'
);

INSERT INTO test_max VALUES
    (1, 25.5, TIMESTAMP '2024-01-01 10:00:00'),
    (1, 28.3, TIMESTAMP '2024-01-01 11:00:00');

SELECT * FROM test_max WHERE id = 1;
+------------+----------------+---------------------+
| id         | temperature    | reading_time        |
+------------+----------------+---------------------+
|          1 | 28.3           | 2024-01-01 11:00:00 |
+------------+----------------+---------------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("max_temperature", DataTypes.DOUBLE(), AggFunctions.MAX())
    .column("max_reading_time", DataTypes.TIMESTAMP(3), AggFunctions.MAX())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 25.5, '2024-01-01 10:00:00'), (1, 28.3, '2024-01-01 11:00:00')
// Result: (1, 28.3, '2024-01-01 11:00:00')
```
</TabItem>
</Tabs>

### min

Identifies and retains the minimum value.

- **Supported Data Types**: `CHAR`, `STRING`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL`, `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMP_LTZ`
- **Behavior**: Keeps the smaller value between accumulator and incoming value
- **Null Handling**: Null values are ignored
- **Retraction**: Not implemented; the stored minimum does not retain the other values needed to recompute a minimum after a contribution is removed. See [validation timing](#retract-validation-and-paimon-compatibility).

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_min  (
    id BIGINT,
    lowest_price DECIMAL(10, 2),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.lowest_price.agg' = 'min'
);

INSERT INTO test_min VALUES
    (1, 99.99),
    (1, 79.99),
    (1, 89.99);

SELECT * FROM test_min WHERE id = 1;
+------------+--------------+
| id         | lowest_price |
+------------+--------------+
|          1 | 79.99        |
+------------+--------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("lowest_price", DataTypes.DECIMAL(10, 2), AggFunctions.MIN())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 99.99), (1, 79.99), (1, 89.99)
// Result: (1, 79.99)
```

</TabItem>
</Tabs>

### last_value

Replaces the previous value with the most recently received value.

- **Supported Data Types**: All data types
- **Behavior**: Always uses the latest incoming value
- **Null Handling**: Null values will overwrite previous values
- **Retraction**: Supported: clears the field to null, including for a null retract input. It does not restore an earlier value. See [validation timing](#retract-validation-and-paimon-compatibility).

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_last_value  (
    id BIGINT,
    status STRING,
    last_login TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.status.agg' = 'last_value',
    'fields.last_login.agg' = 'last_value'
);


INSERT INTO test_last_value VALUES
    (1, 'online', TIMESTAMP '2024-01-01 10:00:00');
INSERT INTO test_last_value VALUES
    (1, 'offline', TIMESTAMP '2024-01-01 11:00:00');
INSERT INTO test_last_value VALUES
    (1, CAST(NULL AS STRING), TIMESTAMP '2024-01-01 12:00:00');  -- Null overwrites previous 'offline' value

SELECT * FROM test_last_value WHERE id = 1;
+------------+---------+---------------------+
| id         | status  | last_login          |
+------------+---------+---------------------+
|          1 | NULL    | 2024-01-01 12:00:00 |
+------------+---------+---------------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("status", DataTypes.STRING(), AggFunctions.LAST_VALUE())
    .column("last_login", DataTypes.TIMESTAMP(3), AggFunctions.LAST_VALUE())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Step 1: Insert initial values
// Input:  (1, 'online', '2024-01-01 10:00:00')
// Result: (1, 'online', '2024-01-01 10:00:00')

// Step 2: Upsert with new values
// Input:  (1, 'offline', '2024-01-01 11:00:00')
// Result: (1, 'offline', '2024-01-01 11:00:00')

// Step 3: Upsert with null status - null overwrites the previous 'offline' value
// Input:  (1, null, '2024-01-01 12:00:00')
// Result: (1, null, '2024-01-01 12:00:00')
// Note: status becomes null (null overwrites previous value), last_login updated
```
</TabItem>
</Tabs>

**Key behavior:** Null values overwrite existing values, treating null as a valid value to be stored.

:::note Retraction
`last_value` clears the field to null on every retract, regardless of whether the input matches the current value. It neither restores an earlier value nor searches for a matching historical contribution. For example, retracting `'B'` when the current value is `'C'` produces null. This is the field's retract rule, aligned with Paimon aggregation semantics. The row itself remains present. See [Retraction](#retraction) for the operation and row-existence contracts.
:::

### last_value_ignore_nulls

Replaces the previous value with the latest non-null value. This is the **default aggregate function** when no function is specified.

- **Supported Data Types**: All data types
- **Behavior**: Uses the latest incoming value only if it's not null
- **Null Handling**: Null values are ignored, previous value is retained
- **Retraction**: Supported: a non-null retract input clears the field to null; a null retract input preserves it. It does not restore an earlier non-null value. See [validation timing](#retract-validation-and-paimon-compatibility).

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_last_value_ignore_nulls  (
    id BIGINT,
    email STRING,
    phone STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.email.agg' = 'last_value_ignore_nulls',
    'fields.phone.agg' = 'last_value_ignore_nulls'
);


INSERT INTO test_last_value_ignore_nulls VALUES
    (1, 'user@example.com', '123-456');
INSERT INTO test_last_value_ignore_nulls VALUES
    (1, CAST(NULL AS STRING), '789-012');  -- Null is ignored, email retains previous value
INSERT INTO test_last_value_ignore_nulls VALUES
    (1, 'new@example.com', CAST(NULL AS STRING));

SELECT * FROM test_last_value_ignore_nulls WHERE id = 1;
+------------+-------------------+---------+
| id         | email             | phone   |
+------------+-------------------+---------+
|          1 | new@example.com   | 789-012 |
+------------+-------------------+---------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("email", DataTypes.STRING(), AggFunctions.LAST_VALUE_IGNORE_NULLS())
    .column("phone", DataTypes.STRING(), AggFunctions.LAST_VALUE_IGNORE_NULLS())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Step 1: Insert initial values
// Input:  (1, 'user@example.com', '123-456')
// Result: (1, 'user@example.com', '123-456')

// Step 2: Upsert with null email - null is ignored, email retains previous value
// Input:  (1, null, '789-012')
// Result: (1, 'user@example.com', '789-012')
// Note: email remains 'user@example.com' (null was ignored), phone updated to '789-012'

// Step 3: Upsert with null phone - null is ignored, phone retains previous value
// Input:  (1, 'new@example.com', null)
// Result: (1, 'new@example.com', '789-012')
// Note: email updated to 'new@example.com', phone remains '789-012' (null was ignored)
```

</TabItem>
</Tabs>

**Key behavior:** Null values do not overwrite existing non-null values, making this function ideal for maintaining the most recent valid data.

### first_value

Retrieves and retains the first non-null value seen for a field.

- **Supported Data Types**: All data types
- **Behavior**: Keeps the first received non-null value, ignores all subsequent values
- **Null Handling**: Null values are treated as unset until a non-null value is received
- **Retraction**: Not implemented; removing the first contribution does not select the next historical value. See [validation timing](#retract-validation-and-paimon-compatibility).

This function has the same behavior as `first_value_ignore_nulls`. For example,
`NULL -> 10 -> 20` produces `10`. In partial updates, omitting a field does not
prevent a later update from supplying its first non-null value.

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_first_value  (
    id BIGINT,
    first_purchase_date DATE,
    first_product STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.first_purchase_date.agg' = 'first_value',
    'fields.first_product.agg' = 'first_value'
);

INSERT INTO test_first_value VALUES
    (1, '2024-01-01', 'ProductA'),
    (1, '2024-02-01', 'ProductB');  -- Ignored, first value retained

SELECT * FROM test_first_value WHERE id = 1;
+------------+---------------------+---------------+
| id         | first_purchase_date | first_product |
+------------+---------------------+---------------+
|          1 | 2024-01-01          | ProductA      |
+------------+---------------------+---------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">


```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("first_purchase_date", DataTypes.DATE(), AggFunctions.FIRST_VALUE())
    .column("first_product", DataTypes.STRING(), AggFunctions.FIRST_VALUE())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, '2024-01-01', 'ProductA'), (1, '2024-02-01', 'ProductB')
// Result: (1, '2024-01-01', 'ProductA')
```

</TabItem>
</Tabs>

### first_value_ignore_nulls

Selects the first non-null value in a data set.

- **Supported Data Types**: All data types
- **Behavior**: Keeps the first received non-null value, ignores all subsequent values
- **Null Handling**: Null values are ignored until a non-null value is received
- **Retraction**: Not implemented; removing the first non-null contribution does not select the next historical value. See [validation timing](#retract-validation-and-paimon-compatibility).

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_first_value_ignore_nulls  (
    id BIGINT,
    email STRING,
    verified_at TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.email.agg' = 'first_value_ignore_nulls',
    'fields.verified_at.agg' = 'first_value_ignore_nulls'
);

INSERT INTO test_first_value_ignore_nulls VALUES
    (1, CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP(3)));
INSERT INTO test_first_value_ignore_nulls VALUES
    (1, 'user@example.com', TIMESTAMP '2024-01-01 10:00:00');
INSERT INTO test_first_value_ignore_nulls VALUES
    (1, 'other@example.com', TIMESTAMP '2024-01-02 10:00:00'); -- Only the first non-null value is retained

SELECT * FROM test_first_value_ignore_nulls WHERE id = 1;
+------------+-------------------+---------------------+
| id         | email             | verified_at         |
+------------+-------------------+---------------------+
|          1 | user@example.com  | 2024-01-01 10:00:00 |
+------------+-------------------+---------------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("email", DataTypes.STRING(), AggFunctions.FIRST_VALUE_IGNORE_NULLS())
    .column("verified_at", DataTypes.TIMESTAMP(3), AggFunctions.FIRST_VALUE_IGNORE_NULLS())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, null, null), (1, 'user@example.com', '2024-01-01 10:00:00'), (1, 'other@example.com', '2024-01-02 10:00:00')
// Result: (1, 'user@example.com', '2024-01-01 10:00:00')
```

</TabItem>
</Tabs>

### listagg

Concatenates multiple string values into a single string with a delimiter.

- **Supported Data Types**: `STRING`, `CHAR`
- **Behavior**: Concatenates values using the specified delimiter
- **Null Handling**: Null values are skipped
- **Delimiter**: Specify delimiter directly in the aggregation function (default is comma `,`)
- **Retraction**: Not implemented; forward concatenation does not remove retracted contributions. See [validation timing](#retract-validation-and-paimon-compatibility).

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_listagg  (
    id BIGINT,
    tags1 STRING,
    tags2 STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.tags1.agg' = 'listagg',
    'fields.tags2.agg' = 'listagg',
    'fields.tags2.listagg.delimiter' = ';'   -- Specify delimiter as parameter
);

INSERT INTO test_listagg VALUES
    (1, 'developer', 'developer'),
    (1, 'java', 'java'),
    (1, 'flink', 'flink');

SELECT * FROM test_listagg WHERE id = 1;
+------------+-----------------------+-----------------------+
| id         | tags1                 | tags2                 |
+------------+-----------------------+-----------------------+
|          1 | developer,java,flink  | developer;java;flink  |
+------------+-----------------------+-----------------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("tags1", DataTypes.STRING(), AggFunctions.LISTAGG())
    .column("tags2", DataTypes.STRING(), AggFunctions.LISTAGG(";"))  // Specify delimiter inline
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 'developer', 'developer'), (1, 'java', 'java'), (1, 'flink', 'flink')
// Result: (1, 'developer,java,flink', 'developer;java;flink')
```

</TabItem>
</Tabs>

### string_agg

Alias for `listagg`. Concatenates multiple string values into a single string with a delimiter.

- **Supported Data Types**: `STRING`, `CHAR`
- **Behavior**: Same as `listagg` - concatenates values using the specified delimiter
- **Null Handling**: Null values are skipped
- **Delimiter**: Specify delimiter directly in the aggregation function (default is comma `,`)
- **Retraction**: Not implemented, as with `listagg`. See [validation timing](#retract-validation-and-paimon-compatibility).

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_string_agg  (
    id BIGINT,
    tags1 STRING,
    tags2 STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.tags1.agg' = 'string_agg',
    'fields.tags2.agg' = 'string_agg',
    'fields.tags2.string_agg.delimiter' = ';'   -- Specify delimiter as parameter
);

INSERT INTO test_string_agg VALUES
    (1, 'developer', 'developer'),
    (1, 'java', 'java'),
    (1, 'flink', 'flink');

SELECT * FROM test_string_agg WHERE id = 1;
+------------+-----------------------+-----------------------+
| id         | tags1                 | tags2                 |
+------------+-----------------------+-----------------------+
|          1 | developer,java,flink  | developer;java;flink  |
+------------+-----------------------+-----------------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("tags1", DataTypes.STRING(), AggFunctions.STRING_AGG())
    .column("tags2", DataTypes.STRING(), AggFunctions.STRING_AGG(";"))  // Specify delimiter inline
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 'developer', 'developer'), (1, 'java', 'java'), (1, 'flink', 'flink')
// Result: (1, 'developer,java,flink', 'developer;java;flink')
```

</TabItem>
</Tabs>

### rbm32

Aggregates serialized 32-bit RoaringBitmap values by union.

- **Supported Data Types**: `BYTES`
- **Behavior**: ORs incoming bitmaps with the accumulator
- **Null Handling**: Null values are ignored
- **Retraction**: Not implemented; bitmap union does not retain contribution counts for removing values. See [validation timing](#retract-validation-and-paimon-compatibility).

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE user_visits (
    user_id    BIGINT,
    visit_bitmap BYTES,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'table.merge-engine'         = 'aggregation',
    'fields.visit_bitmap.agg'    = 'rbm32'
);

-- Insert two bitmaps for the same user; rbm32 unions them on write
INSERT INTO user_visits VALUES
    (1, rb_build(ARRAY[1, 2])),
    (1, rb_build(ARRAY[2, 3]));

-- Query: read the merged bitmap as a human-readable array
SELECT user_id, rb_to_array(visit_bitmap) AS visited_pages
FROM user_visits
WHERE user_id = 1;
-- Output: user_id=1, visited_pages=[1, 2, 3]
```


</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("user_id", DataTypes.BIGINT())
    .column("visit_bitmap", DataTypes.BYTES(), AggFunctions.RBM32())
    .primaryKey("user_id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Serialize bitmaps using the RoaringBitmap library
// RoaringBitmap rbm1 = RoaringBitmap.bitmapOf(1, 2);
// byte[] bytes1 = serialize(rbm1);
// RoaringBitmap rbm2 = RoaringBitmap.bitmapOf(2, 3);
// byte[] bytes2 = serialize(rbm2);

// Input: (1, bitmap{1,2}), (1, bitmap{2,3})
// Result: (1, bitmap{1,2,3}) -- union of the two bitmaps
```

</TabItem>
</Tabs>

### rbm64

Aggregates serialized 64-bit RoaringBitmap values by union.

- **Supported Data Types**: `BYTES`
- **Behavior**: ORs incoming bitmaps with the accumulator
- **Null Handling**: Null values are ignored
- **Retraction**: Not implemented; bitmap union does not retain contribution counts for removing values. See [validation timing](#retract-validation-and-paimon-compatibility).

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE session_interactions (
    session_id BIGINT,
    interaction_bitmap BYTES,
    PRIMARY KEY (session_id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.interaction_bitmap.agg' = 'rbm64'
);

-- Insert serialized Roaring64Bitmap values as hex literals
-- Bitmap {10,20}
INSERT INTO session_interactions VALUES (1, x'01010000000000000004000000000000000000000000000000000001000000FE0100000001020200000 00A00140001000000000000000000000000000000');
-- Bitmap {20,30}
INSERT INTO session_interactions VALUES (1, x'01010000000000000004000000000000000000000000000000000001000000FE01000000010202000000 14001E0001000000000000000000000000000000');

SELECT * FROM session_interactions WHERE session_id = 1;
-- Result: interaction_bitmap contains the union {10,20,30}
-- (serialized as x'01010000000000000004000000000000000000000000000000000001000000FE010000000102030000000A0014001E0001000000000000000000000000000000')
```

:::note
Roaring64Bitmap values must be pre-serialized on the client side. The hex literals above represent bitmaps serialized using the [RoaringBitmap](https://github.com/RoaringBitmap/RoaringBitmap) library's 64-bit format.
:::

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("session_id", DataTypes.BIGINT())
    .column("interaction_bitmap", DataTypes.BYTES(), AggFunctions.RBM64())
    .primaryKey("session_id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Serialize bitmaps using the RoaringBitmap library
// Roaring64Bitmap rbm1 = Roaring64Bitmap.bitmapOf(10, 20);
// byte[] bytes1 = serialize(rbm1);
// Roaring64Bitmap rbm2 = Roaring64Bitmap.bitmapOf(20, 30);
// byte[] bytes2 = serialize(rbm2);

// Input: (1, bitmap{10,20}), (1, bitmap{20,30})
// Result: (1, bitmap{10,20,30}) -- union of the two bitmaps
```

</TabItem>
</Tabs>

:::tip
To query and manipulate bitmap columns from Flink SQL, use the built-in RoaringBitmap SQL
functions (`rb_build_agg`, `rb_or_agg`, `rb_cardinality`, etc.) provided by FlussCatalog.
See [SQL Functions](/engine-flink/sql-functions.md) for the full function reference.
:::

### bool_and

Evaluates whether all boolean values in a set are true (logical AND).

- **Supported Data Types**: `BOOLEAN`
- **Behavior**: Returns true only if all values are true
- **Null Handling**: Null values are ignored
- **Retraction**: Not implemented; the accumulator does not retain the contribution counts needed to reverse AND. See [validation timing](#retract-validation-and-paimon-compatibility).

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_bool_and  (
    id BIGINT,
    has_all_permissions BOOLEAN,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.has_all_permissions.agg' = 'bool_and'
);

INSERT INTO test_bool_and VALUES
    (1, true),
    (1, true),
    (1, false);

SELECT * FROM test_bool_and WHERE id = 1;
+------------+----------------------+
| id         | has_all_permissions  |
+------------+----------------------+
|          1 | false                |
+------------+----------------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("has_all_permissions", DataTypes.BOOLEAN(), AggFunctions.BOOL_AND())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, true), (1, true), (1, false)
// Result: (1, false) -- Not all values are true
```

</TabItem>
</Tabs>

### bool_or

Checks if at least one boolean value in a set is true (logical OR).

- **Supported Data Types**: `BOOLEAN`
- **Behavior**: Returns true if any value is true
- **Null Handling**: Null values are ignored
- **Retraction**: Not implemented; the accumulator does not retain the contribution counts needed to reverse OR. See [validation timing](#retract-validation-and-paimon-compatibility).

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_bool_or  (
    id BIGINT,
    has_any_alert BOOLEAN,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.has_any_alert.agg' = 'bool_or'
);

INSERT INTO test_bool_or VALUES
    (1, false),
    (1, false),
    (1, true);

SELECT * FROM test_bool_or WHERE id = 1;
+------------+------------------+
| id         | has_any_alert    |
+------------+------------------+
|          1 | true             |
+------------+------------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("has_any_alert", DataTypes.BOOLEAN(), AggFunctions.BOOL_OR())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, false), (1, false), (1, true)
// Result: (1, true) -- At least one value is true
```

</TabItem>
</Tabs>

### last_n

Appends input arrays to the stored array and keeps the last `size` elements.

- **Supported Data Types**: `ARRAY` of any element type
- **Behavior**: Appends the input elements, then drops the oldest elements beyond `size`. The first write for a key is trimmed as well
- **Ordering**: Elements are kept in the order writes reach the server. With the default `'sink.distribution-mode'`, Flink writes for the same key go through a single sink task, so the order follows the input. Independent clients writing to the same key may interleave
- **Null Handling**: A null input leaves the column unchanged
- **Size**: Required, a positive integer. Creating a table without it fails
- **Retraction**: Not implemented; the bounded array does not retain discarded elements to restore after a retract. Paimon 2.1.0 has no built-in `last_n` counterpart. See [validation timing](#retract-validation-and-paimon-compatibility).

:::note
Every update rewrites the whole stored array. With the default `'table.changelog.image' = 'full'`, both the old and the new array are also written to the changelog on every update. Keep `size` small.
:::

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_last_n (
    user_id BIGINT,
    recent_events ARRAY<STRING>,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.recent_events.agg' = 'last_n',
    'fields.recent_events.last_n.size' = '3'
);

INSERT INTO test_last_n VALUES
    (1, ARRAY['login']),
    (1, ARRAY['search']),
    (1, ARRAY['view', 'add_to_cart']);

SELECT * FROM test_last_n WHERE user_id = 1;
+------------+----------------------------------+
| user_id    | recent_events                    |
+------------+----------------------------------+
|          1 | [search, view, add_to_cart]      |
+------------+----------------------------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("user_id", DataTypes.BIGINT())
    .column("recent_events", DataTypes.ARRAY(DataTypes.STRING()), AggFunctions.LAST_N(3))
    .primaryKey("user_id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, ['login']), (1, ['search']), (1, ['view', 'add_to_cart'])
// Result: (1, ['search', 'view', 'add_to_cart'])
```

</TabItem>
</Tabs>

### collect

Collects array elements into a single array.

- **Supported Data Types**: `ARRAY<T>`
- **Behavior**: Concatenates array elements; with `distinct=true`, duplicates are removed
- **Ordering**: Elements follow the order writes reach the server. With `distinct=true`, the first occurrence of each element is retained
- **Null Handling**: Null elements are retained; null inputs are ignored
- **Parameters**: `distinct` (`true` or `false`, default is `false`)
- **Retraction**: Removes one matching accumulated occurrence per input element, including null, and preserves the order of the remaining elements. Unmatched elements are ignored. Distinct aggregation does not retain contribution counts, so retracting an element can remove the result of multiple contributions. A retract received before its contribution is not remembered. See [Field Rules](#field-rules) and [validation timing](#retract-validation-and-paimon-compatibility).

:::note
The collected array has no size limit. Every update rewrites the whole stored array. With the default `'table.changelog.image' = 'full'`, both the old and the new array are also written to the changelog on every update. As the array grows, so do memory usage, write costs, and changelog volume. Use `last_n` when only a bounded number of recent elements is needed.
:::

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_collect  (
    id BIGINT,
    tags ARRAY<INT>,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.tags.agg' = 'collect',
    'fields.tags.collect.distinct' = 'true'
);

INSERT INTO test_collect VALUES
    (1, ARRAY[1, 2, 2]),
    (1, ARRAY[2, 3, NULL]);

SELECT * FROM test_collect;
-- Result:
-- (1, [1, 2, 3, NULL])
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("tags", DataTypes.ARRAY(DataTypes.INT()), AggFunctions.COLLECT(true))
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, [1, 2, 2]), (1, [2, 3, null])
// Result: (1, [1, 2, 3, null])
```

</TabItem>
</Tabs>

### merge_map

Merges map values by combining key-value pairs. For duplicate keys, the latest value wins.

- **Supported Data Types**: `MAP<K, V>`
- **Behavior**: Merges maps, overriding earlier values for duplicate keys
- **Null Handling**: Null keys are not allowed; null values are allowed
- **Retraction**: Removes entries whose keys occur in the retract map, regardless of the supplied values, including null. Overwritten values are not restored. A null input preserves the accumulator, and a retract received before its contribution is not remembered. Binary keys are matched by content. See [Field Rules](#field-rules) and [validation timing](#retract-validation-and-paimon-compatibility).

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_merge_map  (
    id BIGINT,
    attributes MAP<INT, STRING>,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.attributes.agg' = 'merge_map'
);

INSERT INTO test_merge_map VALUES
    (1, MAP[1, 'a', 2, 'b']),
    (1, MAP[2, 'c', 3, NULL]);

SELECT * FROM test_merge_map;
-- Result: (1, {1='a', 2='c', 3=NULL})
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("attributes", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()), AggFunctions.MERGE_MAP())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, {1='a', 2='b'}), (1, {2='c', 3=null})
// Result: (1, {1='a', 2='c', 3=null})
```

</TabItem>
</Tabs>

## Retraction

Retraction applies each field's retract rule to an input contribution. It does not restore a previous row or reconstruct the history of values for a key. The implemented rules for `sum`, `product`, `collect`, `merge_map`, `last_value`, and `last_value_ignore_nulls` follow [Paimon 2.1.0 aggregation semantics](https://paimon.apache.org/docs/2.1/primary-key-table/merge-engine/aggregation/), where the last function is named `last_non_null_value`. See [Retract Validation and Paimon Compatibility](#retract-validation-and-paimon-compatibility) for the function coverage and validation timing.

The Java client exposes three distinct operations:

| Operation | Effect |
| --- | --- |
| `upsert(row)` | Aggregate the supplied fields into the current state. |
| `retract(row)` | Apply the supplied fields' retract rules to the current state. The row must include its primary key and the values to retract. |
| `delete(row)` | Apply the configured delete behavior. Full writes delete the record; partial writes clear the target fields. Non-key values supplied in the input do not become retract contributions. |

For example, after `upsert((1, 10))` on a SUM field, `retract((1, 3))` produces `(1, 7)`. Calling `delete((1, 3))` instead requests deletion according to `table.delete.behavior` and the writer's target columns; it does not subtract `3`.

### Input Determinism

To remove or replace a source record, retract its previous primary key and the original contribution to each participating target field. For a key-changing update, retract the contribution under the old key and upsert the contribution under the new key.

Lookup joins against changing dimension data and non-deterministic expressions can produce different values when an old source row is evaluated again. With `amount = 10` and a lookup rate of `2`, a SUM field receives `20`. If the rate changes to `3` and the pipeline recomputes the old contribution, it retracts `30`, leaving `-10`. Removing the original contribution requires retracting `20`.

Flink's `table.optimizer.non-deterministic-update.strategy = TRY_RESOLVE` can add lookup materialization according to the query plan. For the actual query, inspect the physical plan and verify that UPDATE_BEFORE and DELETE carry the original contributions as dimension data changes. See [Flink's determinism guidance](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/sql-table-concepts/determinism/).

### Field Rules

| Function | Retract rule |
| --- | --- |
| `sum` | Subtract a non-null input. A null input leaves the accumulator unchanged. If the accumulator is null, retracting `x` produces `-x`. |
| `product` | Divide when both accumulator and input are non-null; otherwise preserve the accumulator. Integer division truncates, integer/decimal division by zero fails, and non-terminating decimal division fails. Floating-point division can produce Infinity/NaN. |
| `collect` | Remove one accumulated occurrence per matching input element, including null, preserving the remaining order. Null or empty input leaves the accumulator unchanged; a null accumulator stays null. Matching uses logical contents for binary and nested values. |
| `merge_map` | Remove entries by the input keys, ignoring the input values. Null or empty input leaves the accumulator unchanged; a null accumulator stays null. Removing an overwritten key does not restore its previous value. |
| `last_value` | Clear the accumulator to null, including when the input is null. |
| `last_value_ignore_nulls` | Clear the accumulator when the input is non-null; otherwise retain it. |

These rules do not check whether the input matches a previously written contribution. In particular, the last-value functions do not retain earlier values to restore after a retract. SUM uses the field's numeric representation, so subtraction does not imply an exact inverse for every numeric value.

Primary keys are retained. For partial writes, only target value fields are retracted; other fields retain their state. Retraction also applies when the key has no current row: each targeted field starts with a null accumulator. For example, retracting `(1, 3)` from an absent SUM key creates `(1, -3)`.

Zero, a product of one, empty arrays or maps, and all-null aggregate fields do not imply that the record should be removed. Retraction retains the row, including a row whose only non-null fields are its primary key. Explicit deletion has separate rules described below.

### Arithmetic Failures

An arithmetic failure while retracting a field rejects the write batch with `InvalidRecordException`. The error identifies the field, aggregation function, table ID, bucket and encoded primary key, and includes the arithmetic failure reason. Valid writes to other keys in the same batch also fail.

For example, a `DECIMAL(10, 2)` PRODUCT accumulator stores `1.33 * 1.33` as `1.77`. Retracting `1.33` then requires the non-terminating division `1.77 / 1.33`, which fails under the exact division rules shared with Paimon 2.1.0.

In a Flink job, the failed write causes task failure. Recovery can replay the same contribution and fail again until the input or computation is corrected. Locate the record using the error context and inspect its field values and numeric constraints.

### Table Options

| Option | Default | Meaning |
| --- | --- | --- |
| `fields.<column>.ignore-retract` | `false` | Preserve this field on retract while retaining its configured upsert aggregation. This also permits a function without a retract implementation, such as MAX, to coexist with retractable fields. |
| `table.aggregation.remove-record-on-delete` | `false` | With Flink retract mode enabled, interpret changelog DELETE as whole-row deletion instead of field retraction. UPDATE_BEFORE still retracts fields. Requires `table.delete.behavior=allow`. |

Both options belong to the aggregation table. They cannot be enabled together: whole-row deletion and retaining individual fields on retract have conflicting behavior, matching Paimon's restriction. Configure them when creating the table. The whole-row option does not change the Java writer's explicit `delete(row)` or `retract(row)` operations.

Enabling `table.aggregation.remove-record-on-delete` requires an explicit `table.delete.behavior=allow`. Table creation rejects `ignore`, `disable`, and an omitted delete policy, which defaults to `ignore` for aggregation tables. With Flink retract mode enabled, this combination corresponds to Paimon's `aggregation.remove-record-on-delete=true`. With the whole-row option disabled, all three delete policies remain valid and field retraction is independent of them.

For example, an aggregation table can configure `fields.maximum.ignore-retract=true` alongside a SUM field. Retracting a row subtracts its SUM contribution and preserves the existing maximum. Upserting a row still applies MAX to that field. A field missing from an older input schema is preserved, rather than treated as an explicit null retract.

### Flink Changelog

`sink.enable-retract` defaults to `false`. In this default mode, Flink keeps the existing upsert changelog negotiation: UPDATE_BEFORE is excluded, and DELETE follows the existing delete path. Existing jobs do not acquire field retract semantics automatically.

For contribution-based aggregation writes, set the following Flink SQL TableConfig before submitting the INSERT statement, matching [Paimon's aggregation recommendation](https://paimon.apache.org/docs/2.1/primary-key-table/merge-engine/aggregation/):

```sql
SET 'table.exec.sink.upsert-materialize' = 'NONE';
```

The sink upsert materializer maintains an upsert view for each target primary key. It can suppress a contribution's retraction or re-emit an older row, which Fluss then treats as another contribution. Disabling it preserves the input contribution changelog. Pure INSERT queries and queries whose inferred upsert key matches the target primary key may already plan without materialization under `AUTO`.

In Flink 2.3, a direct SELECT from independently keyed source rows into one aggregation key can fail planning under `AUTO` with the default `table.exec.sink.require-on-conflict=true`. Setting materialization to `NONE` permits this query while retaining that default. Setting only `require-on-conflict=false` can still insert the materializer and produce incorrect aggregates.

For an aggregation sink with `sink.enable-retract=true`:

| Changelog kind | Operation |
| --- | --- |
| INSERT / UPDATE_AFTER | Upsert the input contribution. |
| UPDATE_BEFORE | Retract the input contribution. |
| DELETE | Retract the input contribution by default. If `table.aggregation.remove-record-on-delete=true`, delete the entire record, including fields outside a partial write's target columns. |

`sink.ignore-delete=true` drops both UPDATE_BEFORE and DELETE before applying these rules, matching Paimon's `ignore-delete=true` input filtering. Explicit record deletion follows `table.delete.behavior`. The whole-row option requires `allow`; with that option disabled, `ignore` silently skips explicit deletes and `disable` rejects them. Field retraction is independent of that deletion policy.

When the declared input changelog mode requires retraction, Flink validates the participating target fields after resolving partial writes. Each participating field must support retract or set `ignore-retract=true`. Non-target fields do not prevent a partial retract, and an input mode containing only INSERT / UPDATE_AFTER does not require retract-capable functions. See [Retract Validation and Paimon Compatibility](#retract-validation-and-paimon-compatibility) for the distinction between the declared mode and records observed at runtime.

The sink option does not gate the Java client's explicit `retract(row)` API. DataStream applications with custom serializers choose their operation types explicitly; `FlussSinkBuilder.setOption("sink.enable-retract", "true")` enables their RETRACT operations, while the table's field rules remain authoritative.

### Retract Validation and Paimon Compatibility

The comparison below uses Paimon 2.1.0. Retract capability and validation timing are separate contracts: a function can accept forward contributions without being able to retract them.

#### Fluss Validation

With the default `sink.enable-retract=false`, Flink SQL retains the existing upsert changelog negotiation. A field without retract support does not by itself reject an existing upsert job. Enabling retract is an explicit change to this input contract.

With `sink.enable-retract=true` and `sink.ignore-delete=false`, the Flink SQL sink checks the declared input changelog mode. UPDATE_BEFORE requires field retraction; DELETE also requires it unless `table.aggregation.remove-record-on-delete=true`. After resolving target columns, the sink rejects a required retract with a `ValidationException` if any participating value field lacks native retract support and does not set `ignore-retract=true`.

This check uses the declared mode, not the records observed so far. If a job declares UPDATE_BEFORE but happens to produce only INSERT / UPDATE_AFTER, Fluss can still reject it during planning. Conversely, a declared INSERT / UPDATE_AFTER-only input is allowed to aggregate into a MAX field. Fields outside a partial write's target columns do not participate in this check.

For example, a table with a SUM field and a MAX field accepts forward contributions to both. If both fields participate in a declared retract input, planning fails for MAX. Targeting only the SUM field, or configuring the MAX field with `ignore-retract=true`, permits the retract while preserving MAX. Ignoring retract does not recompute the maximum after a historical maximum is removed.

The planning check applies to Flink SQL. The Java client's explicit `retract(row)` and DataStream custom serializers do not have that SQL planning check. The server validates the fields of an actual RETRACT request against its input schema and target columns, and rejects unsupported fields with `InvalidRecordException`. The Sink option does not gate the Java API.

#### Paimon Validation

Paimon 2.1.0's aggregation [Flink sink](https://github.com/apache/paimon/blob/release-2.1.0/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/FlinkTableSinkBase.java) preserves the requested changelog mode without Fluss's per-field planning check. Its [aggregation merge function](https://github.com/apache/paimon/blob/release-2.1.0/paimon-core/src/main/java/org/apache/paimon/mergetree/compact/aggregate/AggregateMergeFunction.java) dispatches an actual field retract to the corresponding aggregator. For a configured function without a retract implementation, the [default implementation](https://github.com/apache/paimon/blob/release-2.1.0/paimon-core/src/main/java/org/apache/paimon/mergetree/compact/aggregate/FieldAggregator.java) throws `UnsupportedOperationException` during merging unless the field ignores retract.

For MAX, this means Paimon can accept a job whose declared mode includes UPDATE_BEFORE and continue processing forward contributions until an actual retract reaches the merge function. Fluss SQL with retract enabled can reject the same declared mode before records are processed. Successful planning in Paimon does not establish that every configured function supports retract. Merge-time failure is not necessarily synchronous with the application's write call.

The following table compares native field retract support, before applying `ignore-retract`. It does not imply that corresponding functions have identical forward aggregation or configuration options.

| Fluss function | Fluss native retract | Paimon 2.1.0 counterpart and native retract |
| --- | --- | --- |
| `sum` | Supported; subtraction | `sum`: supported; subtraction |
| `product` | Supported; division with numeric division constraints | `product`: supported; division with numeric division constraints |
| `max` | Not implemented | `max`: not implemented |
| `min` | Not implemented | `min`: not implemented |
| `last_value` | Supported; clear to null | `last_value`: supported; clear to null |
| `last_value_ignore_nulls` | Supported; clear on non-null input | `last_non_null_value`: supported; clear on non-null input |
| `first_value` | Not implemented | `first_value`: not implemented |
| `first_value_ignore_nulls` | Not implemented | `first_non_null_value`: not implemented |
| `listagg` | Not implemented | `listagg`: not implemented |
| `string_agg` | Not implemented | No separate built-in under this name; the related `listagg` does not implement retract |
| `rbm32` | Not implemented | `rbm32`: not implemented |
| `rbm64` | Not implemented | `rbm64`: not implemented |
| `bool_and` | Not implemented | `bool_and`: not implemented |
| `bool_or` | Not implemented | `bool_or`: not implemented |
| `last_n` | Not implemented | No built-in counterpart |
| `collect` | Best-effort removal of matching elements | `collect`: best-effort removal of matching elements |
| `merge_map` | Best-effort removal by retracted keys; no restoration of overwritten values | `merge_map`: best-effort removal by retracted keys; no restoration of overwritten values |

The [ordering and overlapping-contribution limitations](https://paimon.apache.org/docs/2.1/primary-key-table/merge-engine/aggregation/#retraction) of Paimon's `collect` and `merge_map` also apply to Fluss. For example, collecting the same element from two inputs with `distinct=true` stores one occurrence; retracting one input can remove that stored occurrence. Merging `1 -> A`, then `1 -> B`, and retracting key `1` removes the entry without restoring `A`. A retract processed before its contribution does not cancel a later upsert. These are field rules, not historical restoration guarantees. For fields without native retract support, `ignore-retract=true` preserves the current field value while other fields retract; it does not implement the missing inverse operation.

### Ordering and Recovery

Each retract is processed independently in the order received. An adjacent retract and upsert are two operations with an observable intermediate state; adjacency does not establish an update pair. For a fixed ordered sequence of successful operations, changing the client batch boundaries does not change the resulting aggregate state. This does not provide atomicity across separate requests, and failures are subject to the actual write boundaries.

Retract is not idempotent: applying a SUM retract twice subtracts the contribution twice. The client requires `client.writer.enable-idempotence` so transport retries can use writer-batch deduplication. This deduplication applies within a writer session; application replay with a new writer requires a recovery protocol. Flink undo recovery restores checkpoint state using OVERWRITE and is distinct from normal retract processing.

Recognized duplicate batches carrying mutation types return their original write offsets before field functions are reapplied. A retry therefore does not repeat division or element/key removal for a batch already accepted in the retained duplicate window.

KV batches retain magic 0. Attributes bit 0 indicates that each record carries an explicit MutationType byte, and the batch CRC covers that attribute. This extension leaves the persisted KV and changelog encodings unchanged.

Retract requires PUT_KV API version 4 or later. The RPC connection queues writes until version negotiation completes, without consuming the write retry budget. If the negotiated version is below 4, a PUT_KV request containing any batch carrying mutation types fails with `UnsupportedVersionException` before transmission. This rejects the entire request, including any batches without mutation types grouped with it. Ordinary upsert and delete writes start batches without mutation types and remain compatible when sent in requests without mutation types.

After schema evolution, fields absent from the input row do not participate in retract. This also applies when Flink pads an older input schema to the latest table schema. An explicitly supplied NULL still participates according to the field's aggregation function; for example, LAST_VALUE retract clears that field. Integer SUM uses the same fixed-width arithmetic as forward aggregation, including wraparound at the type boundary.

## Delete Behavior

The aggregation merge engine provides limited support for delete operations. You can configure the behavior using the `'table.delete.behavior'` option:

```java
TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.delete.behavior", "allow")  // Enable delete operations
    .build();
```

**Configuration options**:
- **`'table.delete.behavior' = 'ignore'`** (default): Delete operations will be silently ignored without error
- **`'table.delete.behavior' = 'disable'`**: Delete operations will be rejected with a clear error message
- **`'table.delete.behavior' = 'allow'`**: Delete operations will remove records based on the update mode (see details below)

### Delete Behavior with Different Update Modes

When `'table.delete.behavior' = 'allow'`, the actual delete behavior depends on whether you are using **full update** or **partial update**:

**Full Update (Default Write Mode)**:
- Delete operations remove the **entire record** from the table
- All aggregated values for that primary key are permanently lost

**Example**:
```java
// Full update mode (default)
UpsertWriter writer = table.newUpsert().createWriter();
writer.delete(primaryKeyRow);  // Removes the entire record
```

**Partial Update Mode**:
- Delete operations perform a **partial delete** on target columns only
- **Target columns** (except primary key): Set to null
- **Non-target columns**: Remain unchanged
- **Special case**: If all non-target columns are null after the delete, the entire record is removed

**Example**:
```java
// Partial update mode - only targeting specific columns
UpsertWriter partialWriter = table.newUpsert()
    .partialUpdate("id", "count1", "sum1")  // Target columns
    .createWriter();

// Delete will:
// - Set count1 and sum1 to null
// - Keep count2 and sum2 unchanged (non-target columns)
// - Remove entire record only if count2 and sum2 are both null
partialWriter.delete(primaryKeyRow);
```

## Limitations

:::warning Critical Limitations
When using the `aggregation` merge engine, be aware of the following critical limitations:

### Exactly-Once Semantics

Flink checkpoint-based exactly-once guarantees for an aggregation table assume that a single Flink job has exclusive write access to the table throughout normal processing and recovery. After a failure, the connector uses undo recovery to restore the table state at the last successful checkpoint before replaying input.

Undo recovery restores or deletes records by primary key from the changelog after the checkpoint, using OVERWRITE for restored values. It does not isolate contributions from independent writers. If another Flink job or a direct client writes to the table, recovery can overwrite or remove that writer's already acknowledged changes. Writing different columns of the same key alone does not establish recovery isolation.

[Writer-batch deduplication](#ordering-and-recovery) protects transport retries within a writer session. Exactly-once across application failures and replay requires a recovery protocol. Applications using the Fluss client API directly must implement their own recovery logic and account for concurrent writers when restoring table state.

For detailed information about Exactly-Once implementation, please refer to: [FIP-21: Aggregation Merge Engine](https://cwiki.apache.org/confluence/display/FLUSS/FIP-21%3A+Aggregation+Merge+Engine)

:::

## See Also

- [Default Merge Engine](table-design/merge-engines/default.md)
- [FirstRow Merge Engine](table-design/merge-engines/first-row.md)
- [Versioned Merge Engine](table-design/merge-engines/versioned.md)
- [Primary Key Tables](table-design/table-types/pk-table.md)
- [Fluss Client API](../../apis/java/index.md)
