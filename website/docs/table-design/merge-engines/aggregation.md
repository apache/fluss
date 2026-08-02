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

### sum

Aggregates values by computing the sum across multiple rows.

- **Supported Data Types**: `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL`
- **Behavior**: Adds incoming values to the accumulator
- **Null Handling**: Null values are ignored

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

### last_value_ignore_nulls

Replaces the previous value with the latest non-null value. This is the **default aggregate function** when no function is specified.

- **Supported Data Types**: All data types
- **Behavior**: Uses the latest incoming value only if it's not null
- **Null Handling**: Null values are ignored, previous value is retained

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

Retrieves and retains the first value seen for a field.

- **Supported Data Types**: All data types
- **Behavior**: Keeps the first received value, ignores all subsequent values
- **Null Handling**: Null values are retained if received first

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
    .column("tags", DataTypes.STRING(), AggFunctions.STRING_AGG(";"))  // Specify delimiter inline
    .primaryKey("id")
    .build();
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

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE user_visits (
    user_id BIGINT,
    visit_bitmap BYTES,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.visit_bitmap.agg' = 'rbm32'
);

-- Insert serialized RoaringBitmap values as hex literals
-- Bitmap {1,2}
INSERT INTO user_visits VALUES (1, x'3A30000001000000000001001000000001000200');
-- Bitmap {2,3}
INSERT INTO user_visits VALUES (1, x'3A30000001000000000001001000000002000300');

SELECT * FROM user_visits WHERE user_id = 1;
-- Result: visit_bitmap contains the union {1,2,3}
-- (serialized as x'3A300000010000000000020010000000010002000300')
```

:::note
RoaringBitmap values must be pre-serialized on the client side. The hex literals above represent bitmaps serialized using the [RoaringBitmap](https://github.com/RoaringBitmap/RoaringBitmap) library's standard format.
:::

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

## RoaringBitmap SQL Functions

Apache Fluss provides a set of built-in RoaringBitmap SQL functions registered via `FlussCatalog`.
After `USE CATALOG fluss_catalog`, these functions are available in Flink SQL without any
`CREATE TEMPORARY FUNCTION` statement.

All functions operate on `BYTES` columns containing standard RoaringBitmap 32-bit serialized data,
which is the same wire format used by the `rbm32` storage-level aggregator.

:::note
These are Flink-side SQL functions. They are distinct from the storage-level `rbm32` / `rbm64`
aggregators described above. The storage-level aggregators run inside the Fluss TabletServer on
write; the SQL functions run in Flink during query execution.
:::

### Aggregate Functions

#### rb_build_agg

Aggregates a stream of `INT` values into a single serialized `RoaringBitmap`.

- **Signature**: `rb_build_agg(value INT) -> BYTES`
- **Behavior**: Adds each non-null integer to the accumulator bitmap
- **Null Handling**: Null inputs are ignored
- **Returns**: Serialized bitmap, or `NULL` if all inputs are null

**Example:**

```sql
-- Build a bitmap of unique user IDs per page
SELECT page_id, rb_build_agg(user_id) AS uv_bitmap
FROM page_events
GROUP BY page_id;
```

#### rb_or_agg

Unions multiple serialized `RoaringBitmap` values via bitwise OR across rows.

- **Signature**: `rb_or_agg(bitmap BYTES) -> BYTES`
- **Behavior**: ORs each input bitmap into the accumulator
- **Null Handling**: Null and empty inputs are ignored
- **Returns**: Serialized bitmap representing the union, or `NULL` if all inputs are null

**Example:**

```sql
-- Roll up per-hour bitmaps to a daily unique visitor count
SELECT page_id, rb_cardinality(rb_or_agg(uv_bitmap)) AS daily_uv
FROM uv_agg
WHERE ymd = '20260101'
GROUP BY page_id;
```

#### rb_and_agg

Intersects multiple serialized `RoaringBitmap` values via bitwise AND across rows.

- **Signature**: `rb_and_agg(bitmap BYTES) -> BYTES`
- **Behavior**: ANDs each input bitmap into the accumulator after seeding with the first input
- **Null Handling**: Null and empty inputs are ignored
- **Returns**: Serialized bitmap representing the intersection, or `NULL` if the result is empty

:::note
`rb_and_agg` executes entirely in Flink. There is no server-side counterpart.
Combining it with `table.merge-engine=aggregation` may produce unexpected results during
server-side compaction. Use only on append-only streams.
:::

#### rb_xor_agg

Aggregates multiple serialized `RoaringBitmap` values via bitwise XOR across rows.

- **Signature**: `rb_xor_agg(bitmap BYTES) -> BYTES`
- **Behavior**: XORs each input bitmap into the accumulator; returns elements appearing in an odd number of inputs
- **Null Handling**: Null and empty inputs are ignored
- **Returns**: Serialized bitmap, or `NULL` if the result is empty or all inputs are null

**Use case**: Change detection and symmetric difference analysis.

:::note
`rb_xor_agg` executes entirely in Flink. There is no server-side counterpart.
Combining it with `table.merge-engine=aggregation` may produce unexpected results during
server-side compaction. Use only on append-only streams.
:::

---

### Scalar Functions

#### rb_cardinality

Returns the number of distinct integers in a serialized `RoaringBitmap`.

- **Signature**: `rb_cardinality(bitmap BYTES) -> BIGINT`
- **Null Handling**: Returns `NULL` for null input; returns `0` for an empty bitmap

**Example:**

```sql
SELECT page_id, rb_cardinality(uv_bitmap) AS uv
FROM uv_agg
WHERE ymd = '20260101';
```

#### rb_build

Constructs a serialized `RoaringBitmap` from an array of integers within a single row.

- **Signature**: `rb_build(values ARRAY<INT>) -> BYTES`
- **Null Handling**: Null elements in the array are ignored; returns `NULL` if all elements are null

**Example:**

```sql
SELECT rb_cardinality(rb_build(ARRAY[1, 2, 3, 2]));
-- Result: 3 (duplicate 2 is ignored)
```

#### rb_contains

Checks whether a serialized `RoaringBitmap` contains a specific integer.

- **Signature**: `rb_contains(bitmap BYTES, value INT) -> BOOLEAN`
- **Null Handling**: Returns `NULL` if either argument is null

**Example:**

```sql
SELECT page_id, rb_contains(uv_bitmap, 42) AS visited_user_42
FROM uv_agg
WHERE ymd = '20260101';
```

#### rb_to_array

Converts a serialized `RoaringBitmap` into an array of its integer values in ascending order.

- **Signature**: `rb_to_array(bitmap BYTES) -> ARRAY<INT>`
- **Null Handling**: Returns `NULL` for null input; returns an empty array for an empty bitmap

**Example:**

```sql
SELECT rb_to_array(uv_bitmap) AS user_ids
FROM uv_agg
WHERE page_id = 1 AND ymd = '20260101';
```

#### rb_or

Returns the bitwise OR (union) of two serialized `RoaringBitmap` values.

- **Signature**: `rb_or(left BYTES, right BYTES) -> BYTES`
- **Null Handling**: Returns `NULL` if either argument is null. To union while ignoring nulls across rows, use `rb_or_agg`.

**Example:**

```sql
-- Union two bitmaps from different time windows
SELECT rb_cardinality(rb_or(morning_bitmap, evening_bitmap)) AS all_day_uv
FROM daily_segments
WHERE page_id = 1;
```

#### rb_and

Returns the bitwise AND (intersection) of two serialized `RoaringBitmap` values.

- **Signature**: `rb_and(left BYTES, right BYTES) -> BYTES`
- **Null Handling**: Returns `NULL` if either argument is null
- **Note**: Returns serialized bytes even if the intersection is empty (cardinality 0)

**Example:**

```sql
-- Users who visited both page A and page B
SELECT rb_cardinality(rb_and(a.uv_bitmap, b.uv_bitmap)) AS overlap
FROM uv_agg a, uv_agg b
WHERE a.page_id = 1 AND b.page_id = 2 AND a.ymd = b.ymd;
```

#### rb_xor

Returns the bitwise XOR (symmetric difference) of two serialized `RoaringBitmap` values.

- **Signature**: `rb_xor(left BYTES, right BYTES) -> BYTES`
- **Null Handling**: Returns `NULL` if either argument is null
- **Note**: Returns serialized bytes even if the result is empty

**Example:**

```sql
-- Users who visited page A or page B, but not both
SELECT rb_cardinality(rb_xor(a.uv_bitmap, b.uv_bitmap)) AS exclusive_visitors
FROM uv_agg a, uv_agg b
WHERE a.page_id = 1 AND b.page_id = 2 AND a.ymd = b.ymd;
```

#### rb_andnot

Returns elements present in the left `RoaringBitmap` but not in the right.

- **Signature**: `rb_andnot(left BYTES, right BYTES) -> BYTES`
- **Null Handling**: Returns `NULL` if either argument is null

**Use case**: Exclusion analysis, such as "users who visited page A but not page B."

**Example:**

```sql
-- Users who visited the homepage but not the checkout page
SELECT rb_cardinality(rb_andnot(home.uv_bitmap, checkout.uv_bitmap)) AS bounced
FROM uv_agg home, uv_agg checkout
WHERE home.page_id = 1 AND checkout.page_id = 5 AND home.ymd = checkout.ymd;
```

---

### End-to-End Example

The following example demonstrates the complete bitmap analytics workflow using `FlussCatalog`.

**Step 1: Create a bitmap aggregation table**

```sql
USE CATALOG fluss_catalog;

CREATE TABLE uv_agg (
    page_id   INT,
    ymd       STRING,
    uv_bitmap BYTES,
    PRIMARY KEY (page_id, ymd) NOT ENFORCED
) WITH (
    'table.merge-engine'     = 'aggregation',
    'fields.uv_bitmap.agg'   = 'rbm32'
);
```

**Step 2: Ingest events — build a single-element bitmap per event**

```sql
INSERT INTO uv_agg
SELECT page_id, ymd, rb_build_agg(user_id) AS uv_bitmap
FROM raw_page_events
GROUP BY page_id, ymd;
```

**Step 3: Query unique visitor counts**

```sql
-- Point query: UV for a single page on a single day
SELECT page_id, rb_cardinality(uv_bitmap) AS uv
FROM uv_agg
WHERE page_id = 1 AND ymd = '20260101';

-- Roll-up query: total UV across all pages for a day
SELECT rb_cardinality(rb_or_agg(uv_bitmap)) AS total_daily_uv
FROM uv_agg
WHERE ymd = '20260101';
```

For a full end-to-end tutorial including Docker setup and multi-dimensional roll-up queries,
see the [Real-Time UV Deduplication](https://fluss.apache.org/blog/roaringbitmap-uv-deduplication/) blog post.


### bool_and

Evaluates whether all boolean values in a set are true (logical AND).

- **Supported Data Types**: `BOOLEAN`
- **Behavior**: Returns true only if all values are true
- **Null Handling**: Null values are ignored

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

:::note
**Current Limitation**: The aggregation merge engine does not support retraction semantics (e.g., subtracting from a sum, reverting a max). 

- **Full update mode**: Delete operations can only remove the entire record
- **Partial update mode**: Delete operations can only null out target columns, not retract aggregated values

Future versions may support fine-grained retraction by enhancing the protocol to carry row data with delete operations.
:::

## Limitations

:::warning Critical Limitations
When using the `aggregation` merge engine, be aware of the following critical limitations:

### Exactly-Once Semantics

When writing to an aggregate merge engine table using the Flink engine, Fluss does provide exactly-once guarantees. Thanks to Flink's checkpointing mechanism, in the event of a failure and recovery, the Flink connector automatically performs an undo operation to roll back the table state to what it was at the last successful checkpoint. This ensures no over-counting or under-counting: data remains consistent and accurate.

However, when using the Fluss client API directly (outside of Flink), exactly-once is not provided out of the box. In such cases, users must implement their own recovery logic (similar to what the Flink connector does) by explicitly resetting the table state to a previous version by performing undo operations.

For detailed information about Exactly-Once implementation, please refer to: [FIP-21: Aggregation Merge Engine](https://cwiki.apache.org/confluence/display/FLUSS/FIP-21%3A+Aggregation+Merge+Engine)

:::

## See Also

- [Default Merge Engine](table-design/merge-engines/default.md)
- [FirstRow Merge Engine](table-design/merge-engines/first-row.md)
- [Versioned Merge Engine](table-design/merge-engines/versioned.md)
- [Primary Key Tables](table-design/table-types/pk-table.md)
- [Fluss Client API](../../apis/java/index.md)
