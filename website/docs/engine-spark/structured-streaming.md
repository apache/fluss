---
sidebar_label: Structured Streaming
title: Spark Structured Streaming
sidebar_position: 6
---

# Spark Structured Streaming

Fluss supports [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) for both reading and writing data in a streaming fashion. This enables building real-time data pipelines with Apache Spark and Fluss.

## Streaming Write

Fluss supports writing data to Fluss tables using Spark Structured Streaming. This is useful for continuously ingesting data into Fluss tables from streaming sources.

### Write to Log Table

```scala title="Spark Application"
// Here we use MemoryStream to fake a streaming source.
val inputData = MemoryStream[(Int, String)]
val inputDF = inputData.toDS().toDF("k", "v")

val query = inputDF.writeStream
  .option("checkpointLocation", "/path/to/checkpoint")
  .toTable("fluss_catalog.fluss.log_table")

query.awaitTermination()
```

### Write to Primary Key Table

When writing to a primary key table, if a record with the same primary key already exists, it will be updated (upsert semantics):

```scala title="Spark Application"
// Here we use MemoryStream to fake a streaming source.
val inputData = MemoryStream[(Int, String)]
val inputDF = inputData.toDS().toDF("k", "v")

val query = inputDF.writeStream
  .option("checkpointLocation", "/path/to/checkpoint")
  .toTable("fluss_catalog.fluss.pk_table")

query.awaitTermination()
```

:::note
Fluss supports exactly-once semantics for streaming writes through Spark's checkpoint mechanism. Make sure to specify a `checkpointLocation` for fault tolerance.
:::

## Streaming Read

Fluss supports reading data from Fluss tables using Spark Structured Streaming. The streaming source continuously reads new data as it arrives.

:::caution Limitations
- Streaming read currently only supports the `latest` startup mode. Other modes (`full`, `earliest`, `timestamp`) are not yet supported and will be available in a future release.
:::

### Read from Log Table

```scala title="Spark Application"
val df = spark.readStream
  .option("scan.startup.mode", "latest")
  .table("fluss_catalog.fluss.log_table")

val query = df.writeStream
  .format("console")
  .start()

query.awaitTermination()
```

### Read from Primary Key Table

```scala title="Spark Application"
val df = spark.readStream
  .option("scan.startup.mode", "latest")
  .table("fluss_catalog.fluss.pk_table")

val query = df.writeStream
  .format("console")
  .start()

query.awaitTermination()
```

## Trigger Modes

Fluss Spark streaming source supports the following Spark trigger modes:

| Trigger Mode                      | Description                                                                 |
|-----------------------------------|-----------------------------------------------------------------------------|
| Default (micro-batch)             | Processes data as soon as new data is available.                            |
| `Trigger.ProcessingTime(interval)` | Processes data at fixed time intervals.                                    |

**Example:**

```scala title="Spark Application"
import org.apache.spark.sql.streaming.Trigger

val df = spark.readStream
  .option("scan.startup.mode", "latest")
  .table("fluss_catalog.fluss.my_table")

// Processing time trigger (every 5 seconds)
val query = df.writeStream
  .format("console")
  .trigger(Trigger.ProcessingTime("5 seconds"))
  .start()

query.awaitTermination()
```

## End-to-End Example

Here is a complete example that reads data from one Fluss table and writes to another:

```scala title="Spark Application"
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .config("spark.sql.catalog.fluss_catalog", "org.apache.fluss.spark.SparkCatalog")
  .config("spark.sql.catalog.fluss_catalog.bootstrap.servers", "localhost:9123")
  .config("spark.sql.defaultCatalog", "fluss_catalog")
  .config("spark.sql.extensions", "org.apache.fluss.spark.FlussSparkSessionExtensions")
  .getOrCreate()

// Create source and sink tables
spark.sql("CREATE TABLE IF NOT EXISTS source_table (id INT, data STRING)")
spark.sql("""
  CREATE TABLE IF NOT EXISTS sink_table (id INT, data STRING)
  TBLPROPERTIES ('primary.key' = 'id')
""")

// Read from source table
val sourceDF = spark.readStream
  .option("scan.startup.mode", "latest")
  .table("fluss_catalog.fluss.source_table")

// Write to sink table
val query = sourceDF.writeStream
  .option("checkpointLocation", "/tmp/fluss-streaming-checkpoint")
  .toTable("fluss_catalog.fluss.sink_table")

query.awaitTermination()
```
