---
slug: fluss-java-client
title: "Apache Fluss Java Client: A Deep Dive"
authors: [giannis]
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

![Banner](assets/java_client/banner.png)

## Introduction
Apache Fluss is a streaming data storage system built for real-time analytics, serving as a low-latency data layer in modern data Lakehouses.
It supports sub-second streaming reads and writes, storing data in a columnar format for efficiency, and offers two flexible table types: **append-only Log Tables** and **updatable Primary Key Tables**. 
In practice, this means Fluss can ingest high-throughput event streams *(using log tables)* while also maintaining *up-to-date* reference data or state *(using primary key tables)*, a combination ideal for 
scenarios like IoT, where you might stream sensor readings and look up information for those sensors in real-time, without
the need for external K/V stores.
<!-- truncate -->

In this tutorial, we'll introduce the **Fluss Java Client** by walking through a simple home IoT system example. 
We will use `Fluss's Admin client` to create a primary key table for sensor information and a log table for sensor readings, then use the client 
to write data to these tables and read/enrich the streaming sensor data. 

By the end, you'll see how a sensor reading can be ingested into a log table and immediately enriched with information from a primary key table (essentially performing a real-time lookup join for streaming data enrichment).

## Preflight Check
The full source code can be found [here](https://github.com/ververica/ververica-fluss-examples/tree/main/fluss-java-client).

```shell
docker compose up
```

The first thing we need to do is establish a connection to the Fluss cluster. 
The `Connection` is the main entry point for the Fluss client, from which we obtain an `Admin` (for metadata operations) and Table instances (for data operations)

```java
// Configure connection to Fluss cluster
Configuration conf = new Configuration();
conf.setString("bootstrap.servers", "localhost:9123");  // Fluss server endpoint
Connection connection = ConnectionFactory.createConnection(conf);

// Get Admin client for managing databases and tables
Admin admin = connection.getAdmin();
```
The above code snippet shows the bare minimum requirements for connecting and interacting with a Fluss Cluster.
For our example we will use the following mock data - to keep things simple - which you can find below:
```java
public static final List<SensorReading> readings = List.of(
        new SensorReading(1, LocalDateTime.of(2025, 6, 23, 9, 15), 22.5, 45.0, 1013.2, 87.5),
        new SensorReading(2, LocalDateTime.of(2025, 6, 23, 9, 30), 23.1, 44.5, 1013.1, 88.0),
        new SensorReading(3, LocalDateTime.of(2025, 6, 23, 9, 45), 21.8, 46.2, 1012.9, 86.9),
        new SensorReading(4, LocalDateTime.of(2025, 6, 23, 10, 0), 24.0, 43.8, 1013.5, 89.2),
        new SensorReading(5, LocalDateTime.of(2025, 6, 23, 10, 15), 22.9, 45.3, 1013.0, 87.8),
        new SensorReading(6, LocalDateTime.of(2025, 6, 23, 10, 30), 23.4, 44.9, 1013.3, 88.3),
        new SensorReading(7, LocalDateTime.of(2025, 6, 23, 10, 45), 21.7, 46.5, 1012.8, 86.5),
        new SensorReading(8, LocalDateTime.of(2025, 6, 23, 11, 0), 24.2, 43.5, 1013.6, 89.5),
        new SensorReading(9, LocalDateTime.of(2025, 6, 23, 11, 15), 23.0, 45.1, 1013.2, 87.9),
        new SensorReading(10, LocalDateTime.of(2025, 6, 23, 11, 30), 22.6, 45.7, 1013.0, 87.4)
);
```

```java
public static final List<SensorInfo> sensorInfos = List.of(
        new SensorInfo(1, "Outdoor Temp Sensor", "Temperature", "Roof", LocalDate.of(2024, 1, 15), "OK", LocalDateTime.of(2025, 6, 23, 9, 15)),
        new SensorInfo(2, "Main Lobby Sensor", "Humidity", "Lobby", LocalDate.of(2024, 2, 20), "ERROR", LocalDateTime.of(2025, 6, 23, 9, 30)),
        new SensorInfo(3, "Server Room Sensor", "Temperature", "Server Room", LocalDate.of(2024, 3, 10), "MAINTENANCE", LocalDateTime.of(2025, 6, 23, 9, 45)),
        new SensorInfo(4, "Warehouse Sensor", "Pressure", "Warehouse", LocalDate.of(2024, 4, 5), "OK", LocalDateTime.of(2025, 6, 23, 10, 0)),
        new SensorInfo(5, "Conference Room Sensor", "Humidity", "Conference Room", LocalDate.of(2024, 5, 25), "OK", LocalDateTime.of(2025, 6, 23, 10, 15)),
        new SensorInfo(6, "Office 1 Sensor", "Temperature", "Office 1", LocalDate.of(2024, 6, 18), "LOW_BATTERY", LocalDateTime.of(2025, 6, 23, 10, 30)),
        new SensorInfo(7, "Office 2 Sensor", "Humidity", "Office 2", LocalDate.of(2024, 7, 12), "OK", LocalDateTime.of(2025, 6, 23, 10, 45)),
        new SensorInfo(8, "Lab Sensor", "Temperature", "Lab", LocalDate.of(2024, 8, 30), "ERROR", LocalDateTime.of(2025, 6, 23, 11, 0)),
        new SensorInfo(9, "Parking Lot Sensor", "Pressure", "Parking Lot", LocalDate.of(2024, 9, 14), "OK", LocalDateTime.of(2025, 6, 23, 11, 15)),
        new SensorInfo(10, "Backyard Sensor", "Temperature", "Backyard", LocalDate.of(2024, 10, 3), "OK", LocalDateTime.of(2025, 6, 23, 11, 30)),

        // SEND SOME UPDATES
        new SensorInfo(2, "Main Lobby Sensor", "Humidity", "Lobby", LocalDate.of(2024, 2, 20), "ERROR", LocalDateTime.of(2025, 6, 23, 9, 48)),
        new SensorInfo(8, "Lab Sensor", "Temperature", "Lab", LocalDate.of(2024, 8, 30), "ERROR", LocalDateTime.of(2025, 6, 23, 11, 16))
);
```

## Operating The Cluster
Let's create a database for our IoT data, and within it define two tables:
* **Sensor Readings Table:** A log table that will collect time-series readings from sensors (like temperature and humidity readings). This table is append-only (new records are added continuously, with no updates/deletes) which is ideal for immutable event streams
* **Sensor Information Table:** A primary key table that stores metadata for each sensor (like sensor ID, location, type). Each `sensorId` will be unique and acts as the primary key. This table can be updated as sensor info changes (e.g., sensor relocated or reconfigured). 

Using the Admin client, we can programmatically create these tables. 

First, we'll ensure the database exists (creating it if not), then define schemas for each table and create them:

### Schema Definitions
#### Log table (sensor readings)

```java
public static Schema getSensorReadingsSchema() {
    return Schema.newBuilder()
            .column("sensorId", DataTypes.INT())
            .column("timestamp", DataTypes.TIMESTAMP())
            .column("temperature", DataTypes.DOUBLE())
            .column("humidity", DataTypes.DOUBLE())
            .column("pressure", DataTypes.DOUBLE())
            .column("batteryLevel", DataTypes.DOUBLE())
            .build();
}
```

#### Primary Key table (sensor information)
```java
public static Schema getSensorInfoSchema() {
    return Schema.newBuilder()
            .column("sensorId", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .column("type", DataTypes.STRING())
            .column("location", DataTypes.STRING())
            .column("installationDate", DataTypes.DATE())
            .column("state", DataTypes.STRING())
            .column("lastUpdated", DataTypes.TIMESTAMP())
            .primaryKey("sensorId")             <-- Define a Primary Key
            .build();
}
```

### Table Creation
```java
public static void setupTables(Admin admin) throws ExecutionException, InterruptedException {
    TableDescriptor readingsDescriptor = TableDescriptor.builder()
            .schema(getSensorReadingsSchema())
            .distributedBy(3, "sensorId")
            .comment("This is the sensor readings table")
            .build();

    // drop the tables or ignore if they exist
    admin.dropTable(readingsTablePath, true).get();
    admin.dropTable(sensorInfoTablePath, true).get();
     
    admin.createTable(readingsTablePath, readingsDescriptor, true).get();
    
    TableDescriptor sensorInfoDescriptor = TableDescriptor.builder()
            .schema(getSensorInfoSchema())
            .distributedBy(3, "sensorId")
            .comment("This is the sensor information table")
            .build();
     
    admin.createTable(sensorInfoTablePath, sensorInfoDescriptor, true).get();
}
```
We specify a distribution with `.distributedBy(3, "sensorId")`. 
Fluss tables are partitioned into buckets (similar to partitions in Kafka topics) for scalability. 
Here we use 3 buckets, meaning data gets distributed across 3 buckets. Multiple buckets allow for higher throughput or to parallelize reads/writes. 
If using multiple buckets, Fluss would hash on the bucket key (`sensorId` in our case) to assign records to buckets.

For the `sensor_readings` table, we define a schema without any primary key. In Fluss, a table created without a primary key clause is a Log Table. 
A log table only supports appending new records (no updates or deletes), making it perfect for immutable time-series data or logs.

In the log table, specifying a bucket key like `sensorId` ensures all readings from the same sensor end up to the same bucket providing strict ordering guarantees.

With our tables created let's go and write some data.

## Table Writes
With our tables in place, let's insert some data using the Fluss Java API. 
The client allows us to write or read data from it. 
We'll demonstrate two patterns:
* **Upserting** into the primary key table (sensor information). 
* **Appending** to the log table (sensor readings).

Fluss provides specialized writer interfaces for each table type: an **UpsertWriter** for primary key tables and an **AppendWriter** for log tables. 
Under the hood, the Fluss client currently expects data as **GenericRow** objects (a generic row data format). 

> **Note:** Internally Fluss uses **InternalRow** as an optimized, binary representation of data for better performance and memory efficiency. 
> **GenericRow** is a generic implementation of InternalRow. This allows developers to interact with data easily while Fluss processes it efficiently using the underlying binary format. 

Since we are creating **Pojos** though this means that we need to convert these into a GenericRow in order to write them into Fluss.

```java
public static GenericRow energyReadingToRow(SensorReading reading) {
    GenericRow row = new GenericRow(SensorReading.class.getDeclaredFields().length);
    row.setField(0, reading.sensorId());
    row.setField(1, TimestampNtz.fromLocalDateTime(reading.timestamp()));
    row.setField(2, reading.temperature());
    row.setField(3, reading.humidity());
    row.setField(4, reading.pressure());
    row.setField(5, reading.batteryLevel());
    return row;
}
public static GenericRow sensorInfoToRow(SensorInfo sensorInfo) {
    GenericRow row = new GenericRow(SensorInfo.class.getDeclaredFields().length);
    row.setField(0, sensorInfo.sensorId());
    row.setField(1, BinaryString.fromString(sensorInfo.name()));
    row.setField(2, BinaryString.fromString(sensorInfo.type()));
    row.setField(3, BinaryString.fromString(sensorInfo.location()));
    row.setField(4, (int) sensorInfo.installationDate().toEpochDay());
    row.setField(5, BinaryString.fromString(sensorInfo.state()));
    row.setField(6, TimestampNtz.fromLocalDateTime(sensorInfo.lastUpdated()));     
    return row;
}
```
**Note:** For certain data types like `String` or `LocalDateTime` we need to use certain functions like
`BinaryString.fromString("string_value")` or `TimestampNtz.fromLocalDateTime(datetime)` otherwise you might
come across some conversion exceptions.

Let's start by writing data to the `Log Table`. This requires getting an `AppendWriter` as follows:

```java
logger.info("Creating table writer for table {} ...", AppUtils.SENSOR_READINGS_TBL);
Table table = connection.getTable(AppUtils.getSensorReadingsTablePath());
AppendWriter writer = table.newAppend().createWriter();

AppUtils.readings.forEach(reading -> {
    GenericRow row = energyReadingToRow(reading);
    writer.append(row);
});
writer.flush();

logger.info("Sensor Readings Written Successfully.");
```
At this point we have successfully written 10 sensor readings to our table.


Next, let's write data to the `Primary Key Table`. This requires getting an `UpsertWriter` as follows:
```java
logger.info("Creating table writer for table {} ...", AppUtils.SENSOR_INFORMATION_TBL);
Table sensorInfoTable = connection.getTable(AppUtils.getSensorInfoTablePath());
UpsertWriter upsertWriter = sensorInfoTable.newUpsert().createWriter();

AppUtils.sensorInfos.forEach(sensorInfo -> {
    GenericRow row = sensorInfoToRow(sensorInfo);
    upsertWriter.upsert(row);
});

upsertWriter.flush();
```
At this point we have successfully written 10 sensor information records to our table, because 
updates will be handled on the primary key and merged.

## Scans & Lookups
Now comes the real-time data enrichment part of our example. 
We want to simulate a process where each incoming sensor reading is immediately looked up against the sensor information table to add context (like location and type) to the raw reading.
This is a common pattern in streaming systems, often achieved with lookup joins. 

With the Fluss Java client, we can do this by combining a **log scanner on the readings table** with **point lookups on the sensor information table**.

To consume data from a Fluss table, we use a **Scanner*. 
For a log table, Fluss provides a **LogScanner** that allows us to **subscribe to one or more buckets** and poll for new records.

```java
LogScanner logScanner = readingsTable.newScan()         
        .createLogScanner();
```

```java
Lookuper sensorInforLookuper = sensorInfoTable
        .newLookup()
        .createLookuper();
```

We set up a scanner on the `sensor_readings` table, and next we need to subscribe to all its buckets, and then poll for any available records:
```java
int numBuckets = readingsTable.getTableInfo().getNumBuckets();
for (int i = 0; i < numBuckets; i++) {     
    logger.info("Subscribing to Bucket {}.", i);
    logScanner.subscribeFromBeginning(i);
}
```

Start polling for records. For each incoming record we will use the **Lookuper** to `lookup` sensor information from the primary key table,
and creating a **SensorReadingEnriched** record. 
```java
 while (true) {
    logger.info("Polling for records...");
    ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
    for (TableBucket bucket : scanRecords.buckets()) {
        for (ScanRecord record : scanRecords.records(bucket)) {
            InternalRow row = record.getRow();
            
            logger.info("Received reading from sensor '{}' at '{}'.", row.getInt(0), row.getTimestampNtz(1, 6).toString());
            logger.info("Performing lookup to get the information for sensor '{}'. ", row.getInt(0));
            LookupResult lookupResult = sensorInforLookuper.lookup(row).get();
            SensorInfo sensorInfo = lookupResult.getRowList().stream().map(r -> new SensorInfo(
                    r.getInt(0),
                    r.getString(1).toString(),
                    r.getString(2).toString(),
                    r.getString(3).toString(),
                    LocalDate.ofEpochDay(r.getInt(4)),
                    r.getString(5).toString(),
                    LocalDateTime.parse(r.getTimestampNtz(6, 6).toString(), formatter)
            )).findFirst().get();
            logger.info("Retrieved information for '{}' with id: {}", sensorInfo.name(), sensorInfo.sensorId());

            SensorReading reading = new SensorReading(
                    row.getInt(0),
                    LocalDateTime.parse(row.getTimestampNtz(1, 6).toString(), formatter),
                    row.getDouble(2),
                    row.getDouble(3),
                    row.getDouble(4),
                    row.getDouble(5)
            );

            SensorReadingEnriched readingEnriched = new SensorReadingEnriched(
                    reading.sensorId(),
                    reading.timestamp(),
                    reading.temperature(),
                    reading.humidity(),
                    reading.pressure(),
                    reading.batteryLevel(),
                    sensorInfo.name(),
                    sensorInfo.type(),
                    sensorInfo.location(),
                    sensorInfo.state()
            );
            logger.info("Bucket: {} - {}", bucket, readingEnriched);
            logger.info("---------------------------------------");
        }
    }
}
```
Let's summarize what's happening here:
* We create a LogScanner for the `sensor_readings` table using *table.newScan().createLogScanner()*. 
* We subscribe to each bucket of the table from the beginning (offset 0). Subscribing `from beginning` means we'll read all existing data from the start; alternatively, one could subscribe from the latest position to only get new incoming data or based on other attributes like time. In our case, since we just inserted data, from-beginning will capture those inserts. 
* We then call `poll(Duration)` on the scanner to retrieve available records, waiting up to the given timeout (1 second here). This returns a `ScanRecords` batch containing any records that were present. We iterate over each `TableBucket` and then over each `ScanRecord` within that bucket. 
* For each record, we extract the fields via the InternalRow interface (which provides typed access to each column in the row) and **convert them into a Pojo**. 
* Next, for each reading, we perform a **lookup** on the **sensor_information** table to get the sensor's info. We construct a key (GenericRow with just the sensor_id) and use **sensorTable.newLookup().createLookuper().lookup(key)**. This performs a point lookup by primary key and returns a `LookupResult future`; we call `.get()` to get the result synchronously. If present, we retrieve the InternalRow of the sensor information and **convert it into a Pojo**. 
* We then combine the data: logging an enriched message that includes the sensor's information alongside the reading values. 

Fluss's lookup API gives us quick primary-key retrieval from a table, which is exactly what we need to enrich the streaming data. 
In a real application, this enrichment could be done on the fly in a streaming job (and indeed **Fluss is designed to support high-QPS lookup joins in real-time pipelines**), but here we're simulating it with client calls for clarity.

If you run the above code found [here](https://github.com/ververica/ververica-fluss-examples), you should see an output like the following:
```shell
16:07:13.594 INFO  [DownloadRemoteLog-[sensors_db.sensor_readings_tbl]] c.a.f.c.t.s.l.RemoteLogDownloader$DownloadRemoteLogThread - Starting
16:07:13.599 INFO  [main] com.ververica.scanner.FlussScanner - Subscribing to Bucket 0.
16:07:13.599 INFO  [main] com.ververica.scanner.FlussScanner - Subscribing to Bucket 1.
16:07:13.600 INFO  [main] com.ververica.scanner.FlussScanner - Subscribing to Bucket 2.
16:07:13.600 INFO  [main] com.ververica.scanner.FlussScanner - Polling for records...
16:07:13.965 INFO  [main] com.ververica.scanner.FlussScanner - Received reading from sensor '3' at '2025-06-23T09:45'.
16:07:13.966 INFO  [main] com.ververica.scanner.FlussScanner - Performing lookup to get the information for sensor '3'. 
16:07:14.032 INFO  [main] com.ververica.scanner.FlussScanner - Retrieved information for 'Server Room Sensor' with id: 3
16:07:14.033 INFO  [main] com.ververica.scanner.FlussScanner - Bucket: TableBucket{tableId=2, bucket=1} - SensorReadingEnriched[sensorId=3, timestamp=2025-06-23T09:45, temperature=21.8, humidity=46.2, pressure=1012.9, batteryLevel=86.9, name=Server Room Sensor, type=Temperature, location=Server Room, state=MAINTENANCE]
16:07:14.045 INFO  [main] com.ververica.scanner.FlussScanner - ---------------------------------------
16:07:14.046 INFO  [main] com.ververica.scanner.FlussScanner - Received reading from sensor '4' at '2025-06-23T10:00'.
16:07:14.046 INFO  [main] com.ververica.scanner.FlussScanner - Performing lookup to get the information for sensor '4'. 
16:07:14.128 INFO  [main] com.ververica.scanner.FlussScanner - Retrieved information for 'Warehouse Sensor' with id: 4
16:07:14.128 INFO  [main] com.ververica.scanner.FlussScanner - Bucket: TableBucket{tableId=2, bucket=1} - SensorReadingEnriched[sensorId=4, timestamp=2025-06-23T10:00, temperature=24.0, humidity=43.8, pressure=1013.5, batteryLevel=89.2, name=Warehouse Sensor, type=Pressure, location=Warehouse, state=OK]
16:07:14.129 INFO  [main] com.ververica.scanner.FlussScanner - ---------------------------------------
16:07:14.129 INFO  [main] com.ververica.scanner.FlussScanner - Received reading from sensor '8' at '2025-06-23T11:00'.
16:07:14.129 INFO  [main] com.ververica.scanner.FlussScanner - Performing lookup to get the information for sensor '8'. 
16:07:14.229 INFO  [main] com.ververica.scanner.FlussScanner - Retrieved information for 'Lab Sensor' with id: 8
16:07:14.229 INFO  [main] com.ververica.scanner.FlussScanner - Bucket: TableBucket{tableId=2, bucket=1} - SensorReadingEnriched[sensorId=8, timestamp=2025-06-23T11:00, temperature=24.2, humidity=43.5, pressure=1013.6, batteryLevel=89.5, name=Lab Sensor, type=Temperature, location=Lab, state=ERROR]
16:07:14.229 INFO  [main] com.ververica.scanner.FlussScanner - ---------------------------------------
```

## Column Pruning Scans
Column pruning lets you fetch only the columns you need, **reducing network overhead and improving read performance**. With Fluss’s Java client, you can specify a subset of columns in your scan:
```java
LogScanner logScanner = readingsTable.newScan()
    .project(List.of("sensorId", "timestamp", "temperature"))
    .createLogScanner();
```

Let's break this down:
* `.project(...)` instructs the client to request only the specified columns (sensorId,timestamp and temperature) from the server. 
* Fluss’s columnar storage means non-requested columns (e.g., humidity, etc.) **aren’t transmitted, saving bandwidth and reducing client-side parsing overhead**. 
* You can combine projection with filters or lookups to further optimize your data access patterns.

Example output:
```shell
16:12:35.114 INFO  [main] com.ververica.scanner.FlussScanner - Subscribing to Bucket 0.
16:12:35.114 INFO  [main] com.ververica.scanner.FlussScanner - Subscribing to Bucket 1.
16:12:35.114 INFO  [main] com.ververica.scanner.FlussScanner - Subscribing to Bucket 2.
16:12:35.114 INFO  [main] com.ververica.scanner.FlussScanner - Polling for records...
16:12:35.171 INFO  [main] com.ververica.scanner.FlussScanner - Bucket: TableBucket{tableId=2, bucket=1} - (3,2025-06-23T09:45,21.8)
16:12:35.172 INFO  [main] com.ververica.scanner.FlussScanner - ---------------------------------------
16:12:35.172 INFO  [main] com.ververica.scanner.FlussScanner - Bucket: TableBucket{tableId=2, bucket=1} - (4,2025-06-23T10:00,24.0)
16:12:35.172 INFO  [main] com.ververica.scanner.FlussScanner - ---------------------------------------
16:12:35.172 INFO  [main] com.ververica.scanner.FlussScanner - Bucket: TableBucket{tableId=2, bucket=1} - (8,2025-06-23T11:00,24.2)
16:12:35.172 INFO  [main] com.ververica.scanner.FlussScanner - ---------------------------------------
16:12:35.172 INFO  [main] com.ververica.scanner.FlussScanner - Bucket: TableBucket{tableId=2, bucket=1} - (10,2025-06-23T11:30,22.6)
```
Notice, how only the requested columns are returned from the server.

## Conclusion
In this blog post, we've introduced the Fluss Java Client by guiding you through a full example of creating tables, writing data, and reading/enriching data in real-time. 
We covered how to use the `Admin` client to define a **Primary Key table** (for reference data that can be updated) and a **Log table** (for immutable event streams), and how to use the Fluss client to upsert and append data accordingly. 
We also demonstrated reading from a log table using a scanner and performing a lookup on a primary key table to enrich the streaming data on the fly. 

This IoT sensor scenario is just one example of Fluss in action and also highlights the **Stream/Table duality** within the same system. 
Fluss's ability to handle high-throughput append streams and fast key-based lookups makes it well-suited for real-time analytics use cases like this and many others. 
With this foundation, you can explore more advanced features of Fluss to build robust real-time data applications. Happy streaming! 🌊 

And before you go 😊 don’t forget to give Fluss 🌊 some ❤️ via ⭐ on [GitHub](https://github.com/alibaba/fluss)
