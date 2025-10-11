---
title: "Java Client"
sidebar_position: 1
---

# Fluss Java Client
## Overview
The Fluss Java client provides a comprehensive API for interacting with Fluss resources and data. It consists of two main components:

- **Admin API**: Supports operations for managing Fluss resources:
    * Database operations (create, drop, list)
    * Table operations (create, drop, list)
    * Partition management (create, drop, list)
    * Metadata retrieval (schemas, snapshots, server information)

- **Table API**: Enables data operations on Fluss tables:
    * Reading data (scanning, lookups)
    * Writing data (appends, upserts)

## Getting Started

### Adding the Dependency
Add the following dependency to your `pom.xml` file.

```xml
<!-- https://mvnrepository.com/artifact/org.apache.fluss/fluss-client -->
<dependency>
    <groupId>org.apache.fluss</groupId>
    <artifactId>fluss-client</artifactId>
    <version>$FLUSS_VERSION$</version>
</dependency>
```

### Establishing a Connection

The `Connection` object is the main entry point for the Fluss Java client. It is used to create `Admin` and `Table` instances.
The `Connection` object is created using the `ConnectionFactory` class, which takes a `Configuration` object as an argument.
The `Configuration` object contains parameters required for connecting to the Fluss cluster, such as the bootstrap servers endpoints.

Create a new `Connection` instance:
```java
// Create a Connection to connect with Fluss cluster
Configuration conf = new Configuration(); 
conf.setString("bootstrap.servers", "localhost:9123");
Connection connection = ConnectionFactory.createConnection(conf);
```

#### Authentication

If you are using SASL authentication, set the following properties:
```java
Configuration conf = new Configuration(); 
conf.setString("bootstrap.servers", "localhost:9123");
conf.setString("client.security.protocol", "SASL");
conf.setString("client.security.sasl.mechanism", "PLAIN");
conf.setString("client.security.sasl.username", "alice");
conf.setString("client.security.sasl.password", "alice-secret");

Connection connection = ConnectionFactory.createConnection(conf);
```

The `Connection` object is **thread-safe** and can be shared across multiple threads. It is recommended to create a
single `Connection` instance per application and reuse it to create multiple `Admin` and `Table` instances.

`Admin` and `Table` instances, on the other hand, are **not thread-safe** and should be created for each thread that needs to access them.

> ⚠️ Caching or pooling `Admin` and `Table` instances is not recommended.


## Admin API

Create a new `Admin` instance:
```java
// Obtain Admin instance from the Connection
Admin admin = connection.getAdmin();
```

All methods in `FlussAdmin` return `CompletableFuture` objects. You can use them in both blocking and non-blocking styles.

### Blocking Operations
For synchronous behavior, use the `get()` method:
```java
// Blocking call
List<String> databases = admin.listDatabases().get();
```

### Asynchronous Operations
For non-blocking behavior, use the `thenAccept`, `thenApply`, or other methods:
```java
admin.listDatabases()
    .thenAccept(databases -> {
        System.out.println("Available databases:");
        databases.forEach(System.out::println);
    })
    .exceptionally(ex -> {
        System.err.println("Failed to list databases: " + ex.getMessage());
        return null;
    });
```

### Creating Databases and Tables
#### Creating a Database
```java
// Create database descriptor
DatabaseDescriptor descriptor = DatabaseDescriptor.builder()
    .comment("This is a test database")
    .customProperty("owner", "data-team")
    .build();

// Create database (true = ignore if already exists)
admin.createDatabase("my_db", descriptor, true) // non-blocking call
    .thenAccept(unused -> System.out.println("Database created successfully"))
    .exceptionally(ex -> {
        System.err.println("Failed to create database: " + ex.getMessage());
        return null;
    });
```


#### Creating a Table
```java
Schema schema = Schema.newBuilder()
        .column("id", DataTypes.STRING())
        .column("age", DataTypes.INT())
        .column("created_at", DataTypes.TIMESTAMP())
        .column("is_active", DataTypes.BOOLEAN())
        .primaryKey("id")
        .build();

// Use the schema in a table descriptor
TableDescriptor tableDescriptor = TableDescriptor.builder()
        .schema(schema)
        .distributedBy(1, "id")  // distribute rows by `id` into 1 bucket
//        .partitionedBy("")     // (Optional) partition key(s) if needed
        .build();

TablePath tablePath = TablePath.of("my_db", "user_table");
admin.createTable(tablePath, tableDescriptor, false).get(); // blocking call

TableInfo tableInfo = admin.getTableInfo(tablePath).get(); // blocking call
System.out.println(tableInfo);
```

## Table API

### Writers
Create a new `Table` instance:

```java
TablePath tablePath = TablePath.of("my_db", "user_table");
Table table = connection.getTable(tablePath);
```

Fluss supports two types of tables, each with its own writing mechanism:
- **Primary Key Tables**: Use `UpsertWriter` to insert or update data.
- **Log Tables**: Use `AppendWriter` to append data.

#### Primary Key Table Writers
For primary key tables, use the `UpsertWriter` to insert or update records based on the primary key:

```java
UpsertWriter writer = table.newUpsert().createWriter();
```

Example of writing data to a Primary Key Table:

```java
List<User> users = List.of(
    new User("1", 20, LocalDateTime.now(), true),
    new User("2", 22, LocalDateTime.now(), true),
    new User("3", 23, LocalDateTime.now(), true),
    new User("4", 24, LocalDateTime.now(), true),
    new User("5", 25, LocalDateTime.now(), true)
);

Table table = connection.getTable(tablePath);

// Convert POJOs to GenericRows
List<GenericRow> rows = users.stream().map(user -> {
    GenericRow row = new GenericRow(4);
    row.setField(0, BinaryString.fromString(user.getId()));
    row.setField(1, user.getAge());
    row.setField(2, TimestampNtz.fromLocalDateTime(user.getCreatedAt()));
    row.setField(3, user.isActive());
    return row;
}).collect(Collectors.toList());
    
System.out.println("Upserting rows to the table");
UpsertWriter writer = table.newUpsert().createWriter();

// upsert() is a non-blocking call that sends data to Fluss server with batching and timeout
rows.forEach(writer::upsert);

// call flush() to block the thread until all data is written successfully
writer.flush();
```

**Note:** Currently data in Fluss is written in the form of `GenericRow` objects. The Fluss community is working on providing a more user-friendly API for writing data directly from POJOs.

#### Log Table Writers
For log tables, use the `AppendWriter` to sequentially append records:

```java
AppendWriter writer = table.newAppend().createWriter();
```

Example of writing data to a Log Table:

```java
List<GenericRow> rows = // convert your data to GenericRow as demonstrated in the "Primary Key Table Writers" section above

System.out.println("Appending rows to the log table");
AppendWriter writer = table.newAppend().createWriter();

// append() is a non-blocking call that sends data to Fluss server
rows.forEach(writer::append);

// call flush() to ensure all data is written
writer.flush();
```

### Scanner
In order to read data from Fluss tables, first you need to create a Scanner instance. Then users can subscribe to the table buckets and 
start polling for records.
```java
LogScanner logScanner = table.newScan()
        .createLogScanner();

int numBuckets = table.getTableInfo().getNumBuckets();
System.out.println("Number of buckets: " + numBuckets);
for (int i = 0; i < numBuckets; i++) {     
    System.out.println("Subscribing to bucket " + i);
    logScanner.subscribeFromBeginning(i);
}

long scanned = 0;
Map<Integer, List<String>> rowsMap = new HashMap<>();

while (true) {     
    System.out.println("Polling for records...");
    ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
    for (TableBucket bucket : scanRecords.buckets()) {
        for (ScanRecord record : scanRecords.records(bucket)) {
            InternalRow row = record.getRow();
            // Process the row
            ...
        }
    }
    scanned += scanRecords.count();
}
```

### Lookup
Fluss supports lookups by primary key and by prefix key.

- For primary key lookups:

```java
LookupResult lookup = table.newLookup()
                .createLookuper()
                .lookup(rowKey)
                .get();
```

- For prefix key lookups:

```java
LookupResult prefixLookup = table.newLookup()
        .lookupBy(prefixKeys)
        .createLookuper()
        .lookup(rowKey)
        .get();
```