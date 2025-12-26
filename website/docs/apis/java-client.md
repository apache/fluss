---
title: "Java Client"
sidebar_position: 1
---

# Fluss Java Client
## Overview
Fluss `Admin` API that supports asynchronous operations for managing and inspecting Fluss resources. It communicates with the Fluss cluster and provides methods for:

* Managing databases (create, drop, list)
* Managing tables (create, drop, list)
* Managing partitions (create, drop, list)
* Retrieving metadata (schemas, snapshots, server information)

Fluss `Table` API allows you to interact with Fluss tables for reading and writing data.
## Dependency
In order to use the client, you need to add the following dependency to your `pom.xml` file.

```xml
<!-- https://mvnrepository.com/artifact/org.apache.fluss/fluss-client -->
<dependency>
    <groupId>org.apache.fluss</groupId>
    <artifactId>fluss-client</artifactId>
    <version>$FLUSS_VERSION$</version>
</dependency>
```

## Initialization

`Connection` is the main entry point for the Fluss Java client. It is used to create `Admin` and `Table` instances.
The `Connection` object is created using the `ConnectionFactory` class, which takes a `Configuration` object as an argument.
The `Configuration` object contains the necessary configuration parameters for connecting to the Fluss cluster, such as the bootstrap servers.

The `Connection` object is thread-safe and can be shared across multiple threads. It is recommended to create a
single `Connection` instance per application and use it to create multiple `Admin` and `Table` instances.
`Table` and `Admin` instances, on the other hand, are not thread-safe and should be created for each thread that needs to access them.
 Caching or pooling of `Table` and `Admin` is not recommended.

Create a new `Admin` instance :
```java
// creating Connection object to connect with Fluss cluster
Configuration conf = new Configuration(); 
conf.setString("bootstrap.servers", "localhost:9123");
Connection connection = ConnectionFactory.createConnection(conf);


// obtain Admin instance from the Connection
Admin admin = connection.getAdmin();
admin.listDatabases().get().forEach(System.out::println);

// obtain Table instance from the Connection
Table table = connection.getTable(TablePath.of("my_db", "my_table"));
System.out.println(table.getTableInfo());
```

if you are using SASL authentication, you need to set the following properties:
```java
// creating Connection object to connect with Fluss cluster
Configuration conf = new Configuration(); 
conf.setString("bootstrap.servers", "localhost:9123");
conf.setString("client.security.protocol", "sasl");
conf.setString("client.security.sasl.mechanism", "PLAIN");
conf.setString("client.security.sasl.username", "alice");
conf.setString("client.security.sasl.password", "alice-secret");
Connection connection = ConnectionFactory.createConnection(conf);


// obtain Admin instance from the Connection
Admin admin = connection.getAdmin();
admin.listDatabases().get().forEach(System.out::println);

// obtain Table instance from the Connection
Table table = connection.getTable(TablePath.of("my_db", "my_table"));
System.out.println(table.getTableInfo());
```



## Working Operations
All methods in `FlussAdmin` return `CompletableFuture` objects. You can handle these in two ways:

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

## Creating Databases and Tables
### Creating a Database
```java

// Create database descriptor
DatabaseDescriptor descriptor = DatabaseDescriptor.builder()
    .comment("This is a test database")
    .customProperty("owner", "data-team")
    .build();

// Create database (true means ignore if exists)
admin.createDatabase("my_db", descriptor, true) // non-blocking call
    .thenAccept(unused -> System.out.println("Database created successfully"))
    .exceptionally(ex -> {
        System.err.println("Failed to create database: " + ex.getMessage());
        return null;
    });
```


### Creating a Table
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
        .distributedBy(1, "id")  // Distribute by the id column with 1 buckets
//        .partitionedBy("")     // Partition by the partition key
        .build();

TablePath tablePath = TablePath.of("my_db", "user_table");
admin.createTable(tablePath, tableDescriptor, false).get(); // blocking call

TableInfo tableInfo = admin.getTableInfo(tablePath).get(); // blocking call
System.out.println(tableInfo);
```

## Table API
### Writers
In order to write data to Fluss tables, first you need to create a Table instance.
```java
TablePath tablePath = TablePath.of("my_db", "user_table");
Table table = connection.getTable(tablePath);
```

In Fluss we have both Primary Key Tables and Log Tables, so the client provides different functionality depending on the table type.
You can use an `UpsertWriter` to write data to a Primary Key table, and an `AppendWriter` to write data to a Log Table.
````java
table.newUpsert().createWriter();
table.newAppend().createWriter();
````

Let's take a look at how to write data to a Primary Key table.
```java
List<User> users = List.of(
        new User("1", 20, LocalDateTime.now() , true),
        new User("2", 22, LocalDateTime.now() , true),
        new User("3", 23, LocalDateTime.now() , true),
        new User("4", 24, LocalDateTime.now() , true),
        new User("5", 25, LocalDateTime.now() , true)
);
```

**Note:** Currently data in Fluss is written in the form of `rows`, so we need to convert our POJO to `GenericRow`. For a more user-friendly API for writing data, please refer to the [Java Typed API](#java-typed-api) section below.
```java
Table table = connection.getTable(tablePath);

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

// call flush() to blocking the thread until all data is written successfully
writer.flush();
```

For a Log table you can use the `AppendWriter` API to write data.
```java
table.newAppend().createWriter().append(row);
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
You can also use the Fluss API to perform lookups on a table. This is useful for querying specific records based on their primary key or prefix key.
```java
// Lookup by primary key
LookupResult lookup = table.newLookup()
                .createLookuper()
                .lookup(rowKey)
                .get();
// Lookup by prefix key
LookupResult prefixLookup = table.newLookup()
        .lookupBy(prefixKeys)
        .createLookuper()
        .lookup(rowKey)
        .get();
```

## Java Typed API

Fluss provides a Typed API that allows you to work directly with Java POJOs (Plain Old Java Objects) instead of `InternalRow` objects. This simplifies development by automatically mapping your Java classes to Fluss table schemas.

:::info
The Typed API provides a more user-friendly experience but comes with a performance cost due to the overhead of converting between POJOs and internal row formats. For high-performance use cases, consider using the lower-level `InternalRow` API.
:::

### Defining POJOs

To use the Typed API, define a Java class where the field names and types match your Fluss table schema.

```java
public class User {
    public Integer id;
    public String name;
    public Integer age;

    public User() {}

    public User(Integer id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }
    
    // Getters, setters, equals, hashCode, toString...
}
```

The supported type mappings are:

| Fluss Type | Java Type |
|---|---|
| INT | Integer |
| BIGINT | Long |
| STRING | String |
| BOOLEAN | Boolean |
| FLOAT | Float |
| DOUBLE | Double |
| DECIMAL | BigDecimal |
| DATE | LocalDate |
| TIME | LocalTime |
| TIMESTAMP | LocalDateTime |
| TIMESTAMP_LTZ | Instant |
| BINARY / BYTES | byte[] |

### Writing Data

#### Append Writer

For append-only tables (Log tables), use `TypedAppendWriter`.

```java
TablePath path = TablePath.of("my_db", "users_log");
try (Table table = conn.getTable(path)) {
    TypedAppendWriter<User> writer = table.newAppend().createTypedWriter(User.class);
    
    writer.append(new User(1, "Alice", 30));
    writer.append(new User(2, "Bob", 25));
    
    writer.flush();
}
```

#### Upsert Writer

For primary key tables, use `TypedUpsertWriter`.

```java
TablePath path = TablePath.of("my_db", "users_pk");
try (Table table = conn.getTable(path)) {
    TypedUpsertWriter<User> writer = table.newUpsert().createTypedWriter(User.class);
    
    // Insert or Update
    writer.upsert(new User(1, "Alice", 31));
    
    // Delete
    writer.delete(new User(1, null, null)); // Only PK fields are needed for delete
    
    writer.flush();
}
```

#### Partial Updates

You can perform partial updates by specifying the columns to update.

```java
// Update only 'name' and 'age' for the user with id 1
Upsert upsert = table.newUpsert().partialUpdate("name", "age");
TypedUpsertWriter<User> writer = upsert.createTypedWriter(User.class);

User partialUser = new User();
partialUser.id = 1;
partialUser.name = "Alice Updated";
partialUser.age = 32;

writer.upsert(partialUser);
writer.flush();
```

### Reading Data

Use `TypedLogScanner` to read data as POJOs.

```java
Scan scan = table.newScan();
TypedLogScanner<User> scanner = scan.createTypedLogScanner(User.class);

try (CloseableIterator<TypedScanRecord<User>> iterator = scanner.subscribeFromBeginning()) {
    while (iterator.hasNext()) {
        TypedScanRecord<User> record = iterator.next();
        ChangeType changeType = record.getChangeType();
        User user = record.getValue();
        
        System.out.println(changeType + ": " + user);
    }
}
```

#### Projections

You can also use projections with the Typed API. The POJO fields that are not part of the projection will be null.

```java
// Only read 'id' and 'name'
TypedLogScanner<User> scanner = table.newScan()
    .project("id", "name")
    .createTypedLogScanner(User.class);
```

### Lookups

For primary key tables, you can perform lookups using a POJO that represents the primary key.

```java
// Define a POJO for the key
public class UserId {
    public Integer id;
    
    public UserId(Integer id) { this.id = id; }
}

// Create a TypedLookuper
TypedLookuper<UserId> lookuper = table.newLookup().createTypedLookuper(UserId.class);

// Perform lookup
CompletableFuture<LookupResult> resultFuture = lookuper.lookup(new UserId(1));
LookupResult result = resultFuture.get();

if (result != null) {
    // Convert the result row back to a User POJO if needed
    // Note: You might need a RowToPojoConverter for this part if you want the full User object
    // or you can access fields from the InternalRow directly.
}
```

### Performance Considerations

While the Typed API offers convenience and type safety, it involves an additional layer of conversion between your POJOs and Fluss's internal binary row format (`InternalRow`). This conversion process (serialization and deserialization) introduces CPU overhead.

Benchmarks indicate that using the Typed API can be roughly **2x slower** than using the `InternalRow` API directly for both writing and reading operations.

**Recommendation:**
*   Use the **Typed API** for ease of use, rapid development, and when type safety is preferred over raw performance.
*   Use the **InternalRow API** for high-throughput, latency-sensitive applications where performance is critical.
