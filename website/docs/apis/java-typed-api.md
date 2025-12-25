---
title: Java Typed API
sidebar_position: 3
---

# Java Typed API

Fluss provides a Typed API that allows you to work directly with Java POJOs (Plain Old Java Objects) instead of `InternalRow` objects. This simplifies development by automatically mapping your Java classes to Fluss table schemas.

:::info
The Typed API provides a more user-friendly experience but comes with a performance cost due to the overhead of converting between POJOs and internal row formats. For high-performance use cases, consider using the lower-level `InternalRow` API.
:::

## Defining POJOs

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

## Writing Data

### Append Writer

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

### Upsert Writer

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

### Partial Updates

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

## Reading Data

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

### Projections

You can also use projections with the Typed API. The POJO fields that are not part of the projection will be null.

```java
// Only read 'id' and 'name'
TypedLogScanner<User> scanner = table.newScan()
    .project("id", "name")
    .createTypedLogScanner(User.class);
```

## Lookups

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

## Performance Considerations

While the Typed API offers convenience and type safety, it involves an additional layer of conversion between your POJOs and Fluss's internal binary row format (`InternalRow`). This conversion process (serialization and deserialization) introduces CPU overhead.

Benchmarks indicate that using the Typed API can be roughly **2x slower** than using the `InternalRow` API directly for both writing and reading operations.

**Recommendation:**
*   Use the **Typed API** for ease of use, rapid development, and when type safety is preferred over raw performance.
*   Use the **InternalRow API** for high-throughput, latency-sensitive applications where performance is critical.
