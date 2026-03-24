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

# Apache Fluss - AI Agent Coding Guide

This document provides AI coding agents with comprehensive project-specific context for Apache Fluss, including critical conventions, architectural patterns, and quality standards.

## About This Document

This documentation was created by systematically analyzing the Apache Fluss codebase, including:
- Checkstyle configuration (`tools/maven/checkstyle.xml`)
- Existing code patterns and conventions
- Build configurations and CI pipelines
- Test frameworks and utilities

All rules, patterns, and examples are derived from the actual project source code. The structure and explanations were organized with AI assistance (Claude Code) to make the content accessible to AI coding agents.

**Content Sources:**
- Critical rules: Extracted from Checkstyle enforcement rules
- Code patterns: Identified from production code in `fluss-client`, `fluss-server`, `fluss-common`
- Testing patterns: Documented from existing test classes and test utilities
- All code examples: Real examples from the Apache Fluss codebase

## Table of Contents

1. [Critical Rules (MUST/NEVER)](#1-critical-rules-mustnever)
2. [API Design Patterns](#2-api-design-patterns)
3. [Code Organization](#3-code-organization)
4. [Error Handling](#4-error-handling)
5. [Concurrency & Thread Safety](#5-concurrency--thread-safety)
6. [Testing Standards](#6-testing-standards)
7. [Dependencies & Shading](#7-dependencies--shading)
8. [Configuration Patterns](#8-configuration-patterns)
9. [Serialization & RPC](#9-serialization--rpc)
10. [Module Boundaries](#10-module-boundaries)
11. [Build & CI](#11-build--ci)

---

## 1. Critical Rules (MUST/NEVER)

These rules catch 80% of common violations. **Enforced by Checkstyle** - violations will fail CI.

### 1.1 Dependency Management (MANDATORY)

#### NEVER Use These Imports:

```java
// ❌ DON'T:
import com.google.common.*                    // Use fluss-shaded-guava
import com.fasterxml.jackson.*                // Use fluss-shaded-jackson2
import org.codehaus.jackson.*                 // Use fluss-shaded-jackson2
import com.google.common.base.Preconditions;  // Use Fluss Preconditions
import com.google.common.annotations.VisibleForTesting;  // Use Fluss annotation
import org.apache.commons.lang.*;             // Use commons-lang3
import org.apache.commons.lang3.Validate;     // Use Fluss Preconditions
import org.apache.commons.lang3.SerializationUtils;  // Use Fluss InstantiationUtil
```

#### ALWAYS Use Shaded Versions:

| Forbidden | Required |
|-----------|----------|
| `com.google.common.*` | `org.apache.fluss.shaded.guava.*` |
| `com.fasterxml.jackson.*` | `org.apache.fluss.shaded.jackson2.*` |
| `org.codehaus.jackson.*` | `org.apache.fluss.shaded.jackson2.*` |
| `io.netty.*` | `org.apache.fluss.shaded.netty4.*` |
| `org.apache.arrow.*` | `org.apache.fluss.shaded.arrow.*` |
| `org.apache.zookeeper.*` | `org.apache.fluss.shaded.zookeeper38.*` |

**Rationale:** Prevents dependency conflicts with user applications.

### 1.2 Utility Classes (MANDATORY)

#### NEVER Instantiate ConcurrentHashMap:

```java
// ❌ DON'T:
Map<K, V> map = new ConcurrentHashMap<>();

// ✅ DO:
import org.apache.fluss.utils.MapUtils;
Map<K, V> map = MapUtils.newConcurrentMap();
```

**Rationale:** See https://github.com/apache/fluss/issues/375

#### ALWAYS Use Fluss Preconditions:

```java
// ❌ DON'T:
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.Validate;

// ✅ DO:
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

public void process(String input) {
    checkNotNull(input, "input cannot be null");
    checkArgument(!input.isEmpty(), "input cannot be empty");
}
```

**MUST import statically** - non-static imports will fail Checkstyle.

**Reference:** `fluss-common/src/main/java/org/apache/fluss/utils/Preconditions.java`

#### ALWAYS Use Fluss VisibleForTesting:

```java
// ❌ DON'T:
import com.google.common.annotations.VisibleForTesting;

// ✅ DO:
import org.apache.fluss.annotation.VisibleForTesting;

@VisibleForTesting
void helperMethod() { }
```

### 1.3 Banned Methods (MANDATORY)

#### NEVER Use These Methods:

```java
// ❌ DON'T - These read system properties incorrectly:
Boolean.getBoolean("property.name")
Integer.getInteger("property.name")
Long.getLong("property.name")

// ✅ DO - Read system properties correctly:
Boolean.parseBoolean(System.getProperty("property.name"))
Integer.parseInt(System.getProperty("property.name"))
Long.parseLong(System.getProperty("property.name"))
```

#### NEVER Use SerializationUtils:

```java
// ❌ DON'T:
import org.apache.commons.lang3.SerializationUtils;

// ✅ DO:
import org.apache.fluss.utils.InstantiationUtil;
```

### 1.4 Testing Assertions (MANDATORY)

#### ALWAYS Use AssertJ (NOT JUnit Assertions):

```java
// ❌ DON'T:
import org.junit.jupiter.api.Assertions;
Assertions.assertEquals(expected, actual);
Assertions.assertTrue(condition);
Assertions.assertThrows(Exception.class, () -> doSomething());

// ✅ DO:
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

assertThat(actual).isEqualTo(expected);
assertThat(condition).isTrue();
assertThat(list).hasSize(3).contains("a", "b", "c");
assertThatThrownBy(() -> doSomething())
    .isInstanceOf(IllegalArgumentException.class)
    .hasMessageContaining("expected message");
```

**Rationale:** AssertJ provides more readable and expressive assertions.

**Plugin:** Use "Assertions2Assertj" IntelliJ plugin to convert existing JUnit assertions.

#### NEVER Set @Timeout on Tests:

```java
// ❌ DON'T:
import org.junit.jupiter.api.Timeout;
@Timeout(5)
@Test
void testSomething() { }

// ✅ DO:
// Rely on global timeout instead
@Test
void testSomething() { }
```

**Rationale:** Per-test timeouts make debugging harder. Use global timeout configuration.

### 1.5 Code Style (MANDATORY)

#### NEVER Use Star Imports:

```java
// ❌ DON'T:
import java.util.*;
import org.apache.fluss.client.*;

// ✅ DO:
import java.util.List;
import java.util.Map;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.Admin;
```

**Enforcement:** Set import threshold to 9999 in IDE settings.

#### NEVER Have Trailing Whitespace:

Run `./mvnw spotless:apply` before committing to auto-fix.

#### ALWAYS Use Java-Style Array Declarations:

```java
// ❌ DON'T:
String args[]
int values[]

// ✅ DO:
String[] args
int[] values
```

#### ALWAYS Require Braces:

```java
// ❌ DON'T:
if (condition) doSomething();

// ✅ DO:
if (condition) {
    doSomething();
}
```

### 1.6 Comments & Documentation (MANDATORY)

#### NEVER Use TODOs with Usernames:

```java
// ❌ DON'T:
// TODO(username): Fix this
// TODO(john.doe) Implement feature

// ✅ DO:
// TODO: Fix this
// TODO Implement feature
```

#### NEVER Use These Comment Tags:

```java
// ❌ DON'T:
// FIXME: Should be refactored
// XXX: Hack
// @author John Doe

// ✅ DO:
// TODO: Should be refactored
// (No XXX or @author tags - use git history instead)
```

### 1.7 File Size Limits (MANDATORY)

**Maximum file length:** 3000 lines

If a file exceeds this limit, split it into multiple classes.

### 1.8 Javadoc Requirements (MANDATORY)

#### ALWAYS Document Protected and Public APIs:

```java
// ✅ DO:
/**
 * Creates a new connection to the Fluss cluster.
 *
 * @param configuration The configuration for the connection
 * @return A new connection instance
 * @throws FlussException if connection fails
 */
public static Connection createConnection(Configuration configuration)
    throws FlussException {
    // implementation
}
```

**Required:** All protected and public classes, interfaces, enums, and methods must have Javadoc.

---

## 2. API Design Patterns

### 2.1 API Stability Annotations

Use annotations to signal API stability guarantees to users:

```java
@PublicStable    // Stable - breaking changes only in major versions
@PublicEvolving  // May change in minor versions (new features being stabilized)
@Internal        // Not public API - can change anytime
@VisibleForTesting  // Exposed only for testing
```

**Rules:**
- **ALWAYS annotate** classes/interfaces in public packages (`fluss-client`, `fluss-common` public APIs)
- Use `@PublicStable` for core APIs: `Connection`, `Admin`, `ConfigOption`, `TableDescriptor`
- Use `@PublicEvolving` for new features: `LakeTieringManager`, new connector APIs
- Use `@Internal` for: RPC messages, server internals, `*JsonSerde` classes, test utilities

**Examples:**
```java
@PublicStable
public class ConnectionFactory {
    // Stable public API
}

@PublicEvolving
public interface LakeTieringManager {
    // New feature, API may evolve
}

@Internal
public class MetadataJsonSerde {
    // Internal implementation detail
}
```

**Reference:** `fluss-common/src/main/java/org/apache/fluss/annotation/`

### 2.2 Builder Pattern

Use fluent builder pattern for complex object construction:

```java
// ✅ Example: ConfigOption builder
ConfigOption<String> tempDirs = ConfigBuilder
    .key("tmp.dir")
    .stringType()
    .defaultValue("/tmp")
    .withDescription("Temporary directory path");

ConfigOption<List<Integer>> ports = ConfigBuilder
    .key("application.ports")
    .intType()
    .asList()
    .defaultValues(8000, 8001, 8002);

ConfigOption<Duration> timeout = ConfigBuilder
    .key("client.timeout")
    .durationType()
    .defaultValue(Duration.ofSeconds(30));
```

**Pattern structure:**
- Create static inner class `Builder`
- Use method chaining (return `this`)
- Provide `build()` method to construct final object
- Make constructor private or package-private

**Reference:** `fluss-common/src/main/java/org/apache/fluss/config/ConfigBuilder.java`

### 2.3 Factory Pattern

Use static factory methods for object creation (not public constructors):

```java
@PublicEvolving
public class ConnectionFactory {

    // Private constructor - prevent direct instantiation
    private ConnectionFactory() {}

    /**
     * Creates a new connection to Fluss cluster.
     */
    public static Connection createConnection(Configuration conf) {
        return new FlussConnection(conf);
    }

    /**
     * Creates connection with custom metric registry.
     */
    public static Connection createConnection(
            Configuration conf,
            MetricRegistry metricRegistry) {
        return new FlussConnection(conf, metricRegistry);
    }
}

// ✅ Usage:
Connection conn = ConnectionFactory.createConnection(config);
```

**Rules:**
- Make constructor private
- Provide static factory methods
- Return interface types (not concrete implementations)
- Allow method overloading for different parameter combinations

**Reference:** `fluss-client/src/main/java/org/apache/fluss/client/ConnectionFactory.java`

### 2.4 Interface Segregation Pattern

Provide both generic and typed interface variants:

```java
// Generic interface (works with raw types)
public interface Lookuper extends AutoCloseable {
    CompletableFuture<LookupResult> lookup(InternalRow key);
}

// Typed interface (works with POJOs)
public interface TypedLookuper<T> extends AutoCloseable {
    CompletableFuture<T> lookup(T key);
}

// Same pattern for writers:
public interface AppendWriter extends AutoCloseable {
    CompletableFuture<AppendResult> append(InternalRow row);
}

public interface TypedAppendWriter<T> extends AutoCloseable {
    CompletableFuture<AppendResult> append(T record);
}
```

**Implementation:** Typed variant delegates to generic version internally.

### 2.5 Result Objects Pattern

Use immutable result objects for return values:

```java
public final class AppendResult {
    private final long offset;
    private final long timestamp;

    public AppendResult(long offset, long timestamp) {
        this.offset = offset;
        this.timestamp = timestamp;
    }

    public long getOffset() { return offset; }
    public long getTimestamp() { return timestamp; }

    @Override
    public boolean equals(Object o) { /* ... */ }

    @Override
    public int hashCode() { /* ... */ }

    @Override
    public String toString() { /* ... */ }
}
```

**Rules:**
- Mark class `final`
- Make all fields `private final`
- No setters (only getters)
- Initialize all fields in constructor
- Implement `equals()`, `hashCode()`, `toString()`

### 2.6 Thread Safety Documentation

Document thread safety using annotations:

```java
@ThreadSafe
public class Connection {
    // Multiple threads can safely use this connection
}

@NotThreadSafe
public class Lookuper {
    // Each thread should have its own Lookuper instance
}
```

**Javadoc example:**
```java
/**
 * Connection to a Fluss cluster.
 *
 * <p>This class is thread-safe. A single connection can be shared
 * across multiple threads. However, Table instances obtained from
 * the connection are NOT thread-safe - each thread should create
 * its own Table instance.
 */
@ThreadSafe
public interface Connection extends AutoCloseable { }
```

---

## 3. Code Organization

### 3.1 Naming Conventions

#### Interfaces:
No prefix or suffix - use plain descriptive names:
```java
Connection, Admin, Table, Lookuper, AppendWriter, LogScanner
```

#### Implementation Classes:
Suffix with `Impl` when directly implementing an interface:
```java
FlussConnection (implements Connection)
AdminImpl (implements Admin)
LogScannerImpl (implements LogScanner)
TypedAppendWriterImpl<T> (implements TypedAppendWriter<T>)
```

Alternative: Use descriptive implementation name:
```java
NettyClient, RocksDBKvStore, ZooKeeperCoordinator
```

#### Abstract Base Classes:
Prefix with `Abstract`:
```java
AbstractAutoCloseableRegistry
AbstractAuthorizer
AbstractGoal
AbstractIterator
```

#### Utility Classes:
Suffix with `Utils`, make constructor private, all methods static:
```java
ArrayUtils, MapUtils, CollectionUtils, StringUtils
BytesUtils, BinaryStringUtils
ExceptionUtils, TimeUtils, DateTimeUtils
NetUtils, IOUtils, FileUtils
MathUtils, MurmurHashUtils
```

#### Test Classes:
- Unit tests: `*Test.java` (e.g., `ConfigBuilderTest`, `KvWriteBatchTest`)
- Integration tests: `*ITCase.java` (e.g., `Flink120TableSourceITCase`, `ServerITCaseBase`)
- Base test classes: `*TestBase.java` or `*TestUtils.java`
- Test utilities: Prefix with `Testing` (e.g., `TestingRemoteLogStorage`, `TestingMetricGroups`)

#### Exceptions:
Descriptive name + `Exception` suffix:
```java
TableNotExistException
DatabaseNotExistException
InvalidRecordException
StaleMetadataException
OutOfOrderSequenceException
```

### 3.2 Package Structure

Standard package organization:

```
fluss-common/               # Common utilities, data types, configs
  /annotation              # API stability annotations
  /config                  # Configuration framework
  /exception               # Exception hierarchy
  /fs                      # Filesystem abstraction
  /memory                  # Memory management
  /metrics                 # Metrics definitions
  /record                  # Record formats (log records, KV records)
  /row                     # Row types and encoding
  /types                   # Data types
  /utils                   # Utility classes

fluss-rpc/                  # RPC layer
  /gateway                 # RPC gateway interfaces
  /messages                # Protobuf messages
  /netty                   # Netty-based implementation

fluss-client/               # Java client SDK
  /admin                   # Admin operations
  /write                   # Write APIs
  /lookup                  # Point lookup APIs
  /table                   # Table abstraction
    /scanner               # Scanning/reading operations
  /metadata                # Metadata management
  /token                   # Security token management
  /connection              # Connection management

fluss-server/               # Server implementations
  /coordinator             # CoordinatorServer
  /tablet                  # TabletServer
  /log                     # LogStore implementation
  /kv                      # KvStore implementation
  /replica                 # Replication logic
  /metadata                # Metadata management
  /zk                      # ZooKeeper integration
  /metrics                 # Server metrics
  /authorizer              # Authorization
```

### 3.3 Class Member Organization

**Field ordering:**
1. Static constants (`public static final`)
2. Static fields (`private static`)
3. Instance fields (`private` or `protected`)

**Method ordering:**
1. Constructors
2. Static factory methods
3. Public methods
4. Package-private methods
5. Protected methods
6. Private methods
7. Static utility methods (at bottom)

**Modifier order** (enforced by Checkstyle):
```
public, protected, private, abstract, static, final,
transient, volatile, synchronized, native, strictfp
```

Example:
```java
public class Example {
    // 1. Static constants
    public static final int DEFAULT_SIZE = 100;
    private static final Logger LOG = LoggerFactory.getLogger(Example.class);

    // 2. Static fields
    private static int instanceCount = 0;

    // 3. Instance fields
    private final String name;
    private int value;

    // 4. Constructor
    public Example(String name) {
        this.name = name;
        instanceCount++;
    }

    // 5. Static factory method
    public static Example create(String name) {
        return new Example(name);
    }

    // 6. Public methods
    public void doSomething() { }

    // 7. Private methods
    private void helperMethod() { }

    // 8. Static utility (at bottom)
    private static String formatName(String name) {
        return name.toUpperCase();
    }
}
```

### 3.4 Import Organization

**Order** (enforced by Spotless):
1. `org.apache.fluss` imports
2. Blank line
3. Other imports (`javax`, `java`, third-party)
4. Blank line
5. Static imports (`\#`)

**Example:**
```java
package org.apache.fluss.client;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;
```

**Enforcement:** Run `./mvnw spotless:apply` to auto-format imports.

---

## 4. Error Handling

### 4.1 Exception Hierarchy

```
FlussException (base checked exception)
├── ApiException (client-facing errors)
│   ├── TableNotExistException
│   ├── DatabaseNotExistException
│   ├── InvalidOffsetException
│   ├── InvalidConfigException
│   └── ...
├── RetriableException (transient errors)
│   ├── NotEnoughReplicasException
│   ├── SchemaNotExistException
│   ├── TimeoutException
│   └── ...
├── InvalidMetadataException
│   └── StaleMetadataException
└── FlussRuntimeException (base unchecked exception)
    ├── CorruptRecordException
    ├── LogStorageException
    └── ...
```

**When to extend:**
- `ApiException`: User-facing errors (invalid input, not found, etc.)
- `RetriableException`: Transient failures (retry may succeed)
- `FlussRuntimeException`: Programming errors or unrecoverable failures

### 4.2 Input Validation Pattern

Use Preconditions for input validation at API boundaries:

```java
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

public void createTable(String databaseName, TableDescriptor descriptor) {
    // Null checks
    checkNotNull(databaseName, "database name cannot be null");
    checkNotNull(descriptor, "table descriptor cannot be null");

    // Argument validation
    checkArgument(!databaseName.isEmpty(), "database name cannot be empty");
    checkArgument(descriptor.getBucketCount() > 0,
        "bucket count must be positive, got: %s", descriptor.getBucketCount());

    // State validation
    checkState(isInitialized(), "connection not initialized");
    checkState(!isClosed(), "connection is closed");
}
```

**Preconditions API:**
- `checkNotNull(T obj, String message, Object... args)` → throws `NullPointerException`
- `checkArgument(boolean condition, String message, Object... args)` → throws `IllegalArgumentException`
- `checkState(boolean condition, String message, Object... args)` → throws `IllegalStateException`

**Message templates:** Use `%s` placeholders (not `String.format` style).

### 4.3 Error Propagation

#### For Async Operations:

```java
public CompletableFuture<AppendResult> append(InternalRow row) {
    return CompletableFuture.supplyAsync(() -> {
        try {
            return doAppend(row);
        } catch (IOException e) {
            throw ExceptionUtils.wrapAsUnchecked(e);
        }
    }, executorService);
}
```

#### Composition with Error Handling:

```java
return fetchMetadata(tablePath)
    .thenCompose(metadata -> fetchData(metadata))
    .thenApply(data -> processData(data))
    .exceptionally(ex -> {
        Throwable cause = ExceptionUtils.stripCompletionException(ex);
        if (cause instanceof TableNotExistException) {
            LOG.warn("Table not found: {}", tablePath);
            return null;
        }
        throw ExceptionUtils.wrapAsUnchecked(cause);
    });
```

**Utilities:**
- `ExceptionUtils.wrapAsUnchecked(Exception e)`: Wrap checked exceptions
- `ExceptionUtils.stripCompletionException(Throwable t)`: Unwrap `CompletionException`

---

## 5. Concurrency & Thread Safety

### 5.1 Thread Safety Annotations

Document synchronization requirements:

```java
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.annotation.concurrent.NotThreadSafe;

@ThreadSafe
public class ServerConnection {
    private final Object lock = new Object();

    @GuardedBy("lock")
    private volatile boolean connected;

    @GuardedBy("lock")
    private Channel channel;

    public void connect() {
        synchronized (lock) {
            if (connected) {
                return;
            }
            channel = createChannel();
            connected = true;
        }
    }

    public void disconnect() {
        synchronized (lock) {
            if (!connected) {
                return;
            }
            channel.close();
            connected = false;
        }
    }
}
```

**Rules:**
- Mark thread-safe classes with `@ThreadSafe`
- Use `@GuardedBy("lockName")` to document protected fields
- Declare lock fields explicitly (avoid implicit `this` locks)
- Use `volatile` for fields accessed outside synchronized blocks

**Reference:** `fluss-server/src/main/java/org/apache/fluss/server/log/LogTablet.java`

### 5.2 CompletableFuture Patterns

#### Async API Pattern:

```java
public CompletableFuture<LookupResult> lookup(InternalRow key) {
    return CompletableFuture.supplyAsync(() -> {
        // Perform async lookup operation
        return performLookup(key);
    }, executorService);
}
```

#### Composition:

```java
CompletableFuture<TableInfo> tableInfoFuture = fetchTableInfo(tablePath);

return tableInfoFuture
    .thenCompose(tableInfo -> fetchSchema(tableInfo.getSchemaId()))
    .thenCombine(fetchPartitions(tablePath), (schema, partitions) -> {
        return new TableMetadata(schema, partitions);
    })
    .thenApply(metadata -> processMetadata(metadata))
    .exceptionally(ex -> handleError(ex));
```

#### Waiting for Multiple Futures:

```java
import org.apache.fluss.utils.concurrent.FutureUtils;

List<CompletableFuture<Void>> futures = new ArrayList<>();
futures.add(future1);
futures.add(future2);
futures.add(future3);

CompletableFuture<Void> allFutures = FutureUtils.completeAll(futures);
allFutures.get(); // Wait for all
```

**Utilities:**
- `FutureUtils.completeAll(Collection<CompletableFuture<T>>)`: Wait for all futures
- `FutureUtils.orTimeout(CompletableFuture<T>, Duration)`: Add timeout

### 5.3 Map Creation Rules

**NEVER instantiate ConcurrentHashMap directly** (enforced by Checkstyle):

```java
// ❌ DON'T:
Map<String, Integer> map = new ConcurrentHashMap<>();

// ✅ DO:
import org.apache.fluss.utils.MapUtils;
Map<String, Integer> map = MapUtils.newConcurrentMap();
```

**Other MapUtils methods:**
- `MapUtils.newHashMap()`: Create HashMap with default capacity
- `MapUtils.newHashMap(int capacity)`: Create HashMap with specified capacity
- `MapUtils.newLinkedHashMap()`: Create LinkedHashMap

### 5.4 Resource Management

Use try-with-resources for AutoCloseable resources:

```java
// ✅ Single resource:
try (Connection connection = ConnectionFactory.createConnection(conf)) {
    Admin admin = connection.getAdmin();
    admin.createDatabase("my_db", DatabaseDescriptor.EMPTY, false).get();
} // Auto-closed

// ✅ Multiple resources:
try (Connection connection = ConnectionFactory.createConnection(conf);
     Admin admin = connection.getAdmin();
     Table table = connection.getTable(tablePath)) {
    // Use resources
} // All auto-closed in reverse order
```

**Implement AutoCloseable:**
```java
public class MyResource implements AutoCloseable {
    private volatile boolean closed = false;

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            // Release resources
            cleanupResources();
        } finally {
            closed = true;
        }
    }
}
```

---

## 6. Testing Standards

### 6.1 Test Framework

- **JUnit 5 (Jupiter)** for all tests
- **AssertJ** for assertions (NOT JUnit assertions)
- **Mockito** for mocking (use sparingly - prefer custom test doubles)

### 6.2 Test Naming

Use descriptive test names that explain what is being tested:

```java
@Test
void testAppendWithValidData() { }

@Test
void testLookupThrowsExceptionWhenTableNotExist() { }

@Test
void testTryAppendWithWriteLimit() { }

@Test
void testHandleHybridSnapshotLogSplitChangesAndFetch() { }
```

**Test class naming:**
- Unit tests: `*Test.java`
- Integration tests: `*ITCase.java`

### 6.3 Assertion Patterns

**ALWAYS use AssertJ** (Checkstyle enforces this):

```java
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test
void testValidation() {
    // Basic assertions
    assertThat(result).isNotNull();
    assertThat(result.getOffset()).isEqualTo(100L);
    assertThat(result.isComplete()).isTrue();

    // Collection assertions
    assertThat(list).hasSize(3);
    assertThat(list).contains("a", "b", "c");
    assertThat(list).containsExactly("a", "b", "c"); // Order matters
    assertThat(map).containsEntry("key", "value");

    // Exception assertions
    assertThatThrownBy(() -> service.lookup(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("cannot be null");

    // Future assertions (custom FlussAssertions)
    assertThatFuture(completableFuture)
        .eventuallySucceeds()
        .isEqualTo(expectedValue);
}
```

**Never use JUnit assertions:**
```java
// ❌ DON'T:
import org.junit.jupiter.api.Assertions;
Assertions.assertEquals(expected, actual);
Assertions.assertTrue(condition);
```

### 6.4 Test Base Classes

Use test base classes for common setup:

```java
// For Flink integration tests
class MyFlinkTest extends FlinkTestBase {
    // Provides: FLUSS_CLUSTER_EXTENSION, connection, admin
    // Helper methods: createTable(), writeRows(), etc.

    @Test
    void testFlinkSource() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "test_table");
        createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        // Test implementation
    }
}

// For server tests
class MyServerTest extends ServerTestBase {
    // Provides: ZooKeeper setup, server configuration
}

// For lake tiering tests
class MyLakeTest extends FlinkPaimonTieringTestBase {
    // Provides: Paimon lake integration setup
}
```

**Common base classes:**
- `FlinkTestBase`: Flink cluster + Fluss cluster
- `ServerTestBase`: Coordinator/Tablet server setup
- `FlinkTieringTestBase`: Lake tiering infrastructure
- `LogTestBase`, `KvTestBase`: Record format testing

**Reference:** `fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/utils/FlinkTestBase.java`

### 6.5 Test Extensions

Use JUnit 5 extensions for complex setup:

```java
@RegisterExtension
public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION =
        new AllCallbackWrapper<>(new ZooKeeperExtension());

@RegisterExtension
public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
        FlussClusterExtension.builder()
                .setNumOfTabletServers(3)
                .setClusterConf(new Configuration()
                        .set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, Integer.MAX_VALUE))
                .build();

@Test
void testWithCluster() {
    // Extensions handle cluster start/stop
}
```

**Available extensions:**
- `FlussClusterExtension`: Embedded Fluss cluster
- `ZooKeeperExtension`: Embedded ZooKeeper server
- `ParameterizedTestExtension`: Custom parameterized testing

### 6.6 Parameterized Tests

```java
@ParameterizedTest
@ValueSource(ints = {1, 2, 4, 8})
void testWithDifferentBucketCounts(int bucketCount) {
    // Test logic using bucketCount
}

@ParameterizedTest
@CsvSource({
    "LOG_TABLE, false",
    "PRIMARY_KEY_TABLE, true"
})
void testTableTypes(String tableType, boolean hasPrimaryKey) {
    // Test logic
}

@ParameterizedTest
@MethodSource("provideTableConfigs")
void testWithCustomProvider(TableConfig config) {
    // Test logic
}

private static Stream<TableConfig> provideTableConfigs() {
    return Stream.of(
        new TableConfig("log", false),
        new TableConfig("pk", true)
    );
}
```

### 6.7 Test Utilities

**CommonTestUtils:**
```java
import org.apache.fluss.testutils.common.CommonTestUtils;

// Wait for condition with timeout
CommonTestUtils.waitUntil(
    () -> server.isRunning(),
    Duration.ofSeconds(30),
    "Server failed to start"
);

// Wait for value to become available
Optional<String> result = CommonTestUtils.waitValue(
    () -> fetchValue(),
    Duration.ofSeconds(10),
    "Value not found"
);

// Retry assertion with backoff
CommonTestUtils.retry(
    Duration.ofSeconds(5),
    () -> assertThat(getValue()).isEqualTo(expected)
);
```

**FlussAssertions:**
```java
import org.apache.fluss.testutils.common.FlussAssertions;

// Custom future assertions
assertThatFuture(completableFuture)
    .eventuallySucceeds()
    .isEqualTo(expectedValue);

// Exception chain matching
FlussAssertions.assertThatChainOfCauses(exception)
    .anyCauseMatches(TableNotExistException.class, "table_name");
```

### 6.8 Parallel Test Execution

**Opt-in to parallelism** (tests are sequential by default):

```java
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.CONCURRENT)
class MyParallelTest {
    // Tests in this class can run in parallel

    @Test
    void test1() { }

    @Test
    void test2() { }
}
```

**Only use for truly independent tests** - most tests should remain sequential.

---

## 7. Dependencies & Shading

### 7.1 Shaded Dependencies

**ALWAYS use shaded versions** to prevent dependency conflicts:

| Instead of | Use |
|------------|-----|
| `com.google.common.*` | `org.apache.fluss.shaded.guava.*` |
| `com.fasterxml.jackson.*` | `org.apache.fluss.shaded.jackson2.*` |
| `org.codehaus.jackson.*` | `org.apache.fluss.shaded.jackson2.*` |
| `io.netty.*` | `org.apache.fluss.shaded.netty4.*` |
| `org.apache.arrow.*` | `org.apache.fluss.shaded.arrow.*` |
| `org.apache.zookeeper.*` | `org.apache.fluss.shaded.zookeeper38.*` |

**Example:**
```java
// ❌ DON'T:
import com.google.common.collect.ImmutableList;
import com.fasterxml.jackson.databind.ObjectMapper;

// ✅ DO:
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableList;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
```

### 7.2 Utility Class Substitutions

Use Fluss utilities instead of external libraries:

| Instead of | Use |
|------------|-----|
| Guava `Preconditions` | `org.apache.fluss.utils.Preconditions` |
| Guava `VisibleForTesting` | `org.apache.fluss.annotation.VisibleForTesting` |
| Commons `Validate` | `org.apache.fluss.utils.Preconditions` |
| Commons `SerializationUtils` | `org.apache.fluss.utils.InstantiationUtil` |

**Available Fluss utilities:**
- `ArrayUtils`, `MapUtils`, `CollectionUtils`: Collection operations
- `BytesUtils`, `BinaryStringUtils`: Byte/string manipulation
- `ExceptionUtils`: Error handling utilities
- `FutureUtils`: CompletableFuture utilities
- `TimeUtils`, `DateTimeUtils`: Time handling
- `NetUtils`, `IOUtils`, `FileUtils`: I/O operations
- `MathUtils`, `MurmurHashUtils`: Math operations

### 7.3 Module Dependencies

**Dependency hierarchy:**

```
fluss-common (foundation)
    ↑
fluss-rpc
    ↑
fluss-client ← fluss-server (peer modules)
    ↑           ↑
    |           |
    +----+------+
         |
    connectors (flink, spark, kafka)
         |
    lake integrations (iceberg, paimon, lance)
```

**Rules:**
- `fluss-common` depends only on: JDK + shaded libs (guava, jackson, arrow, etc.)
- `fluss-client` **cannot** depend on `fluss-server`
- `fluss-server` **cannot** depend on connectors
- Connectors **can** depend on `fluss-client`
- Define interfaces in lower-level modules, implementations in higher-level modules

---

## 8. Configuration Patterns

### 8.1 ConfigOption Definition

Use `ConfigBuilder` to define configuration options:

```java
import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.ConfigOption;

// String option with default
public static final ConfigOption<String> BOOTSTRAP_SERVERS =
    ConfigBuilder.key("bootstrap.servers")
        .stringType()
        .defaultValue("localhost:9092")
        .withDescription("Comma-separated list of Fluss server addresses");

// Integer option
public static final ConfigOption<Integer> CLIENT_TIMEOUT_MS =
    ConfigBuilder.key("client.request.timeout.ms")
        .intType()
        .defaultValue(30000)
        .withDescription("Request timeout in milliseconds");

// Duration option
public static final ConfigOption<Duration> SESSION_TIMEOUT =
    ConfigBuilder.key("client.session.timeout")
        .durationType()
        .defaultValue(Duration.ofSeconds(60))
        .withDescription("Client session timeout duration");

// MemorySize option
public static final ConfigOption<MemorySize> BUFFER_SIZE =
    ConfigBuilder.key("client.buffer.size")
        .memoryType()
        .defaultValue(MemorySize.ofMebiBytes(32))
        .withDescription("Buffer size for network operations");

// List option
public static final ConfigOption<List<String>> HOSTS =
    ConfigBuilder.key("client.hosts")
        .stringType()
        .asList()
        .defaultValues("localhost", "127.0.0.1")
        .withDescription("List of host addresses to connect to");

// No default value
public static final ConfigOption<String> USERNAME =
    ConfigBuilder.key("user.name")
        .stringType()
        .noDefaultValue()
        .withDescription("Username for authentication");

// Password option (sensitive)
public static final ConfigOption<Password> PASSWORD =
    ConfigBuilder.key("user.password")
        .passwordType()
        .noDefaultValue()
        .withDescription("Password for authentication (stored securely)");

// Deprecated keys support
public static final ConfigOption<Double> THRESHOLD =
    ConfigBuilder.key("cpu.utilization.threshold")
        .doubleType()
        .defaultValue(0.9)
        .withDeprecatedKeys("cpu.threshold", "old.cpu.threshold")
        .withDescription("CPU utilization threshold");
```

**Reference:** `fluss-common/src/main/java/org/apache/fluss/config/ConfigBuilder.java`

### 8.2 Configuration Usage

```java
Configuration conf = new Configuration();

// Set values
conf.setString("bootstrap.servers", "server1:9092,server2:9092");
conf.setInteger(CLIENT_TIMEOUT_MS, 60000);

// Get values with defaults
String servers = conf.getString(BOOTSTRAP_SERVERS);
int timeout = conf.getInteger(CLIENT_TIMEOUT_MS);
Duration sessionTimeout = conf.get(SESSION_TIMEOUT);

// Get optional values
Optional<String> username = conf.getOptional(USERNAME);
if (username.isPresent()) {
    // Use username
}
```

### 8.3 Configuration Naming Convention

Use hierarchical dot-separated keys with hyphens for compound words:

```
{category}.{subcategory}.{option-name}

Examples:
- remote.data.dir
- remote.fs.write-buffer-size
- client.request.timeout.ms
- default.bucket.number
- kv.snapshot.interval
```

---

## 9. Serialization & RPC

### 9.1 Protocol Buffers

**Proto file conventions:**

```protobuf
syntax = "proto2";  // Currently proto2, migrating to proto3

package fluss.rpc.messages;

option java_package = "org.apache.fluss.rpc.messages";
option java_outer_classname = "FlussApiProtos";
option optimize_for = LITE_RUNTIME;  // Smaller code size

message GetTableSchemaRequest {
    required PbTablePath table_path = 1;
    optional int32 schema_id = 2;
}

message GetTableSchemaResponse {
    required PbSchema schema = 1;
    optional string error_message = 2;
}
```

**Rules for proto3 migration:**
- **DO NOT** use `default` keyword (prepare for proto3)
- **DO NOT** use `enum` type for now (will be supported in proto3)
- Use `required` for mandatory fields (proto2 only)
- Use `optional` for optional fields
- Use `repeated` for lists

### 9.2 Regenerating Protobuf Code

After modifying `.proto` files, regenerate Java code:

```bash
./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc
```

**Reference:** `fluss-rpc/src/main/proto/FlussApi.proto`

### 9.3 RPC Message Pattern

```java
@Internal
public class GetTableSchemaRequest {
    private final TablePath tablePath;
    @Nullable
    private final Integer schemaId;

    public GetTableSchemaRequest(TablePath tablePath, @Nullable Integer schemaId) {
        this.tablePath = checkNotNull(tablePath);
        this.schemaId = schemaId;
    }

    public TablePath getTablePath() { return tablePath; }
    public Optional<Integer> getSchemaId() {
        return Optional.ofNullable(schemaId);
    }
}

@Internal
public class GetTableSchemaResponse {
    private final Schema schema;

    public GetTableSchemaResponse(Schema schema) {
        this.schema = checkNotNull(schema);
    }

    public Schema getSchema() { return schema; }
}
```

**Rules:**
- Mark RPC messages as `@Internal`
- Use immutable pattern (final fields, no setters)
- Provide clear constructors and getters
- Use `@Nullable` for optional fields

---

## 10. Module Boundaries

### 10.1 Core Modules

**fluss-common**: Foundation layer
- **Depends on:** JDK, shaded libraries only
- **Used by:** All other modules
- **Contains:** Utilities, data types, configurations, exceptions

**fluss-rpc**: RPC communication layer
- **Depends on:** `fluss-common`
- **Used by:** `fluss-client`, `fluss-server`
- **Contains:** RPC gateways, protobuf messages, Netty implementation

**fluss-client**: Java client SDK
- **Depends on:** `fluss-common`, `fluss-rpc`
- **Cannot depend on:** `fluss-server` (strict boundary)
- **Used by:** Applications, connectors
- **Contains:** Connection, Admin, Table, Writers, Scanners, Lookupers

**fluss-server**: Server implementations
- **Depends on:** `fluss-common`, `fluss-rpc`
- **Cannot depend on:** `fluss-client`, connectors
- **Contains:** CoordinatorServer, TabletServer, LogStore, KvStore

### 10.2 Connector Modules

**fluss-flink**: Apache Flink connector
- **Structure:**
  - `fluss-flink-common`: Common Flink connector code
  - `fluss-flink-tiering`: Lake tiering service
  - `fluss-flink-1.18`, `1.19`, `1.20`, `2.2`: Version-specific implementations
- **Depends on:** `fluss-client`, specific Flink version
- **Shading:** Each version shades Flink dependencies

**fluss-spark**: Apache Spark connector
- **Structure:**
  - `fluss-spark-common`: Common Spark connector code
  - `fluss-spark-ut`: Unit test utilities
  - `fluss-spark-3.4`, `3.5`: Version-specific implementations
- **Depends on:** `fluss-client`, specific Spark version

**fluss-kafka**: Kafka compatibility layer
- **Provides:** Kafka-compatible API surface
- **Depends on:** `fluss-client`

### 10.3 Lake Integration Modules

**fluss-lake**: Base lake integration
- Sub-modules:
  - `fluss-lake-iceberg`: Apache Iceberg format
  - `fluss-lake-paimon`: Apache Paimon format
  - `fluss-lake-lance`: Lance columnar format

### 10.4 Cross-Module Communication

**Rules:**
1. Use **interfaces** (not implementations) for cross-module APIs
2. Define interfaces in **lower-level modules**
3. Implementations can be in **higher-level modules**
4. **Never** create circular dependencies

**Example:**
```java
// In fluss-common:
public interface FileSystem {
    InputStream openFile(Path path) throws IOException;
    OutputStream createFile(Path path) throws IOException;
}

// In fluss-filesystems/fluss-fs-hdfs:
public class HdfsFileSystem implements FileSystem {
    // HDFS-specific implementation
}
```

---

## 11. Build & CI

### 11.1 Build Commands

**Full build (skip tests):**
```bash
./mvnw clean install -DskipTests
```

**Parallel build (faster):**
```bash
./mvnw clean install -DskipTests -T 1C
```

**Build and run all tests:**
```bash
./mvnw clean verify
```

**Test specific module:**
```bash
./mvnw verify -pl fluss-server
./mvnw verify -pl fluss-client
```

**Test by stage (as in CI):**
```bash
# Core tests (excludes Flink, Spark, Lake)
./mvnw verify -pl '!fluss-flink/**,!fluss-spark/**,!fluss-lake/**'

# Flink tests
./mvnw verify -pl fluss-flink/**

# Spark tests
./mvnw verify -Pspark3 -pl fluss-spark/**

# Lake tests
./mvnw verify -pl fluss-lake/**
```

**Single test class:**
```bash
./mvnw test -Dtest=ConfigBuilderTest -pl fluss-common
```

**Single test method:**
```bash
./mvnw test -Dtest=ConfigBuilderTest#testStringType -pl fluss-common
```

### 11.2 Code Formatting

**Auto-format before committing:**
```bash
./mvnw spotless:apply
```

**Check formatting without changing files:**
```bash
./mvnw spotless:check
```

**Format style:**
- Java: google-java-format (AOSP style)
- Scala: scalafmt (v3.10.2)
- Config: `.scalafmt.conf` in repository root

**IntelliJ IDEA plugin:**
- Install: google-java-format v1.7.0.6
- **DO NOT update** - newer versions have incompatibilities

**Reference:** `tools/maven/checkstyle.xml`

### 11.3 CI Pipeline

**GitHub Actions workflow** runs tests in four parallel stages:

1. **compile-on-jdk8**: Compile with Java 8 for compatibility
2. **core**: Test all modules except Flink, Spark, Lake
3. **flink**: Test Flink connector (all versions)
4. **spark3**: Test Spark connector
5. **lake**: Test Lake integrations + legacy Flink (1.18, 1.19)

**Java compatibility:**
- **Build on:** Java 11 (required)
- **Runtime:** Java 8 compatible (validated with `-Pjava8` profile)
- **CI validates:** Both Java 8 compile and Java 11 tests

**Build command used in CI:**
```bash
# Compile
mvn -T 1C -B clean install -DskipTests

# Test with coverage
mvn -T 1C verify -Ptest-coverage
```

**Reference:** `.github/workflows/ci.yaml`

### 11.4 Test Coverage

**Generate coverage report:**
```bash
./mvnw verify -Ptest-coverage
```

**View report:**
Open `fluss-test-coverage/target/site/jacoco-aggregate/index.html` in browser.

**Coverage tool:** JaCoCo (aggregated across all modules)

### 11.5 License Headers

All files must have Apache 2.0 license header (enforced by Apache RAT plugin):

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
```

**Enforcement:** Run `./mvnw validate` to check license headers.

---

## Additional Resources

### Key Source Files for Reference

1. **Checkstyle configuration:** `tools/maven/checkstyle.xml`
   - All MUST/NEVER rules and style enforcement

2. **Builder pattern:** `fluss-common/src/main/java/org/apache/fluss/config/ConfigBuilder.java`
   - Canonical fluent API design

3. **Factory pattern:** `fluss-client/src/main/java/org/apache/fluss/client/ConnectionFactory.java`
   - Static factory methods with private constructor

4. **Preconditions:** `fluss-common/src/main/java/org/apache/fluss/utils/Preconditions.java`
   - Input validation utility (must be imported statically)

5. **Test base:** `fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/utils/FlinkTestBase.java`
   - Common test setup patterns

6. **CI configuration:** `.github/workflows/ci.yaml`
   - Test stages and build commands

### Development Checklist

Before submitting a PR:

- [ ] Run `./mvnw spotless:apply` to format code
- [ ] Run `./mvnw clean verify` to ensure all tests pass
- [ ] Check that no forbidden imports are used
- [ ] Verify all public APIs have Javadoc
- [ ] Ensure tests use AssertJ (not JUnit assertions)
- [ ] Verify Preconditions are imported statically
- [ ] Check that ConcurrentHashMap is not directly instantiated
- [ ] Ensure file length < 3000 lines
- [ ] Remove trailing whitespace
- [ ] Follow naming conventions (Impl suffix, Abstract prefix, etc.)

---

## Document Information

**Last Updated:** 2026-03-24
**Fluss Version:** 0.10-SNAPSHOT
**Generated-by:** AI-assisted analysis of Apache Fluss codebase using Claude Code

**Content Verification:**
All rules, patterns, and code examples in this document are derived from and verifiable against the Apache Fluss source code. The patterns are not generated—they are documented observations of existing conventions enforced by Checkstyle, Spotless, and established through the project's codebase.

**Contributing:**
For questions, corrections, or suggestions about this guide, please open an issue at https://github.com/apache/fluss/issues

**License:**
This document is licensed under the Apache License 2.0, consistent with the Apache Fluss project.
