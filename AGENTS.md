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

AI coding guide for Apache Fluss with critical rules, patterns, and standards derived from codebase analysis and Checkstyle enforcement.

**Purpose:** This guide helps AI coding agents contribute to Apache Fluss by providing project-specific conventions, architectural patterns, and quality standards. It covers both code contribution (Sections 1-10) and deployment/setup guidance (Section 11).

**Sections:** 1. Critical Rules | 2. API Patterns | 3. Code Organization | 4. Error Handling | 5. Concurrency | 6. Testing | 7. Dependencies | 8. Configuration | 9. Serialization/RPC | 10. Module Boundaries | 11. Build & CI | 12. Git & Pull Requests | 13. AI Agent Boundaries

---

## 1. Critical Rules (MUST/NEVER)

**Enforced by Checkstyle** - violations will fail CI.

### Dependencies & Utilities

**FORBIDDEN imports** (use shaded versions - see Section 7):
```java
import com.google.common.*                              // → org.apache.fluss.shaded.guava.*
import com.fasterxml.jackson.*, org.codehaus.jackson.*  // → org.apache.fluss.shaded.jackson2.*
import io.netty.*                                       // → org.apache.fluss.shaded.netty4.*
import org.apache.arrow.*                               // → org.apache.fluss.shaded.arrow.*
import org.apache.zookeeper.*                           // → org.apache.fluss.shaded.zookeeper38.*
```

**MANDATORY utility substitutions:**
```java
// ❌ new ConcurrentHashMap<>()  → ✅ MapUtils.newConcurrentMap()  (see https://github.com/apache/fluss/issues/375)
// ❌ com.google.common.base.Preconditions  → ✅ org.apache.fluss.utils.Preconditions (import statically)
// ❌ com.google.common.annotations.VisibleForTesting  → ✅ org.apache.fluss.annotation.VisibleForTesting
// ❌ org.apache.commons.lang3.SerializationUtils  → ✅ org.apache.fluss.utils.InstantiationUtil
// ❌ Boolean.getBoolean("prop")  → ✅ Boolean.parseBoolean(System.getProperty("prop"))
```

### Java Version Compatibility

**Source level: Java 8** — All code MUST compile with JDK 8. CI enforces this via `compile-on-jdk8`.

**Build requirement:** Java 11 is required to build the project, but all source code must remain Java 8 compatible.

**FORBIDDEN Java 9+ features:**
```java
// ❌ var keyword (Java 10)
var list = new ArrayList<>();  // → ✅ ArrayList<String> list = new ArrayList<>();

// ❌ List.of(), Map.of(), Set.of() (Java 9)
List.of("a", "b")              // → ✅ Arrays.asList("a", "b")
Map.of("k", "v")               // → ✅ Collections.singletonMap("k", "v")
Set.of("a", "b")               // → ✅ new HashSet<>(Arrays.asList("a", "b"))

// ❌ Optional.isEmpty() (Java 11)
optional.isEmpty()             // → ✅ !optional.isPresent()

// ❌ String.strip(), String.isBlank() (Java 11)
string.strip()                 // → ✅ string.trim()
string.isBlank()               // → ✅ string.trim().isEmpty()

// ❌ Stream.toList() (Java 16)
stream.toList()                // → ✅ stream.collect(Collectors.toList())

// ❌ Map.entry() (Java 9)
Map.entry("k", "v")            // → ✅ new AbstractMap.SimpleEntry<>("k", "v")

// ❌ InputStream.transferTo() (Java 9)
inputStream.transferTo(out)    // → ✅ IOUtils.copyBytes(inputStream, out)

// ❌ Switch expressions, text blocks, records, sealed classes, pattern matching
```

**FORBIDDEN language features:** Switch expressions (Java 12), text blocks (Java 13), records (Java 14), sealed classes (Java 17), pattern matching (Java 16+)

### Testing

**MANDATORY: Use AssertJ, NOT JUnit assertions:**
```java
// ❌ Assertions.assertEquals(expected, actual)
// ✅ assertThat(actual).isEqualTo(expected)

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

assertThat(list).hasSize(3).contains("a", "b");
assertThatThrownBy(() -> doSomething()).isInstanceOf(IllegalArgumentException.class);
```

**NEVER use @Timeout on tests** - rely on global timeout

### Code Style

- **NEVER use star imports** (`import java.util.*;`) - set IDE threshold to 9999
- **NEVER have trailing whitespace** - run `./mvnw spotless:apply`
- **ALWAYS use Java-style arrays:** `String[] args` NOT `String args[]`
- **ALWAYS require braces:** `if (x) { doIt(); }` NOT `if (x) doIt();`
- **NEVER use TODO(username)** - use `TODO:` without username
- **NEVER use FIXME, XXX, or @author tags** - use git history

### Documentation

- **File size limit:** 3000 lines max
- **Javadoc REQUIRED:** All protected/public classes, interfaces, enums, methods

---

## 2. API Design Patterns

### API Stability Annotations

```java
@PublicStable    // Stable - breaking changes only in major versions
@PublicEvolving  // May change in minor versions
@Internal        // Not public API - can change anytime
```

**Usage:** `@PublicStable` for core APIs (`Connection`, `Admin`); `@PublicEvolving` for new features; `@Internal` for RPC/internals
**Reference:** `fluss-common/src/main/java/org/apache/fluss/annotation/`

### Builder Pattern

```java
ConfigOption<Duration> timeout = ConfigBuilder
    .key("client.timeout")
    .durationType()
    .defaultValue(Duration.ofSeconds(30));
```

**Pattern:** Static inner `Builder` class, method chaining, private constructor, `build()` method
**Reference:** `fluss-common/src/main/java/org/apache/fluss/config/ConfigBuilder.java`

### Factory Pattern

```java
public class ConnectionFactory {
    private ConnectionFactory() {}  // Private constructor

    public static Connection createConnection(Configuration conf) {
        return new FlussConnection(conf);
    }
}
```

**Rules:** Private constructor, static factory methods, return interface types
**Reference:** `fluss-client/src/main/java/org/apache/fluss/client/ConnectionFactory.java`

### Additional Patterns

- **Interface Segregation:** Provide generic (`Lookuper`) and typed (`TypedLookuper<T>`) variants
- **Result Objects:** Immutable `final` classes with `private final` fields, no setters, implement `equals()`/`hashCode()`/`toString()`
- **Thread Safety:** Document with `@ThreadSafe` or `@NotThreadSafe` annotations

---

## 3. Code Organization

### Repository Structure

Apache Fluss follows a layered Maven module architecture: `fluss-common` (foundation) → `fluss-rpc` → `fluss-client`/`fluss-server` (peers, cannot cross-depend) → connectors → lake tiering.

#### Core Modules

**`fluss-common`** - Foundation: data types, config, metadata, utilities, exceptions
- Key packages: `annotation`, `config`, `types`, `row`, `record`, `metadata`, `utils`, `exception`, `fs`

**`fluss-metrics`** - Metrics system (reporters, metric groups)

**`fluss-rpc`** - RPC framework, Protocol Buffer messages (proto2)
- Regenerate: `./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc`

**`fluss-client`** - Client library for table operations
- APIs: `Connection`, `Admin`, `Table`, `LogScanner`, `LookupClient`, `UpsertWriter`, `AppendWriter`
- Packages: `admin`, `table`, `write`, `lookup`, `scanner`, `metadata`

**`fluss-server`** - CoordinatorServer (metadata, coordination) + TabletServer (data storage)
- Packages: `coordinator`, `tablet`, `log`, `kv`, `replica`, `zk`, `metadata`, `authorizer`

#### Connectors

**`fluss-flink/`** - Flink connectors: `fluss-flink-common`, `fluss-flink-{1.18,1.19,1.20,2.2}`, `fluss-flink-tiering`

**`fluss-spark/`** - Spark connectors: `fluss-spark-common`, `fluss-spark-{3.4,3.5}`, `fluss-spark-ut`

**`fluss-kafka/`** - Kafka-compatible producer/consumer APIs

#### Lake Tiering

**`fluss-lake/`** - Lake format integrations: `fluss-lake-{iceberg,paimon,lance}`

#### Filesystems

**`fluss-filesystems/`** - Pluggable filesystem implementations: `fluss-fs-{hadoop,hdfs,s3,oss,obs,azure,gs}`

#### Support

**`fluss-test-utils`** - JUnit 5 extensions (`FlussClusterExtension`, `ZooKeeperExtension`), test base classes

**`fluss-dist`** - Binary distribution with `bin/` scripts and `conf/` templates

**`fluss-protogen`** - Protocol Buffer code generation

**`fluss-test-coverage`** - Aggregated JaCoCo test coverage

**`fluss-jmh`** - Performance microbenchmarks

**`fluss-docgen`** - Configuration documentation generation

#### Key Directories

```
fluss/
├── fluss-common/          # Foundation module
├── fluss-rpc/             # RPC framework
├── fluss-client/          # Client APIs
├── fluss-server/          # Server components
├── fluss-flink/           # Flink connectors
├── fluss-spark/           # Spark connectors
├── fluss-lake/            # Lake tiering
├── fluss-filesystems/     # Filesystem plugins
├── fluss-dist/            # Binary distribution
│   └── src/main/resources/
│       ├── bin/           # coordinator-server.sh, tablet-server.sh, local-cluster.sh
│       └── conf/          # server.yaml, log4j.properties
├── .github/workflows/     # CI pipeline (ci.yaml)
└── pom.xml                # Root Maven POM
```

#### Package Conventions

- **`org.apache.fluss.<module>`** - Module root package
- **`org.apache.fluss.shaded.*`** - Shaded dependencies (Guava, Jackson, Netty, Arrow, ZooKeeper)
- **Test packages** - Mirror main structure in `src/test/java`

---

### Naming Conventions

| Type | Convention | Example |
|------|-----------|---------|
| **Interface** | Plain descriptive name | `Connection`, `Admin`, `LogScanner` |
| **Implementation** | Suffix `Impl` or descriptive name | `AdminImpl`, `NettyClient`, `RocksDBKvStore` |
| **Abstract class** | Prefix `Abstract` | `AbstractIterator`, `AbstractGoal` |
| **Utility class** | Suffix `Utils` (private constructor, static methods) | `MapUtils`, `StringUtils`, `IOUtils` |
| **Test class** | Suffix `Test` (unit) or `ITCase` (integration) | `ConfigBuilderTest`, `ServerITCaseBase` |
| **Test utility** | Prefix `Testing` | `TestingRemoteLogStorage` |
| **Exception** | Suffix `Exception` | `TableNotExistException` |

### Package Structure

See CLAUDE.md for full module/package organization. Key modules: `fluss-common`, `fluss-rpc`, `fluss-client`, `fluss-server`.

### Class Member Order

**Fields:** Static constants → static fields → instance fields
**Methods:** Constructors → static factories → public → package-private → protected → private → static utilities
**Modifier order:** `public protected private abstract static final transient volatile synchronized native strictfp` (Checkstyle enforced)

### Imports

**Order:** `org.apache.fluss.*` → blank line → other imports → blank line → static imports
**Enforcement:** `./mvnw spotless:apply`

---

## 4. Error Handling

**Exception hierarchy:** `FlussException` (checked) → `ApiException` (user errors), `RetriableException` (transient), `FlussRuntimeException` (unchecked)

**Input validation:** Use `Preconditions.checkNotNull()`, `checkArgument()`, `checkState()` (see Section 1) at API boundaries with `%s` placeholders

**Error propagation:** Async operations use `ExceptionUtils.wrapAsUnchecked()` to wrap checked exceptions; use `.exceptionally()` for CompletableFuture error handling

---

## 5. Concurrency & Thread Safety

**Thread safety annotations:** `@ThreadSafe`, `@NotThreadSafe`, `@GuardedBy("lockName")` for documentation

**Locking:** Explicit lock objects (`private final Object lock`), use `synchronized(lock)`, `volatile` for fields accessed outside locks

**ConcurrentHashMap:** NEVER instantiate directly - use `MapUtils.newConcurrentMap()` (Checkstyle enforced - see Section 1)

**CompletableFuture:** Use `.thenCompose()`, `.thenCombine()`, `.thenApply()` for composition; `FutureUtils.completeAll()` for multiple futures

**Resources:** Use try-with-resources for `AutoCloseable`; implement with idempotent `close()` method

---

## 6. Testing Standards

**Framework:** JUnit 5 with AssertJ assertions (MANDATORY - see Section 1.4)

**Test naming:** Descriptive method names (`testAppendWithValidData`); classes: `*Test.java` (unit), `*ITCase.java` (integration)

### Assertions

```java
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

assertThat(result).isNotNull().isEqualTo(expected);
assertThat(list).hasSize(3).contains("a", "b");
assertThatThrownBy(() -> service.lookup(null))
    .isInstanceOf(NullPointerException.class)
    .hasMessageContaining("cannot be null");
```

### Test Base Classes

Common base classes for setup:
- `FlinkTestBase`: Flink + Fluss cluster (see `fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/utils/FlinkTestBase.java`)
- `ServerTestBase`: Coordinator/TabletServer setup
- `FlinkTieringTestBase`: Lake tiering infrastructure
- `LogTestBase`, `KvTestBase`: Record format testing

### Extensions

```java
@RegisterExtension
public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
    FlussClusterExtension.builder().setNumOfTabletServers(3).build();
```

**Available:** `FlussClusterExtension`, `ZooKeeperExtension`

---

## 7. Dependencies & Shading

**Shaded dependencies:** See Section 1 for forbidden imports. Always use `org.apache.fluss.shaded.*` versions (guava, jackson, netty, arrow, zookeeper)

**Fluss utilities:** `Preconditions`, `MapUtils`, `ArrayUtils`, `CollectionUtils`, `BytesUtils`, `ExceptionUtils`, `FutureUtils`, `TimeUtils`, `IOUtils`, `FileUtils`

**Module dependency rules:**
- `fluss-common` (foundation) → `fluss-rpc` → `fluss-client`/`fluss-server` (peers) → connectors → lake
- `fluss-client` CANNOT depend on `fluss-server`; define interfaces in lower modules

---

## 8. Configuration Patterns

**ConfigOption definition:** Use `ConfigBuilder.key("name").{type}Type().defaultValue(x).withDescription("...")`
**Types:** `stringType()`, `intType()`, `durationType()`, `memoryType()`, `passwordType()`, `asList()`, `noDefaultValue()`, `withDeprecatedKeys()`
**Reference:** `fluss-common/src/main/java/org/apache/fluss/config/ConfigBuilder.java`

**Usage:** `conf.setString()`, `conf.getInteger()`, `conf.get()`, `conf.getOptional()`

**Naming:** Hierarchical dot-separated keys with hyphens: `{category}.{subcategory}.{option-name}` (e.g., `client.request.timeout.ms`)

---

## 9. Serialization & RPC

**Protocol Buffers:** proto2 (migrating to proto3); DO NOT use `default` keyword or `enum` type; use `required`/`optional`/`repeated`
**Regenerate:** `./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc`
**RPC messages:** Mark `@Internal`, immutable (final fields, no setters), use `@Nullable` for optional fields

---

## 10. Module Boundaries

**Module structure:** See CLAUDE.md for full module organization

**Core:** `fluss-common` (foundation) → `fluss-rpc` → `fluss-client`/`fluss-server` (peers, cannot cross-depend)
**Connectors:** `fluss-flink` (1.18/1.19/1.20/2.2), `fluss-spark` (3.4/3.5), `fluss-kafka` - depend on `fluss-client`
**Lake:** `fluss-lake-iceberg`, `fluss-lake-paimon`, `fluss-lake-lance`

**Rules:** Define interfaces in lower modules, implementations in higher modules; no circular dependencies

---

## 11. Build, CI & Deployment

### Quick Start (Deployment)

**Prerequisites:** Java 11, Maven 3.8.6+, Unix-like environment

**Build from source:**
```bash
git clone https://github.com/apache/fluss.git
cd fluss
./mvnw clean install -DskipTests -T 1C
```

**Binary location:** `fluss-dist/target/fluss-*-bin.tgz`

**Start cluster (local development):**
```bash
# 1. Start ZooKeeper (separate process)
# 2. Start CoordinatorServer
./bin/coordinator-server.sh start
# 3. Start TabletServer(s)
./bin/tablet-server.sh start
```

**Configuration:** Edit `conf/server.yaml` for `zookeeper.address`, `bind.listeners`, `tablet-server.id`

### Build Commands

**Build:** `./mvnw clean install -DskipTests` (parallel: `-T 1C`); test: `./mvnw clean verify`
**Test specific:** `./mvnw verify -pl fluss-server`; single test: `./mvnw test -Dtest=ConfigBuilderTest -pl fluss-common`
**Format code:** `./mvnw spotless:apply` (google-java-format AOSP style; IntelliJ plugin v1.7.0.6 - DO NOT update)

### CI Pipeline

**CI stages:** compile-on-jdk8 → core (excludes Flink/Spark/Lake) → flink → spark3 → lake (`.github/workflows/ci.yaml`)
**Java:** Build on Java 11 (required); runtime Java 8 compatible

**Test coverage:** `./mvnw verify -Ptest-coverage` → view `fluss-test-coverage/target/site/jacoco-aggregate/index.html`
**License headers:** Apache 2.0 required (enforced by RAT); check with `./mvnw validate`

---

## 12. Git & Pull Request Workflow

### Fork Management

**ALWAYS push to your fork, NEVER to apache/fluss upstream.**

Verify fork remote exists:
```bash
git remote -v
# Should show: fork https://github.com/<your-username>/fluss.git
```

If fork remote doesn't exist:
```bash
# Via GitHub CLI (recommended)
gh repo fork apache/fluss --remote --remote-name fork

# Or manually
git remote add fork https://github.com/<your-username>/fluss.git
```

### Commit Guidelines

**Commit message format:**
```
[component] Brief description (under 70 chars)

Detailed explanation of changes and motivation.
```

**Component tags:** `[client]`, `[server]`, `[rpc]`, `[flink]`, `[spark]`, `[docs]`, `[build]`, `[test]`

### Pre-Push Self-Review

Before pushing, conduct thorough self-review:

1. **Review full diff:** `git diff main...HEAD` - verify only intentional changes
2. **Check code style:** `./mvnw spotless:check`
3. **Run tests:** `./mvnw verify -pl <affected-modules>`
4. **Verify Checkstyle:** Check no forbidden imports, AssertJ usage, MapUtils, etc.
5. **Audit security:** No secrets, credentials, or sensitive data committed

### Creating Pull Request

**Rebase onto main:**
```bash
git fetch upstream main
git rebase upstream/main
```

**Push to fork:**
```bash
git push -u fork <branch-name>
```

**Create PR:**
```bash
gh pr create --web --title "[component] Brief title (under 70 chars)"
```

The `--web` flag opens browser for final review before submission.

**PR description template:**
```markdown
## Summary
- Bullet point summary of changes
- Fixes issue #XXX (if applicable)

## Test Plan
- How changes were tested
- Affected modules/tests run

🤖 AI-assisted changes - reviewed by human developer
```

---

## 13. AI Agent Boundaries

### Ask Before Acting

**Large-scale changes requiring approval:**
- Cross-module refactoring affecting 5+ files
- New dependencies with broad impact
- Database schema or migration changes
- Changes to build system or CI pipeline
- Destructive operations (delete branches, force-push, reset --hard)

### Never Do (Without Explicit Permission)

**Prohibited actions:**
- ❌ Commit secrets, credentials, API keys, or tokens
- ❌ Push directly to `apache/fluss` upstream (always use fork)
- ❌ Force-push to shared branches (main, release branches)
- ❌ Execute destructive git operations (`reset --hard`, `clean -fdx`, `branch -D`)
- ❌ Modify generated files when code generation workflows exist
- ❌ Skip pre-commit hooks (`--no-verify`) without explicit request
- ❌ Add dependencies without discussing compatibility/licensing
- ❌ Disable Checkstyle/Spotless rules to make code pass

### Safe to Do (Within Scope)

**Encouraged autonomous actions:**
- ✅ Read any file in the repository
- ✅ Run tests and build commands
- ✅ Format code with `./mvnw spotless:apply`
- ✅ Fix Checkstyle violations following Section 1 rules
- ✅ Create feature branches in your fork
- ✅ Commit changes with proper attribution
- ✅ Push to your fork and create PRs

### Verification Requirements

**Before committing code:**
1. All tests pass: `./mvnw verify`
2. Code formatted: `./mvnw spotless:check`
3. No Checkstyle violations
4. License headers present: `./mvnw validate`
5. Self-review completed (Section 12)

**When in doubt:** Ask the user before proceeding with potentially destructive or far-reaching changes.

---

**Version:** 0.10-SNAPSHOT | **License:** Apache License 2.0
