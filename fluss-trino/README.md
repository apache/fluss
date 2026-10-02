<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Fluss Trino connector

This read-only connector targets **Trino 483 and JDK 25**. It is built as a
standalone Maven module. Source conventions follow Fluss, including Java 8 syntax
and collection idioms; the resulting plugin requires JDK 25 and is not a Java 8
binary.

## Supported operations

| Capability | Support |
| --- | --- |
| Database, table and column discovery | Supported |
| `DESCRIBE`, `SHOW CREATE TABLE`, `"table$columns"` | Supported |
| Non-partitioned and partitioned Log Table reads | Supported, with bounded offset ranges |
| Non-partitioned and partitioned Primary Key Table reads | Supported, using bucket snapshots |
| Lakehouse table reads | Not supported; metadata remains available |
| Writes, DDL, lookup and Union Read | Not supported |
| Predicate, aggregation, limit and partition pruning pushdown | Not implemented |
| Task retries / fault-tolerant execution | Not supported |
| SASL authentication on JDK 25 | Not supported; configuration properties are retained |

Ordinary `SELECT`, column selection and ordering, `COUNT(*)`, `WHERE`, `ORDER BY`
and `LIMIT` use Trino's execution engine. Filters and other operations without
pushdown are evaluated by Trino. Partitioned scans enumerate physical buckets;
a partition predicate does not currently avoid scanning other partitions.

Trino-visible database and table names are normalized to lowercase. When multiple
Fluss names differ only by case, resolving the ambiguous name fails rather than
arbitrarily choosing a physical table. Case-colliding column names are rejected.

Supported values include BOOLEAN, integer types, FLOAT/DOUBLE, CHAR/STRING,
BINARY/BYTES, DECIMAL, DATE/TIME, TIMESTAMP, TIMESTAMP_LTZ, and recursively
ARRAY/MAP/ROW, subject to Trino's map-key type constraints. Decimal conversion
preserves decimal precision. TIMESTAMP_LTZ preserves the instant using UTC;
TIMESTAMP has no timezone conversion. Fluss TIME values contain milliseconds of
the day, even when their schema declares a higher precision. Null map keys and
keys that become duplicates under Trino semantics are rejected.

## Build

First install the matching Fluss dependencies from the repository root using
JDK 17, as in the connector CI workflow:

```sh
JAVA_HOME=/path/to/jdk-17 ./mvnw -B \
  -pl fluss-client,fluss-server,fluss-test-utils -am install -DskipTests
```

The standalone connector build does not build sibling modules automatically.
This step installs the current checkout's Fluss artifacts and test JARs into the
local Maven repository. It is required in a fresh environment.

Then build the connector with its own Maven wrapper and JDK 25:

```sh
cd fluss-trino
JAVA_HOME=/path/to/jdk-25 ./mvnw clean package -DskipTests
```

## Install and configure

Copy the contents of the assembled `target/fluss-trino-<version>/` directory,
including its dependency JARs, into a dedicated `plugin/fluss/` directory on every
Trino coordinator and worker. Use the assembled plugin, not just the connector
JAR, and deploy the same version on all nodes.

Create `etc/catalog/fluss.properties` on the Trino nodes:

```properties
connector.name=fluss
bootstrap.servers=localhost:9123
```

Replace the bootstrap address with Fluss endpoints reachable from every Trino
node. The current connector supports only unauthenticated `PLAINTEXT` connections.

### SASL limitation on JDK 25

**SASL authentication is currently unsupported.** The Fluss client's SASL callback
uses `Subject.getSubject()`, which is unsupported on JDK 25. A separate Fluss
client compatibility fix is planned.

The properties `client.security.protocol`, `client.security.sasl.mechanism`,
`client.security.sasl.username`, and `client.security.sasl.password` remain
recognized configuration keys for future compatibility. Retaining them does not
enable authentication: the connector currently rejects initialization if a
protocol other than `PLAINTEXT`, or any SASL property, is configured. Leave SASL
properties unset, even when explicitly selecting `PLAINTEXT`.

Client initialization is lazy, so this rejection occurs on the first operation
that needs the Fluss client, rather than during catalog creation. Upgrading the
Fluss client alone will not enable SASL while the connector's explicit restriction
remains in place; it must also be removed after compatibility is verified.

The Arrow JVM option below does not resolve the SASL incompatibility.

### Required JVM option for Arrow reads

Add this line to **Trino's `etc/jvm.config` on every coordinator and worker**:

```text
--add-opens=java.base/java.nio=ALL-UNNAMED
```

Fluss uses Arrow for Arrow-backed data reads. On JDK 25, Arrow needs reflective
access to `java.nio` internals; without this option, data reads can fail even when
metadata queries work. Applying it to all nodes also covers coordinators that
execute queries. Restart the Trino processes after updating their JVM settings.

This is a **server JVM option**, not a catalog property or a Trino CLI option.
Setting it only in Maven, an IDE test runner, or the CLI does not configure the
Trino server. If your deployment manages JVM arguments outside `etc/jvm.config`,
add it to the equivalent server JVM configuration.

Retain the other JVM options required by your Trino 483 distribution. The
connector's integration-test JVM uses the following options in its POM:

```text
--add-modules=jdk.incubator.vector
--enable-native-access=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
```

These test options are not a complete replacement for Trino's server JVM
configuration, and Maven does not install them into the server configuration.

### Example queries

For an existing Fluss database `trino_test` and table `users`:

```sql
SHOW SCHEMAS FROM fluss;
SHOW TABLES FROM fluss.trino_test;
DESCRIBE fluss.trino_test.users;
SHOW CREATE TABLE fluss.trino_test.users;
SELECT * FROM fluss.trino_test."users$columns";
SELECT * FROM fluss.trino_test.users LIMIT 10;
SELECT count(*) FROM fluss.trino_test.users;
```

The catalog name `fluss` comes from the catalog properties filename.
`SHOW CREATE TABLE` describes the existing table; it does not imply that the
connector supports executing `CREATE TABLE`.

## Read semantics

### Log Tables

For non-partitioned tables, planning performs two sequential bulk Admin API
lookups: earliest offsets, then latest offsets, each covering the selected
buckets. For partitioned tables, each phase issues one bulk lookup per selected
partition, covering that partition's buckets. The client may distribute these
API calls across multiple network RPCs.

Each nonempty bucket becomes a split with a fixed `[startOffset, stoppingOffset)`
range. These are per-bucket boundaries, not a global transactional snapshot.
Records appended beyond a split's stopping offset do not extend the query.

An empty poll is not EOF. The reader uses records and consumed-offset progress to
detect completion, including progress without rows. An expired start offset fails
instead of silently resetting to a later offset. Records below the planned start
are treated as an invariant failure; records at or beyond the stop are discarded.

### Primary Key Tables

Planning creates one KV split per physical bucket, without offset or statistics
lookups. Each worker uses a single-bucket BatchScanner to read the live rows in
that bucket's snapshot. Updates and deletes are reflected as of that snapshot.

A snapshot opens when the server processes its initial scan request, not when
Trino plans the query. Different buckets can open at different times, so a query
does not provide a globally consistent snapshot across buckets or partitions.

An empty batch yields; only a null batch means EOF. Unconsumed rows remain in the
current iterator across page requests. The connector does not reopen a failed
scanner and combine rows from different snapshots. Limited open retries belong
to the Fluss client; continuation failures fail the query.

Remote scanner closure is best effort. A lost open response or failed close RPC
can leave a server session until its idle TTL expires. A slow consumer can also
exceed the session TTL; the connector does not add keepalive requests.

### Resource ownership and cancellation

Workers validate table identity, schema identity and split compatibility before
reading. Each PageSource owns its Table and reader; each reader owns its scanner
and buffered batch. The connector owns the shared Connection and Admin client.

Reads are synchronous. Each page request makes at most one scanner poll with a
100 ms timeout argument. That argument does not bound initialization, metadata
RPCs or all client retry behavior. Reads occupy a Trino driver thread;
cancellation relies on Trino's serialized PageSource close contract and the
underlying calls returning. Cancellation within 100 ms is not guaranteed.
