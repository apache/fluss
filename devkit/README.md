# Fluss DevKit

Fluss DevKit starts a local Fluss environment with Docker Compose and `just`. It uses upstream
runtime images and mounts the local `../build-target` directory read-only, so testing source changes
never requires building a Docker image. It is a contributor tool for validating a local distribution,
startup scripts, and plugin classloaders; use the [deployment documentation](../website/docs/install-deploy/)
for production environments.

## Requirements

- JDK 11 or later
- A Unix-like environment with Bash and `curl`
- Docker with Docker Compose v2
- [just](https://github.com/casey/just)

## Quick Start

Build and start the core development environment:

```bash
cd devkit
just build
just up
```

For a lake profile, build the tiering artifacts and select a format:

```bash
just build-tiering
just up iceberg
```

`just up` waits for the Fluss and Flink clusters and, for lake profiles, the tiering job to become
ready. Run `just --list` to see all available commands.

## How local code is loaded

The containers use the local build output; the DevKit does not rebuild a Docker image for every
source change:

| Local content | How it reaches the running container |
|---|---|
| Fluss Server changes | `just build` packages `fluss-dist`; `build-target` points to that distribution and is mounted read-only as `/opt/fluss`. The Fluss startup scripts run from this mount. |
| Flink 1.20 Connector changes | `just build` produces `fluss-flink-1.20/target/fluss-flink-1.20-*.jar`. `just up` stages it as `devkit/.deps/flink/active/lib/fluss-flink-1.20.jar`, mounts that directory into the Flink containers, and copies it to `/opt/flink/lib` during container startup. |
| SQL changes | `just run-sql path/to/query.sql` reads the file from the host and sends it to the SQL Client running in the JobManager container. Flink then plans and executes the job in the Flink cluster. |
| Lake Tiering changes | `just build-tiering` additionally builds the Lake plugin and Tiering Job. `just up <profile>` stages the plugin under Flink `lib/` and submits the local Tiering Job JAR to Flink. |

This means the SQL you write locally is used directly for that invocation, but Java source code is
used through a built JAR or distribution. After changing Fluss Server or the Flink Connector code,
rebuild and restart the selected profile so the containers load the new artifacts:

```bash
# Core changes
just build
just up core

# Lake Tiering changes
just build-tiering
just up paimon                 # or iceberg / lance

# SQL-only changes need no Maven build
just run-sql ./my-query.sql
```

The Flink image itself remains the upstream runtime image. The Fluss Connector is an extension
loaded by Flink; it is not a replacement for Flink and it is not loaded from the Java source tree
at runtime. A running Flink process also does not hot-reload a newly built JAR, so use `just up`
after rebuilding.

## Profiles

| Profile | Services |
|---|---|
| `core` | ZooKeeper, Fluss, and Flink |
| `iceberg` | Core, RustFS, PostgreSQL JDBC Catalog, Flink, and Iceberg tiering |
| `paimon` | Core, RustFS, Flink, and Paimon tiering |
| `lance` | Core, RustFS, Flink, and Lance tiering |

These profiles are selected local-development combinations, not copies of every documented
deployment. In particular, `core` does not include the Faker source and S3 dependencies bundled in
the Flink quickstart image. The lake profiles use primary-key tables so the default workflow also
exercises KV snapshots; restart recovery is still outside the smoke workflow.

Profiles start one TabletServer by default. Pass `3` as the second argument to start three:

```bash
just up core 3
just up paimon 3
```

## Core SQL examples

The small set of examples in `examples/core/` covers the main Flink access patterns without
turning the DevKit into a complete test suite. They follow the same basic flow as the Flink
quickstart: create a Fluss catalog, prepare Log and Primary Key Tables, write data, and query it.
The core profile deliberately does not bundle the Faker connector, so these examples use fixed
`VALUES` data instead of adding another runtime dependency. Start the core profile and run the
setup file once:

```bash
just up core
just run-sql examples/core/01-setup.sql
```

Then use the focused examples:

| File | Demonstrates |
|---|---|
| `02-scan.sql` | Streaming and batch scans of Log and Primary Key Tables |
| `03-lookup-join.sql` | Point lookup and prefix lookup joins |
| `04-changelog-binlog.sql` | Changelog and binlog virtual tables |

The scan and virtual-table examples use batch mode and terminate. The streaming scan and lookup
join examples are intentionally long-running; the lookup file contains one active section at a
time, with the alternative prefix lookup shown in comments. The setup file must be run before the
other examples because they use its tables.

## Lakehouse examples

Lakehouse examples are also kept under `examples/`, so the files used by the smoke commands and the
files intended for interactive exploration have one home. The Paimon example is the complete
reference path for a table with `table.datalake.enabled = true`:

```text
examples/lake/paimon/
├── setup.sql             # Log Table and Primary Key Table, plus sample data
├── union-read.sql        # Batch Union Read and Streaming Union Read
└── lake-only-read.sql    # Read the Paimon layer through the $lake table
```

Run it with a clean environment when starting for the first time:

```bash
just build-tiering
just up paimon
just run-sql examples/lake/paimon/setup.sql
just run-sql examples/lake/paimon/union-read.sql
```

The direct table reads in `union-read.sql` combine the lake snapshot with the Fluss log. The batch
query terminates; uncomment the streaming section to keep consuming new records. After the
tiering job has committed a snapshot, run the lake-only example:

```bash
just run-sql examples/lake/paimon/lake-only-read.sql
```

The Paimon, Iceberg, and Lance `create-table.sql`, `write-data.sql`, and `query-lake.sql` files are
also available as standalone SQL files. They use the fixed table name `lake_table`, so they can be
run directly with `just run-sql`:

```bash
just run-sql examples/lake/paimon/create-table.sql
just run-sql examples/lake/paimon/write-data.sql
just run-sql examples/lake/paimon/query-lake.sql
```

Iceberg and Lance keep the shorter format-specific path; use Paimon when you want to explore the
full Log Table, Primary Key Table, Union Read, and Lake-only Read chain.

## Validate Lake Tiering

The validation workflow is intentionally split into SQL files that can be run and inspected one at
a time:

```bash
just build-tiering
just up paimon
just run-sql examples/lake/paimon/setup.sql
just run-sql examples/lake/paimon/union-read.sql
just tiering-status
just run-sql examples/lake/paimon/lake-only-read.sql
```

Use `paimon` for the complete Log Table, Primary Key Table, Union Read, and Lake-only Read
workflow. Use the standalone SQL files under `examples/lake/iceberg/` or `examples/lake/lance/` to
experiment with those formats.
Tiering is asynchronous, so repeat `lake-only-read.sql` if the first query runs before the lake
commit is visible.

The sample writes three rows. A successful Paimon lake query reports row count `3`, ID sum `6`,
three distinct payloads, and the payload range `alpha` to `gamma`. Run `just clean` before repeating
the fixed-name examples if the tables already exist.

Lance supports table creation, writing, and tiering, but the DevKit does not bundle a Flink SQL
reader for a Lance lake-only query. Inspect the generated objects in RustFS when validating Lance.

## Operations

```bash
just status
just logs
just logs tablet-server-0 200
just exec tablet-server-0 java -version
just run-sql ./my-query.sql
just down
just clean
```

Start a profile with `just up` before using `run-sql`. SQL files are read from the host and passed to
the Flink SQL Client. Relative paths are resolved from the current working directory.

You can write your own SQL file to experiment with Fluss instead of modifying the bundled examples:

```bash
cat > /tmp/my-fluss-query.sql <<'EOF'
USE CATALOG fluss_catalog;
SHOW TABLES;
EOF

just run-sql /tmp/my-fluss-query.sql
```

Your SQL file can contain Fluss DDL, batch or streaming queries, writes, lookups, and virtual-table
queries. The Flink SQL Client runs inside the Compose environment, so use container addresses such
as `coordinator-server:19123` when defining a Fluss catalog. Start the desired profile first; a
custom SQL file does not require any DevKit source change.

`just up` replaces containers from the previous profile but preserves named volumes. `down` removes
containers and keeps data; `clean` also removes Compose volumes and staged Server JARs. Downloaded
JARs remain in the ignored `.deps` cache.

Default endpoints:

| Service | Address |
|---|---|
| CoordinatorServer | `localhost:9123` |
| TabletServer 0 | `localhost:9124` |
| TabletServers 1 and 2 | `localhost:9125` and `localhost:9126` (three-node mode) |
| CoordinatorServer JDWP | `localhost:15005` |
| TabletServer JDWP | `localhost:15006` to `localhost:15008` |
| CoordinatorServer metrics | `http://localhost:9249/metrics` |
| TabletServer metrics | `http://localhost:9250/metrics` to `http://localhost:9252/metrics` |
| Flink UI | `http://localhost:8083` |
| RustFS S3 API | `http://localhost:9000` |
| RustFS Console | `http://localhost:9001` |

JDWP is enabled for every Fluss process with `suspend=n`, so a remote debugger can attach without
blocking startup. Prometheus export is also enabled by default and can be checked directly, for
example with `curl http://localhost:9249/metrics`.

The default Fluss runtime is `eclipse-temurin:17-jre-noble`; set `FLUSS_DEVKIT_IMAGE` to use
another Java 17 runtime. Flink defaults to `flink:1.20.3-scala_2.12-java17`, matching the local Flink
1.20 Connector and Tiering build baseline; set `FLUSS_DEVKIT_FLINK_IMAGE` to use another compatible
image. Port environment variables in the Compose files can override the default addresses, for
example `COORDINATOR_DEBUG_PORT=5005 just up` or
`TABLET_SERVER_0_METRICS_PORT=9300 just up`.

## Adding a Profile

Create a directory under `profiles/`. The directory contains one required file and up to four
optional files:

| File | Purpose |
|---|---|
| `server.yaml` | Fluss and lake configuration |
| `jars.urls` | JARs used by both Fluss Server and Flink |
| `server.urls` | Additional Fluss Server-only JARs |
| `flink.urls` | Additional Flink-only JARs |
| `compose.files` | Compose overlays, relative to `devkit/`, for extra local services |

Put one URL or Compose path on each line; empty lines and `#` comments are ignored. `just up`
applies the declared JARs and Compose overlays for every profile. Server JARs use a plugin directory
named after the profile, or after the lake format when `server.yaml` contains `datalake.format`. Lake
profiles also stage the matching locally built lake plugin and start Flink tiering. Server JARs are
added across profile switches and removed by `just clean`.

Start the new profile with `just up <profile>`. No `justfile` change is required.
