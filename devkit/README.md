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

## Profiles

| Profile | Services |
|---|---|
| `core` | ZooKeeper, Fluss, and Flink |
| `iceberg` | Core, RustFS, PostgreSQL JDBC Catalog, Flink, and Iceberg tiering |
| `paimon` | Core, RustFS, Flink, and Paimon tiering |
| `lance` | Core, RustFS, Flink, and Lance tiering |
| `hudi` | Core, RustFS, Flink, and Hudi tiering |

These profiles are selected local-development combinations, not copies of every documented
deployment. In particular, `core` does not include the Faker source and S3 dependencies bundled in
the Flink quickstart image. The lake profiles currently exercise append-only table workflows; they
do not yet cover primary-key snapshot and restart recovery.

Profiles start one TabletServer by default. Pass `3` as the second argument to start three:

```bash
just up core 3
just up paimon 3
```

## Validate Lake Tiering

The validation workflow is intentionally split into commands that can be run and inspected one at
a time:

```bash
just build-tiering
just up paimon

format=paimon
table="${format}_manual_$(date +%s)"

just create-table "$format" "$table"
just write-data "$format" "$table"
just tiering-status
just query-lake "$format" "$table"
```

Use `iceberg`, `paimon`, or `hudi` for a complete create, write, tier, and lake-query workflow.
Tiering is asynchronous, so repeat `query-lake` if the first query runs before the lake commit is
visible.

The sample writes three rows. A successful lake query reports row count `3`, ID sum `6`, three
distinct payloads, and the payload range `alpha` to `gamma`. Use a new table name for each run:
dropping a non-empty Fluss table does not remove its lake table.

Lance supports `create-table`, `write-data`, and tiering, but the DevKit does not bundle a Flink SQL
reader for `query-lake`. Inspect the generated objects in RustFS when validating Lance.

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
