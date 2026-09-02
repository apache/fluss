# Fluss DevKit

DevKit is a local development tool for running end-to-end tests against the Fluss code in the
current checkout. It assembles local Fluss, Flink, lake, and Gateway artifacts and runs the selected
test scenario with Docker Compose.

DevKit is for local testing, not deployment. For a first Fluss tutorial use the
[Quickstart](../website/docs/quickstart/flink.md). For persistent or production environments use
the [deployment documentation](../website/docs/install-deploy/).

## Profile Model

A **profile** is a complete, repeatable local test scenario under `profiles/<profile>/`. It defines
the server configuration, optional dependency downloads, and supporting Compose files needed for
that scenario. `just build <profile>` and `just up <profile>` both use the profile name as their
single entry point. A profile names a scenario, not an individual component or build step.

`runtime.targets` is the profile's only declaration of DevKit-managed runtime groups. Each target
selects the artifacts to build, Compose services to start, and readiness checks to run. The file is
a whitespace-separated set of target names; order has no meaning, and blank lines and `#` comments
are ignored.

| Target | Meaning | Required dependencies |
|---|---|---|
| `core` | ZooKeeper, CoordinatorServer, and TabletServer | None |
| `flink` | Flink JobManager and TaskManager for SQL or connector tests | `core` |
| `tiering` | The format-specific Fluss Lake Tiering Job | `core`, `flink` |
| `gateway` | The Fluss Gateway process | `core` |

Every profile must include `core`. Targets must be known, their required targets must be present,
and targets must not be repeated. The built-in profiles are:

| Profile | Scenario | Runtime targets |
|---|---|---|
| `core` | Fluss Server and Flink Connector SQL tests | `core flink` |
| `gateway` | REST Gateway against a Fluss cluster | `core gateway` |
| `paimon` | Paimon tiering, Union Read, and Lake-only Read | `core flink tiering` |
| `iceberg` | Iceberg tiering with a JDBC catalog | `core flink tiering` |
| `lance` | Lance writes and tiering | `core flink tiering` |

`core` or `flink` builds the Fluss distribution and Flink connector; `tiering` adds the Tiering Job
and lake plugin; `gateway` builds the Gateway executable. Compose overlays for these targets are
selected automatically. Supporting services outside these built-in groups, such as RustFS or
PostgreSQL, come from the profile-specific overlays listed in `compose.files`.

The other profile files have narrow responsibilities:

| File | Purpose |
|---|---|
| `server.yaml` | Fluss Server and Lake Tiering configuration |
| `jars.urls` | JARs shared by Fluss Server and Flink |
| `server.urls` | Fluss Server-only JARs |
| `flink.urls` | Flink-only JARs |
| `compose.files` | Additional Compose overlays |

## Quick Start

Requirements:

- JDK 11 or later
- Bash, `curl`, and a Unix-like environment
- Docker with Docker Compose v2
- [just](https://github.com/casey/just)
- Network access to Maven Central and container registries on first use

Run commands from `devkit/`:

```bash
cd devkit
just build core
just up core
```

`core` is the default, so `just build` and `just up` use that profile. Pass `3` to run three
TabletServers instead of one:

```bash
just up core 3
```

`just build <profile>` compiles the local artifacts and downloads the profile's external JARs.
`just up <profile>` only checks those artifacts, starts the profile, waits for Compose healthchecks,
and runs the profile checks. It does not run Maven or Cargo, download dependencies, or detect newer
source files. Run `just up` again after rebuilding; running containers are not hot-reloaded.

## Workflows

### Core and Flink

Create the sample catalog and tables, then run an example:

```bash
just run-sql examples/core/01-setup.sql
just run-sql examples/core/02-scan.sql
```

The remaining examples cover lookup joins and changelog/binlog virtual tables:

| Example | Covers |
|---|---|
| `examples/core/03-lookup-join.sql` | Point and prefix lookup joins |
| `examples/core/04-changelog-binlog.sql` | Changelog and binlog virtual tables |

SQL runs inside the Flink container. Use Compose hostnames such as
`coordinator-server:19123` in Fluss catalog definitions. Any local SQL file can be run with
`just run-sql /absolute/path/test.sql`.

### Gateway

```bash
just build gateway
just up gateway
curl -fsS http://localhost:8080/ready
curl -fsS http://localhost:8080/v1/clusters/default/databases
```

The readiness endpoint checks that Gateway accepts requests. The databases request exercises the
complete Gateway to Fluss path. The executable is built in the pinned Rust container and mounted
read-only; the standard `fluss-gateway/conf/gateway.yaml` is used by the process.

### Lake Tiering

Paimon is the smallest complete tiering workflow:

```bash
just build paimon
just up paimon
just run-sql examples/lake/paimon/setup.sql
just run-sql examples/lake/paimon/union-read.sql
just tiering-status
just run-sql examples/lake/paimon/lake-only-read.sql
```

Tiering is asynchronous, so wait for `just tiering-status` before the Lake-only query. Iceberg and
Lance examples are under `examples/lake/iceberg/` and `examples/lake/lance/`. Lance has no Flink
SQL Lake-only reader; inspect its objects in RustFS instead.

All examples use fixed table names. Run `just clean` before repeating a workflow if existing tables
or lake data would conflict.

## Operations

```bash
just status
just logs
just logs tablet-server-0 200
just exec tablet-server-0 java -version
just tiering-status
just down
just clean
```

`just up` replaces containers from the previous profile, stages the current profile's JARs, and
preserves named volumes. `just down` removes containers but keeps data and the selected profile, so
the lifecycle commands continue to use the same Compose configuration. `just clean` removes
containers, volumes, staged Server JARs, and the selected profile. Downloaded dependencies remain in
the ignored `devkit/.deps` cache.

## Default Endpoints

| Runtime | Address |
|---|---|
| CoordinatorServer | `localhost:9123` |
| TabletServer 0 | `localhost:9124` |
| TabletServers 1 and 2 | `localhost:9125`, `localhost:9126` |
| Flink UI | `http://localhost:8083` |
| Gateway REST | `http://localhost:8080` |
| Gateway metrics | `http://localhost:9095/metrics` |
| RustFS S3 API | `http://localhost:9000` |
| RustFS Console | `http://localhost:9001` |

Run `just --list` for all commands. Fluss metrics and JDWP ports are defined in
`docker-compose.yml` and can be overridden with their environment variables.
