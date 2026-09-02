# Fluss DevKit

DevKit is a contributor tool for running end-to-end tests against code in the current checkout. It
builds the local Fluss distribution, connectors, plugins, or Gateway executable and mounts those
artifacts into Docker containers. You do not need to build a Fluss development image.

If you want to learn Fluss without changing its code, start with the
[Quickstart](../website/docs/quickstart/flink.md), which uses published images and bundles the
dependencies needed by the tutorial. DevKit is intended for the next step: rebuild the current
checkout, start only the components needed for a test, and exercise those local changes end to end.
It is not a deployment tool; its profiles use local data, development ports, and sample
dependencies. Use the [deployment documentation](../website/docs/install-deploy/) for persistent or
production environments.

## Getting Started

### Requirements

- JDK 11 or later
- A Unix-like environment with Bash and `curl`
- Docker with Docker Compose v2
- [just](https://github.com/casey/just)
- Network access to Maven Central and container registries on first use

A profile represents one local test scenario and starts only the runtime components that scenario
needs.

| Profile | Use it to test | Components started |
|---|---|---|
| `core` | Fluss Server, Flink Connector, SQL reads and writes | ZooKeeper, Fluss, Flink |
| `gateway` | REST Gateway against a real Fluss cluster | ZooKeeper, Fluss, Gateway |
| `paimon` | Paimon tiering, Union Read, and Lake-only Read | ZooKeeper, Fluss, Flink, Tiering, RustFS |
| `iceberg` | Iceberg tiering with a JDBC catalog | ZooKeeper, Fluss, Flink, Tiering, RustFS, PostgreSQL |
| `lance` | Lance writes and tiering | ZooKeeper, Fluss, Flink, Tiering, RustFS |

Profiles start one TabletServer by default. Pass `3` to test a three-TabletServer topology:

```bash
just up core 3
just up gateway 3
```

### Build and Start

Run commands from `devkit/`:

```bash
cd devkit
just build core
just up core
```

`core` is the default, so `just build` and `just up` are equivalent. To use another scenario:

```bash
just build gateway
just up gateway
```

`just build <profile>` compiles local code and downloads the external JARs declared by the profile.
It refreshes only the assembled Fluss distribution, while other Maven modules remain incremental.
`just up <profile>` checks those prepared artifacts, starts the declared runtime components, and
waits for them to become ready. It does not run Maven or Cargo, download profile JARs, or detect
whether source code is newer than an artifact. Docker may still pull a missing image on first start.

## Testing Local Changes

### Core and Flink Connector

Build and start the Core scenario, then create the sample catalog and tables:

```bash
just build core
just up core
just run-sql examples/core/01-setup.sql
```

Run the example that exercises the code you changed:

| Example | Covers |
|---|---|
| `examples/core/02-scan.sql` | Batch and streaming scans of Log and Primary Key Tables |
| `examples/core/03-lookup-join.sql` | Point and prefix lookup joins |
| `examples/core/04-changelog-binlog.sql` | Changelog and binlog virtual tables |

For example:

```bash
just run-sql examples/core/02-scan.sql
```

The setup uses fixed `VALUES` data and requires no Faker connector. You can also run any SQL file
from the host:

```bash
just run-sql /absolute/path/my-test.sql
```

The SQL Client runs inside the Flink container. Use container addresses such as
`coordinator-server:19123` when defining a Fluss catalog.

### Gateway

Build and start Fluss with the locally compiled Gateway executable:

```bash
just build gateway
just up gateway
```

Check the process first, then make a request that reaches Fluss:

```bash
curl -fsS http://localhost:8080/ready
curl -fsS http://localhost:8080/v1/clusters/default/databases
```

`/ready` only verifies that Gateway accepts requests. The databases request verifies the complete
Gateway → `fluss-rust` → Fluss path.

The Gateway executable is built in the pinned Rust environment and mounted read-only into a
Bookworm runtime container. The standard
[`fluss-gateway/conf/gateway.yaml`](../fluss-gateway/conf/gateway.yaml) is mounted and loaded by
the Gateway process.

### Lake Tiering

Paimon provides the most complete example:

```bash
just build paimon
just up paimon
just run-sql examples/lake/paimon/setup.sql
just run-sql examples/lake/paimon/union-read.sql
just tiering-status
just run-sql examples/lake/paimon/lake-only-read.sql
```

Tiering is asynchronous, so the first Lake-only query may run before a snapshot is committed. Once
the snapshot is available, `lake-only-read.sql` returns the two profiles for Alice and Bob that were
inserted by `setup.sql`.

Iceberg provides focused `create-table.sql`, `write-data.sql`, and `query-lake.sql` files under
`examples/lake/iceberg/`. Lance provides `create-table.sql` and `write-data.sql`; it does not include
a Flink SQL reader for Lake-only queries, so inspect its objects in RustFS instead.

The examples use fixed table names. Run `just clean` before repeating a workflow when existing
tables or lake data would conflict.

### Rebuild and Retest

Choose the smallest profile that exercises the behavior you changed:

| Change | Rebuild | Restart | Verify |
|---|---|---|---|
| Fluss Server or Client | `just build <profile>` | `just up <profile>` | Relevant SQL or Gateway request |
| Flink Connector | `just build core` | `just up core` | A file under `examples/core/` |
| Gateway Rust code | `just build gateway` | `just up gateway` | A REST request that reaches Fluss |
| Lake plugin or Tiering Job | `just build <lake-profile>` | `just up <lake-profile>` | Lake SQL and `just tiering-status` |
| `server.yaml` | No | `just up <profile>` | Relevant endpoint or SQL |
| SQL only | No | No | `just run-sql <file>` |

The runtime never loads Java or Rust source directly:

- Fluss Server uses the local `build-target` distribution mounted at `/opt/fluss`.
- Flink receives the locally built Connector and Lake JARs during container startup.
- Gateway uses `devkit/.deps/gateway/debug/fluss-gateway` as a read-only executable mount.
- The Tiering Job is submitted from the locally built JAR after Flink becomes ready.

Rebuilding does not hot-reload running processes. Run `just up <profile>` again after every source
build.

## Tailoring a Profile

If an existing profile is close but not sufficient, change the files under `profiles/<profile>/`.
There is no need to add a new `just` command.

| File | Purpose |
|---|---|
| `runtime.targets` | Components started and checked by `just up`: `core`, `flink`, `gateway`, or `tiering` |
| `server.yaml` | Fluss Server and Lake Tiering configuration |
| `jars.urls` | Additional JARs shared by Fluss Server and Flink |
| `server.urls` | Additional Fluss Server-only JARs |
| `flink.urls` | Additional Flink-only JARs |
| `compose.files` | Compose overlays for supporting containers such as RustFS or PostgreSQL |

The target file contains whitespace-separated names. URL and Compose files contain one entry per line.
Blank lines and `#` comments are ignored.

`just build` derives the required build steps from `runtime.targets`: `tiering` includes the Core
and Flink artifacts, while `gateway` adds the Gateway executable. Add `gateway` to a copied Lake
profile when the scenario also needs the Gateway executable.
For Paimon and Iceberg URLs, use the versions managed by `paimon.version` and `iceberg.version` in
the root `pom.xml`; the DevKit smoke workflow checks this alignment before building.

To create a reusable Lake Tiering scenario with Gateway, copy the closest profile:

```bash
cp -R profiles/paimon profiles/paimon-gateway
```

Then add Gateway to the copied profile's runtime targets:

```text
# profiles/paimon-gateway/runtime.targets
core flink tiering gateway
```

Build and start the new scenario like any other profile:

```bash
just build paimon-gateway
just up paimon-gateway
```

The `flink` and `gateway` runtime targets load their built-in Compose overlays automatically, so
`compose.files` needs no Gateway entry. The copied Paimon files continue to provide RustFS and the
Lake configuration.

To add a different supporting container, define it in a Compose overlay and list that overlay in
the profile's `compose.files`. Put the component's Fluss or Lake settings in the same profile's
`server.yaml`. DevKit records the selected profile when `just up` runs, so subsequent lifecycle
commands use the same overlays.

Gateway always mounts the standard `fluss-gateway/conf/gateway.yaml`. Edit that file and restart the
profile to test Gateway configuration changes. Docker overrides these three values so the process is
reachable inside the Compose network:

- `gateway.rest.listen=0.0.0.0:8080`
- `gateway.metrics.exporter.prometheus.listen=0.0.0.0:9095`
- `gateway.cluster.default.bootstrap.servers=coordinator-server:19123`

Host ports remain configurable per invocation, for example:

```bash
GATEWAY_REST_PORT=18080 GATEWAY_METRICS_PORT=19095 just up gateway
```

## Reference

### Operations

```bash
just status
just logs
just logs tablet-server-0 200
just exec tablet-server-0 java -version
just tiering-status
just down
just clean
```

`just up` replaces containers from the previous profile, reconciles the current profile's staged
Server JARs, and preserves named volumes. `just down` removes containers while retaining both data
and the selected profile, so `status`, `logs`, `exec`, and `clean` keep using the same Compose
overlays. `just clean` also removes Compose volumes, all staged Server JARs, and the selected profile.
Downloaded dependencies remain in the ignored `.deps` cache.

`just tiering-status` succeeds only when the active Lake profile's format-specific Tiering Job is
running.

### Default Endpoints

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

Run `just --list` for the complete command list. Fluss metrics and JDWP ports are defined in
`docker-compose.yml` and can be overridden with the corresponding environment variables.
