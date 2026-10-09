# Fluss DevKit

DevKit runs end-to-end tests against the Fluss code in your current checkout. It builds local Fluss,
Flink, lake, and Gateway artifacts, then runs the selected test scenario with Docker Compose.

Use DevKit when you have changed Fluss code and want to exercise that change locally. For a first
Fluss tutorial, use the [Quickstart](../website/docs/quickstart/flink.md). For persistent or
production environments, use the [deployment documentation](../website/docs/install-deploy/).

## Get Started

You need:

- JDK 11 or later
- Bash, `curl`, and a Unix-like environment
- Docker with Docker Compose v2
- [just 1.58.0](https://github.com/casey/just)
- Network access to Maven Central and container registries on first use

Start by choosing the scenario closest to the code path you changed:

| Profile | Use it when you need to test | Starts |
|---|---|---|
| `core` | Fluss Server, Client, Flink Connector, or SQL behavior | ZooKeeper, Fluss, Flink |
| `gateway` | REST Gateway against a real Fluss cluster | ZooKeeper, Fluss, Gateway |
| `paimon` | Paimon tiering, Union Read, or Lake-only Read | ZooKeeper, Fluss, Flink, Tiering, RustFS |
| `iceberg` | Iceberg tiering with a JDBC catalog | ZooKeeper, Fluss, Flink, Tiering, RustFS, PostgreSQL |
| `lance` | Lance writes or tiering | ZooKeeper, Fluss, Flink, Tiering, RustFS |

Run commands from `devkit/`. Build the selected profile, then start it:

```bash
cd devkit
just build core
just up core
```

`core` is the default, so `just build` and `just up` use that profile. For another scenario, pass
its name to both commands:

```bash
just build gateway
just up gateway
```

`just build <profile>` compiles the artifacts and downloads the external JARs needed by that
profile. `just up <profile>` checks the prepared artifacts, starts the runtime, waits for Compose
healthchecks, and runs the profile checks. Startup does not run Maven or Cargo, download
dependencies, or detect source changes.

## Validate Your Change

### Core and Flink

After starting the `core` profile, create the sample catalog and tables:

```bash
just run-sql examples/core/01-setup.sql
```

Then choose the example that covers your change:

| Example | Covers |
|---|---|
| `examples/core/02-scan.sql` | Bounded batch scans of Log and Primary Key Tables |
| `examples/core/02-streaming-scan.sql` | Long-running streaming scan of a Log Table |
| `examples/core/03-lookup-join.sql` | Point and prefix lookup joins |
| `examples/core/04-changelog-binlog.sql` | Changelog and binlog virtual tables |

```bash
just run-sql examples/core/02-scan.sql
# Run separately; this command intentionally remains running.
just run-sql examples/core/02-streaming-scan.sql
```

The scan example runs in Flink batch mode. It reads the current bounded contents
of both tables: the Log Table starts from `earliest`, while the Primary Key Table
uses `full` to read its current materialized state.

The streaming scan keeps a Flink Job running and receives records appended after startup. Press
Ctrl-C when you are done, then use the [Flink UI](http://localhost:8083) to confirm that the Job has
stopped and cancel it if it is still running. Canceling a Job keeps the DevKit services and data;
`just down` stops the whole profile, while `just clean` also removes its test data.

The Lookup Join example prints the joined rows to the TaskManager logs:

```bash
just run-sql examples/core/03-lookup-join.sql
just logs taskmanager 100
```

Look for `POINT_LOOKUP` rows mapping orders `1000`, `1001`, and `1002` to Alice, Bob, and Carol.
The submitted lookup query is also a streaming Job; cancel it in the Flink UI after inspecting the
results. To try the prefix lookup, follow the comments in the SQL file and run that section alone.

You can also run your own SQL file:

```bash
just run-sql /absolute/path/test.sql
```

SQL runs inside the Flink container. Use Compose hostnames such as
`coordinator-server:19123` in Fluss catalog definitions.

### Gateway

After changing Gateway or its Fluss integration, build and start the `gateway` profile:

```bash
just build gateway
just up gateway
```

Create a Log Table in the default `fluss` database:

```bash
curl -fsS -X POST \
  -H 'Content-Type: application/json' \
  http://localhost:8080/v1/clusters/default/databases/fluss/tables \
  -d '{
    "table_name": "gateway_events",
    "columns": [
      {"name": "event_id", "data_type": {"type": "BIGINT"}, "nullable": false},
      {"name": "message", "data_type": {"type": "STRING"}, "nullable": false}
    ],
    "distribution": {"bucket_count": 1, "bucket_keys": []}
  }'
```

Describe the table:

```bash
curl -fsS \
  http://localhost:8080/v1/clusters/default/databases/fluss/tables/gateway_events
```

Append a record whose fields match the table schema:

```bash
curl -fsS -X POST \
  -H 'Content-Type: application/json' \
  http://localhost:8080/v1/clusters/default/databases/fluss/tables/gateway_events/records \
  -d '{
    "entries": [
      {"id": "event-1", "append": {"event_id": "1", "message": "hello from Gateway"}}
    ]
  }'
```

### Lake Tiering

Use Paimon for the smallest complete tiering workflow:

```bash
just build paimon
just up paimon
just run-sql examples/lake/paimon/setup.sql
just run-sql examples/lake/paimon/union-read.sql
just run-sql examples/lake/paimon/lake-only-read.sql
```

If the lake-only query is empty, wait a few seconds and run it again until it returns Alice and Bob.
Tiering is asynchronous. `just tiering-status` is only a diagnostic check that the
job is running; the lake-only query itself must return the sample data before the
workflow is considered successful.
Iceberg examples are under `examples/lake/iceberg/`. Lance examples are under
`examples/lake/lance/`; Lance has no Flink SQL Lake-only reader. The CI smoke opens the dataset
through RustFS's S3 API with the same Python example shown below. The Lance example uses an
append-only `embedding_events` Log Table:

```bash
just build lance
just up lance
just run-sql examples/lake/lance/create-table.sql
just run-sql examples/lake/lance/write-data.sql
python -m pip install 'pylance==0.33.0'
python examples/lake/lance/read-data.py
```

The Python example waits for asynchronous Tiering, opens the resulting dataset with the Lance
client, prints the three embedding events, and verifies their IDs and embedding type. Run the two
Python commands in a virtual environment if you do not want to install `pylance` globally. The
RustFS console is only a troubleshooting aid.

The examples use fixed table names. Run `just clean` before repeating a workflow if existing tables
or lake data would conflict.

## Iterate and Inspect

As you continue working, use the command that matches the next step:

| When you need to | Use |
|---|---|
| Recompile local code or refresh profile dependencies | `just build <profile>` |
| Restart with the prepared artifacts | `just up <profile>` |
| Test three TabletServers instead of one | `just up <profile> 3` |
| Check the active services | `just status` |
| Read logs | `just logs [service] [lines]` |
| Run a command in a container | `just exec <service> <command>` |
| Check the active Lake Tiering Job | `just tiering-status` |
| Re-submit Tiering after a JobManager restart | `just tiering-restart` |
| Stop containers but keep data | `just down` |
| Remove containers and test data | `just clean` |

For example:

```bash
just logs tablet-server-0 200
just exec tablet-server-0 java -version
```

After changing source code, run `just build <profile>` and `just up <profile>` again. Running
containers are not hot-reloaded. If the JobManager is restarted, run `just tiering-restart` to
submit the active profile's Tiering Job again. Restarting the same profile preserves named volumes; switching
to another profile requires `just clean` first so lake metadata and objects from different formats
cannot be mixed. `just down` keeps the data and active-profile selection, so the inspection and
cleanup commands continue to use the same Compose configuration.

`just clean` removes containers, volumes, staged Server JARs, and the active-profile selection.
Downloaded dependencies remain in the ignored `devkit/.deps` cache.

Run `just --list` for all commands. Fluss metrics and JDWP ports are defined in
`docker-compose.yml` and can be overridden with their environment variables.

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

## Profile Reference

A profile is a complete, repeatable local test scenario under `profiles/<profile>/`. It keeps the
scenario's server configuration, dependencies, and supporting services together. Both
`just build <profile>` and `just up <profile>` use its name; a profile names a scenario, not an
individual process or build step.

`runtime.targets` is the profile's only declaration of DevKit-managed runtime groups. Each target
selects the artifacts to build, Compose services to start, and readiness checks to run:

| Target | Meaning | Required targets |
|---|---|---|
| `core` | ZooKeeper, CoordinatorServer, and TabletServer | None |
| `flink` | Flink JobManager and TaskManager | `core` |
| `tiering` | The format-specific Fluss Lake Tiering Job | `core`, `flink` |
| `gateway` | The Fluss Gateway process | `core` |

Every profile must include `core`. Target order has no meaning; unknown targets, duplicates, and
missing required targets are rejected. Blank lines and `#` comments are ignored.

`core` or `flink` builds the Fluss distribution and Flink Connector. `tiering` adds the Tiering Job
and lake plugin. `gateway` builds the Gateway executable. Supporting services outside these groups,
such as RustFS or PostgreSQL, come from the Compose overlays listed in `compose.files`.
