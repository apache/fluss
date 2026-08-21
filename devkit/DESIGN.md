<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to you under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# DevKit Design and Dependency Sources

This document records why the DevKit is assembled this way and where its configuration and
dependency choices come from. The DevKit runs a source build on one Docker host for contributor
validation; it is not another production deployment method.

## Scope and Sources

The DevKit combines existing Fluss workflows:

1. The core CoordinatorServer, TabletServer, ZooKeeper, listener, and shared-storage topology comes
   from the [Docker deployment guide](../website/docs/install-deploy/deploying-with-docker.md).
2. RustFS, PostgreSQL, and the Paimon and Iceberg examples come from the
   [Flink quickstart](../website/docs/quickstart/flink.md) and
   [Lakehouse quickstart](../website/docs/quickstart/lakehouse.md).
3. Lake plugin and Tiering Job dependencies come from
   [Deploying Streaming Lakehouse](../website/docs/install-deploy/deploying-streaming-lakehouse.md),
   the [data lake format guides](../website/docs/streaming-lakehouse/datalake-formats/), and the
   [Flink engine dependency guide](../website/docs/engine-flink/getting-started.md).

Unlike the quickstart images, the DevKit mounts the local `build-target` distribution and runs its
real `bin/` scripts and `plugins/<format>/` classloaders. This avoids rebuilding a Fluss image after
every source change while still exercising the assembled distribution.

The distributed and Helm guides remain authoritative for multiple hosts, persistent volumes,
production secrets, rolling upgrades, resource sizing, and external service deployment.

## Startup Flow

```text
just up <profile>
├─ ensure-dist
│  └─ require a local build-target distribution
├─ ensure-flink
│  └─ reset the active Flink directory and stage the local Fluss Connector
├─ stage-profile-jars
│  ├─ download the profile URL lists into .deps/cache
│  ├─ jars.urls   ──► Server plugin + Flink lib
│  ├─ server.urls ──► Server plugin
│  └─ flink.urls  ──► Flink lib
├─ when datalake.format is configured: ensure-tiering
│  ├─ stage the local lake connector
│  ├─ stage the local Tiering Job
│  └─ stage the Fluss filesystem plugin selected by remote.data.dir
├─ apply every compose.files overlay
└─ start Compose and wait for Fluss, Flink, and optional tiering readiness
```

Profile JAR and Compose processing is unconditional. Only lake connector and Tiering Job assembly
depends on `datalake.format`. A future non-lake profile can therefore add Faker, Kafka,
authentication, or another local service without entering the lake branch.

`ensure-dist` checks both Server launch scripts, the default configuration, and the library
directory. Checking only `coordinator-server.sh` can mistake a stale or partially assembled
`build-target` directory for a runnable distribution and defer the failure to Docker mount time.

## Classloader Boundaries

Server and Flink dependencies are deliberately installed into different locations:

```text
Fluss Server JVM
└─ build-target/plugins/<format-or-profile>
   ├─ local lake plugin from the distribution
   └─ jars.urls + server.urls

Flink JVM
└─ /opt/flink/lib
   ├─ local Fluss Flink Connector
   ├─ local lake connector for lake profiles
   ├─ local Fluss filesystem plugin selected by remote.data.dir
   └─ jars.urls + flink.urls

Flink job submission
└─ fluss-flink-tiering.jar
```

The Fluss Connector is always present because Flink SQL needs it to discover the `fluss` catalog
and table factories. Lake profile JARs do not replace the Connector; they add the format and storage
implementations needed by the Tiering Job and lake reads.

Downloaded Server JARs are prefixed with `devkit-` to identify files owned by this tool. Profile
switches only add or replace JARs with the same name; they do not delete previously staged Server
JARs. `just clean` removes Compose volumes and all `devkit-*` Server JARs. The Flink active directory
is rebuilt on every `just up`, so Flink does not retain dependencies from the previous profile.

## Runtime and Service Choices

| Choice | DevKit value | Reason and source |
|---|---|---|
| Fluss runtime | Eclipse Temurin Java 17 JRE | The local and distributed deployment guides require Java 11+ and recommend Java 17+. |
| Flink runtime | 1.20.3, Java 17 | Matches the Flink 1.20 Connector and Tiering module build baseline and the Engine Flink download example. Override with `FLUSS_DEVKIT_FLINK_IMAGE` when testing another compatible image. |
| ZooKeeper | 3.9.2 | Matches the Docker deployment and quickstart examples. |
| RustFS | `1.0.0-alpha.83` | Matches the Flink and Lakehouse quickstarts. It is an S3-compatible local fixture, not a cloud-storage certification environment. |
| MinIO client | `RELEASE.2025-08-13T08-35-41Z` | Pins the official multi-architecture `minio/mc` image instead of inheriting the quickstart's floating tag. |
| PostgreSQL | 17 | Matches the Iceberg JDBC Catalog quickstart. |
| KV snapshot interval | Disabled (`0s`) | Matches the Docker and Security quickstart examples. Current fixtures do not claim PK snapshot/restart coverage. |
| Tiering table polling | Product default (30 seconds) | There is no format-specific reason for Hudi or Lance to use a different discovery interval. Per-table `table.datalake.freshness` separately controls the maximum synchronization interval. |
| Lance task off-heap | 512 MiB | The Lance guide requires at least 512 MiB because Arrow uses direct/off-heap memory. |

The Compose files use three listener names:

- `INTERNAL` is used for Fluss server-to-server communication.
- `CLIENT` is advertised as `localhost` for clients running on the host.
- `DEVKIT` uses Docker DNS names and port 19123 for Flink running inside the Compose network.

The extra `DEVKIT` listener prevents a containerized Flink client from receiving an advertised
`localhost` address that would resolve back to the Flink container instead of the Fluss server.

## Profile Dependency Matrix

| Profile | Additional Server plugin JARs | Additional Flink JARs | Primary source |
|---|---|---|---|
| `core` | None | Local Fluss Flink Connector | [Flink Engine: Dependencies](../website/docs/engine-flink/getting-started.md#dependencies) |
| `paimon` | `paimon-s3` | Paimon Flink runtime, `paimon-s3`, Hadoop, Fluss S3 filesystem, local Paimon lake connector | [Paimon guide](../website/docs/streaming-lakehouse/datalake-formats/paimon.md) and [`prepare_build.sh`](../docker/quickstart-flink/prepare_build.sh) |
| `iceberg` | Iceberg AWS, AWS bundle, Failsafe, PostgreSQL JDBC | The same shared JARs, Iceberg Flink runtime, Hadoop, Fluss S3 filesystem, local Iceberg lake connector | [Iceberg guide](../website/docs/streaming-lakehouse/datalake-formats/iceberg.md) and [Lakehouse quickstart](../website/docs/quickstart/lakehouse.md) |
| `hudi` | Hudi Flink bundle, Hadoop, Flink core/table 1.20.1 | Hudi Flink bundle, Hadoop, Fluss S3 filesystem, local Hudi lake connector | [Hudi guide](../website/docs/streaming-lakehouse/datalake-formats/hudi.md) |
| `lance` | No external download; support is in the distribution | Fluss S3 filesystem and local Lance lake connector | [Lance guide](../website/docs/streaming-lakehouse/datalake-formats/lance.md) |

The Hudi Server plugin contains Flink 1.20.1 API/runtime JARs because the lake modules compile
against the version in `fluss-lake/pom.xml` and Hudi requires those classes in its isolated Server
plugin classloader. These JARs are not added to the Flink 1.20.3 container runtime.

The Iceberg profile includes Failsafe even though the short quickstart download list omits it. The
Iceberg format documentation lists it as one of the required S3 FileIO JARs.

## Intentional Coverage Limits

- `core` is a minimal Fluss + Flink development cluster. It does not bundle Faker or S3 support and
  is not an exact replacement for the Faker-based Flink quickstart.
- The bundled Iceberg profile selects the JDBC Catalog used by the Lakehouse quickstart. Hive,
  Glue, REST, Lakekeeper, Polaris, and Gravitino can be added as separate profiles rather than making
  the built-in profile ambiguous.
- Lance validation covers table creation, writes, and tiering. The DevKit does not bundle a Flink
  SQL Lance reader.
- Hudi lake-only validation uses a native Hudi Catalog because the Fluss `$lake` suffix currently
  exposes Paimon and Iceberg lake tables.
- The current SQL fixtures are append-only. A primary-key fixture and restart/recovery smoke test
  remain follow-up work.
- Security/SASL is documented by the Security quickstart but is not yet provided as a built-in
  profile.

## Deferred Work

The first version leaves the following work explicit instead of adding speculative abstractions:

- The download cache is keyed by JAR basename. A future change should either key by URL digest or
  reject different URLs that resolve to the same basename.
- The Hudi S3A configuration propagation fix changes production code and should carry its own
  regression test, independently of the DevKit smoke test.
- CI should eventually run a small core and lake profile smoke test so Compose-only changes do not
  decay silently.
- Primary-key, changelog, snapshot, and restart recovery coverage should be added before the DevKit
  is presented as validation for those capabilities.
