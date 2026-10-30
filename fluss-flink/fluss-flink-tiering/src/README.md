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

# Fluss Flink Tiering

This module provides the infrastructure for tiering Fluss data to lake formats (e.g., Apache Paimon),
consisting of FlussLakeTiering which encapsulates the core configuration and job graph logic,
and FlussLakeTieringEntrypoint which serves as the official Flink job main class and entrypoint.

The reason for extracting it as a separate module is that: When executing the Flink jar job, a jar must be specified.
If a `fluss-flink.jar` is specified, it may cause various classloader issues, as there are also `fluss-flink.jar`
in flink/lib and User Classloader, which will cause classes conflicts.

## Running with an embedded Flink runtime

Besides submitting the tiering job to a Flink cluster with `flink run`, the same tiering job can run
on an embedded, in-process Flink runtime inside a single JVM process, so no separate Flink cluster
needs to be provisioned. The entrypoint for this mode is `EmbeddedFlussLakeTiering`:

```shell
java -cp "$FLINK_HOME/lib/*" \
    org.apache.fluss.flink.tiering.EmbeddedFlussLakeTiering \
    --config.file /path/to/tiering.yaml
```

The tiering job is unbounded and completely stateless: progress is persisted through lake snapshots
tracked by Fluss, so no Flink checkpoint storage is needed. On a terminal job failure the process
exits with a non-zero code, and a container runtime or process supervisor simply starts it again.

Configuration is read from a YAML config file, using the same key-value syntax as Fluss's
`server.yaml`, and from `--key value` command line parameters, which take precedence. Parameters
carrying the tiering prefixes configure the tiering service exactly like the parameters of
`FlussLakeTieringEntrypoint`; all remaining parameters configure the embedded Flink runtime:

```yaml
# tiering service parameters
fluss.bootstrap.servers: fluss:9123
datalake.format: paimon
datalake.paimon.metastore: filesystem
datalake.paimon.warehouse: /tmp/paimon
fluss.tiering.poll.table.interval: 30 s

# embedded Flink runtime parameters
parallelism.default: 4
taskmanager.numberOfTaskSlots: 4
pipeline.name: fluss-lake-tiering
```

A value containing `#` is truncated at the comment marker (inherited `server.yaml` parser
behavior), so avoid `#` in configuration values.

The parallelism must stay within the number of slots the embedded runtime offers, so configure
`taskmanager.numberOfTaskSlots` alongside `parallelism.default`; startup fails fast when it does
not. If `parallelism.default` is unset, the embedded runtime runs the tiering source at the
machine's CPU core count (Flink's default local parallelism). When `taskmanager.numberOfTaskSlots`
is pinned below that, the service fails fast at startup. Set both `parallelism.default` and
`taskmanager.numberOfTaskSlots` explicitly to control the parallelism and slot budget.

### Docker image

`docker/fluss-tiering` packages this embedded mode into a self-contained image: the official Flink
distribution carries the runtime, and the Fluss tiering jars plus the lake format bundles live in
its `lib` directory. Build and smoke-test it with:

```shell
# Build the Fluss jars first: the tiering module plus the Flink 1.20 connector and the
# lake format artifacts that prepare_build.sh resolves from the local Maven repository.
./mvnw clean install -DskipTests \
    -pl fluss-flink/fluss-flink-tiering,fluss-flink/fluss-flink-1.20,fluss-lake/fluss-lake-paimon,fluss-lake/fluss-lake-iceberg,fluss-lake/fluss-lake-lance \
    -am
docker/fluss-tiering/build.sh
RUN_SMOKE=true docker/fluss-tiering/build.sh
```

The image bundles the Paimon, Iceberg, and Lance lake plugins with their runtime bundles; for other
formats or extra filesystem jars, extend the image by adding jars to `/opt/flink/lib`. The bundled
Hadoop `flink-shaded-hadoop-2-uber` jar suits Paimon but must be swapped for `hadoop-client-api`
and `hadoop-client-runtime` for Iceberg with the Glue or Hadoop catalog (see the Glue catalog doc
at `website/docs/streaming-lakehouse/datalake-catalogs/glue.md`). Configuration
is provided by mounting a file at `/opt/fluss/conf/tiering.yaml` (or setting `TIERING_CONFIG_FILE`),
or by passing `key: value` pairs through the `FLUSS_TIERING_PROPERTIES` environment variable;
values may reference environment variables as `${VAR}`. A `${VAR}` reference to a variable that
is not set aborts startup with exit code 1 and an error naming the variable.

Unless `JAVA_TOOL_OPTIONS` is set, the image starts the JVM with
`-XX:MaxRAMPercentage=60.0 -XX:MaxDirectMemorySize=512m`, sizing the heap from the container memory
limit; setting `JAVA_TOOL_OPTIONS` replaces these defaults entirely, so an override must configure
both the heap and the direct memory.

### Helm chart

The `helm` chart can deploy the tiering service alongside the Fluss cluster as a stateless
Deployment. It is disabled by default and uses the image above:

```shell
helm install fluss helm/ \
    --set tiering.enabled=true \
    --set tiering.image.repository=<registry>/fluss-tiering \
    --set tiering.image.tag=<tag>
```

By default the service tiers this release's own cluster and reads the lake catalog configuration
from `tiering.configurationOverrides`. Set `tiering.configurationOverrides."fluss.bootstrap.servers"`
to tier for a Fluss cluster outside the release. The chart validates that `datalake.format` is set
when the service is enabled.
