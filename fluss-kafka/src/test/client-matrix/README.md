# Isolated Kafka producer compatibility matrix

This test facility runs real Kafka producers against the three-node Fluss test
cluster and reads their records through the native Fluss client. Each selected
client runs in a separate JVM whose classpath contains only a standalone probe,
one `kafka-clients` JAR and that release's runtime libraries. Prepared classpaths
also include a matching SLF4J Simple binding so client network-thread failures
appear in the log. The probe
checks `AppInfoParser.getVersion()` and records its code-source JAR, Java version,
classpath and results. No production dependency is replaced.

## Requirements and preparation

Use Java 11 for the reactor and child JVMs, Python 3.8+, curl and Maven. Initial
artifact preparation needs access to Maven Central; subsequent reactor runs can
be offline once the project's own dependencies have been cached.

The FIP target is every formal Java client release from **0.11.0.0 through 4.3.1**.
[Apache Kafka downloads](https://kafka.apache.org/community/downloads/) records
both boundary releases. The preparation script reads the actual formal versions
from [official Maven metadata](https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/maven-metadata.xml),
fetches each client POM and its runtime JARs, verifies Central SHA-1 sidecars, and
records local SHA-256 hashes. Test dependencies from the published POM are excluded.
It fails explicitly on an unfamiliar runtime dependency instead of guessing its
transitive dependency closure.

Kafka **2.6.0** needs one explicit exception: its generated ApiVersions messages
load Jackson `JsonNode`, although its published POM declares `jackson-databind`
as `provided`. This is recorded in
[KAFKA-10378](https://issues.apache.org/jira/browse/KAFKA-10378). The script supplies
the release's exact Jackson **2.10.2** databind, core and annotations JARs, all
checksum-verified. These and the diagnostic binding are listed separately under
`test_runtime_additions` in the preparation report. Other provided dependencies
and published test dependencies are not implicitly added. These additions affect
only the isolated test processes, not Fluss production dependencies.

Start with the boundaries and representative cached releases:

```sh
python3 fluss-kafka/src/test/client-matrix/prepare.py \
  --output /tmp/fluss-kafka-client-matrix \
  --versions 0.11.0.0,3.6.1,3.9.1,3.9.2,4.2.0,4.3.1

# Set JAVA_HOME to a Java 11 installation before running Maven.
./mvnw -o -pl fluss-kafka -am verify \
  -Dtest=KafkaClientMatrixITCase \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dkafka.client.matrix.file=/tmp/fluss-kafka-client-matrix/clients.properties
```

After the boundary run, prepare and execute the complete formal interval:

```sh
python3 fluss-kafka/src/test/client-matrix/prepare.py \
  --output /tmp/fluss-kafka-client-matrix --versions all

./mvnw -o -pl fluss-kafka -am verify \
  -Dtest=KafkaClientMatrixITCase \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dkafka.client.matrix.file=/tmp/fluss-kafka-client-matrix/clients.properties
```

Do not remove `-am`: server, client, RPC and test artifacts must come from this
worktree's reactor, not another branch's installed SNAPSHOTs. A normal build
without `kafka.client.matrix.file` still runs an isolated smoke test using the
module's actual declared client version; that single-version run is not a matrix.

## Coverage and result interpretation

For each selected version the probe uses PLAINTEXT and
`enable.idempotence=false`, negotiates APIs through the real producer, and tests
`acks=0`, `acks=1` and `acks=all`. The same producer alternates canonical and bare
names for the same table, checking acknowledged topic names and offsets. Native
readback verifies all 12 records, their real storage offsets, keys, last-header
wins semantics and null binary rescue fields. Bucket leaders are ready before
unacknowledged writes begin. Producer connections stay open until the parent
confirms native readback; a stdin handshake then permits the probe to close. This
avoids racing unacknowledged appends against connection-close cancellation.

Preparation is not execution. `preparation.json` lists every requested version as
prepared, unavailable or not executed, including exact artifacts and any errors.
The script deletes the previous executable manifest at startup, including when
metadata retrieval or parsing fails. It only publishes `clients.properties` when
all requested versions are prepared. If downloads are blocked, keep that failure
report and make an explicit, separately recorded subset request for the available
versions; never call the subset a full interval pass.

Runtime evidence lives under:

- `fluss-kafka/target/client-matrix-results/<version>/command.txt`
- `fluss-kafka/target/client-matrix-results/<version>/client.log`
- `fluss-kafka/target/client-matrix-results/<version>/result.txt`
- `fluss-kafka/target/surefire-reports/TEST-org.apache.fluss.kafka.KafkaClientMatrixITCase.xml`

Each invocation deletes the old result and log, writes `FAIL` with the error on
failure, and only writes `PASS` after successful native readback. A client failure
fails its parameterized test; other prepared versions still run. A missing JAR,
version mismatch, process timeout or unsupported JDK is a failure, not a skipped
compatibility result. Archive the preparation report, manifest, Maven log, runtime
logs and current Surefire XML together before `clean` or another matrix run.
The POM runs the selected tests in both Surefire phases; count distinct versions
once and require both executions to succeed.

This is a bounded Produce compatibility acceptance flow, not proof of every
operation in the FIP. Leadership, lifecycle, min-ISR, invalid-record atomicity and
null-value filtering run separately in the cumulative integration tests using the
module client. They are not yet repeated for every matrix version. SASL is reserved
for PR19. Idempotence, other client SDKs, pre-releases, versions outside the target
interval, and persisted per-row `_kafka_metadata.offset` are not covered here.
