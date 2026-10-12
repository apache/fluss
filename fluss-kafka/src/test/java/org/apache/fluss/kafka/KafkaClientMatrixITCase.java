/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.kafka.matrix.KafkaClientProbe;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs actual, version-checked Kafka clients in isolated JVMs and verifies native Fluss storage.
 */
class KafkaClientMatrixITCase {
    private static final String DATABASE = "client_matrix";
    private static final Path OUTPUT =
            Paths.get(System.getProperty("project.basedir"), "target", "client-matrix-results");

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(clusterConfig())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    static Stream<String> clients() throws Exception {
        return installations().stringPropertyNames().stream().sorted();
    }

    private static Properties installations() throws Exception {
        Properties properties = new Properties();
        String manifest = System.getProperty("kafka.client.matrix.file");
        if (manifest != null) {
            try (InputStream input = Files.newInputStream(Paths.get(manifest))) {
                properties.load(input);
            }
            assertThat(properties).as("The requested matrix must not be empty").isNotEmpty();
        } else {
            // Normal builds still exercise isolation. Other versions require an explicit manifest.
            String classpath =
                    Arrays.stream(
                                    System.getProperty("java.class.path")
                                            .split(Pattern.quote(File.pathSeparator)))
                            .filter(
                                    entry ->
                                            Paths.get(entry)
                                                    .getFileName()
                                                    .toString()
                                                    .matches(
                                                            "(kafka-clients|slf4j-api|zstd-jni|snappy-java|lz4|lz4-java)-.+\\.jar"))
                            .collect(Collectors.joining(File.pathSeparator));
            properties.setProperty(AppInfoParser.getVersion(), classpath);
        }
        return properties;
    }

    @ParameterizedTest(name = "Kafka client {0}")
    @MethodSource("clients")
    void testIsolatedClientWithAllAckModesAndBothNames(String version) throws Exception {
        Path output = OUTPUT.resolve(version);
        Files.createDirectories(output);
        Files.deleteIfExists(output.resolve("result.txt"));
        Files.deleteIfExists(output.resolve("client.log"));
        Files.deleteIfExists(output.resolve("command.txt"));
        try {
            runClient(version, output);
        } catch (Exception | AssertionError failure) {
            Files.write(
                    output.resolve("result.txt"),
                    Arrays.asList("FAIL " + version, failure.toString()),
                    StandardCharsets.UTF_8);
            throw failure;
        }
    }

    private static void runClient(String version, Path output) throws Exception {
        String classpath = installations().getProperty(version);
        List<String> jars = Arrays.asList(classpath.split(Pattern.quote(File.pathSeparator)));
        assertThat(jars).allSatisfy(jar -> assertThat(Paths.get(jar)).isRegularFile());
        assertThat(
                        jars.stream()
                                .filter(
                                        jar ->
                                                Paths.get(jar)
                                                        .getFileName()
                                                        .toString()
                                                        .startsWith("kafka-clients-"))
                                .collect(Collectors.toList()))
                .singleElement()
                .satisfies(
                        jar ->
                                assertThat(Paths.get(jar).getFileName().toString())
                                        .isEqualTo("kafka-clients-" + version + ".jar"));
        // Export just the probe bytecode, never the server/test classpath, to the child JVM.
        String resource = KafkaClientProbe.class.getName().replace('.', '/') + ".class";
        Path probe = output.resolve("probe").resolve(resource);
        Files.createDirectories(probe.getParent());
        try (InputStream input =
                KafkaClientProbe.class.getClassLoader().getResourceAsStream(resource)) {
            assertThat(input).isNotNull();
            Files.copy(input, probe, StandardCopyOption.REPLACE_EXISTING);
        }
        String prefix = "client_" + version.replace('.', '_');
        List<TablePath> paths = new ArrayList<>();
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            try {
                admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
                for (String acks : Arrays.asList("0", "1", "all")) {
                    TablePath path = TablePath.of(DATABASE, prefix + "_" + acks);
                    paths.add(path);
                    admin.createTable(path, descriptor(), false).get();
                    CLUSTER.waitAndGetLeaderReplica(
                            new TableBucket(admin.getTableInfo(path).get().getTableId(), 0));
                }
                ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
                List<String> command =
                        Arrays.asList(
                                Paths.get(System.getProperty("java.home"), "bin", "java")
                                        .toString(),
                                "-cp",
                                output.resolve("probe") + File.pathSeparator + classpath,
                                KafkaClientProbe.class.getName(),
                                version,
                                node.host() + ":" + node.port(),
                                DATABASE,
                                prefix);
                Files.write(output.resolve("command.txt"), command, StandardCharsets.UTF_8);
                Path log = output.resolve("client.log");
                Process process =
                        new ProcessBuilder(command)
                                .redirectErrorStream(true)
                                .redirectOutput(log.toFile())
                                .start();
                try {
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
                    String text = "";
                    while (process.isAlive() && System.nanoTime() < deadline) {
                        text = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
                        if (text.contains("WRITES_COMPLETE")) {
                            break;
                        }
                        process.waitFor(100, TimeUnit.MILLISECONDS);
                    }
                    text = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
                    assertThat(text)
                            .as("Client %s log: %s", version, log)
                            .contains(
                                    "VERSION=" + version,
                                    "JAR=",
                                    "JAVA=",
                                    "ACKS=0:SENT",
                                    "ACKS=1:SENT",
                                    "ACKS=all:SENT",
                                    "WRITES_COMPLETE");
                    assertThat(process.isAlive())
                            .as("Client must stay connected during native readback")
                            .isTrue();
                    for (String acks : Arrays.asList("0", "1", "all")) {
                        assertNativeReadback(
                                connection,
                                TablePath.of(DATABASE, prefix + "_" + acks),
                                version,
                                acks);
                    }
                    process.getOutputStream().write('\n');
                    process.getOutputStream().flush();
                    assertThat(process.waitFor(30, TimeUnit.SECONDS))
                            .as("Client log: %s", log)
                            .isTrue();
                    text = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
                    assertThat(process.exitValue()).as("Client %s: %s", version, text).isZero();
                    assertThat(text).contains("NATIVE_READBACK_CONFIRMED");
                    Files.write(
                            output.resolve("result.txt"),
                            Collections.singletonList(
                                    "PASS "
                                            + version
                                            + " PLAINTEXT non-idempotent; acks=0,1,all; same producer full/alias; 12 native rows and offsets verified"),
                            StandardCharsets.UTF_8);
                } finally {
                    if (process.isAlive()) {
                        process.destroyForcibly();
                        process.waitFor(10, TimeUnit.SECONDS);
                    }
                }
            } finally {
                for (TablePath path : paths) {
                    admin.dropTable(path, true).get();
                }
            }
        }
    }

    private static void assertNativeReadback(
            Connection connection, TablePath path, String version, String acks) throws Exception {
        try (Table table = connection.getTable(path);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            int count = 0;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (count < 4 && System.nanoTime() < deadline) {
                for (ScanRecord record : scanner.poll(Duration.ofSeconds(1))) {
                    InternalRow row = record.getRow();
                    assertThat(row.getBytes(0))
                            .isEqualTo(
                                    (version + ":" + acks + ":" + count)
                                            .getBytes(StandardCharsets.UTF_8));
                    assertThat(row.getBytes(1)).isEqualTo("key".getBytes(StandardCharsets.UTF_8));
                    assertThat(row.getMap(2).size()).isEqualTo(1);
                    assertThat(row.getMap(2).keyArray().getString(0).toString())
                            .isEqualTo("source");
                    assertThat(row.getMap(2).valueArray().getBytes(0)).containsExactly((byte) 2);
                    assertThat(row.isNullAt(3)).isTrue();
                    assertThat(record.logOffset()).isEqualTo(count);
                    count++;
                }
            }
            assertThat(count)
                    .as("Native readback for client %s, acks %s", version, acks)
                    .isEqualTo(4);
        }
    }

    private static TableDescriptor descriptor() {
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("_kafka_value", DataTypes.BYTES())
                                .column("_kafka_key", DataTypes.BYTES())
                                .column(
                                        "_kafka_headers",
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES()))
                                .column("_kafka_rescue", DataTypes.STRING())
                                .build())
                .distributedBy(1)
                .logFormat(LogFormat.ARROW)
                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, "binary")
                .build();
    }

    private static Configuration clusterConfig() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_ENABLED, true);
        config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, DATABASE);
        config.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        config.set(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 2);
        return config;
    }
}
