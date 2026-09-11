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

package org.apache.fluss.client.table;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metrics.TestingWriterMetricGroup;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.write.WriteFormat;
import org.apache.fluss.client.write.WriteRecord;
import org.apache.fluss.client.write.WriterClient;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real RPC tests for retrying writes rejected by disk protection. */
class DiskWriteBackoffITCase {

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfig())
                    .build();

    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true"})
    void testDiskProtectionBackoffAndRecovery(boolean kv, boolean closeWhileLocked)
            throws Exception {
        Configuration conf = CLUSTER.getClientConfig();
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT, Duration.ZERO);
        conf.set(ConfigOptions.CLIENT_WRITER_DISK_WRITE_LOCKED_BACKOFF, Duration.ofSeconds(1));
        conf.set(ConfigOptions.CLIENT_WRITER_DISK_WRITE_LOCKED_BACKOFF_MAX, Duration.ofSeconds(1));
        TestingWriterMetricGroup metrics = TestingWriterMetricGroup.newInstance();
        ReplicaManager replicas = CLUSTER.getTabletServers().iterator().next().getReplicaManager();
        TablePath path =
                TablePath.of(
                        "disk_backoff",
                        closeWhileLocked ? "close_table" : kv ? "kv_table" : "log_table");
        try (Connection connection = ConnectionFactory.createConnection(conf);
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(
                            path,
                            TableDescriptor.builder()
                                    .schema(kv ? DATA1_SCHEMA_PK : DATA1_SCHEMA)
                                    .distributedBy(1)
                                    .build(),
                            false)
                    .get();
            try (Table table = connection.getTable(path)) {
                WriterClient writer =
                        new WriterClient(
                                conf,
                                ((FlussConnection) connection).getMetadataUpdater(),
                                metrics,
                                admin);
                try {
                    // Establish a writable leader before simulating disk protection.
                    send(writer, table.getTableInfo(), kv, 1).get(30, TimeUnit.SECONDS);
                    long retriesBeforeRejection = metrics.recordsRetryTotal().getCount();
                    replicas.getDiskUsageMonitor().updateWriteLimitConfig(0.85, 0.80);
                    replicas.getDiskUsageMonitor().update(0.99);
                    assertThat(replicas.isDiskWriteLocked()).isTrue();
                    CompletableFuture<Void> rejected = send(writer, table.getTableInfo(), kv, 2);
                    retry(
                            Duration.ofSeconds(10),
                            () ->
                                    assertThat(metrics.recordsRetryTotal().getCount())
                                            .isEqualTo(retriesBeforeRejection + 1));
                    long bytesAfterRejection = metrics.bytesSendTotal().getCount();
                    // The callback remains pending, and the payload is not sent again in the
                    // window.
                    assertThatThrownBy(() -> rejected.get(200, TimeUnit.MILLISECONDS))
                            .isInstanceOf(TimeoutException.class);
                    assertThat(metrics.recordsRetryTotal().getCount())
                            .isEqualTo(retriesBeforeRejection + 1);
                    assertThat(metrics.bytesSendTotal().getCount()).isEqualTo(bytesAfterRejection);
                    if (closeWhileLocked) {
                        long start = System.nanoTime();
                        writer.close(Duration.ofMillis(50));
                        assertThat(Duration.ofNanos(System.nanoTime() - start))
                                .isLessThan(Duration.ofSeconds(2));
                        assertThat(metrics.recordsRetryTotal().getCount())
                                .isEqualTo(retriesBeforeRejection + 1);
                        return;
                    }
                    replicas.getDiskUsageMonitor().update(0.5);
                    assertThat(replicas.isDiskWriteLocked()).isFalse();
                    rejected.get(10, TimeUnit.SECONDS);
                    writer.flush();
                    if (kv) {
                        assertThatRow(
                                        table.newLookup()
                                                .createLookuper()
                                                .lookup(row(2))
                                                .get()
                                                .getSingletonRow())
                                .withSchema(DATA1_ROW_TYPE)
                                .isEqualTo(row(2, "payload"));
                    } else {
                        try (LogScanner scanner = table.newScan().createLogScanner()) {
                            scanner.subscribeFromBeginning(0);
                            CompletableFuture<Void> read = new CompletableFuture<>();
                            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                            while (!read.isDone() && System.nanoTime() < deadline) {
                                scanner.poll(Duration.ofMillis(100))
                                        .forEach(
                                                record -> {
                                                    if (record.getRow().getInt(0) == 2) {
                                                        assertThatRow(record.getRow())
                                                                .withSchema(DATA1_ROW_TYPE)
                                                                .isEqualTo(row(2, "payload"));
                                                        read.complete(null);
                                                    }
                                                });
                            }
                            assertThat(read).isCompleted();
                        }
                    }
                } finally {
                    replicas.getDiskUsageMonitor().update(0.5);
                    writer.close(Duration.ofSeconds(5));
                }
            }
        }
    }

    private static CompletableFuture<Void> send(
            WriterClient writer, TableInfo info, boolean kv, int key) {
        PhysicalTablePath path = PhysicalTablePath.of(info.getTablePath());
        WriteRecord record;
        if (kv) {
            BinaryRow value = compactedRow(DATA1_ROW_TYPE, new Object[] {key, "payload"});
            byte[] encodedKey =
                    new CompactedKeyEncoder(DATA1_ROW_TYPE, DATA1_SCHEMA_PK.getPrimaryKeyIndexes())
                            .encodeKey(value);
            record =
                    WriteRecord.forUpsert(
                            info,
                            path,
                            value,
                            encodedKey,
                            encodedKey,
                            WriteFormat.COMPACTED_KV,
                            null);
        } else {
            record = WriteRecord.forArrowAppend(info, path, row(key, "payload"), null);
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        writer.send(
                record,
                (bucket, offset, error) -> {
                    if (error == null) {
                        result.complete(null);
                    } else {
                        result.completeExceptionally(error);
                    }
                });
        return result;
    }

    private static Configuration clusterConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        // Keep the real periodic sampler from overwriting the simulated usage during each test.
        conf.set(ConfigOptions.SERVER_DATA_DISK_CHECK_INTERVAL, Duration.ofHours(1));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));
        return conf;
    }
}
