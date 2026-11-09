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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.KvBatchStrategy;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.flink.utils.TestLogScanner;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests snapshot bootstrap, CDC updates, failure publication and cache lifecycle. */
class FullCacheLookupTableTest extends FlinkTestBase {

    @TempDir private Path tempDir;

    @ParameterizedTest
    @MethodSource("snapshotAndChangelogArgs")
    void testSnapshotAndChangelog(KvFormat format, KvBatchStrategy strategy, boolean withSnapshot)
            throws Exception {
        TablePath path =
                TablePath.of(
                        DEFAULT_DB,
                        "full-cache-cdc-" + format + "-" + strategy + "-" + withSnapshot);
        createTable(
                path,
                TableDescriptor.builder()
                        .schema(DEFAULT_PK_TABLE_SCHEMA)
                        .distributedBy(3, "id")
                        .property(ConfigOptions.TABLE_KV_FORMAT, format)
                        .build());
        try (Table table = conn.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int id = 0; id < 20; id++) {
                writer.upsert(row(id, "snapshot-" + id));
            }
            writer.flush();
            if (withSnapshot) {
                FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(path);
            }
            // Changes after the SST snapshot must be replayed before lookups are served.
            writer.upsert(row(1, "bootstrap-updated"));
            writer.delete(row(2, "snapshot-2"));
            writer.upsert(row(20, "bootstrap-inserted"));
            writer.flush();
            try (FullCacheLookupTable lookupTable = lookupTable(path, 3, strategy)) {
                MetricGroup metricGroup = mock(MetricGroup.class);
                ArgumentCaptor<Gauge<Long>> snapshotRows = ArgumentCaptor.forClass(Gauge.class);
                TableInfo info = lookupTable.open(0, 1, metricGroup);
                assertThat(lookupTable.lookup(null, false)).isEmpty();
                verify(metricGroup).gauge(eq("snapshotRows"), snapshotRows.capture());
                assertThat(snapshotRows.getValue().getValue())
                        .isEqualTo(
                                withSnapshot || strategy == KvBatchStrategy.SERVER_SCAN ? 20L : 0L);
                for (int id = 3; id < 20; id++) {
                    assertValue(lookupTable, info, id, "snapshot-" + id);
                }
                assertValue(lookupTable, info, 0, "snapshot-0");
                assertValue(lookupTable, info, 1, "bootstrap-updated");
                assertValue(lookupTable, info, 20, "bootstrap-inserted");
                assertThat(lookupTable.lookup(key(info, 2), false)).isEmpty();
                // Retain returned bytes across later writes to detect accidental serializer reuse.
                List<byte[]> original = lookupTable.lookup(key(info, 1), false);
                writer.upsert(row(1, "updated"));
                writer.delete(row(20, "bootstrap-inserted"));
                writer.upsert(row(21, "inserted"));
                writer.flush();
                retry(
                        Duration.ofSeconds(30),
                        () -> {
                            assertValue(lookupTable, info, 1, "updated");
                            assertValue(lookupTable, info, 21, "inserted");
                            assertThat(lookupTable.lookup(key(info, 20), false)).isEmpty();
                        });
                assertThat(decode(info, original.get(0)).getString(1).toString())
                        .isEqualTo("bootstrap-updated");
            }
        }
        assertThat(tempDir.toFile().listFiles()).isEmpty();
    }

    private static Stream<Arguments> snapshotAndChangelogArgs() {
        return Arrays.stream(KvFormat.values())
                .flatMap(
                        format ->
                                Stream.of(
                                        Arguments.of(format, KvBatchStrategy.SNAPSHOT_MERGE, false),
                                        Arguments.of(format, KvBatchStrategy.SNAPSHOT_MERGE, true),
                                        Arguments.of(format, KvBatchStrategy.SERVER_SCAN, false)));
    }

    @Test
    void testEmptySnapshotAndClose() throws Exception {
        TablePath path = TablePath.of(DEFAULT_DB, "full-cache-empty");
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(createTable(path, DEFAULT_PK_TABLE_DESCRIPTOR));
        FullCacheLookupTable lookupTable = lookupTable(path, DEFAULT_BUCKET_NUM);
        MetricGroup metricGroup = mock(MetricGroup.class);
        ArgumentCaptor<Gauge<Long>> diskSizeGauge = ArgumentCaptor.forClass(Gauge.class);
        ArgumentCaptor<Gauge<Long>> memoryGauge = ArgumentCaptor.forClass(Gauge.class);
        try {
            TableInfo info = lookupTable.open(0, 1, metricGroup);
            assertThat(tempDir.toFile().listFiles())
                    .singleElement()
                    .satisfies(
                            directory ->
                                    assertThat(directory.toPath().resolve("CURRENT"))
                                            .isRegularFile());
            verify(metricGroup).gauge(eq("rocksdbDiskBytes"), diskSizeGauge.capture());
            verify(metricGroup).gauge(eq("rocksdbEstimatedMemoryBytes"), memoryGauge.capture());
            assertThat(memoryGauge.getValue().getValue()).isGreaterThan(0L);
            assertThat(diskSizeGauge.getValue().getValue()).isGreaterThanOrEqualTo(0L);
            assertThat(lookupTable.lookup(key(info, 1), false)).isEmpty();
        } finally {
            lookupTable.close();
        }
        lookupTable.close();
        assertThat(diskSizeGauge.getValue().getValue()).isZero();
        assertThat(memoryGauge.getValue().getValue()).isZero();
        assertThatThrownBy(() -> lookupTable.lookup(null, false)).hasMessageContaining("not ready");
        assertThat(tempDir.toFile().listFiles()).isEmpty();
    }

    @Test
    void testCloseWinsOverQueuedBootstrapPublication() throws Exception {
        TablePath path = TablePath.of(DEFAULT_DB, "full-cache-bootstrap-close");
        try (FullCacheLookupTable lookupTable = lookupTable(path, DEFAULT_BUCKET_NUM)) {
            Field lockField = FullCacheLookupTable.class.getDeclaredField("stateLock");
            lockField.setAccessible(true);
            Object stateLock = lockField.get(lookupTable);
            FutureTask<Boolean> publication = new FutureTask<>(lookupTable::completeBootstrap);
            Thread publisher = new Thread(publication, "full-cache-bootstrap-publisher");
            synchronized (stateLock) {
                publisher.start();
                retry(
                        Duration.ofSeconds(30),
                        () -> {
                            assertThat(publisher.getState()).isEqualTo(Thread.State.BLOCKED);
                            assertThat(publisher.getStackTrace())
                                    .anySatisfy(
                                            frame -> {
                                                assertThat(frame.getClassName())
                                                        .isEqualTo(
                                                                FullCacheLookupTable.class
                                                                        .getName());
                                                assertThat(frame.getMethodName())
                                                        .isEqualTo("completeBootstrap");
                                            });
                        });
                lookupTable.close();
            }
            assertThat(publication.get()).isFalse();
            assertThatThrownBy(() -> lookupTable.lookup(null, false))
                    .hasMessageContaining("not ready");
        }
    }

    @Test
    void testCloseInterruptsWorkerWhileBatchHoldsCacheLock() throws Exception {
        TablePath path = TablePath.of(DEFAULT_DB, "full-cache-close-blocked-batch");
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(createTable(path, DEFAULT_PK_TABLE_DESCRIPTOR));
        CountDownLatch batchStarted = new CountDownLatch(1);
        CountDownLatch batchInterrupted = new CountDownLatch(1);
        CountDownLatch releaseBatch = new CountDownLatch(1);
        ScanRecord blocked =
                new ScanRecord(row(1, "blocked")) {
                    @Override
                    public ChangeType getChangeType() {
                        batchStarted.countDown();
                        try {
                            releaseBatch.await();
                        } catch (InterruptedException e) {
                            batchInterrupted.countDown();
                            throw new RuntimeException(e);
                        }
                        return super.getChangeType();
                    }
                };
        Configuration config = new Configuration(clientConf);
        config.set(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR, tempDir.toString());
        config.set(ConfigOptions.CLIENT_SCANNER_KV_BATCH_STRATEGY, KvBatchStrategy.SERVER_SCAN);
        TestLogScanner scanner = new TestLogScanner();
        TestingMetricGroup metrics = new TestingMetricGroup();
        FullCacheLookupTable lookupTable =
                new FullCacheLookupTable(
                        config,
                        path,
                        DEFAULT_BUCKET_NUM,
                        new FullCacheLookupOptions(
                                new org.apache.flink.configuration.Configuration()),
                        table -> {
                            scanner.setRecordsForBucket(
                                    new TableBucket(table.getTableInfo().getTableId(), 0),
                                    Collections.singletonList(blocked));
                            return scanner;
                        });
        FutureTask<Void> closing =
                new FutureTask<>(
                        () -> {
                            lookupTable.close();
                            return null;
                        });
        Thread closer = new Thread(closing, "full-cache-closer");
        try {
            lookupTable.open(0, 1, metrics);
            batchStarted.await();
            Field lockField = FullCacheLookupTable.class.getDeclaredField("rwLock");
            lockField.setAccessible(true);
            ReentrantReadWriteLock lock = (ReentrantReadWriteLock) lockField.get(lookupTable);
            closer.start();
            retry(
                    Duration.ofSeconds(30),
                    () ->
                            assertThat(
                                            batchInterrupted.getCount() == 0
                                                    || lock.hasQueuedThread(closer))
                                    .isTrue());
            assertThat(batchInterrupted.getCount()).isZero();
            closing.get();
            assertThat(metrics.gauges.get("ready").getValue()).isEqualTo(0);
            assertThat(metrics.gauges.get("workerFailed").getValue()).isEqualTo(0);
        } finally {
            releaseBatch.countDown();
            if (closer.getState() != Thread.State.NEW) {
                closing.get();
            }
            lookupTable.close();
        }
        assertThat(tempDir.toFile().listFiles()).isEmpty();
    }

    @Test
    void testClosedBatchDoesNotPublishWorkerFailure() throws Exception {
        TablePath path = TablePath.of(DEFAULT_DB, "full-cache-closed-batch");
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(createTable(path, DEFAULT_PK_TABLE_DESCRIPTOR));
        TestingMetricGroup metrics = new TestingMetricGroup();
        try (FullCacheLookupTable lookupTable = lookupTable(path, DEFAULT_BUCKET_NUM)) {
            lookupTable.open(0, 1, metrics);
            assertThat(lookupTable.lookup(null, false)).isEmpty();
            lookupTable.close();
            assertThatThrownBy(() -> lookupTable.applyLogRecords(ScanRecords.EMPTY))
                    .hasMessageContaining("not ready");
            assertThat(metrics.gauges.get("ready").getValue()).isEqualTo(0);
            assertThat(metrics.gauges.get("workerFailed").getValue()).isEqualTo(0);
        }
        assertThat(tempDir.toFile().listFiles()).isEmpty();
    }

    @Test
    void testFirstWorkerFailureCannotBeOverwrittenOrMadeReady() throws Exception {
        TablePath path = TablePath.of(DEFAULT_DB, "full-cache-first-failure");
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(createTable(path, DEFAULT_PK_TABLE_DESCRIPTOR));
        TestingMetricGroup metrics = new TestingMetricGroup();
        try (FullCacheLookupTable lookupTable = lookupTable(path, DEFAULT_BUCKET_NUM)) {
            TableInfo info = lookupTable.open(0, 1, metrics);
            assertThat(lookupTable.lookup(null, false)).isEmpty();
            IllegalStateException failure = new IllegalStateException("injected batch failure");
            ScanRecord broken =
                    new ScanRecord(row(1, "broken")) {
                        @Override
                        public ChangeType getChangeType() {
                            throw failure;
                        }
                    };
            ScanRecords records =
                    new ScanRecords(
                            Collections.singletonMap(
                                    new TableBucket(info.getTableId(), 0),
                                    Collections.singletonList(broken)));
            assertThatThrownBy(() -> lookupTable.applyLogRecords(records)).isSameAs(failure);
            assertThatThrownBy(() -> lookupTable.applyLogRecords(ScanRecords.EMPTY))
                    .hasCause(failure);
            assertThatThrownBy(() -> lookupTable.lookup(null, false)).hasCause(failure);
            assertThat(lookupTable.completeBootstrap()).isFalse();
            assertThat(metrics.gauges.get("ready").getValue()).isEqualTo(0);
            assertThat(metrics.gauges.get("workerFailed").getValue()).isEqualTo(1);
        }
        assertThat(tempDir.toFile().listFiles()).isEmpty();
    }

    @Test
    void testFailedBatchCannotBeRead() throws Exception {
        TablePath path = TablePath.of(DEFAULT_DB, "full-cache-failure");
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(createTable(path, DEFAULT_PK_TABLE_DESCRIPTOR));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch partialWrite = new CountDownLatch(1);
        CountDownLatch failBatch = new CountDownLatch(1);
        try (FullCacheLookupTable lookupTable = lookupTable(path, DEFAULT_BUCKET_NUM)) {
            TableInfo info = lookupTable.open(0, 1, new UnregisteredMetricsGroup());
            assertThat(lookupTable.lookup(null, false)).isEmpty();
            IllegalStateException failure = new IllegalStateException("injected batch failure");
            ScanRecord broken = mock(ScanRecord.class);
            when(broken.getChangeType())
                    .thenAnswer(
                            invocation -> {
                                partialWrite.countDown();
                                failBatch.await();
                                throw failure;
                            });
            ScanRecords records =
                    new ScanRecords(
                            Collections.singletonMap(
                                    new TableBucket(info.getTableId(), 0),
                                    Arrays.asList(new ScanRecord(row(1, "partial")), broken)));
            Future<?> update =
                    executor.submit(
                            () -> {
                                lookupTable.applyLogRecords(records);
                                return null;
                            });
            partialWrite.await();
            CountDownLatch readerStarted = new CountDownLatch(1);
            Future<?> read =
                    executor.submit(
                            () -> {
                                readerStarted.countDown();
                                return lookupTable.lookup(key(info, 1), false);
                            });
            readerStarted.await();
            failBatch.countDown();
            assertThatThrownBy(update::get).hasCause(failure);
            assertThatThrownBy(read::get).hasRootCause(failure);
            assertThatThrownBy(() -> lookupTable.lookup(key(info, 1), false)).hasCause(failure);
        } finally {
            failBatch.countDown();
            executor.shutdownNow();
        }
    }

    @ParameterizedTest
    @MethodSource("pollingFailures")
    void testEscapedPollingFailureStopsWorker(RuntimeException failure) throws Exception {
        TablePath path =
                TablePath.of(
                        DEFAULT_DB,
                        "full-cache-poll-failure-" + failure.getClass().getSimpleName());
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(createTable(path, DEFAULT_PK_TABLE_DESCRIPTOR));
        Configuration config = new Configuration(clientConf);
        config.set(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR, tempDir.toString());
        config.set(ConfigOptions.CLIENT_SCANNER_KV_BATCH_STRATEGY, KvBatchStrategy.SERVER_SCAN);
        CompletableFuture<RuntimeException> pollFailure = new CompletableFuture<>();
        LogScanner scanner = mock(LogScanner.class);
        // Model an error that escapes the scanner, not a network failure handled inside it.
        when(scanner.poll(any(Duration.class)))
                .thenAnswer(
                        invocation -> {
                            throw pollFailure.get();
                        });
        MetricGroup metrics = mock(MetricGroup.class);
        ArgumentCaptor<Gauge<Integer>> workerFailed = ArgumentCaptor.forClass(Gauge.class);
        ArgumentCaptor<Gauge<Integer>> ready = ArgumentCaptor.forClass(Gauge.class);
        try (FullCacheLookupTable lookupTable =
                new FullCacheLookupTable(
                        config,
                        path,
                        DEFAULT_BUCKET_NUM,
                        new FullCacheLookupOptions(
                                new org.apache.flink.configuration.Configuration()),
                        table -> scanner)) {
            TableInfo info = lookupTable.open(0, 1, metrics);
            assertThat(lookupTable.lookup(null, false)).isEmpty();
            verify(metrics).gauge(eq("workerFailed"), workerFailed.capture());
            verify(metrics).gauge(eq("ready"), ready.capture());
            pollFailure.complete(failure);
            retry(
                    Duration.ofSeconds(30),
                    () -> {
                        assertThat(workerFailed.getValue().getValue()).isOne();
                        assertThat(ready.getValue().getValue()).isZero();
                    });
            assertThatThrownBy(() -> lookupTable.lookup(key(info, 1), false)).hasCause(failure);
            assertThatThrownBy(() -> lookupTable.lookup(null, false)).hasCause(failure);
            verify(scanner).poll(any(Duration.class));
        }
        verify(scanner).close();
        assertThat(tempDir.toFile().listFiles()).isEmpty();
    }

    private static Stream<RuntimeException> pollingFailures() {
        return Stream.of(
                new AuthorizationException("permission denied"),
                new NetworkException("retriable error escaped the scanner"));
    }

    private FullCacheLookupTable lookupTable(TablePath path, int buckets) {
        return lookupTable(path, buckets, KvBatchStrategy.SNAPSHOT_MERGE);
    }

    private FullCacheLookupTable lookupTable(
            TablePath path, int buckets, KvBatchStrategy strategy) {
        Configuration config = new Configuration(clientConf);
        config.set(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR, tempDir.toString());
        config.set(ConfigOptions.CLIENT_SCANNER_KV_BATCH_STRATEGY, strategy);
        return new FullCacheLookupTable(
                config,
                path,
                buckets,
                new FullCacheLookupOptions(new org.apache.flink.configuration.Configuration()));
    }

    private static byte[] key(TableInfo info, int id) {
        return KeyEncoder.ofPrimaryKeyEncoder(
                        info.getRowType(),
                        info.getPhysicalPrimaryKeys(),
                        info.getTableConfig(),
                        info.isDefaultBucketKey())
                .encodeKey(row(id, null));
    }

    private static InternalRow decode(TableInfo info, byte[] value) {
        return RowDecoder.create(
                        info.getTableConfig().getKvFormat(),
                        info.getRowType().getChildren().toArray(new DataType[0]))
                .decode(value);
    }

    private static void assertValue(
            FullCacheLookupTable lookupTable, TableInfo info, int id, String expected)
            throws Exception {
        List<byte[]> values = lookupTable.lookup(key(info, id), false);
        assertThat(values).hasSize(1);
        InternalRow row = decode(info, values.get(0));
        assertThat(row.getInt(0)).isEqualTo(id);
        assertThat(row.getString(1).toString()).isEqualTo(expected);
    }

    private static final class TestingMetricGroup extends UnregisteredMetricsGroup {

        private final Map<String, Gauge<?>> gauges = new HashMap<>();

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            gauges.put(name, gauge);
            return gauge;
        }
    }
}
