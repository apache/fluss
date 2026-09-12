/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.client.write;

import org.apache.fluss.client.metrics.TestingWriterMetricGroup;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.BucketRescaleException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.record.DefaultKvRecordBatch;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordReadContext;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.InternalRowUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.indexedRow;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the ordering barrier between tentative routing and the resolved partition layout. */
class BucketRoutingTest {
    private static final PhysicalTablePath PATH =
            PhysicalTablePath.of(TablePath.of("db", "routing"), "p");
    private static final long PARTITION_ID = 42;
    private static final ServerNode NODE =
            new ServerNode(1, "localhost", 1234, ServerType.TABLET_SERVER);

    @ParameterizedTest
    @MethodSource("hashLayouts")
    void testResolveHashBatchesAndRetryAfterRescale(WriteFormat format, int oldCount, int newCount)
            throws Exception {
        TableInfo table = tableInfo(oldCount, true, format.isKv());
        RecordAccumulator accumulator = accumulator(ConfigOptions.NoKeyAssigner.STICKY);
        Cluster resolved = cluster(newCount, true);
        List<CompletableFuture<Integer>> completions = new ArrayList<>();
        List<WriteBatch> originals = new ArrayList<>();
        AtomicInteger callbackCount = new AtomicInteger();
        try {
            // Append multiple keys and batches with no partition ID. Refreshing metadata on an
            // append must not switch just the newer records to another layout.
            for (int version = 1; version <= 2; version++) {
                for (int key = 0; key < 12; key++) {
                    CompletableFuture<Integer> completion = new CompletableFuture<>();
                    completions.add(completion);
                    final int value = version;
                    WriteRecord record = record(table, format, key, version);
                    accumulator.append(
                            record,
                            (bucket, offset, error) -> {
                                callbackCount.incrementAndGet();
                                if (error == null) {
                                    completion.complete(value);
                                } else {
                                    completion.completeExceptionally(error);
                                }
                            },
                            version == 1 ? Cluster.empty() : resolved);
                }
                for (int bucket = 0; bucket < oldCount; bucket++) {
                    Deque<WriteBatch> deque = accumulator.getDequeOrThrow(PATH, bucket);
                    if (deque != null) {
                        assertThat(deque)
                                .allSatisfy(
                                        batch ->
                                                assertThat(batch.getBucketCount())
                                                        .isEqualTo(oldCount));
                        deque.peekLast().close();
                    }
                }
            }
            for (int bucket = 0; bucket < oldCount; bucket++) {
                if (accumulator.getDequeOrThrow(PATH, bucket) != null) {
                    originals.addAll(accumulator.getDequeOrThrow(PATH, bucket));
                }
            }
            CompletableFuture<Void> originalCompletion =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    for (WriteBatch batch : originals) {
                                        batch.getRequestFuture().await();
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException(e);
                                }
                            });
            assertThat(accumulator.drain(resolved, Collections.singleton(1), Integer.MAX_VALUE))
                    .isEmpty();
            assertThat(accumulator.ready(cluster(newCount, false)).readyNodes).isEmpty();
            assertThat(accumulator.getReadyDeque(PATH, 0)).isNull();
            accumulator.beginFlush();
            accumulator.ready(resolved);
            if (oldCount != newCount) {
                assertThat(completions)
                        .allSatisfy(
                                future ->
                                        assertThatThrownBy(future::get)
                                                .hasCauseInstanceOf(BucketRescaleException.class)
                                                .hasStackTraceContaining("Bucket rescale")
                                                .hasStackTraceContaining(
                                                        "from " + oldCount + " to " + newCount)
                                                .hasStackTraceContaining(
                                                        "Retry the failed records"));
                originalCompletion.get(10, TimeUnit.SECONDS);
                assertThat(accumulator.hasIncomplete()).isFalse();
                assertThat(accumulator.hasUnDrained()).isFalse();
                // Application retries reuse this accumulator and the resolved count.
                for (int version = 1; version <= 2; version++) {
                    for (int key = 0; key < 12; key++) {
                        accumulator.append(
                                record(table, format, key, version),
                                (bucket, offset, error) -> {},
                                resolved);
                    }
                }
            } else {
                assertThat(originalCompletion).isNotDone();
                assertThat(completions).allSatisfy(future -> assertThat(future).isNotDone());
            }

            // Later writes use the same authoritative layout as the retried records.
            for (int key = 0; key < 12; key++) {
                accumulator.append(
                        record(table, format, key, 3), (bucket, offset, error) -> {}, resolved);
            }
            Map<Integer, List<Integer>> versions = new HashMap<>();
            HashBucketAssigner assigner = new HashBucketAssigner();
            while (accumulator.hasUnDrained()) {
                List<ReadyWriteBatch> batches =
                        accumulator
                                .drain(resolved, Collections.singleton(1), Integer.MAX_VALUE)
                                .get(1);
                assertThat(batches).isNotEmpty();
                for (ReadyWriteBatch ready : batches) {
                    WriteBatch batch = ready.writeBatch();
                    assertThat(ready.tableBucket().getPartitionId()).isEqualTo(PARTITION_ID);
                    assertThat(batch.getBucketCount()).isEqualTo(newCount);
                    for (InternalRow row : readRows(batch, table, format)) {
                        int key = row.getInt(0);
                        assertThat(batch.bucketId())
                                .isEqualTo(
                                        assigner.assignBucket(new byte[] {(byte) key}, newCount));
                        versions.computeIfAbsent(key, k -> new ArrayList<>()).add(row.getInt(1));
                    }
                    batch.complete(ready.tableBucket(), 10L);
                    accumulator.deallocate(batch);
                }
            }
            assertThat(versions).hasSize(12);
            assertThat(versions.values())
                    .allSatisfy(values -> assertThat(values).containsExactly(1, 2, 3));
            originalCompletion.get(10, TimeUnit.SECONDS);
            accumulator.awaitFlushCompletion();
            assertThat(completions).allSatisfy(future -> assertThat(future).isDone());
            assertThat(accumulator.hasIncomplete()).isFalse();
            assertThat(callbackCount.get()).isEqualTo(24);
        } finally {
            accumulator.abortAllBatches(new RuntimeException("test cleanup"));
            accumulator.close();
            accumulator.destroyResources();
        }
    }

    @ParameterizedTest
    @MethodSource("noKeyLayouts")
    void testResolveBatchesWithoutBucketKey(
            WriteFormat format, int newCount, boolean highBucketHasRecords) throws Exception {
        TableInfo table = tableInfo(3, false, false);
        TestingBucketAssigner bucketAssigner = new TestingBucketAssigner();
        RecordAccumulator accumulator =
                new RecordAccumulator(
                        configuration(ConfigOptions.NoKeyAssigner.STICKY, 64 * 1024),
                        new IdempotenceManager(false, 5, null, null),
                        TestingWriterMetricGroup.newInstance(),
                        SystemClock.getInstance(),
                        (tableInfo, path) -> bucketAssigner);
        List<CompletableFuture<Exception>> completions = new ArrayList<>();
        try {
            // Leave an empty high bucket queue behind. Empty queues must not fail a shrink.
            bucketAssigner.setBucketId(2);
            accumulator.append(
                    record(table, format, 0, 0), (bucket, offset, error) -> {}, Cluster.empty());
            accumulator.abortAllBatches(new RuntimeException("discard seed batch"));
            for (int i = 0; i < 12; i++) {
                CompletableFuture<Exception> completion = new CompletableFuture<>();
                completions.add(completion);
                bucketAssigner.setBucketId(highBucketHasRecords && i % 2 == 1 ? 2 : 0);
                accumulator.append(
                        record(table, format, i, 1),
                        (bucket, offset, error) -> completion.complete(error),
                        Cluster.empty());
            }
            List<WriteBatch> originals = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                Deque<WriteBatch> deque = accumulator.getDequeOrThrow(PATH, i);
                if (deque != null) {
                    originals.addAll(deque);
                }
            }
            Cluster resolved = cluster(newCount, true);
            accumulator.beginFlush();
            accumulator.ready(resolved);
            if (newCount < 3 && highBucketHasRecords) {
                // All batches fail, including the records assigned to bucket 0.
                for (CompletableFuture<Exception> completion : completions) {
                    assertThat(completion.get(10, TimeUnit.SECONDS))
                            .isInstanceOf(BucketRescaleException.class);
                }
                assertThat(accumulator.hasUnDrained()).isFalse();
                assertThat(accumulator.hasIncomplete()).isFalse();
                bucketAssigner.setBucketId(0);
                accumulator.append(
                        record(table, format, 0, 2), (bucket, offset, error) -> {}, resolved);
            } else {
                assertThat(completions).allSatisfy(future -> assertThat(future).isNotDone());
            }
            List<WriteBatch> drained = new ArrayList<>();
            while (accumulator.hasUnDrained()) {
                for (ReadyWriteBatch ready :
                        accumulator
                                .drain(resolved, Collections.singleton(1), Integer.MAX_VALUE)
                                .get(1)) {
                    WriteBatch batch = ready.writeBatch();
                    assertThat(batch.bucketId()).isBetween(0, newCount - 1);
                    assertThat(batch.getBucketCount()).isEqualTo(newCount);
                    drained.add(batch);
                    batch.complete();
                    accumulator.deallocate(batch);
                }
            }
            if (newCount >= 3 || !highBucketHasRecords) {
                assertThat(drained).containsExactlyInAnyOrderElementsOf(originals);
            }
            accumulator.awaitFlushCompletion();
        } finally {
            accumulator.close();
            accumulator.abortAllBatches(new RuntimeException("test cleanup"));
            accumulator.destroyResources();
        }
    }

    @Test
    void testRescaleDoesNotFailAnotherPartition() throws Exception {
        TableInfo table = tableInfo(2, true, false);
        PhysicalTablePath otherPath = PhysicalTablePath.of(PATH.getTablePath(), "other");
        RecordAccumulator accumulator = accumulator(ConfigOptions.NoKeyAssigner.STICKY);
        CompletableFuture<Exception> rescaled = new CompletableFuture<>();
        CompletableFuture<Exception> other = new CompletableFuture<>();
        try {
            accumulator.append(
                    record(table, WriteFormat.ARROW_LOG, 0, 1),
                    (bucket, offset, error) -> rescaled.complete(error),
                    Cluster.empty());
            accumulator.append(
                    WriteRecord.forArrowAppend(
                            table, otherPath, row(0, 1, "other"), new byte[] {0}),
                    (bucket, offset, error) -> other.complete(error),
                    Cluster.empty());
            accumulator.ready(cluster(4, true));
            assertThat(rescaled.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(BucketRescaleException.class);
            assertThat(other).isNotDone();
            assertThat(accumulator.hasIncomplete()).isTrue();
        } finally {
            accumulator.close();
            accumulator.abortAllBatches(new RuntimeException("test cleanup"));
            accumulator.destroyResources();
        }
    }

    @ParameterizedTest
    @EnumSource(ConfigOptions.NoKeyAssigner.class)
    void testAssignerCountChanges(ConfigOptions.NoKeyAssigner strategy) {
        BucketAssigner assigner =
                strategy == ConfigOptions.NoKeyAssigner.STICKY
                        ? new StickyBucketAssigner(PATH)
                        : new RoundRobinBucketAssigner(PATH);
        for (int count : new int[] {4, 1, 2, 5}) {
            for (int i = 0; i < 30; i++) {
                int bucket = assigner.assignBucket(null, cluster(5, true), count);
                assertThat(bucket).isBetween(0, count - 1);
                assigner.onNewBatch(cluster(5, true), count, bucket);
            }
        }
    }

    @Test
    void testNonPartitionedTableUsesMetadataBucketCount() throws Exception {
        TableInfo table =
                TableInfo.of(
                        PATH.getTablePath(),
                        10,
                        1,
                        TableDescriptor.builder()
                                .schema(tableInfo(3, false, false).getSchema())
                                .distributedBy(3)
                                .build(),
                        "/tmp/fluss-routing",
                        0,
                        0);
        PhysicalTablePath path = PhysicalTablePath.of(PATH.getTablePath());
        Cluster partialMetadata =
                new Cluster(
                        Collections.singletonMap(1, NODE),
                        null,
                        Collections.singletonMap(
                                path,
                                Collections.singletonList(
                                        new BucketLocation(
                                                path, new TableBucket(10, 0), 1, new int[] {1}))),
                        Collections.singletonMap(path.getTablePath(), 10L),
                        Collections.emptyMap(),
                        Collections.singletonMap(TableOrPartition.ofTable(10), 1));
        RecordAccumulator accumulator = accumulator(ConfigOptions.NoKeyAssigner.STICKY);
        try {
            accumulator.append(
                    WriteRecord.forArrowAppend(table, path, row(0, 1, "p"), null),
                    (bucket, offset, error) -> {},
                    partialMetadata);
            assertThat(accumulator.getReadyDeque(path, 0).getFirst().getBucketCount()).isEqualTo(1);
        } finally {
            accumulator.close();
            accumulator.abortAllBatches(new RuntimeException("test cleanup"));
            accumulator.destroyResources();
        }
    }

    @Test
    void testTemporaryBucketCountUsesFirstClusterSnapshot() throws Exception {
        TableInfo table = tableInfo(2, true, true);
        Cluster tableMetadata =
                new Cluster(
                        Collections.emptyMap(),
                        null,
                        Collections.emptyMap(),
                        Collections.singletonMap(PATH.getTablePath(), table.getTableId()),
                        Collections.emptyMap(),
                        Collections.singletonMap(TableOrPartition.ofTable(table.getTableId()), 4));
        RecordAccumulator accumulator = accumulator(ConfigOptions.NoKeyAssigner.STICKY);
        List<CompletableFuture<Exception>> completions = new ArrayList<>();
        List<WriteBatch> originals = new ArrayList<>();
        try {
            for (int key = 0; key < 12; key++) {
                CompletableFuture<Exception> completion = new CompletableFuture<>();
                completions.add(completion);
                accumulator.append(
                        record(table, WriteFormat.COMPACTED_KV, key, 1),
                        (bucket, offset, error) -> completion.complete(error),
                        key == 0 ? tableMetadata : Cluster.empty());
            }
            for (int bucket = 0; bucket < 4; bucket++) {
                Deque<WriteBatch> deque = accumulator.getDequeOrThrow(PATH, bucket);
                if (deque != null) {
                    originals.addAll(deque);
                }
            }
            assertThat(originals)
                    .isNotEmpty()
                    .allSatisfy(batch -> assertThat(batch.getBucketCount()).isEqualTo(4));
            assertThat(accumulator.ready(tableMetadata).readyNodes).isEmpty();
            Cluster resolved = cluster(4, true);
            accumulator.ready(resolved);
            assertThat(completions).allSatisfy(future -> assertThat(future).isNotDone());
            List<WriteBatch> drained = new ArrayList<>();
            while (accumulator.hasUnDrained()) {
                for (ReadyWriteBatch ready :
                        accumulator
                                .drain(resolved, Collections.singleton(1), Integer.MAX_VALUE)
                                .get(1)) {
                    drained.add(ready.writeBatch());
                    ready.writeBatch().complete();
                    accumulator.deallocate(ready.writeBatch());
                }
            }
            assertThat(drained).containsExactlyInAnyOrderElementsOf(originals);
            for (CompletableFuture<Exception> completion : completions) {
                assertThat(completion.get(10, TimeUnit.SECONDS)).isNull();
            }
        } finally {
            accumulator.close();
            accumulator.abortAllBatches(new RuntimeException("test cleanup"));
            accumulator.destroyResources();
        }
    }

    @Test
    void testResolvePartitionIdAndBucketCountFromSameSnapshot() throws Exception {
        TableInfo table = tableInfo(4, true, true);
        RecordAccumulator accumulator = accumulator(ConfigOptions.NoKeyAssigner.STICKY);
        CompletableFuture<Exception> completion = new CompletableFuture<>();
        try {
            Cluster partialMetadata = cluster(4, false);
            accumulator.append(
                    record(table, WriteFormat.COMPACTED_KV, 0, 1),
                    (bucket, offset, error) -> completion.complete(error),
                    partialMetadata);
            assertThat(accumulator.ready(partialMetadata).readyNodes).isEmpty();
            assertThat(accumulator.getReadyDeque(PATH, 0)).isNull();
            // The path now resolves to a recreated partition. The old partition's count is still
            // cached, so reusing the earlier partition ID would incorrectly abort this batch.
            long newPartitionId = PARTITION_ID + 1;
            Cluster newPartition = cluster(4, true, newPartitionId);
            Map<TableOrPartition, Integer> counts =
                    new HashMap<>(newPartition.getBucketCountByTableOrPartition());
            counts.put(TableOrPartition.ofPartition(PARTITION_ID), 1);
            Cluster resolved =
                    new Cluster(
                            newPartition.getAliveTabletServers(),
                            null,
                            newPartition.getBucketLocationsByPath(),
                            newPartition.getTableIdByPath(),
                            newPartition.getPartitionIdByPath(),
                            counts);
            assertThat(accumulator.ready(resolved).readyNodes).containsExactly(1);
            assertThat(completion).isNotDone();
            List<ReadyWriteBatch> drained =
                    accumulator.drain(resolved, Collections.singleton(1), Integer.MAX_VALUE).get(1);
            assertThat(drained).hasSize(1);
            ReadyWriteBatch ready = drained.get(0);
            assertThat(ready.tableBucket().getPartitionId()).isEqualTo(newPartitionId);
            assertThat(ready.writeBatch().getBucketCount()).isEqualTo(4);
            ready.writeBatch().complete();
            accumulator.deallocate(ready.writeBatch());
            assertThat(completion.get(10, TimeUnit.SECONDS)).isNull();
        } finally {
            accumulator.close();
            accumulator.abortAllBatches(new RuntimeException("test cleanup"));
            accumulator.destroyResources();
        }
    }

    @Test
    void testStickyAssignerChangesBucketWhenBatchIsFull() throws Exception {
        TableInfo table = tableInfo(2, false, false);
        RecordAccumulator accumulator = accumulator(ConfigOptions.NoKeyAssigner.STICKY);
        Cluster resolved = cluster(2, true);
        try {
            accumulator.append(
                    record(table, WriteFormat.COMPACTED_LOG, 0, 1),
                    (bucket, offset, error) -> {},
                    resolved);
            int firstBucket =
                    accumulator.getDequeOrThrow(PATH, 0) != null
                                    && !accumulator.getDequeOrThrow(PATH, 0).isEmpty()
                            ? 0
                            : 1;
            Deque<WriteBatch> firstDeque = accumulator.getDequeOrThrow(PATH, firstBucket);
            assertThat(firstDeque).hasSize(1);
            WriteBatch firstBatch = firstDeque.getFirst();
            accumulator.append(
                    record(table, WriteFormat.COMPACTED_LOG, 0, 2),
                    (bucket, offset, error) -> {},
                    resolved);
            firstBatch.close();
            assertThat(readRows(firstBatch, table, WriteFormat.COMPACTED_LOG))
                    .extracting(row -> row.getInt(1))
                    .containsExactly(1, 2);
            accumulator.append(
                    record(table, WriteFormat.COMPACTED_LOG, 0, 3),
                    (bucket, offset, error) -> {},
                    resolved);
            assertThat(firstDeque).containsExactly(firstBatch);
            Deque<WriteBatch> nextDeque = accumulator.getDequeOrThrow(PATH, 1 - firstBucket);
            assertThat(nextDeque).hasSize(1);
            WriteBatch nextBatch = nextDeque.getFirst();
            nextBatch.close();
            assertThat(readRows(nextBatch, table, WriteFormat.COMPACTED_LOG))
                    .extracting(row -> row.getInt(1))
                    .containsExactly(3);
        } finally {
            accumulator.close();
            accumulator.abortAllBatches(new RuntimeException("test cleanup"));
            accumulator.destroyResources();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void testResolveWhileAppendWaitsForExhaustedBuffer(int newCount) throws Exception {
        TestingWriterMetricGroup metrics = TestingWriterMetricGroup.newInstance();
        RecordAccumulator accumulator =
                accumulator(ConfigOptions.NoKeyAssigner.STICKY, 512, metrics);
        TableInfo table = tableInfo(1, true, true);
        Cluster resolved = cluster(newCount, true);
        try {
            for (int version = 1; version <= 2; version++) {
                accumulator.append(
                        record(table, WriteFormat.COMPACTED_KV, 0, version),
                        (bucket, offset, error) -> {},
                        Cluster.empty());
                accumulator.getDequeOrThrow(PATH, 0).peekLast().close();
            }
            CompletableFuture<Void> append =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    accumulator.append(
                                            record(table, WriteFormat.COMPACTED_KV, 0, 3),
                                            (bucket, offset, error) -> {},
                                            resolved);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
            Gauge<?> waiting =
                    (Gauge<?>) metrics.getMetrics().get(MetricNames.WRITER_BUFFER_WAITING_THREADS);
            retry(Duration.ofSeconds(10), () -> assertThat(waiting.getValue()).isEqualTo(1));
            // Resolving routing must release failed batches without waiting for pooled memory.
            // A blocked append uses the actual count after acquiring the released memory.
            accumulator.ready(resolved);
            List<Integer> versions = new ArrayList<>();
            if (newCount == 1) {
                // An unchanged layout reuses the original batches. Drain one to free memory.
                ReadyWriteBatch first =
                        accumulator
                                .drain(resolved, Collections.singleton(1), Integer.MAX_VALUE)
                                .get(1)
                                .get(0);
                for (InternalRow row :
                        readRows(first.writeBatch(), table, WriteFormat.COMPACTED_KV)) {
                    versions.add(row.getInt(1));
                }
                first.writeBatch().complete();
                accumulator.deallocate(first.writeBatch());
            }
            append.get(10, TimeUnit.SECONDS);
            while (accumulator.hasUnDrained()) {
                for (ReadyWriteBatch ready :
                        accumulator
                                .drain(resolved, Collections.singleton(1), Integer.MAX_VALUE)
                                .get(1)) {
                    assertThat(ready.writeBatch().getBucketCount()).isEqualTo(newCount);
                    for (InternalRow row :
                            readRows(ready.writeBatch(), table, WriteFormat.COMPACTED_KV)) {
                        versions.add(row.getInt(1));
                    }
                    ready.writeBatch().complete();
                    accumulator.deallocate(ready.writeBatch());
                }
            }
            if (newCount == 1) {
                assertThat(versions).containsExactly(1, 2, 3);
            } else {
                assertThat(versions).containsExactly(3);
            }
            Gauge<?> available =
                    (Gauge<?>) metrics.getMetrics().get(MetricNames.WRITER_BUFFER_AVAILABLE_BYTES);
            assertThat(available.getValue()).isEqualTo(512L);
        } finally {
            accumulator.close();
            accumulator.abortAllBatches(new RuntimeException("test cleanup"));
            accumulator.destroyResources();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 4})
    void testResolveWaitsForTentativeAppend(int newCount) throws Exception {
        CompletableFuture<Void> assigning = new CompletableFuture<>();
        CompletableFuture<Void> resumeAppend = new CompletableFuture<>();
        AtomicBoolean pauseAssignment = new AtomicBoolean();
        HashBucketAssigner assigner =
                new HashBucketAssigner() {
                    @Override
                    public int assignBucket(byte[] bucketKey, int bucketCount) {
                        if (pauseAssignment.get()) {
                            assigning.complete(null);
                            resumeAppend.join();
                        }
                        return super.assignBucket(bucketKey, bucketCount);
                    }
                };
        RecordAccumulator accumulator =
                new RecordAccumulator(
                        configuration(ConfigOptions.NoKeyAssigner.STICKY, 64 * 1024),
                        new IdempotenceManager(false, 5, null, null),
                        TestingWriterMetricGroup.newInstance(),
                        SystemClock.getInstance(),
                        (table, path) -> assigner);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        TableInfo table = tableInfo(2, true, true);
        Cluster resolved = cluster(newCount, true);
        CompletableFuture<Exception> first = new CompletableFuture<>();
        CompletableFuture<Exception> second = new CompletableFuture<>();
        AtomicReference<Thread> resolvingThread = new AtomicReference<>();
        try {
            accumulator.append(
                    record(table, WriteFormat.COMPACTED_KV, 0, 1),
                    (bucket, offset, error) -> first.complete(error),
                    Cluster.empty());
            pauseAssignment.set(true);
            Future<?> append =
                    executor.submit(
                            () -> {
                                accumulator.append(
                                        record(table, WriteFormat.COMPACTED_KV, 0, 2),
                                        (bucket, offset, error) -> second.complete(error),
                                        Cluster.empty());
                                return null;
                            });
            assigning.get(10, TimeUnit.SECONDS);
            Future<?> resolve =
                    executor.submit(
                            () -> {
                                resolvingThread.set(Thread.currentThread());
                                accumulator.ready(resolved);
                            });
            retry(
                    Duration.ofSeconds(10),
                    () -> {
                        assertThat(resolvingThread.get()).isNotNull();
                        assertThat(resolvingThread.get().getState())
                                .isEqualTo(Thread.State.BLOCKED);
                    });
            assertThat(resolve.isDone()).isFalse();
            assertThat(accumulator.getReadyDeque(PATH, 0)).isNull();
            assertThat(accumulator.getReadyDeque(PATH, 1)).isNull();
            resumeAppend.complete(null);
            append.get(10, TimeUnit.SECONDS);
            resolve.get(10, TimeUnit.SECONDS);
            if (newCount != 2) {
                assertThat(first.get(10, TimeUnit.SECONDS))
                        .isInstanceOf(BucketRescaleException.class);
                assertThat(second.get(10, TimeUnit.SECONDS))
                        .isInstanceOf(BucketRescaleException.class);
                assertThat(accumulator.hasIncomplete()).isFalse();
                assertThat(accumulator.hasUnDrained()).isFalse();
            } else {
                List<ReadyWriteBatch> drained =
                        accumulator
                                .drain(resolved, Collections.singleton(1), Integer.MAX_VALUE)
                                .get(1);
                assertThat(drained).hasSize(1);
                WriteBatch batch = drained.get(0).writeBatch();
                assertThat(readRows(batch, table, WriteFormat.COMPACTED_KV))
                        .extracting(row -> row.getInt(1))
                        .containsExactly(1, 2);
                batch.complete();
                accumulator.deallocate(batch);
                assertThat(first.get(10, TimeUnit.SECONDS)).isNull();
                assertThat(second.get(10, TimeUnit.SECONDS)).isNull();
            }
        } finally {
            resumeAppend.complete(null);
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            accumulator.close();
            accumulator.abortAllBatches(new RuntimeException("test cleanup"));
            accumulator.destroyResources();
        }
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void testResolvedBucketsAppendIndependently(boolean initiallyResolved, boolean samePartition)
            throws Exception {
        RecordAccumulator accumulator = accumulator(ConfigOptions.NoKeyAssigner.STICKY);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        TableInfo table = tableInfo(2, true, true);
        Cluster resolved = cluster(4, true);
        HashBucketAssigner assigner = new HashBucketAssigner();
        int firstBucket = assigner.assignBucket(new byte[] {0}, 4);
        int secondKey = 1;
        while (assigner.assignBucket(new byte[] {(byte) secondKey}, 4) == firstBucket) {
            secondKey++;
        }
        PhysicalTablePath secondPath =
                samePartition ? PATH : PhysicalTablePath.of(PATH.getTablePath(), "other");
        Cluster secondCluster =
                samePartition ? resolved : cluster(4, true, PARTITION_ID + 1, secondPath);
        WriteRecord secondRecord =
                record(table, WriteFormat.COMPACTED_KV, secondKey, 1, secondPath);
        int secondBucket = assigner.assignBucket(secondRecord.getBucketKey(), 4);
        AtomicReference<Thread> firstThread = new AtomicReference<>();
        try {
            accumulator.append(
                    record(table, WriteFormat.COMPACTED_KV, 0, 1),
                    (bucket, offset, error) -> {},
                    initiallyResolved ? resolved : Cluster.empty());
            accumulator.ready(resolved);
            if (!initiallyResolved) {
                accumulator.append(
                        record(table, WriteFormat.COMPACTED_KV, 0, 1),
                        (bucket, offset, error) -> {},
                        resolved);
            }
            Deque<WriteBatch> firstDeque = accumulator.getReadyDeque(PATH, firstBucket);
            Future<?> firstAppend;
            Future<?> secondAppend;
            synchronized (firstDeque) {
                firstAppend =
                        executor.submit(
                                () -> {
                                    firstThread.set(Thread.currentThread());
                                    accumulator.append(
                                            record(table, WriteFormat.COMPACTED_KV, 0, 2),
                                            (bucket, offset, error) -> {},
                                            resolved);
                                    return null;
                                });
                retry(
                        Duration.ofSeconds(10),
                        () -> {
                            assertThat(firstThread.get()).isNotNull();
                            assertThat(firstThread.get().getState())
                                    .isEqualTo(Thread.State.BLOCKED);
                        });
                // Resolved normal buckets must not hold the context lock. Another bucket can
                // create and register a batch even when it belongs to the same partition.
                secondAppend =
                        executor.submit(
                                () -> {
                                    accumulator.append(
                                            secondRecord,
                                            (bucket, offset, error) -> {},
                                            secondCluster);
                                    return null;
                                });
                secondAppend.get(10, TimeUnit.SECONDS);
                assertThat(firstAppend.isDone()).isFalse();
            }
            firstAppend.get(10, TimeUnit.SECONDS);
            secondAppend.get(10, TimeUnit.SECONDS);
            assertThat(accumulator.getReadyDeque(secondPath, secondBucket)).hasSize(1);
        } finally {
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            accumulator.close();
            accumulator.abortAllBatches(new RuntimeException("test cleanup"));
            accumulator.destroyResources();
        }
    }

    @Test
    void testResolvedNewBatchesAppendIndependently() throws Exception {
        CompletableFuture<Void> creatingBatch = new CompletableFuture<>();
        CompletableFuture<Void> resumeAppend = new CompletableFuture<>();
        AtomicBoolean pauseNextBatch = new AtomicBoolean(true);
        Clock clock =
                new Clock() {
                    @Override
                    public long milliseconds() {
                        if (pauseNextBatch.compareAndSet(true, false)) {
                            creatingBatch.complete(null);
                            resumeAppend.join();
                        }
                        return System.currentTimeMillis();
                    }

                    @Override
                    public long nanoseconds() {
                        return System.nanoTime();
                    }
                };
        RecordAccumulator accumulator =
                accumulator(
                        ConfigOptions.NoKeyAssigner.STICKY,
                        64 * 1024,
                        TestingWriterMetricGroup.newInstance(),
                        clock);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        TableInfo table = tableInfo(2, true, true);
        Cluster resolved = cluster(2, true);
        HashBucketAssigner assigner = new HashBucketAssigner();
        int firstBucket = assigner.assignBucket(new byte[] {0}, 2);
        int secondKey = 1;
        while (assigner.assignBucket(new byte[] {(byte) secondKey}, 2) == firstBucket) {
            secondKey++;
        }
        WriteRecord secondRecord = record(table, WriteFormat.COMPACTED_KV, secondKey, 1);
        try {
            Future<?> firstAppend =
                    executor.submit(
                            () -> {
                                accumulator.append(
                                        record(table, WriteFormat.COMPACTED_KV, 0, 1),
                                        (bucket, offset, error) -> {},
                                        resolved);
                                return null;
                            });
            // Pause the first bucket while it creates a new batch, before registration.
            creatingBatch.get(10, TimeUnit.SECONDS);
            executor.submit(
                            () -> {
                                accumulator.append(
                                        secondRecord, (bucket, offset, error) -> {}, resolved);
                                return null;
                            })
                    .get(10, TimeUnit.SECONDS);
            assertThat(firstAppend.isDone()).isFalse();
            resumeAppend.complete(null);
            firstAppend.get(10, TimeUnit.SECONDS);
            assertThat(accumulator.getReadyDeque(PATH, 0)).hasSize(1);
            assertThat(accumulator.getReadyDeque(PATH, 1)).hasSize(1);
        } finally {
            resumeAppend.complete(null);
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            accumulator.close();
            accumulator.abortAllBatches(new RuntimeException("test cleanup"));
            accumulator.destroyResources();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testBatchRegistrationBlocksCloseOrHistoricalRouting(boolean historicalRouting)
            throws Exception {
        CompletableFuture<Void> creatingBatch = new CompletableFuture<>();
        CompletableFuture<Void> resumeAppend = new CompletableFuture<>();
        AtomicBoolean pauseClock = new AtomicBoolean();
        Clock clock =
                new Clock() {
                    @Override
                    public long milliseconds() {
                        if (pauseClock.get()) {
                            creatingBatch.complete(null);
                            resumeAppend.join();
                        }
                        return System.currentTimeMillis();
                    }

                    @Override
                    public long nanoseconds() {
                        return System.nanoTime();
                    }
                };
        RecordAccumulator accumulator =
                accumulator(
                        ConfigOptions.NoKeyAssigner.STICKY,
                        64 * 1024,
                        TestingWriterMetricGroup.newInstance(),
                        clock);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        TableInfo table = tableInfo(1, true, true, historicalRouting);
        accumulator.checkAndCacheHistoricalPartitionEnabled(table);
        Cluster resolved = cluster(1, true);
        CompletableFuture<Exception> completion = new CompletableFuture<>();
        AtomicReference<Thread> barrierThread = new AtomicReference<>();
        try {
            accumulator.append(
                    record(table, WriteFormat.COMPACTED_KV, 0, 1),
                    (bucket, offset, error) -> {},
                    resolved);
            accumulator.getReadyDeque(PATH, 0).peekLast().close();
            pauseClock.set(true);
            Future<?> append =
                    executor.submit(
                            () -> {
                                accumulator.append(
                                        record(table, WriteFormat.COMPACTED_KV, 0, 2),
                                        (bucket, offset, error) -> completion.complete(error),
                                        resolved);
                                return null;
                            });
            // Batch creation reads the clock after checking closed, before registering the
            // new batch as incomplete. Neither close nor a historical route switch may overtake
            // registration, even though ordinary resolved appends skip the context lock.
            creatingBatch.get(10, TimeUnit.SECONDS);
            Future<?> barrier =
                    executor.submit(
                            () -> {
                                barrierThread.set(Thread.currentThread());
                                if (historicalRouting) {
                                    accumulator.routeWritesTo(
                                            table,
                                            PATH,
                                            PhysicalTablePath.of(
                                                    PATH.getTablePath(),
                                                    HISTORICAL_PARTITION_VALUE),
                                            PARTITION_ID + 1);
                                } else {
                                    accumulator.close();
                                }
                            });
            retry(
                    Duration.ofSeconds(10),
                    () -> {
                        assertThat(barrierThread.get()).isNotNull();
                        assertThat(barrierThread.get().getState()).isEqualTo(Thread.State.BLOCKED);
                    });
            assertThat(barrier.isDone()).isFalse();
            resumeAppend.complete(null);
            append.get(10, TimeUnit.SECONDS);
            if (historicalRouting) {
                assertThatThrownBy(() -> barrier.get(10, TimeUnit.SECONDS))
                        .hasRootCauseInstanceOf(FlussRuntimeException.class)
                        .hasStackTraceContaining("incomplete writes");
                assertThat(accumulator.hasHistoricalWriteTarget(PATH)).isFalse();
                accumulator.close();
            } else {
                barrier.get(10, TimeUnit.SECONDS);
            }
            RuntimeException failure = new RuntimeException("fatal write failure");
            accumulator.abortAllBatches(failure);
            assertThat(completion.get(10, TimeUnit.SECONDS)).isSameAs(failure);
            assertThat(accumulator.hasIncomplete()).isFalse();
            assertThat(accumulator.hasUnDrained()).isFalse();
            assertThatThrownBy(
                            () ->
                                    accumulator.append(
                                            record(table, WriteFormat.COMPACTED_KV, 0, 3),
                                            (bucket, offset, error) -> {},
                                            resolved))
                    .hasMessageContaining("Writer closed");
        } finally {
            resumeAppend.complete(null);
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            accumulator.close();
            accumulator.abortAllBatches(new RuntimeException("test cleanup"));
            accumulator.destroyResources();
        }
    }

    private static Stream<Arguments> noKeyLayouts() {
        return Stream.of(WriteFormat.ARROW_LOG, WriteFormat.COMPACTED_LOG, WriteFormat.INDEXED_LOG)
                .flatMap(
                        format ->
                                Stream.of(1, 2, 3, 5)
                                        .flatMap(
                                                count ->
                                                        Stream.of(false, true)
                                                                .map(
                                                                        highBucket ->
                                                                                Arguments.of(
                                                                                        format,
                                                                                        count,
                                                                                        highBucket))));
    }

    private static Stream<Arguments> hashLayouts() {
        return Arrays.stream(WriteFormat.values())
                .flatMap(
                        format ->
                                Stream.of(
                                        Arguments.of(format, 2, 4),
                                        Arguments.of(format, 4, 2),
                                        Arguments.of(format, 2, 2)));
    }

    private static RecordAccumulator accumulator(ConfigOptions.NoKeyAssigner strategy) {
        return accumulator(strategy, 64 * 1024, TestingWriterMetricGroup.newInstance());
    }

    private static RecordAccumulator accumulator(
            ConfigOptions.NoKeyAssigner strategy,
            int memorySize,
            TestingWriterMetricGroup metrics) {
        return accumulator(strategy, memorySize, metrics, SystemClock.getInstance());
    }

    private static RecordAccumulator accumulator(
            ConfigOptions.NoKeyAssigner strategy,
            int memorySize,
            TestingWriterMetricGroup metrics,
            Clock clock) {
        return new RecordAccumulator(
                configuration(strategy, memorySize),
                new IdempotenceManager(false, 5, null, null),
                metrics,
                clock);
    }

    private static Configuration configuration(
            ConfigOptions.NoKeyAssigner strategy, int memorySize) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.CLIENT_WRITER_BUCKET_NO_KEY_ASSIGNER, strategy);
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, new MemorySize(256));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, new MemorySize(256));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, new MemorySize(memorySize));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT, Duration.ZERO);
        return conf;
    }

    private static TableInfo tableInfo(int count, boolean hash, boolean primaryKey) {
        return tableInfo(count, hash, primaryKey, false);
    }

    private static TableInfo tableInfo(
            int count, boolean hash, boolean primaryKey, boolean historicalPartitionEnabled) {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .column("dt", DataTypes.STRING());
        if (primaryKey) {
            schema.primaryKey("id", "dt");
        }
        TableDescriptor.Builder descriptor =
                TableDescriptor.builder()
                        .schema(schema.build())
                        .partitionedBy("dt")
                        .property(
                                ConfigOptions.TABLE_DATALAKE_HISTORICAL_PARTITION_ENABLED,
                                historicalPartitionEnabled);
        if (hash) {
            descriptor.distributedBy(count, "id");
        } else {
            descriptor.distributedBy(count);
        }
        return TableInfo.of(
                PATH.getTablePath(),
                10,
                1,
                descriptor.build(),
                "/tmp/fluss-routing",
                0,
                0,
                historicalPartitionEnabled ? 0 : 1);
    }

    private static Cluster cluster(int count, boolean includeCount) {
        return cluster(count, includeCount, PARTITION_ID);
    }

    private static Cluster cluster(int count, boolean includeCount, long partitionId) {
        return cluster(count, includeCount, partitionId, PATH);
    }

    private static Cluster cluster(
            int count, boolean includeCount, long partitionId, PhysicalTablePath path) {
        List<BucketLocation> locations = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            locations.add(
                    new BucketLocation(
                            path, new TableBucket(10, partitionId, i), 1, new int[] {1}));
        }
        return new Cluster(
                Collections.singletonMap(1, NODE),
                null,
                Collections.singletonMap(path, locations),
                Collections.singletonMap(path.getTablePath(), 10L),
                Collections.singletonMap(path, partitionId),
                includeCount
                        ? Collections.singletonMap(TableOrPartition.ofPartition(partitionId), count)
                        : Collections.emptyMap());
    }

    private static WriteRecord record(TableInfo table, WriteFormat format, int key, int version) {
        return record(table, format, key, version, PATH);
    }

    private static WriteRecord record(
            TableInfo table, WriteFormat format, int key, int version, PhysicalTablePath path) {
        Object[] fields = {key, version, path.getPartitionName()};
        byte[] bucketKey = table.getBucketKeys().isEmpty() ? null : new byte[] {(byte) key};
        switch (format) {
            case COMPACTED_KV:
            case INDEXED_KV:
                return WriteRecord.forUpsert(
                        table,
                        path,
                        format == WriteFormat.COMPACTED_KV
                                ? compactedRow(table.getRowType(), fields)
                                : indexedRow(table.getRowType(), fields),
                        bucketKey,
                        bucketKey,
                        format,
                        null);
            case COMPACTED_LOG:
                return WriteRecord.forCompactedAppend(
                        table, path, compactedRow(table.getRowType(), fields), bucketKey);
            case INDEXED_LOG:
                return WriteRecord.forIndexedAppend(
                        table, path, indexedRow(table.getRowType(), fields), bucketKey);
            case ARROW_LOG:
                return WriteRecord.forArrowAppend(table, path, row(fields), bucketKey);
            default:
                throw new IllegalArgumentException("Unexpected format: " + format);
        }
    }

    private static List<InternalRow> readRows(WriteBatch batch, TableInfo table, WriteFormat format)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        TestingSchemaGetter getter = new TestingSchemaGetter(1, table.getSchema());
        if (format.isKv()) {
            for (KvRecord record :
                    DefaultKvRecordBatch.pointToBytesView(batch.build())
                            .records(
                                    KvRecordReadContext.createReadContext(
                                            format.toKvFormat(), getter))) {
                rows.add(InternalRowUtils.copyRow(record.getRow(), table.getRowType()));
            }
        } else {
            LogRecordReadContext context;
            if (format == WriteFormat.ARROW_LOG) {
                context =
                        LogRecordReadContext.createArrowReadContext(table.getRowType(), 1, getter);
            } else if (format == WriteFormat.INDEXED_LOG) {
                context =
                        LogRecordReadContext.createIndexedReadContext(
                                table.getRowType(), 1, getter);
            } else {
                context =
                        LogRecordReadContext.createCompactedRowReadContext(
                                table.getRowType(), 1, getter);
            }
            try (LogRecordReadContext ignored = context;
                    CloseableIterator<LogRecord> records =
                            MemoryLogRecords.pointToBytesView(batch.build())
                                    .batchIterator()
                                    .next()
                                    .records(context)) {
                while (records.hasNext()) {
                    rows.add(InternalRowUtils.copyRow(records.next().getRow(), table.getRowType()));
                }
            }
        }
        return rows;
    }
}
