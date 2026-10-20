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

package org.apache.fluss.client.write;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/** Tests how explicit bucket writes resolve a partition's actual layout. */
class WriterClientTest {
    private static final PhysicalTablePath PATH =
            PhysicalTablePath.of(TablePath.of("db", "writer_client"), "p");
    private static final long TABLE_ID = 10;
    private static final long PARTITION_ID = 20;
    private static final TableInfo TABLE =
            TableInfo.of(
                    PATH.getTablePath(),
                    TABLE_ID,
                    1,
                    TableDescriptor.builder()
                            .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                            .distributedBy(2)
                            .partitionedBy("id")
                            .build(),
                    "/tmp/fluss-writer-client",
                    0,
                    0,
                    1);

    @Test
    void testCachedValidBucketDoesNotRefresh() throws Exception {
        Cluster cached = cluster(2);
        MetadataUpdater updater = mock(MetadataUpdater.class);
        when(updater.getCluster()).thenReturn(cached);

        assertThat(
                        WriterClient.waitForBucketMetadata(
                                record(1), PATH, updater, false, Duration.ZERO, () -> {}))
                .isSameAs(cached);
        verify(updater).getCluster();
        verifyNoMoreInteractions(updater);
    }

    @Test
    void testUnknownPartitionResolvesAfterRefresh() throws Exception {
        Cluster resolved = cluster(4);
        AtomicReference<Cluster> snapshot = new AtomicReference<>(Cluster.empty());
        MetadataUpdater updater = updater(snapshot, resolved);

        Cluster actual =
                WriterClient.waitForBucketMetadata(
                        record(3), PATH, updater, false, Duration.ofSeconds(1), () -> {});

        assertThat(actual).isSameAs(resolved);
        assertThat(actual.getBucketCount(TableOrPartition.ofPartition(PARTITION_ID))).contains(4);
        verify(updater).updatePhysicalTableMetadata(PATH);
    }

    @Test
    void testCachedOutOfRangeBucketRefreshesLayout() throws Exception {
        Cluster resolved = cluster(4, PARTITION_ID + 1);
        AtomicReference<Cluster> snapshot = new AtomicReference<>(cluster(2));
        MetadataUpdater updater = updater(snapshot, resolved);

        Cluster actual =
                WriterClient.waitForBucketMetadata(
                        record(3), PATH, updater, false, Duration.ofSeconds(1), () -> {});

        assertThat(actual).isSameAs(resolved);
        assertThat(actual.getBucketCount(TableOrPartition.ofPartition(PARTITION_ID + 1)))
                .contains(4);
        verify(updater).updatePhysicalTableMetadata(PATH);
    }

    @Test
    void testFailedRefreshDoesNotAcceptCachedOutOfRangeLayout() throws Exception {
        Cluster resolved = cluster(4, PARTITION_ID + 1);
        AtomicReference<Cluster> snapshot = new AtomicReference<>(cluster(2));
        MetadataUpdater updater = mock(MetadataUpdater.class);
        when(updater.getCluster()).thenAnswer(ignored -> snapshot.get());
        AtomicInteger refreshes = new AtomicInteger();
        doAnswer(
                        ignored -> {
                            if (refreshes.incrementAndGet() > 1) {
                                snapshot.set(resolved);
                                return resolved;
                            }
                            return null;
                        })
                .when(updater)
                .updatePhysicalTableMetadata(PATH);

        assertThat(
                        WriterClient.waitForBucketMetadata(
                                record(3), PATH, updater, false, Duration.ofSeconds(1), () -> {}))
                .isSameAs(resolved);
        assertThat(refreshes).hasValue(2);
    }

    @Test
    void testMissingMetadataTimesOut() {
        MetadataUpdater updater = mock(MetadataUpdater.class);
        when(updater.getCluster()).thenReturn(Cluster.empty());

        assertThatThrownBy(
                        () ->
                                WriterClient.waitForBucketMetadata(
                                        record(3), PATH, updater, false, Duration.ZERO, () -> {}))
                .isInstanceOf(TimeoutException.class)
                .hasMessageContaining(PATH.toString());
        verify(updater, never()).updatePhysicalTableMetadata(PATH);
    }

    @Test
    void testInterruptedWhileWaiting() {
        MetadataUpdater updater = mock(MetadataUpdater.class);
        when(updater.getCluster()).thenReturn(Cluster.empty());

        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(
                            () ->
                                    WriterClient.waitForBucketMetadata(
                                            record(3),
                                            PATH,
                                            updater,
                                            false,
                                            Duration.ofSeconds(1),
                                            () -> {}))
                    .isInstanceOf(InterruptedException.class);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void testStopsWhenWriterClosesDuringMetadataWait() {
        MetadataUpdater updater = mock(MetadataUpdater.class);
        when(updater.getCluster()).thenReturn(Cluster.empty());
        AtomicInteger refreshes = new AtomicInteger();
        when(updater.updatePhysicalTableMetadata(PATH))
                .thenAnswer(
                        ignored -> {
                            refreshes.incrementAndGet();
                            return null;
                        });
        Runnable checkWriterState =
                () -> {
                    if (refreshes.get() > 0) {
                        throw new IllegalStateException("writer closed");
                    }
                };

        assertThatThrownBy(
                        () ->
                                WriterClient.waitForBucketMetadata(
                                        record(3),
                                        PATH,
                                        updater,
                                        false,
                                        Duration.ofSeconds(1),
                                        checkWriterState))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("writer closed");
        assertThat(refreshes).hasValue(1);
        verify(updater).updatePhysicalTableMetadata(PATH);
    }

    @Test
    void testWrappedMetadataInterruptionRestoresThreadFlag() {
        MetadataUpdater updater = mock(MetadataUpdater.class);
        when(updater.getCluster()).thenReturn(Cluster.empty());
        FlussRuntimeException interrupted =
                new FlussRuntimeException("metadata", new InterruptedException());
        doThrow(interrupted).when(updater).updatePhysicalTableMetadata(PATH);

        try {
            assertThatThrownBy(
                            () ->
                                    WriterClient.waitForBucketMetadata(
                                            record(3),
                                            PATH,
                                            updater,
                                            false,
                                            Duration.ofSeconds(1),
                                            () -> {}))
                    .isSameAs(interrupted);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void testPartitionAppearsAfterInitialMissingMetadata() throws Exception {
        Cluster resolved = cluster(4);
        AtomicReference<Cluster> snapshot = new AtomicReference<>(Cluster.empty());
        MetadataUpdater updater = mock(MetadataUpdater.class);
        when(updater.getCluster()).thenAnswer(ignored -> snapshot.get());
        AtomicInteger refreshes = new AtomicInteger();
        doAnswer(
                        ignored -> {
                            if (refreshes.incrementAndGet() == 1) {
                                throw new PartitionNotExistException("not created yet");
                            }
                            snapshot.set(resolved);
                            return resolved;
                        })
                .when(updater)
                .updatePhysicalTableMetadata(PATH);

        Cluster actual =
                WriterClient.waitForBucketMetadata(
                        record(3), PATH, updater, true, Duration.ofSeconds(1), () -> {});

        assertThat(actual).isSameAs(resolved);
        assertThat(actual.getBucketCount(TableOrPartition.ofPartition(PARTITION_ID))).contains(4);
        assertThat(refreshes).hasValue(2);
    }

    @Test
    void testSuccessfulRefreshCanRejectOutOfRangeBucket() throws Exception {
        Cluster refreshed = cluster(2);
        MetadataUpdater updater = updater(new AtomicReference<>(cluster(2)), refreshed);

        assertThat(
                        WriterClient.waitForBucketMetadata(
                                record(3), PATH, updater, false, Duration.ofSeconds(1), () -> {}))
                .isSameAs(refreshed);
        verify(updater).updatePhysicalTableMetadata(PATH);
    }

    @Test
    void testMissingPartitionWithoutCreationFailsImmediately() {
        Cluster cached = cluster(2).invalidPhysicalTableBucketMeta(Collections.singleton(PATH));
        assertThat(cached.getPartitionId(PATH)).contains(PARTITION_ID);
        MetadataUpdater updater = mock(MetadataUpdater.class);
        when(updater.getCluster()).thenReturn(cached);
        PartitionNotExistException missing = new PartitionNotExistException("partition dropped");
        when(updater.updatePhysicalTableMetadata(PATH)).thenThrow(missing);

        assertThatThrownBy(
                        () ->
                                WriterClient.waitForBucketMetadata(
                                        record(1),
                                        PATH,
                                        updater,
                                        false,
                                        Duration.ofSeconds(1),
                                        () -> {}))
                .isSameAs(missing);
        verify(updater).updatePhysicalTableMetadata(PATH);
    }

    @Test
    void testConcurrentWaitersReuseResolvedLayout() throws Exception {
        Cluster resolved = cluster(4);
        AtomicReference<Cluster> snapshot = new AtomicReference<>(Cluster.empty());
        MetadataUpdater updater = mock(MetadataUpdater.class);
        when(updater.getCluster()).thenAnswer(ignored -> snapshot.get());
        CompletableFuture<Void> refreshing = new CompletableFuture<>();
        CompletableFuture<Void> resumeRefresh = new CompletableFuture<>();
        when(updater.updatePhysicalTableMetadata(PATH))
                .thenAnswer(
                        ignored -> {
                            refreshing.complete(null);
                            resumeRefresh.get(10, TimeUnit.SECONDS);
                            snapshot.set(resolved);
                            return resolved;
                        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicReference<Thread> waitingThread = new AtomicReference<>();
        try {
            Future<Cluster> first =
                    executor.submit(
                            () ->
                                    WriterClient.waitForBucketMetadata(
                                            record(3),
                                            PATH,
                                            updater,
                                            true,
                                            Duration.ofSeconds(10),
                                            () -> {}));
            refreshing.get(10, TimeUnit.SECONDS);
            Future<Cluster> second =
                    executor.submit(
                            () -> {
                                waitingThread.set(Thread.currentThread());
                                return WriterClient.waitForBucketMetadata(
                                        record(3),
                                        PATH,
                                        updater,
                                        true,
                                        Duration.ofSeconds(10),
                                        () -> {});
                            });
            retry(
                    Duration.ofSeconds(10),
                    () -> {
                        assertThat(waitingThread.get()).isNotNull();
                        assertThat(waitingThread.get().getState()).isEqualTo(Thread.State.BLOCKED);
                    });
            resumeRefresh.complete(null);
            assertThat(first.get(10, TimeUnit.SECONDS)).isSameAs(resolved);
            assertThat(second.get(10, TimeUnit.SECONDS)).isSameAs(resolved);
            verify(updater).updatePhysicalTableMetadata(PATH);
        } finally {
            resumeRefresh.complete(null);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testExpiredWaiterDoesNotRefreshAfterAcquiringLock() throws Exception {
        MetadataUpdater updater = mock(MetadataUpdater.class);
        when(updater.getCluster()).thenReturn(Cluster.empty());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<Thread> waitingThread = new AtomicReference<>();
        AtomicReference<Long> waitStarted = new AtomicReference<>();
        Duration timeout = Duration.ofMillis(100);
        try {
            Future<Cluster> result;
            synchronized (updater) {
                result =
                        executor.submit(
                                () -> {
                                    waitingThread.set(Thread.currentThread());
                                    return WriterClient.waitForBucketMetadata(
                                            record(3),
                                            PATH,
                                            updater,
                                            false,
                                            timeout,
                                            () ->
                                                    waitStarted.compareAndSet(
                                                            null, System.nanoTime()));
                                });
                retry(
                        Duration.ofSeconds(10),
                        () -> {
                            assertThat(waitingThread.get()).isNotNull();
                            assertThat(waitingThread.get().getState())
                                    .isEqualTo(Thread.State.BLOCKED);
                            assertThat(waitStarted.get()).isNotNull();
                            assertThat(System.nanoTime() - waitStarted.get())
                                    .isGreaterThan(timeout.toNanos());
                        });
            }
            assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(TimeoutException.class);
            verify(updater, never()).updatePhysicalTableMetadata(PATH);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static MetadataUpdater updater(
            AtomicReference<Cluster> snapshot, Cluster afterRefresh) {
        MetadataUpdater updater = mock(MetadataUpdater.class);
        when(updater.getCluster()).thenAnswer(ignored -> snapshot.get());
        doAnswer(
                        ignored -> {
                            snapshot.set(afterRefresh);
                            return afterRefresh;
                        })
                .when(updater)
                .updatePhysicalTableMetadata(PATH);
        return updater;
    }

    private static WriteRecord record(int bucketId) {
        return WriteRecord.forArrowAppend(TABLE, PATH, row(1), null).withBucket(bucketId);
    }

    private static Cluster cluster(int bucketCount) {
        return cluster(bucketCount, PARTITION_ID);
    }

    private static Cluster cluster(int bucketCount, long partitionId) {
        return new Cluster(
                Collections.emptyMap(),
                null,
                Collections.emptyMap(),
                Collections.singletonMap(PATH.getTablePath(), TABLE_ID),
                Collections.singletonMap(PATH, partitionId),
                Collections.singletonMap(TableOrPartition.ofPartition(partitionId), bucketCount));
    }
}
