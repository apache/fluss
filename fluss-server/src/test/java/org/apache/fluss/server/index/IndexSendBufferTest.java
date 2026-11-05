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

package org.apache.fluss.server.index;

import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.PutIndexRequest;
import org.apache.fluss.rpc.messages.PutIndexResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexSendBufferTest {

    @Test
    void competingConstructionYieldsItsPagesSoAnotherReaderCanFinish() throws Exception {
        IndexSendBuffer buffer = new IndexSendBuffer(4096, 8192);
        CountDownLatch allocated = new CountDownLatch(2);
        CountDownLatch released = new CountDownLatch(1);
        ExecutorService readers = Executors.newFixedThreadPool(2);
        try {
            Future<?> first =
                    readers.submit(
                            () -> {
                                try (IndexSendBuffer.Reservation reservation =
                                        buffer.newReservation()) {
                                    reservation.allocatePage(4096);
                                    allocated.countDown();
                                    assertThat(allocated.await(5, TimeUnit.SECONDS)).isTrue();
                                    assertThatThrownBy(() -> reservation.allocatePage(4096))
                                            .isInstanceOf(
                                                    IndexSendBuffer.BufferFullException.class);
                                } finally {
                                    released.countDown();
                                }
                                return null;
                            });
            Future<?> second =
                    readers.submit(
                            () -> {
                                try (IndexSendBuffer.Reservation reservation =
                                        buffer.newReservation()) {
                                    reservation.allocatePage(4096);
                                    allocated.countDown();
                                    assertThat(allocated.await(5, TimeUnit.SECONDS)).isTrue();
                                    assertThat(released.await(5, TimeUnit.SECONDS)).isTrue();
                                    reservation.allocatePage(4096);
                                    assertThat(buffer.pendingBytes()).isEqualTo(8192);
                                }
                                return null;
                            });
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
            assertThat(buffer.pendingBytes()).isZero();
        } finally {
            readers.shutdownNow();
        }
    }

    @Test
    void admissionBackpressureWakesReadersAfterAcknowledgement() {
        IndexSendBuffer buffer = new IndexSendBuffer(1, 1);
        AtomicInteger wakeups = new AtomicInteger();
        buffer.setCapacityListener(wakeups::incrementAndGet);
        try (IndexReplicator owner = owner(buffer, 0)) {
            IndexBatch batch = batch(owner, new TableBucket(2L, 0));
            assertThat(buffer.tryAppendWindow(Collections.singletonList(batch))).isTrue();
            assertThat(buffer.isFull()).isTrue();
            assertThat(buffer.claimFirstReady(batch.targetBucket())).isSameAs(batch);
            assertThat(buffer.acknowledgeClaim(batch)).isTrue();
            assertThat(wakeups).hasValue(1);
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @Test
    void constructionReservationTransfersToAdmissionAndReleasesOnAck() {
        IndexSendBuffer buffer = new IndexSendBuffer(1, 4096);
        try (IndexReplicator owner = owner(buffer, 0);
                IndexSendBuffer.Reservation reservation = buffer.newReservation()) {
            MemorySegment page = reservation.allocatePage(4096);
            assertThat(buffer.pendingBytes()).isEqualTo(4096);
            IndexBatch batch =
                    new IndexBatch(
                            new TableBucket(2L, 0),
                            new byte[0],
                            new MemorySegmentBytesView(page, 0, 1),
                            4096,
                            new IndexReplicationWindow("idx", 1L, 1, owner));
            assertThat(buffer.tryAppendWindow(Collections.singletonList(batch), reservation))
                    .isTrue();
            reservation.close();
            assertThat(buffer.pendingBytes()).isEqualTo(4096);
            assertThat(buffer.claimFirstReady(batch.targetBucket())).isSameAs(batch);
            assertThat(buffer.acknowledgeClaim(batch)).isTrue();
            batch.window().onBatchAcked();
            assertThat(owner.getAllIndexPushedOffset()).isEqualTo(1L);
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @Test
    void abandonedConstructionReleasesBudgetAndWakesWaiters() {
        IndexSendBuffer buffer = new IndexSendBuffer(4096, 4096);
        AtomicInteger wakeups = new AtomicInteger();
        buffer.setCapacityListener(wakeups::incrementAndGet);
        try (IndexSendBuffer.Reservation first = buffer.newReservation();
                IndexSendBuffer.Reservation second = buffer.newReservation()) {
            first.allocatePage(4096);
            assertThatThrownBy(() -> second.allocatePage(4096))
                    .isInstanceOf(IndexSendBuffer.BufferFullException.class);
            assertThat(buffer.pendingBytes()).isEqualTo(4096);
            first.close();
            assertThat(wakeups).hasValue(1);
            second.allocatePage(4096);
            assertThatThrownBy(() -> second.allocatePage(4096))
                    .isInstanceOf(RecordTooLargeException.class)
                    .hasMessageContaining("index.replication.buffer.max-bytes");
            assertThat(buffer.pendingBytes()).isEqualTo(4096);
        }
        assertThat(buffer.pendingBytes()).isZero();
    }

    @Test
    void oversizedTransportBatchKeepsItsOwnerActiveWhileReportingFailure() throws Exception {
        IndexSendBuffer buffer = new IndexSendBuffer();
        TabletServerMetricGroup metrics =
                new TabletServerMetricGroup(NOPMetricRegistry.INSTANCE, "test", "rack", "host", 0);
        try (IndexReplicator owner = owner(buffer, 0)) {
            IndexBatch batch = batch(owner, new TableBucket(2L, 0));
            buffer.tryAppendWindow(Collections.singletonList(batch));
            try (IndexSender sender =
                    new IndexSender(
                            buffer,
                            (table, bucket) -> OptionalInt.of(0),
                            server -> {
                                throw new AssertionError("Oversized requests must stay local");
                            },
                            metrics,
                            1,
                            1,
                            1,
                            1,
                            1000)) {
                retry(
                        Duration.ofSeconds(5),
                        () ->
                                assertThat(metrics.indexReplicationRetries().getCount())
                                        .isGreaterThanOrEqualTo(2));
                assertThat(batch.window().isActive()).isTrue();
                assertThat(owner.isFailed()).isTrue();
                assertThat(owner.isClosed()).isFalse();
                assertThat(owner.getAllIndexPushedOffset()).isZero();
                assertThat(buffer.pendingBytes()).isEqualTo(1);
            }
        } finally {
            metrics.close();
        }
        assertThat(buffer.pendingBytes()).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void targetErrorIsCountedAndRecoversAfterOperatorRepair(boolean rpcFailure) throws Exception {
        IndexSendBuffer buffer = new IndexSendBuffer();
        AtomicBoolean repaired = new AtomicBoolean();
        TabletServerMetricGroup metrics =
                new TabletServerMetricGroup(NOPMetricRegistry.INSTANCE, "test", "rack", "host", 0);
        TestingTabletGatewayService gateway =
                new TestingTabletGatewayService() {
                    @Override
                    public CompletableFuture<PutIndexResponse> putIndex(PutIndexRequest request) {
                        if (!repaired.get() && rpcFailure) {
                            return FutureUtils.completedExceptionally(
                                    Errors.INVALID_TABLE_EXCEPTION.exception(
                                            "Index configuration needs repair"));
                        }
                        PutIndexResponse response = new PutIndexResponse();
                        response.addBucketsResp().setBucketId(0);
                        if (!repaired.get()) {
                            response.getBucketsRespAt(0)
                                    .setErrorCode(Errors.INVALID_TABLE_EXCEPTION.code())
                                    .setErrorMessage("Index configuration needs repair");
                        }
                        return CompletableFuture.completedFuture(response);
                    }
                };
        try (IndexReplicator owner = owner(buffer, 0)) {
            IndexBatch batch = batch(owner, new TableBucket(2L, 0));
            buffer.tryAppendWindow(Collections.singletonList(batch));
            try (IndexSender sender =
                    new IndexSender(
                            buffer,
                            (table, bucket) -> OptionalInt.of(0),
                            server -> gateway,
                            metrics,
                            1,
                            1,
                            1024,
                            1024,
                            1000)) {
                retry(
                        Duration.ofSeconds(5),
                        () ->
                                assertThat(metrics.indexReplicationRetries().getCount())
                                        .isGreaterThanOrEqualTo(2));
                assertThat(owner.getAllIndexPushedOffset()).isZero();
                assertThat(metrics.indexReplicationFailures().getCount()).isGreaterThanOrEqualTo(2);
                repaired.set(true);
                retry(
                        Duration.ofSeconds(5),
                        () -> assertThat(owner.getAllIndexPushedOffset()).isEqualTo(1L));
                assertThat(buffer.pendingBytes()).isZero();
            }
        } finally {
            metrics.close();
        }
    }

    @Test
    void sourceCleanupReschedulesRemainingBatchAndFencesLateCompletion() throws Exception {
        IndexSendBuffer buffer = new IndexSendBuffer();
        Queue<TableBucket> ready = new ArrayDeque<>();
        buffer.setAppendListener(ready::add);
        TableBucket target = new TableBucket(2L, 0);
        try (IndexReplicator first = owner(buffer, 0);
                IndexReplicator second = owner(buffer, 1)) {
            IndexBatch firstBatch = batch(first, target);
            IndexBatch secondBatch = batch(second, target);
            assertThat(buffer.tryAppendWindow(Collections.singletonList(firstBatch))).isTrue();
            assertThat(buffer.claimFirstReady(ready.remove())).isSameAs(firstBatch);
            assertThat(buffer.tryAppendWindow(Collections.singletonList(secondBatch))).isTrue();
            assertThat(buffer.hasClaim(ready.remove())).isTrue();
            assertThat(ready).isEmpty();

            first.close();

            assertThat(ready).containsExactly(target);
            assertThat(buffer.pendingBytes()).isEqualTo(1L);
            assertThat(buffer.claimFirstReady(ready.remove())).isSameAs(secondBatch);
            assertThat(buffer.acknowledgeClaim(firstBatch)).isFalse();
            assertThat(buffer.retryClaim(firstBatch)).isFalse();
            assertThat(buffer.ownsClaim(secondBatch)).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(1L);
            assertThat(buffer.acknowledgeClaim(secondBatch)).isTrue();
            secondBatch.window().onBatchAcked();
            assertThat(second.getAllIndexPushedOffset()).isEqualTo(1L);
            assertThat(buffer.pendingBytes()).isZero();
            assertThat(buffer.buckets()).isEmpty();
        }
    }

    private static IndexBatch batch(IndexReplicator owner, TableBucket target) {
        return new IndexBatch(
                target,
                new byte[0],
                new MemorySegmentBytesView(MemorySegment.wrap(new byte[1]), 0, 1),
                1L,
                new IndexReplicationWindow("idx", 1L, 1, owner));
    }

    private static IndexReplicator owner(IndexSendBuffer buffer, int bucket) {
        IndexSourceReader.SourceLog log =
                new IndexSourceReader.SourceLog() {
                    @Override
                    public TableBucket tableBucket() {
                        return new TableBucket(1L, bucket);
                    }

                    @Override
                    public long highWatermark() {
                        return 1L;
                    }

                    @Override
                    public long logStartOffset() {
                        return 0L;
                    }

                    @Override
                    public FetchDataInfo read(
                            long offset,
                            int maxBytes,
                            FetchIsolation isolation,
                            boolean minOneMessage) {
                        throw new UnsupportedOperationException();
                    }
                };
        LogRecordReadContext context =
                LogRecordReadContext.createCompactedRowReadContext(
                        RowType.of(DataTypes.INT()), 1, null);
        return new IndexReplicator(
                new IndexSourceReader(log, null, Runnable::run, context),
                Collections.emptyList(),
                buffer,
                context,
                0L,
                1024,
                (syncOffset, allOffset) -> {});
    }
}
