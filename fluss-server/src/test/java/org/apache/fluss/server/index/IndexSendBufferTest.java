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
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.PutIndexRequest;
import org.apache.fluss.rpc.messages.PutIndexResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.MessageCodec;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexSendBufferTest {

    @Test
    void competingConstructionYieldsItsPagesSoAnotherReaderCanFinish() throws Exception {
        IndexSendBuffer buffer = new IndexSendBuffer(8192);
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
        IndexSendBuffer buffer = new IndexSendBuffer(1);
        AtomicInteger wakeups = new AtomicInteger();
        buffer.setCapacityListener(wakeups::incrementAndGet);
        try (IndexReplicator owner = owner(buffer, 0)) {
            IndexBatch batch = batch(owner, new TableBucket(2L, 0));
            assertThat(buffer.tryAppendWindow(Collections.singletonList(batch))).isTrue();
            assertThat(buffer.requestCapacityWakeupIfFull()).isTrue();
            assertThat(buffer.claimFirstReady(batch.targetBucket())).isSameAs(batch);
            assertThat(buffer.completeClaim(batch)).isTrue();
            assertThat(wakeups).hasValue(1);
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @Test
    void constructionReservationTransfersToAdmissionAndReleasesOnAck() {
        IndexSendBuffer buffer = new IndexSendBuffer(4096);
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
                            window(owner));
            assertThat(buffer.tryAppendWindow(Collections.singletonList(batch), reservation))
                    .isTrue();
            reservation.close();
            assertThat(buffer.pendingBytes()).isEqualTo(4096);
            assertThat(buffer.claimFirstReady(batch.targetBucket())).isSameAs(batch);
            assertThat(buffer.completeClaim(batch)).isTrue();
            batch.window().onBatchAcked();
            assertThat(owner.getAllIndexPushedOffset()).isEqualTo(1L);
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @Test
    void abandonedConstructionReleasesBudgetAndWakesWaiters() {
        IndexSendBuffer buffer = new IndexSendBuffer(4096);
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
                assertThat(owner.hasFailure()).isTrue();
                assertThat(owner.isClosed()).isFalse();
                assertThat(owner.getAllIndexPushedOffset()).isZero();
                assertThat(buffer.pendingBytes()).isEqualTo(1);
            }
        } finally {
            metrics.close();
        }
        assertThat(buffer.pendingBytes()).isZero();
    }

    @Test
    void unresponsiveTargetTimesOutAndRecovers() throws Exception {
        IndexSendBuffer buffer = new IndexSendBuffer();
        AtomicBoolean repaired = new AtomicBoolean();
        AtomicReference<CompletableFuture<PutIndexResponse>> firstRequest = new AtomicReference<>();
        TabletServerMetricGroup metrics =
                new TabletServerMetricGroup(NOPMetricRegistry.INSTANCE, "test", "rack", "host", 0);
        TestingTabletGatewayService gateway =
                new TestingTabletGatewayService() {
                    @Override
                    public CompletableFuture<PutIndexResponse> putIndex(PutIndexRequest request) {
                        if (!repaired.get()) {
                            CompletableFuture<PutIndexResponse> pending = new CompletableFuture<>();
                            firstRequest.compareAndSet(null, pending);
                            return pending;
                        }
                        PutIndexResponse response = new PutIndexResponse();
                        response.addBucketsResp().setBucketId(0);
                        return CompletableFuture.completedFuture(response);
                    }
                };
        try (IndexReplicator owner = owner(buffer, 0)) {
            buffer.tryAppendWindow(Collections.singletonList(batch(owner, new TableBucket(2L, 0))));
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
                            50)) {
                retry(
                        Duration.ofSeconds(5),
                        () ->
                                assertThat(metrics.indexReplicationRetries().getCount())
                                        .isPositive());
                assertThat(metrics.indexReplicationFailures().getCount()).isPositive();
                assertThatThrownBy(() -> firstRequest.get().join())
                        .hasCauseInstanceOf(TimeoutException.class);
                assertThat(owner.getAllIndexPushedOffset()).isZero();
                assertThat(buffer.pendingBytes()).isEqualTo(1);
                repaired.set(true);
                retry(
                        Duration.ofSeconds(5),
                        () -> assertThat(owner.getAllIndexPushedOffset()).isEqualTo(1));
                assertThat(buffer.pendingBytes()).isZero();
            }
        } finally {
            metrics.close();
        }
    }

    @Test
    void unknownLeaderIsReportedWithoutLoggingEveryRetryAndRecovers() throws Exception {
        Queue<LogEvent> events = new ConcurrentLinkedQueue<>();
        AbstractAppender appender =
                new AbstractAppender("index-leader-test", null, null, true, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        events.add(event.toImmutable());
                    }
                };
        appender.start();
        LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        Configuration logging = loggerContext.getConfiguration();
        String loggerName = IndexSender.class.getName();
        LoggerConfig previous = logging.getLoggers().get(loggerName);
        LoggerConfig logger = new LoggerConfig(loggerName, Level.WARN, false);
        logger.addAppender(appender, Level.WARN, null);
        logging.addLogger(loggerName, logger);
        loggerContext.updateLoggers();
        IndexSendBuffer buffer = new IndexSendBuffer();
        AtomicBoolean leaderAvailable = new AtomicBoolean();
        AtomicInteger requests = new AtomicInteger();
        TabletServerMetricGroup metrics =
                new TabletServerMetricGroup(NOPMetricRegistry.INSTANCE, "test", "rack", "host", 0);
        TestingTabletGatewayService gateway =
                new TestingTabletGatewayService() {
                    @Override
                    public CompletableFuture<PutIndexResponse> putIndex(PutIndexRequest request) {
                        requests.incrementAndGet();
                        PutIndexResponse response = new PutIndexResponse();
                        response.addBucketsResp().setBucketId(0);
                        return CompletableFuture.completedFuture(response);
                    }
                };
        try (IndexReplicator owner = owner(buffer, 0)) {
            TableBucket target = new TableBucket(2L, 0);
            buffer.tryAppendWindow(Collections.singletonList(batch(owner, target)));
            try (IndexSender sender =
                    new IndexSender(
                            buffer,
                            (table, bucket) ->
                                    leaderAvailable.get() ? OptionalInt.of(0) : OptionalInt.empty(),
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
                assertThat(requests).hasValue(0);
                assertThat(owner.getAllIndexPushedOffset()).isZero();
                assertThat(owner.hasFailure()).isTrue();
                leaderAvailable.set(true);
                retry(
                        Duration.ofSeconds(5),
                        () -> assertThat(owner.getAllIndexPushedOffset()).isEqualTo(1));
                assertThat(buffer.pendingBytes()).isZero();
            }
            assertThat(events)
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                                assertThat(event.getMessage().getFormattedMessage())
                                        .contains(
                                                target.toString(),
                                                owner.sourceBucket().toString(),
                                                "end offset 1");
                            });
        } finally {
            metrics.close();
            logging.removeLogger(loggerName);
            if (previous != null) {
                logging.addLogger(loggerName, previous);
            }
            loggerContext.updateLoggers();
            appender.stop();
        }
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void sourceCleanupReschedulesRemainingBatchAndFencesLateCompletion(boolean successorQueued)
            throws Exception {
        IndexSendBuffer buffer = new IndexSendBuffer();
        Queue<TableBucket> ready = new ArrayDeque<>();
        buffer.setReadyBucketListener(ready::add);
        TableBucket target = new TableBucket(2L, 0);
        try (IndexReplicator first = owner(buffer, 0);
                IndexReplicator second = owner(buffer, 1)) {
            IndexBatch firstBatch = batch(first, target);
            IndexBatch secondBatch = batch(second, target);
            assertThat(buffer.tryAppendWindow(Collections.singletonList(firstBatch))).isTrue();
            assertThat(buffer.claimFirstReady(ready.remove())).isSameAs(firstBatch);
            if (successorQueued) {
                assertThat(buffer.tryAppendWindow(Collections.singletonList(secondBatch))).isTrue();
                assertThat(buffer.hasClaim(ready.remove())).isTrue();
            }
            assertThat(ready).isEmpty();

            first.close();

            assertThat(ready).containsExactly(target);
            if (!successorQueued) {
                assertThat(buffer.buckets()).isEmpty();
                assertThat(buffer.pendingBytes()).isZero();
                ready.clear();
                assertThat(buffer.tryAppendWindow(Collections.singletonList(secondBatch))).isTrue();
            }
            assertThat(buffer.pendingBytes()).isEqualTo(1L);
            assertThat(buffer.claimFirstReady(ready.remove())).isSameAs(secondBatch);
            assertThat(buffer.completeClaim(firstBatch)).isFalse();
            assertThat(buffer.retryClaim(firstBatch)).isFalse();
            assertThat(buffer.ownsClaim(secondBatch)).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(1L);
            assertThat(buffer.completeClaim(secondBatch)).isTrue();
            secondBatch.window().onBatchAcked();
            assertThat(second.getAllIndexPushedOffset()).isEqualTo(1L);
            assertThat(buffer.pendingBytes()).isZero();
            assertThat(buffer.buckets()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {127, 128, 16383, 16384})
    void requestSizeMatchesEncodedFrame(int payloadBytes) {
        IndexSendBuffer buffer = new IndexSendBuffer();
        try (IndexReplicator source = owner(buffer, 0)) {
            long timeoutMs = 128L;
            IndexReplicationWindow window =
                    new IndexReplicationWindow(source.indexStates().get(0), 128L, 2, source);
            List<IndexBatch> batches = new ArrayList<>();
            IndexRequestFactory.SizeAccumulator size =
                    new IndexRequestFactory.SizeAccumulator(2L, 1L, timeoutMs);
            for (int bucket = 0; bucket < 2; bucket++) {
                IndexBatch batch =
                        new IndexBatch(
                                new TableBucket(2L, bucket),
                                new byte[128],
                                new MemorySegmentBytesView(
                                        MemorySegment.wrap(new byte[payloadBytes]),
                                        0,
                                        payloadBytes),
                                payloadBytes,
                                window);
                batches.add(batch);
                size.addBucket(batch);
            }
            PutIndexRequest request = IndexRequestFactory.build(2L, batches, timeoutMs);
            ByteBuf encoded =
                    MessageCodec.encodeRequest(
                            UnpooledByteBufAllocator.DEFAULT,
                            ApiKeys.PUT_INDEX.id,
                            (short) 0,
                            1,
                            request);
            try {
                assertThat(size.framedBytes()).isEqualTo(encoded.readableBytes());
                assertThat(size.exceedsLimit(encoded.readableBytes())).isFalse();
                assertThat(size.exceedsLimit(encoded.readableBytes() - 1L)).isTrue();
            } finally {
                encoded.release();
            }
        }
    }

    private static IndexBatch batch(IndexReplicator owner, TableBucket target) {
        return new IndexBatch(
                target,
                new byte[0],
                new MemorySegmentBytesView(MemorySegment.wrap(new byte[1]), 0, 1),
                1L,
                window(owner));
    }

    private static IndexReplicationWindow window(IndexReplicator owner) {
        IndexReplicator.IndexReplicationState index = owner.indexStates().get(0);
        IndexReplicationWindow window = new IndexReplicationWindow(index, 1L, 1, owner);
        index.installWindow(window);
        return window;
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
        IndexSpec spec =
                new IndexSpec(
                        "idx",
                        IndexVisibility.SYNC,
                        2L,
                        1,
                        KvFormat.COMPACTED,
                        new int[] {0},
                        row -> {
                            throw new AssertionError("unexpected source read");
                        },
                        (source, target, end) -> {
                            throw new AssertionError("unexpected source read");
                        });
        IndexReplicationScheduler scheduler = new IndexReplicationScheduler(1);
        IndexReplicator owner =
                new IndexReplicator(
                        new IndexSourceReader(log, null, Runnable::run),
                        Collections.singletonList(spec),
                        buffer,
                        context,
                        0L,
                        1024,
                        (syncOffset, allOffset) -> {},
                        scheduler,
                        () -> {});
        scheduler.register(0, owner);
        return owner;
    }
}
