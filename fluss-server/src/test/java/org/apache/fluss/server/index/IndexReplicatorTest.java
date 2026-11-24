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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.PutIndexRequest;
import org.apache.fluss.rpc.messages.PutIndexResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.index.TestingIndexSourceLog.RemoteSessions;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.testutils.ListLogRecords;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.server.index.TestingIndexEncoders.countingEncoder;
import static org.apache.fluss.server.index.TestingIndexEncoders.encoder;
import static org.apache.fluss.server.index.TestingIndexEncoders.fanout;
import static org.apache.fluss.server.index.TestingIndexSourceLog.LOCAL_WAL_ONLY;
import static org.apache.fluss.server.index.TestingIndexSourceLog.sourceRecords;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class IndexReplicatorTest {
    private final IndexReplicationScheduler scheduler = new IndexReplicationScheduler(8);

    @Test
    void closingSourceReleasesEveryIndexWindowAndFencesLateAcknowledgements() throws Exception {
        MemoryLogRecords records = sourceRecords(RowType.of(DataTypes.INT()), 0);
        IndexSendBuffer buffer = new IndexSendBuffer();
        try (IndexReplicator replicator =
                replicator(
                        sourceLog(() -> records, new AtomicInteger()),
                        buffer,
                        new ManualClock(),
                        Arrays.asList(encoder("first", 2L), encoder("second", 3L)))) {
            assertThat(scheduler.pollWorker(0)).isTrue();
            IndexBatch first = buffer.claimFirstReady(new TableBucket(2L, 0));
            IndexBatch second = buffer.claimFirstReady(new TableBucket(3L, 0));
            assertThat(first).isNotNull();
            assertThat(second).isNotNull();

            replicator.close();

            assertThat(buffer.buckets()).isEmpty();
            assertThat(buffer.pendingBytes()).isZero();
            for (IndexBatch batch : Arrays.asList(first, second)) {
                assertThat(buffer.completeClaim(batch)).isFalse();
                assertThat(buffer.retryClaim(batch)).isFalse();
                batch.window().onBatchAcked();
            }
            assertThat(buffer.pendingBytes()).isZero();
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
        }
    }

    @Test
    void acknowledgedAsyncHandsOffTheSourceLockAndReusesPendingInput() throws Exception {
        MemoryLogRecords first = sourceRecords(RowType.of(DataTypes.INT()), 0);
        MemoryLogRecords second = sourceRecords(RowType.of(DataTypes.INT()), 2);
        AtomicInteger reads = new AtomicInteger();
        AtomicLong highWatermark = new AtomicLong(2);
        AtomicReference<CompletableFuture<Void>> completion = new AtomicReference<>();
        IndexSendBuffer buffer = new IndexSendBuffer(8192);
        IndexReplicationEncoder async = encoder("async", 3L, IndexVisibility.ASYNC);
        IndexSourceReader.SourceLog log =
                sourceLog(
                        new TableBucket(1L, 0),
                        () -> {
                            if (highWatermark.get() == 2) {
                                return first;
                            }
                            IndexBatch batch = buffer.claimFirstReady(new TableBucket(3L, 0));
                            assertThat(batch).isNotNull();
                            assertThat(buffer.completeClaim(batch)).isTrue();
                            completion.set(
                                    CompletableFuture.runAsync(batch.window()::onBatchAcked));
                            // The ACK has arrived, but its owner callback needs the lock held by
                            // this read.
                            try {
                                retry(
                                        Duration.ofSeconds(10),
                                        () -> assertThat(batch.window().isAcknowledged()).isTrue());
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            return second;
                        },
                        reads,
                        highWatermark::get);
        try (IndexReplicator replicator =
                replicator(
                        log,
                        buffer,
                        new ManualClock(),
                        Arrays.asList(encoder("sync", 2L), async))) {
            assertThat(scheduler.pollWorker(0)).isTrue();
            acknowledge(buffer, new TableBucket(2L, 0));
            highWatermark.set(4);
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(replicator.hasPendingRead()).isTrue();
            completion.get().get(10, TimeUnit.SECONDS);
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(replicator.hasPendingRead()).isFalse();
            assertThat(reads).hasValue(2);
            acknowledge(buffer, new TableBucket(2L, 0));
            acknowledge(buffer, new TableBucket(3L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(4L);
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {4096, 8192})
    void asyncReusesSyncInputWithoutHoldingBackSync(long payloadBudget) throws Exception {
        MemoryLogRecords records = sourceRecords(RowType.of(DataTypes.INT()), 0);
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger asyncEncodes = new AtomicInteger();
        IndexReplicationEncoder async =
                countingEncoder("async", 3L, IndexVisibility.ASYNC, asyncEncodes);
        IndexSendBuffer buffer = new IndexSendBuffer(payloadBudget);
        try (IndexReplicator replicator =
                replicator(
                        sourceLog(() -> records, reads),
                        buffer,
                        new ManualClock(),
                        Arrays.asList(encoder("sync", 2L), async))) {
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(reads).hasValue(1);
            assertThat(replicator.hasFailure()).isFalse();
            acknowledge(buffer, new TableBucket(2L, 0));
            assertThat(replicator.getSyncIndexPushedOffset()).isEqualTo(2L);
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            if (payloadBudget == 4096) {
                assertThat(buffer.pendingBytes()).isZero();
                // Optional construction did not introduce a backoff or a replay fence.
                assertThat(scheduler.pollWorker(0)).isTrue();
                assertThat(reads).hasValue(2);
            } else {
                assertThat(reads).hasValue(1);
                assertThat(asyncEncodes).hasValue(2);
            }
            acknowledge(buffer, new TableBucket(3L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void oversizedOutputRetriesACompleteSourceBatchPrefix(boolean payloadLimited) throws Exception {
        MemoryLogRecords first = sourceRecords(RowType.of(DataTypes.INT()), 0);
        MemoryLogRecords second = sourceRecords(RowType.of(DataTypes.INT()), 2);
        LogRecords records = new ListLogRecords(Arrays.asList(first, second));
        CompactedRow value = binaryValue(1000);
        IndexReplicationEncoder index =
                encoder(
                        "idx",
                        2L,
                        IndexVisibility.SYNC,
                        row ->
                                new IndexReplicationEncoder.IndexEntry(
                                        new byte[] {(byte) row.getInt(0)}, value, 0),
                        (target, end) ->
                                new IndexReplicationEncoder.IndexEntry(
                                        new byte[] {0}, value, target));
        ManualClock clock = new ManualClock();
        AtomicInteger reads = new AtomicInteger();
        IndexSendBuffer buffer = new IndexSendBuffer(payloadLimited ? 4096 : Long.MAX_VALUE);
        LogRecordReadContext context =
                LogRecordReadContext.createIndexedReadContext(RowType.of(DataTypes.INT()), 1, null);
        try (IndexReplicator replicator =
                new IndexReplicator(
                        new IndexSourceReader(
                                sourceLog(new TableBucket(1L, 0), () -> records, reads, () -> 4L),
                                LOCAL_WAL_ONLY,
                                Runnable::run),
                        Collections.singletonList(index),
                        buffer,
                        context,
                        0,
                        1024,
                        (sync, all) -> {},
                        scheduler,
                        () -> {},
                        new ExponentialBackoff(10, 2, 100, 0),
                        clock,
                        null,
                        payloadLimited ? Long.MAX_VALUE : 3500)) {
            scheduler.register(0, replicator);
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(buffer.pendingBytes()).isZero();
            assertThat(replicator.hasFailure()).isFalse();
            assertThat(scheduler.pollWorker(0)).isTrue();
            acknowledge(buffer, new TableBucket(2L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(scheduler.pollWorker(0)).isTrue();
            acknowledge(buffer, new TableBucket(2L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(4L);
            assertThat(reads).hasValue(3);
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @Test
    void concurrentFailuresShareOneBackoffPeriod() {
        ManualClock clock = new ManualClock();
        IndexReplicationRetryState retry =
                new IndexReplicationRetryState(new ExponentialBackoff(100, 2, 10000, 0), clock);
        retry.onFailure();
        clock.advanceTime(50, TimeUnit.MILLISECONDS);
        retry.onFailure();
        assertThat(retry.remainingDelayNanos()).isEqualTo(TimeUnit.MILLISECONDS.toNanos(50));
        clock.advanceTime(50, TimeUnit.MILLISECONDS);
        retry.onFailure();
        assertThat(retry.remainingDelayNanos()).isEqualTo(TimeUnit.MILLISECONDS.toNanos(200));
    }

    @Test
    void yieldingPreservesFailureAgeAndBackoffUntilProgress() {
        ManualClock clock = new ManualClock();
        IndexReplicationRetryState retry =
                new IndexReplicationRetryState(new ExponentialBackoff(1000, 2, 10000, 0), clock);
        retry.onFailure();
        clock.advanceTime(1, TimeUnit.SECONDS);
        retry.onFailure();
        clock.advanceTime(2, TimeUnit.SECONDS);
        retry.onFailure();
        assertThat(retry.shouldYieldPayload(TimeUnit.SECONDS.toNanos(10))).isFalse();
        clock.advanceTime(4, TimeUnit.SECONDS);
        retry.onFailure();
        assertThat(retry.shouldYieldPayload(TimeUnit.SECONDS.toNanos(10))).isTrue();
        clock.advanceTime(8, TimeUnit.SECONDS);
        retry.deferRetryAfterYield();
        assertThat(retry.remainingDelayNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(8));
        clock.advanceTime(8, TimeUnit.SECONDS);
        retry.onFailure();
        assertThat(retry.remainingDelayNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(10));
        assertThat(retry.shouldYieldPayload(TimeUnit.SECONDS.toNanos(10))).isTrue();
        retry.reset();
        assertThat(retry.hasFailure()).isFalse();
        assertThat(retry.remainingDelayNanos()).isZero();
        assertThat(retry.shouldYieldPayload(TimeUnit.SECONDS.toNanos(10))).isFalse();
    }

    @Test
    void retryMaximumKeepsInitialDelayAndBoundsLongFailures() {
        ExponentialBackoff backoff = IndexReplicationManager.createRetryBackoff(20_000);
        assertThat(backoff.backoff(0)).isBetween(80L, 120L);
        assertThat(backoff.backoff(100)).isGreaterThan(10_000L).isLessThanOrEqualTo(20_000L);
        assertThat(IndexReplicationManager.createRetryBackoff(100).backoff(100)).isEqualTo(100L);
        for (long invalidMaximum : new long[] {0, 99, (long) Integer.MAX_VALUE + 1}) {
            assertThatThrownBy(() -> IndexReplicationManager.createRetryBackoff(invalidMaximum))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxRetryBackoffMs");
        }
    }

    @Test
    void capacityContentionPacesWorkerWithoutReportingFailure() throws Exception {
        MemoryLogRecords records = sourceRecords(RowType.of(DataTypes.INT()), 0);
        IndexSendBuffer buffer = new IndexSendBuffer(8192);
        ManualClock clock = new ManualClock();
        AtomicInteger reads = new AtomicInteger();
        CompletableFuture<Void> yielded = new CompletableFuture<>();
        CompletableFuture<Void> secondRead = new CompletableFuture<>();
        IndexSourceReader.SourceLog log =
                sourceLog(
                        () -> {
                            if (reads.get() > 1) {
                                secondRead.complete(null);
                            }
                            return records;
                        },
                        reads);
        IndexReplicationScheduler scheduler = new IndexReplicationScheduler(1);
        IndexReplicationManager.ReplicatorWorker worker =
                new IndexReplicationManager.ReplicatorWorker(
                        "index-replicator-pool-0", 0, 1, scheduler);
        worker.start();
        try (IndexSendBuffer.Reservation blocker = buffer.newReservation();
                IndexReplicator replicator =
                        unregisteredReplicator(
                                new IndexSourceReader(log, LOCAL_WAL_ONLY, Runnable::run),
                                buffer,
                                clock,
                                Collections.singletonList(fanout("idx", 2L)),
                                scheduler,
                                worker::wakeup)) {
            blocker.allocatePage(4096);
            buffer.setCapacityListener(
                    () -> {
                        worker.wakeup();
                        yielded.complete(null);
                    });
            scheduler.register(0, replicator);
            worker.wakeup();
            yielded.get(5, TimeUnit.SECONDS);
            assertThatThrownBy(() -> secondRead.get(200, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertThat(reads).hasValue(1);
            assertThat(replicator.hasFailure()).isFalse();
            assertThat(buffer.pendingBytes()).isEqualTo(4096);
            blocker.close();
            clock.advanceTime(50, TimeUnit.MILLISECONDS);
            worker.wakeup();
            retry(Duration.ofSeconds(5), () -> acknowledge(buffer, new TableBucket(2L, 1)));
            acknowledge(buffer, new TableBucket(2L, 2));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(buffer.pendingBytes()).isZero();
        } finally {
            worker.initiateShutdown();
            worker.wakeup();
            worker.awaitShutdown();
        }
    }

    @Test
    void yieldedWindowPreservesReplayBoundaryAndFencesLateCompletion() throws Exception {
        RowType rowType = RowType.of(DataTypes.INT());
        MemoryLogRecords first = sourceRecords(rowType, 0);
        MemoryLogRecords second =
                createBasicMemoryLogRecords(
                        rowType,
                        1,
                        2L,
                        0L,
                        CURRENT_LOG_MAGIC_VALUE,
                        NO_WRITER_ID,
                        NO_BATCH_SEQUENCE,
                        Collections.singletonList(ChangeType.INSERT),
                        Collections.singletonList(new Object[] {3}),
                        LogFormat.INDEXED,
                        DEFAULT_COMPRESSION);
        LogRecords records = new ListLogRecords(Arrays.asList(first, second));
        AtomicLong highWatermark = new AtomicLong(2);
        AtomicBoolean encodingFailed = new AtomicBoolean(true);
        CompactedRow largeValue = binaryValue(5000);
        CompactedRow smallValue = binaryValue(1);
        IndexReplicationEncoder delegate = encoder("idx", 2L);
        IndexReplicationEncoder fanout =
                encoder(
                        "idx",
                        2L,
                        IndexVisibility.SYNC,
                        row -> {
                            if (encodingFailed.get()) {
                                throw new IllegalStateException("index needs repair");
                            }
                            int target = row.getInt(0);
                            return new IndexReplicationEncoder.IndexEntry(
                                    new byte[] {(byte) target},
                                    target == 2 ? smallValue : largeValue,
                                    target);
                        },
                        delegate::encodeProgress);
        IndexSendBuffer buffer = new IndexSendBuffer(16384);
        ManualClock clock = new ManualClock();
        try (IndexReplicator replicator =
                replicator(
                        sourceLog(
                                new TableBucket(1L, 0),
                                () -> records,
                                new AtomicInteger(),
                                highWatermark::get),
                        buffer,
                        clock,
                        Arrays.asList(fanout, encoder("other", 3L)))) {
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(4096L);
            encodingFailed.set(false);
            highWatermark.set(3);
            clock.advanceTime(10, TimeUnit.MILLISECONDS);
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(4096L);
            IndexBatch oldOther = buffer.claimFirstReady(new TableBucket(3L, 0));
            assertThat(oldOther).isNotNull();
            assertThat(buffer.retryClaim(oldOther)).isTrue();
            clock.advanceTime(10, TimeUnit.SECONDS);
            scheduler.pollWorker(0);
            assertThat(buffer.pendingBytes()).isZero();
            clock.advanceTime(100, TimeUnit.MILLISECONDS);
            assertThat(scheduler.pollWorker(0)).isTrue();
            IndexBatch oldBatch = buffer.claimFirstReady(new TableBucket(2L, 1));
            assertThat(oldBatch).isNotNull();
            assertThat(oldBatch.sourceEndOffset()).isEqualTo(2L);
            IndexReplicationWindow oldWindow = oldBatch.window();
            assertThat(buffer.pendingBytes()).isEqualTo(16384L);
            IndexBatch rebuiltOther = buffer.claimFirstReady(new TableBucket(3L, 0));
            assertThat(rebuiltOther).isNotNull();
            assertThat(rebuiltOther.sourceEndOffset()).isEqualTo(2L);
            oldOther.window().onBatchAcked();
            assertThat(buffer.completeClaim(oldOther)).isFalse();
            assertThat(buffer.retryClaim(oldOther)).isFalse();
            assertThat(buffer.ownsClaim(rebuiltOther)).isTrue();
            assertThat(buffer.completeClaim(rebuiltOther)).isTrue();
            rebuiltOther.window().onBatchAcked();
            assertThat(buffer.retryClaim(oldBatch)).isTrue();
            assertThat(scheduler.pollWorker(0)).isFalse();
            assertThat(buffer.pendingBytes()).isZero();
            clock.advanceTime(100, TimeUnit.MILLISECONDS);
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(12288L);
            IndexBatch rebuilt = buffer.claimFirstReady(new TableBucket(2L, 1));
            assertThat(rebuilt).isNotNull();
            assertThat(rebuilt.sourceEndOffset()).isEqualTo(2L);
            assertThat(rebuilt.window()).isNotSameAs(oldWindow);
            oldWindow.onBatchAcked();
            assertThat(buffer.completeClaim(oldBatch)).isFalse();
            assertThat(buffer.retryClaim(oldBatch)).isFalse();
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            assertThat(buffer.ownsClaim(rebuilt)).isTrue();
            assertThat(buffer.completeClaim(rebuilt)).isTrue();
            rebuilt.window().onBatchAcked();
            acknowledge(buffer, new TableBucket(2L, 2));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(replicator.hasFailure()).isFalse();
            assertThat(scheduler.pollWorker(0)).isTrue();
            acknowledge(buffer, new TableBucket(2L, 3));
            acknowledge(buffer, new TableBucket(3L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(3L);
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void stalledWindowYieldsBudgetAndRecoversAfterRepair(boolean rpcFailure) throws Exception {
        MemoryLogRecords records = sourceRecords(RowType.of(DataTypes.INT()), 0);
        IndexSendBuffer buffer = new IndexSendBuffer(4096);
        ManualClock clock = new ManualClock();
        AtomicBoolean repaired = new AtomicBoolean();
        TabletServerMetricGroup metrics =
                new TabletServerMetricGroup(NOPMetricRegistry.INSTANCE, "test", "rack", "host", 0);
        TestingTabletGatewayService gateway =
                new TestingTabletGatewayService() {
                    @Override
                    public CompletableFuture<PutIndexResponse> putIndex(PutIndexRequest request) {
                        if (request.getTableId() == 2L && !repaired.get() && rpcFailure) {
                            return FutureUtils.completedExceptionally(
                                    Errors.INVALID_TABLE_EXCEPTION.exception(
                                            "Index needs operator repair"));
                        }
                        PutIndexResponse response = new PutIndexResponse();
                        response.addBucketsResp().setBucketId(0);
                        if (request.getTableId() == 2L && !repaired.get()) {
                            response.getBucketsRespAt(0)
                                    .setErrorCode(Errors.INVALID_TABLE_EXCEPTION.code())
                                    .setErrorMessage("Index needs operator repair");
                        }
                        return CompletableFuture.completedFuture(response);
                    }
                };
        try (IndexReplicator stalled =
                        replicator(
                                sourceLog(
                                        new TableBucket(1L, 0), () -> records, new AtomicInteger()),
                                buffer,
                                clock,
                                Collections.singletonList(encoder("stalled", 2L)));
                IndexReplicator healthy =
                        unregisteredReplicator(
                                new IndexSourceReader(
                                        sourceLog(
                                                new TableBucket(4L, 0),
                                                () -> records,
                                                new AtomicInteger()),
                                        LOCAL_WAL_ONLY,
                                        Runnable::run),
                                buffer,
                                clock,
                                Collections.singletonList(encoder("healthy", 3L)),
                                scheduler,
                                () -> {})) {
            scheduler.register(1, healthy);
            assertThat(scheduler.pollWorker(0)).isTrue();
            try (IndexSender sender =
                    new IndexSender(
                            buffer,
                            (table, bucket) -> OptionalInt.of(0),
                            server -> gateway,
                            metrics,
                            1,
                            1,
                            1024,
                            1024 * 1024,
                            1000)) {
                retry(
                        Duration.ofSeconds(5),
                        () ->
                                assertThat(metrics.indexReplicationRetries().getCount())
                                        .isPositive());
                assertThat(scheduler.pollWorker(1)).isFalse();
                clock.advanceTime(10, TimeUnit.SECONDS);
                assertThat(scheduler.pollWorker(0)).isFalse();
                assertThat(buffer.pendingBytes()).isZero();
                assertThat(stalled.getAllIndexPushedOffset()).isZero();
                assertThat(scheduler.pollWorker(0)).isFalse();
                assertThat(scheduler.pollWorker(1)).isTrue();
                retry(
                        Duration.ofSeconds(5),
                        () -> assertThat(healthy.getAllIndexPushedOffset()).isEqualTo(2L));
                long retries = metrics.indexReplicationRetries().getCount();
                clock.advanceTime(100, TimeUnit.MILLISECONDS);
                assertThat(scheduler.pollWorker(0)).isTrue();
                retry(
                        Duration.ofSeconds(5),
                        () ->
                                assertThat(metrics.indexReplicationRetries().getCount())
                                        .isGreaterThan(retries));
                assertThat(scheduler.pollWorker(0)).isFalse();
                assertThat(buffer.pendingBytes()).isZero();
                assertThat(scheduler.pollWorker(0)).isFalse();
                repaired.set(true);
                clock.advanceTime(100, TimeUnit.MILLISECONDS);
                assertThat(scheduler.pollWorker(0)).isTrue();
                retry(
                        Duration.ofSeconds(5),
                        () -> assertThat(stalled.getAllIndexPushedOffset()).isEqualTo(2L));
                assertThat(buffer.pendingBytes()).isZero();
            }
        } finally {
            metrics.close();
        }
    }

    @Test
    void slowerIndexAcknowledgementDoesNotClearFailureAtALaterOffset() throws Exception {
        RowType rowType = RowType.of(DataTypes.INT());
        MemoryLogRecords first = sourceRecords(rowType, 0);
        MemoryLogRecords second = sourceRecords(rowType, 2);
        AtomicLong highWatermark = new AtomicLong(2);
        AtomicInteger reads = new AtomicInteger();
        AtomicBoolean failed = new AtomicBoolean();
        IndexSourceReader.SourceLog log =
                new TestingIndexSourceLog(
                        new TableBucket(1L, 0),
                        highWatermark::get,
                        0L,
                        offset -> offset == 0 ? first : second,
                        reads);
        IndexReplicationEncoder delegate = encoder("fast", 2L);
        IndexReplicationEncoder fast =
                encoder(
                        "fast",
                        2L,
                        IndexVisibility.SYNC,
                        row -> {
                            if (failed.get()) {
                                throw new IllegalStateException("index needs repair");
                            }
                            return delegate.encodeDataEntry(row);
                        },
                        delegate::encodeProgress);
        IndexSendBuffer buffer = new IndexSendBuffer();
        ManualClock clock = new ManualClock();
        try (IndexReplicator replicator =
                replicator(log, buffer, clock, Arrays.asList(fast, encoder("slow", 3L)))) {
            assertThat(scheduler.pollWorker(0)).isTrue();
            acknowledge(buffer, new TableBucket(2L, 0));
            highWatermark.set(4);
            failed.set(true);
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(replicator.hasFailure()).isTrue();
            acknowledge(buffer, new TableBucket(3L, 0));
            assertThat(replicator.hasFailure()).isTrue();
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(reads).hasValue(3);
            acknowledge(buffer, new TableBucket(3L, 0));
            assertThat(replicator.hasFailure()).isTrue();
            failed.set(false);
            clock.advanceTime(10, TimeUnit.MILLISECONDS);
            assertThat(scheduler.pollWorker(0)).isTrue();
            acknowledge(buffer, new TableBucket(2L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(4L);
            assertThat(replicator.hasFailure()).isFalse();
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"local gap", "remote gap", "remote failure"})
    void sourceFailureResumesAtTheSameOffsetAfterRepair(String failure) throws Exception {
        RowType rowType = RowType.of(DataTypes.INT());
        MemoryLogRecords valid = sourceRecords(rowType, 0);
        MemoryLogRecords gapped = sourceRecords(rowType, 1);
        AtomicBoolean repaired = new AtomicBoolean();
        AtomicInteger reads = new AtomicInteger();
        boolean remote = !failure.equals("local gap");
        IndexSourceReader.SourceLog log =
                new TestingIndexSourceLog(
                        new TableBucket(1L, 0),
                        () -> 2L,
                        remote ? 2L : 0L,
                        offset -> {
                            assertThat(remote)
                                    .as("a remote-only range must never be read locally")
                                    .isFalse();
                            assertThat(offset).isZero();
                            return repaired.get() ? valid : gapped;
                        },
                        reads);
        RemoteSessions sessions =
                new RemoteSessions(
                        offset -> {
                            assertThat(offset).isZero();
                            reads.incrementAndGet();
                            if (!repaired.get() && failure.equals("remote failure")) {
                                throw new java.io.IOException("remote storage unavailable");
                            }
                            return repaired.get() ? valid.batches() : Collections.emptyList();
                        });
        ManualClock clock = new ManualClock();
        IndexSendBuffer buffer = new IndexSendBuffer();
        TabletServerMetricGroup metrics =
                new TabletServerMetricGroup(NOPMetricRegistry.INSTANCE, "test", "rack", "host", 0);
        try (IndexReplicator replicator =
                new IndexReplicator(
                        new IndexSourceReader(
                                log, remote ? sessions : LOCAL_WAL_ONLY, Runnable::run),
                        Collections.singletonList(encoder("idx", 2L)),
                        buffer,
                        LogRecordReadContext.createIndexedReadContext(rowType, 1, null),
                        0L,
                        1024,
                        (sync, all) -> {},
                        scheduler,
                        () -> {},
                        new ExponentialBackoff(10, 2, 100, 0),
                        clock,
                        metrics,
                        ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE.defaultValue().getBytes())) {
            scheduler.register(0, replicator);
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(replicator.hasFailure()).isTrue();
            assertThat(reads).hasValue(1);
            sessions.assertClosedSessions(remote ? 1 : 0);
            assertThat(metrics.indexReplicationRetries().getCount()).isEqualTo(1);
            assertThat(scheduler.pollWorker(0)).isFalse();
            assertThat(reads).hasValue(1);
            assertThat(metrics.indexReplicationRetries().getCount()).isEqualTo(1);
            clock.advanceTime(10, TimeUnit.MILLISECONDS);
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(reads).hasValue(2);
            sessions.assertClosedSessions(remote ? 2 : 0);
            assertThat(metrics.indexReplicationRetries().getCount()).isEqualTo(2);
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            assertThat(replicator.getSyncIndexPushedOffset()).isZero();
            assertThat(buffer.buckets()).isEmpty();
            repaired.set(true);
            assertThat(scheduler.pollWorker(0)).isFalse();
            clock.advanceTime(20, TimeUnit.MILLISECONDS);
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(reads).hasValue(3);
            sessions.assertClosedSessions(remote ? 3 : 0);
            assertThat(metrics.indexReplicationRetries().getCount()).isEqualTo(2);
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            acknowledge(buffer, new TableBucket(2L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(replicator.getSyncIndexPushedOffset()).isEqualTo(2L);
            assertThat(replicator.hasFailure()).isFalse();
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @Test
    void oversizedWindowReleasesConstructionAndRemainsRetryable() throws Exception {
        RowType rowType = RowType.of(DataTypes.INT());
        MemoryLogRecords records = sourceRecords(rowType, 0);
        AtomicInteger reads = new AtomicInteger();
        IndexSourceReader.SourceLog log = sourceLog(() -> records, reads);
        ManualClock clock = new ManualClock();
        IndexReplicationEncoder fanout = fanout("idx", 2L);
        IndexSendBuffer small = new IndexSendBuffer(4096);
        try (IndexReplicator replicator =
                replicator(log, small, clock, Collections.singletonList(fanout))) {
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(replicator.hasFailure()).isTrue();
            assertThat(small.pendingBytes()).isZero();
            assertThat(small.buckets()).isEmpty();
            clock.advanceTime(10, TimeUnit.MILLISECONDS);
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(reads).hasValue(2);
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            assertThat(small.pendingBytes()).isZero();
        }
        IndexSendBuffer larger = new IndexSendBuffer(8192);
        try (IndexReplicator replicator =
                replicator(log, larger, clock, Collections.singletonList(fanout))) {
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(larger.pendingBytes()).isEqualTo(8192);
            acknowledge(larger, new TableBucket(2L, 1));
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            acknowledge(larger, new TableBucket(2L, 2));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(larger.pendingBytes()).isZero();
        }
    }

    @Test
    void indexesSharingAReadProgressWhenOnlyOneWindowFits() throws Exception {
        RowType rowType = RowType.of(DataTypes.INT());
        MemoryLogRecords records = sourceRecords(rowType, 0);
        IndexSendBuffer buffer = new IndexSendBuffer(4096);
        ManualClock clock = new ManualClock();
        try (IndexReplicator replicator =
                replicator(
                        sourceLog(() -> records, new AtomicInteger()),
                        buffer,
                        clock,
                        Arrays.asList(encoder("first", 2L), encoder("second", 3L)))) {
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(4096);
            acknowledge(buffer, new TableBucket(2L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            clock.advanceTime(50, TimeUnit.MILLISECONDS);
            assertThat(scheduler.pollWorker(0)).isTrue();
            acknowledge(buffer, new TableBucket(3L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @Test
    void publishedAndEmptyWindowsSurviveLaterConstructionError() throws Exception {
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.INT());
        MemoryLogRecords records =
                createBasicMemoryLogRecords(
                        rowType,
                        1,
                        0L,
                        0L,
                        CURRENT_LOG_MAGIC_VALUE,
                        NO_WRITER_ID,
                        NO_BATCH_SEQUENCE,
                        Arrays.asList(ChangeType.INSERT, ChangeType.INSERT),
                        Arrays.asList(new Object[] {1, null}, new Object[] {2, null}),
                        LogFormat.INDEXED,
                        DEFAULT_COMPRESSION);
        AtomicInteger reads = new AtomicInteger();
        AtomicBoolean fail = new AtomicBoolean(true);
        IndexReplicationEncoder first = encoder("published", 2L);
        IndexReplicationEncoder empty =
                new IndexReplicationEncoder(
                        "empty",
                        IndexVisibility.SYNC,
                        3L,
                        1,
                        KvFormat.COMPACTED,
                        new int[] {1},
                        row -> {
                            throw new AssertionError("null index must not encode an entry");
                        },
                        (target, end) -> {
                            throw new AssertionError("empty window must not encode progress");
                        });
        IndexReplicationEncoder delegate = encoder("interrupted", 4L);
        IndexReplicationEncoder interrupted =
                encoder(
                        "interrupted",
                        4L,
                        IndexVisibility.SYNC,
                        delegate::encodeDataEntry,
                        (target, end) -> {
                            if (fail.get()) {
                                throw new AssertionError("construction interrupted");
                            }
                            return delegate.encodeProgress(target, end);
                        });
        IndexSendBuffer buffer = new IndexSendBuffer();
        LogRecordReadContext context =
                LogRecordReadContext.createIndexedReadContext(rowType, 1, null);
        try (IndexReplicator replicator =
                sourceLog(() -> records, reads)
                        .registerReplicator(
                                scheduler,
                                buffer,
                                context,
                                Arrays.asList(first, empty, interrupted))) {
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(4096L);
            assertThat(buffer.buckets()).containsExactly(new TableBucket(2L, 0));
            acknowledge(buffer, new TableBucket(2L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            fail.set(false);
            assertThat(scheduler.pollWorker(0)).isTrue();
            acknowledge(buffer, new TableBucket(4L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(reads).hasValue(2);
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    private static TestingIndexSourceLog sourceLog(
            Supplier<LogRecords> records, AtomicInteger reads) {
        return sourceLog(new TableBucket(1L, 0), records, reads);
    }

    private static TestingIndexSourceLog sourceLog(
            TableBucket source, Supplier<LogRecords> records, AtomicInteger reads) {
        return sourceLog(source, records, reads, () -> 2L);
    }

    private static TestingIndexSourceLog sourceLog(
            TableBucket source,
            Supplier<LogRecords> records,
            AtomicInteger reads,
            LongSupplier highWatermark) {
        return new TestingIndexSourceLog(
                source, highWatermark, 0L, ignored -> records.get(), reads);
    }

    private IndexReplicator replicator(
            IndexSourceReader.SourceLog log,
            IndexSendBuffer buffer,
            ManualClock clock,
            List<IndexReplicationEncoder> indexes) {
        IndexSourceReader sourceReader = new IndexSourceReader(log, LOCAL_WAL_ONLY, Runnable::run);
        IndexReplicator replicator =
                unregisteredReplicator(sourceReader, buffer, clock, indexes, scheduler, () -> {});
        scheduler.register(0, replicator);
        return replicator;
    }

    private static IndexReplicator unregisteredReplicator(
            IndexSourceReader sourceReader,
            IndexSendBuffer buffer,
            ManualClock clock,
            List<IndexReplicationEncoder> indexes,
            IndexReplicationScheduler scheduler,
            Runnable wakeupSignal) {
        LogRecordReadContext context =
                LogRecordReadContext.createIndexedReadContext(RowType.of(DataTypes.INT()), 1, null);
        return new IndexReplicator(
                sourceReader,
                indexes,
                buffer,
                context,
                0L,
                1024,
                (sync, all) -> {},
                scheduler,
                wakeupSignal,
                new ExponentialBackoff(10, 2, 100, 0),
                clock,
                null,
                ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE.defaultValue().getBytes());
    }

    @Test
    void sharesSourceReadAndDecodeAcrossIndexes() throws Exception {
        TableBucket sourceBucket = new TableBucket(1L, 0);
        AtomicInteger sourceReads = new AtomicInteger();
        AtomicInteger firstIndexEncodes = new AtomicInteger();
        AtomicInteger secondIndexEncodes = new AtomicInteger();

        RowType rowType = RowType.of(DataTypes.INT());
        List<LogRecordBatch> batches = new ArrayList<>();
        for (int offset = 0; offset < 4; offset += 2) {
            MemoryLogRecords records =
                    createBasicMemoryLogRecords(
                            rowType,
                            1,
                            offset,
                            0L,
                            CURRENT_LOG_MAGIC_VALUE,
                            NO_WRITER_ID,
                            NO_BATCH_SEQUENCE,
                            Arrays.asList(ChangeType.DELETE, ChangeType.DELETE),
                            Arrays.asList(new Object[] {offset + 1}, new Object[] {offset + 2}),
                            LogFormat.COMPACTED,
                            DEFAULT_COMPRESSION);
            batches.add(spy(records.batchIterator().next()));
        }
        LogRecords records =
                new LogRecords() {
                    @Override
                    public int sizeInBytes() {
                        return batches.stream().mapToInt(LogRecordBatch::sizeInBytes).sum();
                    }

                    @Override
                    public Iterable<LogRecordBatch> batches() {
                        return batches;
                    }
                };

        TestingIndexSourceLog sourceLog =
                new TestingIndexSourceLog(
                        sourceBucket, () -> 4L, 0L, ignored -> records, sourceReads);

        LogRecordReadContext readContext =
                LogRecordReadContext.createCompactedRowReadContext(rowType, 1, null);
        IndexReplicationEncoder first =
                countingEncoder("idx_first", 2L, IndexVisibility.SYNC, firstIndexEncodes);
        IndexReplicationEncoder second =
                countingEncoder("idx_second", 3L, IndexVisibility.SYNC, secondIndexEncodes);
        IndexSendBuffer sendBuffer = new IndexSendBuffer();

        try (IndexReplicator replicator =
                sourceLog.registerReplicator(
                        scheduler, sendBuffer, readContext, Arrays.asList(first, second))) {
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(sourceReads).hasValue(1);
            for (LogRecordBatch batch : batches) {
                verify(batch).records(any());
            }
            assertThat(firstIndexEncodes).hasValue(4);
            assertThat(secondIndexEncodes).hasValue(4);

            TableBucket firstTarget = new TableBucket(2L, 0);
            TableBucket secondTarget = new TableBucket(3L, 0);
            assertThat(sendBuffer.buckets()).containsExactlyInAnyOrder(firstTarget, secondTarget);
            acknowledge(sendBuffer, firstTarget);
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            acknowledge(sendBuffer, secondTarget);
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(4L);
            assertThat(sendBuffer.pendingBytes()).isZero();
        }
    }

    private static CompactedRow binaryValue(int length) {
        CompactedRowWriter writer = new CompactedRowWriter(1);
        writer.writeBytes(new byte[length]);
        CompactedRow value = new CompactedRow(new DataType[] {DataTypes.BYTES()});
        value.pointTo(writer.segment(), 0, writer.position());
        return value;
    }

    private static void acknowledge(IndexSendBuffer sendBuffer, TableBucket target) {
        IndexBatch batch = sendBuffer.claimFirstReady(target);
        assertThat(batch).isNotNull();
        assertThat(sendBuffer.completeClaim(batch)).isTrue();
        batch.window().onBatchAcked();
    }
}
