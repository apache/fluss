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

import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.DefaultLogRecordBatch;
import org.apache.fluss.record.LogRecord;
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
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexReplicatorTest {

    @Test
    void retryMaximumKeepsInitialDelayAndBoundsLongFailures() throws Exception {
        try (IndexReplicatorPool pool = new IndexReplicatorPool(1, 1024, 20_000, 1, Runnable::run);
                IndexReplicatorPool fixedDelayPool =
                        new IndexReplicatorPool(1, 1024, 100, 1, Runnable::run)) {
            assertThat(pool.retryBackoff().backoff(0)).isBetween(80L, 120L);
            assertThat(pool.retryBackoff().backoff(100))
                    .isGreaterThan(10_000L)
                    .isLessThanOrEqualTo(20_000L);
            assertThat(fixedDelayPool.retryBackoff().backoff(100)).isEqualTo(100L);
        }
        for (long invalidMaximum : new long[] {0, 99, (long) Integer.MAX_VALUE + 1}) {
            assertThatThrownBy(
                            () ->
                                    new IndexReplicatorPool(
                                            1, 1024, invalidMaximum, 1, Runnable::run))
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
        try (IndexSendBuffer.Reservation blocker = buffer.newReservation();
                IndexReplicator replicator =
                        unboundReplicator(
                                log, buffer, clock, Collections.singletonList(fanoutSpec()));
                IndexReplicatorPool pool =
                        new IndexReplicatorPool(1, 1024, 100, 1, Runnable::run)) {
            blocker.allocatePage(4096);
            buffer.setCapacityListener(
                    () -> {
                        pool.wakeup();
                        yielded.complete(null);
                    });
            pool.register(log.tableBucket(), replicator);
            yielded.get(5, TimeUnit.SECONDS);
            assertThatThrownBy(() -> secondRead.get(200, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertThat(reads).hasValue(1);
            assertThat(replicator.isFailed()).isFalse();
            assertThat(buffer.pendingBytes()).isEqualTo(4096);
            blocker.close();
            clock.advanceTime(50, TimeUnit.MILLISECONDS);
            pool.signal(log.tableBucket());
            retry(Duration.ofSeconds(5), () -> acknowledge(buffer, new TableBucket(2L, 1)));
            acknowledge(buffer, new TableBucket(2L, 2));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(buffer.pendingBytes()).isZero();
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
        LogRecords records =
                new LogRecords() {
                    @Override
                    public int sizeInBytes() {
                        return first.sizeInBytes() + second.sizeInBytes();
                    }

                    @Override
                    public Iterable<LogRecordBatch> batches() {
                        return Arrays.asList(
                                first.batches().iterator().next(),
                                second.batches().iterator().next());
                    }
                };
        AtomicLong highWatermark = new AtomicLong(2);
        AtomicBoolean encodingFailed = new AtomicBoolean(true);
        CompactedRow largeValue = binaryValue(5000);
        CompactedRow smallValue = binaryValue(1);
        IndexSpec delegate = indexSpec("idx", 2L, new AtomicInteger());
        IndexSpec fanout =
                new IndexSpec(
                        "idx",
                        IndexVisibility.SYNC,
                        2L,
                        1,
                        KvFormat.COMPACTED,
                        new int[] {0},
                        row -> {
                            if (encodingFailed.get()) {
                                throw new IllegalStateException("index needs repair");
                            }
                            int target = row.getInt(0);
                            return new IndexSpec.IndexEntry(
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
                        Arrays.asList(fanout, indexSpec("other", 3L, new AtomicInteger())))) {
            assertThat(replicator.poll()).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(4096L);
            encodingFailed.set(false);
            highWatermark.set(3);
            clock.advanceTime(10, TimeUnit.MILLISECONDS);
            assertThat(replicator.poll()).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(4096L);
            IndexBatch oldOther = buffer.claimFirstReady(new TableBucket(3L, 0));
            assertThat(oldOther).isNotNull();
            assertThat(buffer.retryClaim(oldOther)).isTrue();
            clock.advanceTime(10, TimeUnit.SECONDS);
            assertThat(replicator.poll()).isFalse();
            assertThat(buffer.pendingBytes()).isZero();
            clock.advanceTime(100, TimeUnit.MILLISECONDS);
            assertThat(replicator.poll()).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(12288L);
            IndexBatch oldBatch = buffer.claimFirstReady(new TableBucket(2L, 1));
            assertThat(oldBatch).isNotNull();
            assertThat(oldBatch.sourceEndOffset()).isEqualTo(2L);
            IndexReplicationWindow oldWindow = oldBatch.window();
            assertThat(replicator.poll()).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(16384L);
            IndexBatch rebuiltOther = buffer.claimFirstReady(new TableBucket(3L, 0));
            assertThat(rebuiltOther).isNotNull();
            assertThat(rebuiltOther.sourceEndOffset()).isEqualTo(2L);
            oldOther.window().onBatchAcked();
            replicator.onWindowComplete(oldOther.window(), 0L);
            assertThat(buffer.acknowledgeClaim(oldOther)).isFalse();
            assertThat(buffer.retryClaim(oldOther)).isFalse();
            assertThat(buffer.ownsClaim(rebuiltOther)).isTrue();
            assertThat(buffer.acknowledgeClaim(rebuiltOther)).isTrue();
            rebuiltOther.window().onBatchAcked();
            assertThat(buffer.retryClaim(oldBatch)).isTrue();
            assertThat(replicator.poll()).isFalse();
            assertThat(buffer.pendingBytes()).isZero();
            clock.advanceTime(100, TimeUnit.MILLISECONDS);
            assertThat(replicator.poll()).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(12288L);
            IndexBatch rebuilt = buffer.claimFirstReady(new TableBucket(2L, 1));
            assertThat(rebuilt).isNotNull();
            assertThat(rebuilt.sourceEndOffset()).isEqualTo(2L);
            assertThat(rebuilt.window()).isNotSameAs(oldWindow);
            oldWindow.onBatchAcked();
            replicator.onWindowComplete(oldWindow, 0L);
            assertThat(buffer.acknowledgeClaim(oldBatch)).isFalse();
            assertThat(buffer.retryClaim(oldBatch)).isFalse();
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            assertThat(buffer.ownsClaim(rebuilt)).isTrue();
            assertThat(buffer.acknowledgeClaim(rebuilt)).isTrue();
            rebuilt.window().onBatchAcked();
            acknowledge(buffer, new TableBucket(2L, 2));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(replicator.isFailed()).isFalse();
            assertThat(replicator.poll()).isTrue();
            acknowledge(buffer, new TableBucket(2L, 3));
            acknowledge(buffer, new TableBucket(3L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(3L);
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @Test
    void stalledWindowYieldsBudgetAndRecoversAfterRepair() throws Exception {
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
                                Collections.singletonList(
                                        indexSpec("stalled", 2L, new AtomicInteger())));
                IndexReplicator healthy =
                        replicator(
                                sourceLog(
                                        new TableBucket(4L, 0), () -> records, new AtomicInteger()),
                                buffer,
                                clock,
                                Collections.singletonList(
                                        indexSpec("healthy", 3L, new AtomicInteger())))) {
            assertThat(stalled.poll()).isTrue();
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
                assertThat(healthy.poll()).isFalse();
                clock.advanceTime(10, TimeUnit.SECONDS);
                assertThat(stalled.poll()).isFalse();
                assertThat(buffer.pendingBytes()).isZero();
                assertThat(stalled.getAllIndexPushedOffset()).isZero();
                assertThat(stalled.poll()).isFalse();
                assertThat(healthy.poll()).isTrue();
                retry(
                        Duration.ofSeconds(5),
                        () -> assertThat(healthy.getAllIndexPushedOffset()).isEqualTo(2L));
                long retries = metrics.indexReplicationRetries().getCount();
                clock.advanceTime(100, TimeUnit.MILLISECONDS);
                assertThat(stalled.poll()).isTrue();
                retry(
                        Duration.ofSeconds(5),
                        () ->
                                assertThat(metrics.indexReplicationRetries().getCount())
                                        .isGreaterThan(retries));
                assertThat(stalled.poll()).isFalse();
                assertThat(buffer.pendingBytes()).isZero();
                assertThat(stalled.poll()).isFalse();
                repaired.set(true);
                clock.advanceTime(100, TimeUnit.MILLISECONDS);
                assertThat(stalled.poll()).isTrue();
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
                new IndexSourceReader.SourceLog() {
                    @Override
                    public TableBucket tableBucket() {
                        return new TableBucket(1L, 0);
                    }

                    @Override
                    public long highWatermark() {
                        return highWatermark.get();
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
                        reads.incrementAndGet();
                        return new FetchDataInfo(offset == 0 ? first : second);
                    }
                };
        IndexSpec delegate = indexSpec("fast", 2L, new AtomicInteger());
        IndexSpec fast =
                new IndexSpec(
                        "fast",
                        IndexVisibility.SYNC,
                        2L,
                        1,
                        KvFormat.COMPACTED,
                        new int[] {0},
                        row -> {
                            if (failed.get()) {
                                throw new IllegalStateException("index needs repair");
                            }
                            return delegate.encodeEntry(row);
                        },
                        delegate::encodeProgress);
        IndexSendBuffer buffer = new IndexSendBuffer();
        ManualClock clock = new ManualClock();
        try (IndexReplicator replicator =
                replicator(
                        log,
                        buffer,
                        clock,
                        Arrays.asList(fast, indexSpec("slow", 3L, new AtomicInteger())))) {
            assertThat(replicator.poll()).isTrue();
            acknowledge(buffer, new TableBucket(2L, 0));
            highWatermark.set(4);
            failed.set(true);
            assertThat(replicator.poll()).isTrue();
            assertThat(replicator.isFailed()).isTrue();
            acknowledge(buffer, new TableBucket(3L, 0));
            assertThat(replicator.isFailed()).isTrue();
            assertThat(replicator.poll()).isTrue();
            assertThat(reads).hasValue(3);
            acknowledge(buffer, new TableBucket(3L, 0));
            assertThat(replicator.isFailed()).isTrue();
            failed.set(false);
            clock.advanceTime(10, TimeUnit.MILLISECONDS);
            assertThat(replicator.poll()).isTrue();
            acknowledge(buffer, new TableBucket(2L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(4L);
            assertThat(replicator.isFailed()).isFalse();
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @Test
    void sourceGapResumesAtTheSameOffsetAfterRepair() throws Exception {
        RowType rowType = RowType.of(DataTypes.INT());
        MemoryLogRecords valid = sourceRecords(rowType, 0);
        MemoryLogRecords gapped = sourceRecords(rowType, 1);
        AtomicBoolean repaired = new AtomicBoolean();
        AtomicInteger reads = new AtomicInteger();
        IndexSourceReader.SourceLog log = sourceLog(() -> repaired.get() ? valid : gapped, reads);
        ManualClock clock = new ManualClock();
        IndexSendBuffer buffer = new IndexSendBuffer();
        try (IndexReplicator replicator =
                replicator(
                        log,
                        buffer,
                        clock,
                        Collections.singletonList(indexSpec("idx", 2L, new AtomicInteger())))) {
            assertThat(replicator.poll()).isTrue();
            assertThat(replicator.isFailed()).isTrue();
            assertThat(reads).hasValue(1);
            repaired.set(true);
            assertThat(replicator.poll()).isFalse();
            assertThat(reads).hasValue(1);
            clock.advanceTime(10, TimeUnit.MILLISECONDS);
            assertThat(replicator.poll()).isTrue();
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            acknowledge(buffer, new TableBucket(2L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(replicator.isFailed()).isFalse();
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
        IndexSpec fanout = fanoutSpec();
        IndexSendBuffer small = new IndexSendBuffer(4096);
        try (IndexReplicator replicator =
                replicator(log, small, clock, Collections.singletonList(fanout))) {
            assertThat(replicator.poll()).isTrue();
            assertThat(replicator.isFailed()).isTrue();
            assertThat(small.pendingBytes()).isZero();
            assertThat(small.buckets()).isEmpty();
            clock.advanceTime(10, TimeUnit.MILLISECONDS);
            assertThat(replicator.poll()).isTrue();
            assertThat(reads).hasValue(2);
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            assertThat(small.pendingBytes()).isZero();
        }
        IndexSendBuffer larger = new IndexSendBuffer(8192);
        try (IndexReplicator replicator =
                replicator(log, larger, clock, Collections.singletonList(fanout))) {
            assertThat(replicator.poll()).isTrue();
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
                        Arrays.asList(
                                indexSpec("first", 2L, new AtomicInteger()),
                                indexSpec("second", 3L, new AtomicInteger())))) {
            assertThat(replicator.poll()).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(4096);
            acknowledge(buffer, new TableBucket(2L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            clock.advanceTime(50, TimeUnit.MILLISECONDS);
            assertThat(replicator.poll()).isTrue();
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
        IndexSpec first = indexSpec("published", 2L, new AtomicInteger());
        IndexSpec empty =
                new IndexSpec(
                        "empty",
                        IndexVisibility.SYNC,
                        3L,
                        1,
                        KvFormat.COMPACTED,
                        new int[] {1},
                        row -> {
                            throw new AssertionError("null index must not encode an entry");
                        },
                        (source, target, end) -> {
                            throw new AssertionError("empty window must not encode progress");
                        });
        IndexSpec delegate = indexSpec("interrupted", 4L, new AtomicInteger());
        IndexSpec interrupted =
                new IndexSpec(
                        "interrupted",
                        IndexVisibility.SYNC,
                        4L,
                        1,
                        KvFormat.COMPACTED,
                        new int[] {0},
                        delegate::encodeEntry,
                        (source, target, end) -> {
                            if (fail.get()) {
                                throw new AssertionError("construction interrupted");
                            }
                            return delegate.encodeProgress(source, target, end);
                        });
        IndexSendBuffer buffer = new IndexSendBuffer();
        LogRecordReadContext context =
                LogRecordReadContext.createIndexedReadContext(rowType, 1, null);
        FairIndexReplicationScheduler scheduler = new FairIndexReplicationScheduler();
        try (IndexReplicator replicator =
                new IndexReplicator(
                        new IndexSourceReader(
                                sourceLog(() -> records, reads), null, Runnable::run, context),
                        Arrays.asList(first, empty, interrupted),
                        buffer,
                        context,
                        0L,
                        1024,
                        (sync, all) -> {})) {
            replicator.setScheduler(scheduler);
            scheduler.register(0, replicator);
            assertThatThrownBy(replicator::poll).isInstanceOf(AssertionError.class);
            assertThat(replicator.pollServiced()).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(4096L);
            assertThat(buffer.buckets()).containsExactly(new TableBucket(2L, 0));
            IndexReplicator.ReadGroup remaining = scheduler.selectRead(replicator, 2L);
            assertThat(remaining).isNotNull();
            assertThat(remaining.states())
                    .extracting(state -> state.spec().getIndexName())
                    .containsExactly("interrupted");
            acknowledge(buffer, new TableBucket(2L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            fail.set(false);
            assertThat(replicator.poll()).isTrue();
            acknowledge(buffer, new TableBucket(4L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(reads).hasValue(2);
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    private static IndexSpec fanoutSpec() {
        IndexSpec delegate = indexSpec("idx", 2L, new AtomicInteger());
        return new IndexSpec(
                "idx",
                IndexVisibility.SYNC,
                2L,
                1,
                KvFormat.COMPACTED,
                new int[] {0},
                row -> {
                    IndexSpec.IndexEntry entry = delegate.encodeEntry(row);
                    return new IndexSpec.IndexEntry(entry.key(), entry.value(), row.getInt(0));
                },
                delegate::encodeProgress);
    }

    private static MemoryLogRecords sourceRecords(RowType rowType, long offset) throws Exception {
        return createBasicMemoryLogRecords(
                rowType,
                1,
                offset,
                0L,
                CURRENT_LOG_MAGIC_VALUE,
                NO_WRITER_ID,
                NO_BATCH_SEQUENCE,
                Arrays.asList(ChangeType.INSERT, ChangeType.INSERT),
                Arrays.asList(new Object[] {1}, new Object[] {2}),
                LogFormat.INDEXED,
                DEFAULT_COMPRESSION);
    }

    private static IndexSourceReader.SourceLog sourceLog(
            Supplier<LogRecords> records, AtomicInteger reads) {
        return sourceLog(new TableBucket(1L, 0), records, reads);
    }

    private static IndexSourceReader.SourceLog sourceLog(
            TableBucket source, Supplier<LogRecords> records, AtomicInteger reads) {
        return sourceLog(source, records, reads, () -> 2L);
    }

    private static IndexSourceReader.SourceLog sourceLog(
            TableBucket source,
            Supplier<LogRecords> records,
            AtomicInteger reads,
            LongSupplier highWatermark) {
        return new IndexSourceReader.SourceLog() {
            @Override
            public TableBucket tableBucket() {
                return source;
            }

            @Override
            public long highWatermark() {
                return highWatermark.getAsLong();
            }

            @Override
            public long logStartOffset() {
                return 0L;
            }

            @Override
            public FetchDataInfo read(
                    long offset, int maxBytes, FetchIsolation isolation, boolean minOneMessage) {
                reads.incrementAndGet();
                return new FetchDataInfo(records.get());
            }
        };
    }

    private static IndexReplicator replicator(
            IndexSourceReader.SourceLog log,
            IndexSendBuffer buffer,
            ManualClock clock,
            java.util.List<IndexSpec> indexes) {
        return bindScheduler(unboundReplicator(log, buffer, clock, indexes));
    }

    private static IndexReplicator bindScheduler(IndexReplicator replicator) {
        FairIndexReplicationScheduler scheduler = new FairIndexReplicationScheduler();
        replicator.setScheduler(scheduler);
        scheduler.register(0, replicator);
        return replicator;
    }

    private static IndexReplicator unboundReplicator(
            IndexSourceReader.SourceLog log,
            IndexSendBuffer buffer,
            ManualClock clock,
            java.util.List<IndexSpec> indexes) {
        LogRecordReadContext context =
                LogRecordReadContext.createIndexedReadContext(RowType.of(DataTypes.INT()), 1, null);
        return new IndexReplicator(
                new IndexSourceReader(log, null, Runnable::run, context),
                indexes,
                buffer,
                context,
                0L,
                1024,
                (sync, all) -> {},
                new ExponentialBackoff(10, 2, 100, 0),
                clock,
                null);
    }

    @Test
    void sourceReadFormsOneWindowAcrossCompleteBatches() throws Exception {
        RowType rowType = RowType.of(DataTypes.INT());
        MemoryLogRecords first =
                createBasicMemoryLogRecords(
                        rowType,
                        1,
                        0L,
                        0L,
                        CURRENT_LOG_MAGIC_VALUE,
                        NO_WRITER_ID,
                        NO_BATCH_SEQUENCE,
                        Collections.singletonList(ChangeType.INSERT),
                        Collections.singletonList(new Object[] {1}),
                        LogFormat.INDEXED,
                        DEFAULT_COMPRESSION);
        MemoryLogRecords second =
                createBasicMemoryLogRecords(
                        rowType,
                        1,
                        1L,
                        0L,
                        CURRENT_LOG_MAGIC_VALUE,
                        NO_WRITER_ID,
                        NO_BATCH_SEQUENCE,
                        Collections.singletonList(ChangeType.INSERT),
                        Collections.singletonList(new Object[] {2}),
                        LogFormat.INDEXED,
                        DEFAULT_COMPRESSION);
        LogRecords records =
                new LogRecords() {
                    @Override
                    public int sizeInBytes() {
                        return first.sizeInBytes() + second.sizeInBytes();
                    }

                    @Override
                    public Iterable<LogRecordBatch> batches() {
                        return Arrays.asList(
                                first.batches().iterator().next(),
                                second.batches().iterator().next());
                    }
                };
        TableBucket source = new TableBucket(1L, 0);
        IndexSourceReader.SourceLog log =
                new IndexSourceReader.SourceLog() {
                    @Override
                    public TableBucket tableBucket() {
                        return source;
                    }

                    @Override
                    public long highWatermark() {
                        return 2L;
                    }

                    @Override
                    public long logStartOffset() {
                        return 0L;
                    }

                    @Override
                    public FetchDataInfo read(
                            long offset,
                            int maxBytes,
                            org.apache.fluss.server.log.FetchIsolation isolation,
                            boolean minOneMessage) {
                        return new FetchDataInfo(records);
                    }
                };
        AtomicInteger encodes = new AtomicInteger();
        IndexSendBuffer buffer = new IndexSendBuffer();
        LogRecordReadContext context =
                LogRecordReadContext.createIndexedReadContext(rowType, 1, null);
        try (IndexReplicator replicator =
                new IndexReplicator(
                        new IndexSourceReader(log, null, Runnable::run, context),
                        Collections.singletonList(indexSpec("idx", 2L, encodes)),
                        buffer,
                        context,
                        0L,
                        1024,
                        (sync, all) -> {})) {
            bindScheduler(replicator);
            assertThat(replicator.poll()).isTrue();
            assertThat(encodes).hasValue(2);
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            acknowledge(buffer, new TableBucket(2L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @Test
    void sharesSourceDecodeAcrossIndexesWithoutSplittingSourceBatch() throws Exception {
        TableBucket sourceBucket = new TableBucket(1L, 0);
        AtomicInteger sourceReads = new AtomicInteger();
        AtomicInteger recordDecodes = new AtomicInteger();
        AtomicInteger firstIndexEncodes = new AtomicInteger();
        AtomicInteger secondIndexEncodes = new AtomicInteger();

        RowType rowType = RowType.of(DataTypes.INT());
        MemoryLogRecords sourceRecords =
                createBasicMemoryLogRecords(
                        rowType,
                        1,
                        0L,
                        0L,
                        CURRENT_LOG_MAGIC_VALUE,
                        NO_WRITER_ID,
                        NO_BATCH_SEQUENCE,
                        Arrays.asList(ChangeType.DELETE, ChangeType.DELETE),
                        Arrays.asList(new Object[] {1}, new Object[] {2}),
                        LogFormat.COMPACTED,
                        DEFAULT_COMPRESSION);
        DefaultLogRecordBatch batch =
                new DefaultLogRecordBatch() {
                    @Override
                    public CloseableIterator<LogRecord> records(ReadContext context) {
                        recordDecodes.incrementAndGet();
                        return super.records(context);
                    }
                };
        batch.pointTo(sourceRecords.getMemorySegment(), sourceRecords.getPosition());
        LogRecords records =
                new LogRecords() {
                    @Override
                    public int sizeInBytes() {
                        return sourceRecords.sizeInBytes();
                    }

                    @Override
                    public Iterable<LogRecordBatch> batches() {
                        return Collections.singletonList(batch);
                    }
                };

        IndexSourceReader.SourceLog sourceLog =
                new IndexSourceReader.SourceLog() {
                    @Override
                    public TableBucket tableBucket() {
                        return sourceBucket;
                    }

                    @Override
                    public long highWatermark() {
                        return 2L;
                    }

                    @Override
                    public long logStartOffset() {
                        return 0L;
                    }

                    @Override
                    public FetchDataInfo read(
                            long offset,
                            int maxBytes,
                            org.apache.fluss.server.log.FetchIsolation isolation,
                            boolean minOneMessage) {
                        sourceReads.incrementAndGet();
                        return new FetchDataInfo(records);
                    }
                };

        LogRecordReadContext readContext =
                LogRecordReadContext.createCompactedRowReadContext(rowType, 1, null);
        IndexSourceReader sourceReader =
                new IndexSourceReader(sourceLog, null, Runnable::run, readContext);
        IndexSpec first = indexSpec("idx_first", 2L, firstIndexEncodes);
        IndexSpec second = indexSpec("idx_second", 3L, secondIndexEncodes);
        IndexSendBuffer sendBuffer = new IndexSendBuffer();

        try (IndexReplicator replicator =
                new IndexReplicator(
                        sourceReader,
                        Arrays.asList(first, second),
                        sendBuffer,
                        readContext,
                        0L,
                        1024,
                        (syncOffset, allOffset) -> {})) {
            bindScheduler(replicator);
            assertThat(replicator.poll()).isTrue();
            assertThat(sourceReads).hasValue(1);
            assertThat(recordDecodes).hasValue(1);
            assertThat(firstIndexEncodes).hasValue(2);
            assertThat(secondIndexEncodes).hasValue(2);

            TableBucket firstTarget = new TableBucket(2L, 0);
            TableBucket secondTarget = new TableBucket(3L, 0);
            assertThat(sendBuffer.buckets()).containsExactlyInAnyOrder(firstTarget, secondTarget);
            acknowledge(sendBuffer, firstTarget);
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            acknowledge(sendBuffer, secondTarget);
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
        }
    }

    private static CompactedRow binaryValue(int length) {
        CompactedRowWriter writer = new CompactedRowWriter(1);
        writer.writeBytes(new byte[length]);
        CompactedRow value = new CompactedRow(new DataType[] {DataTypes.BYTES()});
        value.pointTo(writer.segment(), 0, writer.position());
        return value;
    }

    private static IndexSpec indexSpec(String name, long tableId, AtomicInteger encodes) {
        CompactedRowWriter writer = new CompactedRowWriter(1);
        writer.writeByte((byte) 1);
        CompactedRow value = new CompactedRow(new DataType[] {DataTypes.TINYINT()});
        value.pointTo(writer.segment(), 0, writer.position());
        return new IndexSpec(
                name,
                IndexVisibility.SYNC,
                tableId,
                1,
                KvFormat.COMPACTED,
                new int[] {0},
                ignored -> {
                    encodes.incrementAndGet();
                    return new IndexSpec.IndexEntry(name.getBytes(), value, 0);
                },
                (sourceBucket, targetBucket, sourceEndOffset) ->
                        new IndexSpec.IndexEntry(
                                (name + "-progress").getBytes(), value, targetBucket));
    }

    private static void acknowledge(IndexSendBuffer sendBuffer, TableBucket target) {
        IndexBatch batch = sendBuffer.claimFirstReady(target);
        assertThat(batch).isNotNull();
        assertThat(sendBuffer.acknowledgeClaim(batch)).isTrue();
        batch.window().onBatchAcked();
    }
}
