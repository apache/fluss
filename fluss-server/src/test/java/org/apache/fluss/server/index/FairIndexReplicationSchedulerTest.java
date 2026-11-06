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
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.assertj.core.api.Assertions.assertThat;

class FairIndexReplicationSchedulerTest {

    @Test
    void serviceOnAnotherWorkerDoesNotConsumeLocalTablePosition() throws Exception {
        FairIndexReplicationScheduler scheduler = new FairIndexReplicationScheduler();
        IndexSendBuffer buffer = new IndexSendBuffer();
        ManualClock clock = new ManualClock();
        try (Source firstA =
                        new Source(
                                scheduler,
                                0,
                                new TableBucket(1L, 0),
                                buffer,
                                clock,
                                spec("a0", 10L));
                Source secondA =
                        new Source(
                                scheduler,
                                1,
                                new TableBucket(1L, 1),
                                buffer,
                                clock,
                                spec("a1", 11L));
                Source firstB =
                        new Source(
                                scheduler,
                                1,
                                new TableBucket(2L, 0),
                                buffer,
                                clock,
                                spec("b0", 12L));
                Source secondB =
                        new Source(
                                scheduler,
                                1,
                                new TableBucket(2L, 1),
                                buffer,
                                clock,
                                spec("b1", 13L))) {
            for (int round = 0; round < 6; round++) {
                assertThat(scheduler.pollWorker(0)).isTrue();
                drain(buffer);
                assertThat(scheduler.pollWorker(1)).isTrue();
                drain(buffer);
            }
            assertThat(firstA.reads).hasValue(6);
            assertThat(secondA.reads.get()).isGreaterThanOrEqualTo(2);
            assertThat(firstB.reads.get()).isPositive();
            assertThat(secondB.reads.get()).isPositive();
        }
        assertThat(buffer.pendingBytes()).isZero();
    }

    @Test
    void syncLookaheadChecksStartWithoutTruncatingEndAndIncludesBlockedMinimum() throws Exception {
        FairIndexReplicationScheduler scheduler = new FairIndexReplicationScheduler();
        IndexSendBuffer buffer = new IndexSendBuffer();
        ManualClock clock = new ManualClock();
        try (Source source =
                new Source(
                        scheduler,
                        0,
                        new TableBucket(1L, 0),
                        buffer,
                        clock,
                        spec("fast", 10L),
                        spec("slow", 11L))) {
            source.highWatermark.set(2);
            assertThat(scheduler.pollWorker(0)).isTrue();
            acknowledge(buffer, 10L, 0);
            source.highWatermark.set(6);
            assertThat(scheduler.pollWorker(0)).isTrue();
            IndexBatch ahead = buffer.claimFirstReady(new TableBucket(10L, 0));
            assertThat(ahead).isNotNull();
            assertThat(ahead.sourceEndOffset()).isEqualTo(4L);
            assertThat(buffer.acknowledgeClaim(ahead)).isTrue();
            ahead.window().onBatchAcked();
            assertThat(scheduler.pollWorker(0)).isFalse();
            assertThat(source.reads).hasValue(2);
            IndexBatch slow = buffer.claimFirstReady(new TableBucket(11L, 0));
            assertThat(buffer.retryClaim(slow)).isTrue();
            clock.advanceTime(10, TimeUnit.MILLISECONDS);
            assertThat(scheduler.pollWorker(0)).isFalse();
            assertThat(source.reads).hasValue(2);
            acknowledge(buffer, 11L, 0);
            assertThat(scheduler.pollWorker(0)).isTrue();
            IndexBatch catchup = buffer.claimFirstReady(new TableBucket(11L, 0));
            assertThat(catchup).isNotNull();
            assertThat(catchup.sourceEndOffset()).isEqualTo(4L);
            assertThat(source.replicator.getAllIndexPushedOffset()).isEqualTo(2L);
        }
        assertThat(buffer.pendingBytes()).isZero();
    }

    @Test
    void unavailableAsyncDoesNotHideHealthyWorkAndRecoversWithItsOwnProbe() throws Exception {
        FairIndexReplicationScheduler scheduler = new FairIndexReplicationScheduler();
        IndexSendBuffer buffer = new IndexSendBuffer();
        ManualClock clock = new ManualClock();
        AtomicBoolean broken = new AtomicBoolean(true);
        IndexSpec delegate = spec("recovering", 11L);
        IndexSpec recovering =
                new IndexSpec(
                        "recovering",
                        IndexVisibility.ASYNC,
                        11L,
                        1,
                        KvFormat.COMPACTED,
                        new int[] {0},
                        row -> {
                            if (broken.get()) {
                                throw new IllegalStateException("index needs repair");
                            }
                            return delegate.encodeEntry(row);
                        },
                        delegate::encodeProgress);
        try (Source source =
                new Source(
                        scheduler,
                        0,
                        new TableBucket(1L, 0),
                        buffer,
                        clock,
                        spec("sync", 10L),
                        recovering,
                        spec("healthyAsync", 12L, IndexVisibility.ASYNC))) {
            source.highWatermark.set(2);
            assertThat(scheduler.pollWorker(0)).isTrue();
            if (!buffer.hasPending(new TableBucket(12L, 0))) {
                assertThat(scheduler.pollWorker(0)).isTrue();
            }
            acknowledge(buffer, 10L, 0);
            acknowledge(buffer, 12L, 0);
            source.highWatermark.set(6);
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(scheduler.pollWorker(0)).isTrue();
            IndexBatch async = buffer.claimFirstReady(new TableBucket(12L, 0));
            assertThat(async).isNotNull();
            assertThat(async.sourceEndOffset()).isEqualTo(4L);
            assertThat(source.replicator.getSyncIndexPushedOffset()).isEqualTo(2L);
            assertThat(buffer.acknowledgeClaim(async)).isTrue();
            async.window().onBatchAcked();
            assertThat(scheduler.pollWorker(0)).isFalse();
            acknowledge(buffer, 10L, 0);
            broken.set(false);
            clock.advanceTime(10, TimeUnit.MILLISECONDS);
            for (int attempt = 0; attempt < 6 && source.replicator.isFailed(); attempt++) {
                scheduler.pollWorker(0);
                drain(buffer);
            }
            assertThat(source.replicator.isFailed()).isFalse();
            assertThat(source.replicator.getAllIndexPushedOffset()).isGreaterThanOrEqualTo(2L);
            assertThat(source.replicator.getSyncIndexPushedOffset()).isGreaterThanOrEqualTo(4L);
        }
        assertThat(buffer.pendingBytes()).isZero();
    }

    @Test
    void capacityHandoffIsByTableAndReadyOwnerProtectsBeforePagesAreFree() throws Exception {
        FairIndexReplicationScheduler scheduler = new FairIndexReplicationScheduler();
        IndexSendBuffer buffer = new IndexSendBuffer(8192);
        ManualClock clock = new ManualClock();
        QueuedExecutor io = new QueuedExecutor();
        try (IndexSendBuffer.Reservation blocker = buffer.newReservation();
                Source first =
                        new Source(
                                scheduler,
                                0,
                                new TableBucket(1L, 0),
                                buffer,
                                clock,
                                io,
                                fanout("a0", 10L),
                                fanout("a1", 11L),
                                fanout("a2", 12L));
                Source sameTable =
                        new Source(
                                scheduler,
                                0,
                                new TableBucket(1L, 1),
                                buffer,
                                clock,
                                io,
                                fanout("a3", 14L));
                Source second =
                        new Source(
                                scheduler,
                                1,
                                new TableBucket(2L, 0),
                                buffer,
                                clock,
                                io,
                                fanout("b", 13L))) {
            first.highWatermark.set(2);
            sameTable.highWatermark.set(2);
            second.highWatermark.set(2);
            blocker.allocatePage(4096);
            submitAndComplete(first, io);
            assertThat(first.replicator.poll()).isTrue();
            submitAndComplete(sameTable, io);
            assertThat(sameTable.replicator.poll()).isTrue();
            submitAndComplete(second, io);
            assertThat(second.replicator.poll()).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(4096L);
            clock.advanceTime(50, TimeUnit.MILLISECONDS);
            submitAndComplete(first, io);
            submitAndComplete(sameTable, io);
            submitAndComplete(second, io);
            blocker.close();
            assertThat(first.replicator.poll()).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(8192L);
            int sameTableReads = sameTable.reads.get();
            int secondReads = second.reads.get();
            assertThat(second.replicator.poll()).isFalse();
            assertThat(second.replicator.hasPendingRead()).isTrue();
            drain(buffer);
            assertThat(sameTable.replicator.poll()).isFalse();
            assertThat(sameTable.replicator.hasPendingRead()).isTrue();
            assertThat(sameTable.reads).hasValue(sameTableReads);
            assertThat(second.replicator.poll()).isTrue();
            assertThat(second.reads).hasValue(secondReads);
            assertThat(buffer.buckets())
                    .containsExactlyInAnyOrder(new TableBucket(13L, 1), new TableBucket(13L, 2));
            drain(buffer);
            assertThat(second.replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(first.replicator.getAllIndexPushedOffset()).isZero();
            assertThat(sameTable.replicator.getAllIndexPushedOffset()).isZero();
            assertThat(first.replicator.isFailed()).isFalse();
            assertThat(sameTable.replicator.isFailed()).isFalse();
            assertThat(second.replicator.isFailed()).isFalse();
        }
        assertThat(buffer.pendingBytes()).isZero();
    }

    @Test
    void ordinaryPublicationDoesNotConsumeCapacityOwnersTableTurn() throws Exception {
        FairIndexReplicationScheduler scheduler = new FairIndexReplicationScheduler();
        IndexSendBuffer buffer = new IndexSendBuffer(4096);
        ManualClock clock = new ManualClock();
        QueuedExecutor io = new QueuedExecutor();
        AtomicBoolean activateOwner = new AtomicBoolean(true);
        IndexSpec delegate = spec("a0", 10L);
        try (IndexSendBuffer.Reservation blocker = buffer.newReservation();
                Source owner =
                        new Source(
                                scheduler,
                                2,
                                new TableBucket(3L, 0),
                                buffer,
                                clock,
                                io,
                                spec("c", 13L));
                Source first =
                        new Source(
                                scheduler,
                                0,
                                new TableBucket(1L, 0),
                                buffer,
                                clock,
                                io,
                                new IndexSpec(
                                        "a0",
                                        IndexVisibility.SYNC,
                                        10L,
                                        1,
                                        KvFormat.COMPACTED,
                                        new int[] {0},
                                        delegate::encodeEntry,
                                        (source, target, end) -> {
                                            if (activateOwner.compareAndSet(true, false)) {
                                                assertThat(owner.replicator.poll()).isFalse();
                                                assertThat(owner.replicator.hasPendingRead())
                                                        .isTrue();
                                            }
                                            return delegate.encodeProgress(source, target, end);
                                        }));
                Source sameTable =
                        new Source(
                                scheduler,
                                0,
                                new TableBucket(1L, 1),
                                buffer,
                                clock,
                                io,
                                spec("a1", 11L));
                Source second =
                        new Source(
                                scheduler,
                                1,
                                new TableBucket(2L, 0),
                                buffer,
                                clock,
                                io,
                                spec("b", 12L));
                Source idle =
                        new Source(
                                scheduler,
                                3,
                                new TableBucket(4L, 0),
                                buffer,
                                clock,
                                spec("idle", 14L))) {
            first.highWatermark.set(2);
            sameTable.highWatermark.set(2);
            second.highWatermark.set(2);
            owner.highWatermark.set(2);
            idle.highWatermark.set(0);
            assertThat(first.replicator.poll()).isTrue();
            assertThat(sameTable.replicator.poll()).isTrue();
            assertThat(second.replicator.poll()).isTrue();
            assertThat(owner.replicator.poll()).isTrue();
            blocker.allocatePage(4096);
            for (Source source : Arrays.asList(first, sameTable, second, owner)) {
                io.runNext();
                assertThat(source.replicator.poll()).isTrue();
            }
            assertThat(buffer.pendingBytes()).isEqualTo(4096L);
            blocker.close();
            clock.advanceTime(50, TimeUnit.MILLISECONDS);
            assertThat(owner.replicator.poll()).isTrue();
            assertThat(first.replicator.poll()).isTrue();
            assertThat(sameTable.replicator.poll()).isTrue();
            assertThat(second.replicator.poll()).isTrue();
            io.runNext();
            assertThat(scheduler.pollWorker(3)).isFalse();
            io.runNext();
            io.runNext();
            io.runNext();
            assertThat(first.replicator.poll()).isTrue();
            assertThat(activateOwner).isFalse();
            acknowledge(buffer, 10L, 0);
            assertThat(owner.replicator.poll()).isTrue();
            assertThat(sameTable.replicator.poll()).isFalse();
            assertThat(sameTable.replicator.hasPendingRead()).isTrue();
            acknowledge(buffer, 13L, 0);
            assertThat(sameTable.replicator.poll()).isTrue();
            acknowledge(buffer, 11L, 0);
            assertThat(first.replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(owner.replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(sameTable.replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(second.replicator.getAllIndexPushedOffset()).isZero();
            assertThat(second.replicator.hasPendingRead()).isTrue();
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @Test
    void coldCapacityWaiterDoesNotFreezeOthersAndErrorReleasesProtection() throws Exception {
        FairIndexReplicationScheduler scheduler = new FairIndexReplicationScheduler();
        IndexSendBuffer buffer = new IndexSendBuffer(8192);
        ManualClock clock = new ManualClock();
        QueuedExecutor io = new QueuedExecutor();
        AtomicBoolean fail = new AtomicBoolean();
        IndexSpec delegate = fanout("owner", 10L);
        IndexSpec owner =
                new IndexSpec(
                        "owner",
                        IndexVisibility.SYNC,
                        10L,
                        1,
                        KvFormat.COMPACTED,
                        new int[] {0},
                        delegate::encodeEntry,
                        (source, target, end) -> {
                            if (fail.get()) {
                                throw new AssertionError("window construction interrupted");
                            }
                            return delegate.encodeProgress(source, target, end);
                        });
        try (IndexSendBuffer.Reservation blocker = buffer.newReservation();
                Source first =
                        new Source(scheduler, 0, new TableBucket(1L, 0), buffer, clock, io, owner);
                Source other =
                        new Source(
                                scheduler,
                                1,
                                new TableBucket(2L, 0),
                                buffer,
                                clock,
                                spec("other", 11L))) {
            first.highWatermark.set(2);
            other.highWatermark.set(4);
            blocker.allocatePage(4096);
            submitAndComplete(first, io);
            assertThat(first.replicator.poll()).isTrue();
            clock.advanceTime(50, TimeUnit.MILLISECONDS);
            assertThat(first.replicator.poll()).isTrue();
            assertThat(first.replicator.poll()).isFalse();
            assertThat(other.replicator.poll()).isTrue();
            acknowledge(buffer, 11L, 0);
            blocker.close();
            io.runNext();
            fail.set(true);
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(buffer.pendingBytes()).isZero();
            assertThat(other.replicator.poll()).isTrue();
            acknowledge(buffer, 11L, 0);
            assertThat(other.replicator.getAllIndexPushedOffset()).isEqualTo(4L);
        }
        assertThat(buffer.pendingBytes()).isZero();
    }

    @Test
    void oldUnregisterCannotRemoveReplacementForSameBucket() throws Exception {
        FairIndexReplicationScheduler scheduler = new FairIndexReplicationScheduler();
        IndexSendBuffer buffer = new IndexSendBuffer();
        ManualClock clock = new ManualClock();
        TableBucket bucket = new TableBucket(1L, 0);
        try (Source previous = new Source(scheduler, 0, bucket, buffer, clock, spec("old", 10L));
                Source replacement =
                        new Source(scheduler, 0, bucket, buffer, clock, spec("new", 11L))) {
            scheduler.unregister(0, previous.replicator);
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(previous.reads).hasValue(0);
            assertThat(replacement.reads).hasValue(1);
            assertThat(buffer.buckets()).containsExactly(new TableBucket(11L, 0));
            drain(buffer);
            assertThat(replacement.replicator.getAllIndexPushedOffset()).isEqualTo(2L);
        }
    }

    private static void submitAndComplete(Source source, QueuedExecutor io) {
        assertThat(source.replicator.poll()).isTrue();
        io.runNext();
    }

    private static IndexSpec spec(String name, long tableId) {
        return spec(name, tableId, IndexVisibility.SYNC);
    }

    private static IndexSpec spec(String name, long tableId, IndexVisibility visibility) {
        CompactedRowWriter writer = new CompactedRowWriter(1);
        writer.writeByte((byte) 1);
        CompactedRow value = new CompactedRow(new DataType[] {DataTypes.TINYINT()});
        value.pointTo(writer.segment(), 0, writer.position());
        return new IndexSpec(
                name,
                visibility,
                tableId,
                1,
                KvFormat.COMPACTED,
                new int[] {0},
                row -> new IndexSpec.IndexEntry(name.getBytes(), value, 0),
                (source, target, end) ->
                        new IndexSpec.IndexEntry((name + "-progress").getBytes(), value, target));
    }

    private static IndexSpec fanout(String name, long tableId) {
        IndexSpec delegate = spec(name, tableId);
        return new IndexSpec(
                name,
                IndexVisibility.SYNC,
                tableId,
                1,
                KvFormat.COMPACTED,
                new int[] {0},
                row -> {
                    IndexSpec.IndexEntry entry = delegate.encodeEntry(row);
                    return new IndexSpec.IndexEntry(entry.key(), entry.value(), row.getInt(0));
                },
                delegate::encodeProgress);
    }

    private static void acknowledge(IndexSendBuffer buffer, long table, int bucket) {
        IndexBatch batch = buffer.claimFirstReady(new TableBucket(table, bucket));
        assertThat(batch).isNotNull();
        assertThat(buffer.acknowledgeClaim(batch)).isTrue();
        batch.window().onBatchAcked();
    }

    private static void drain(IndexSendBuffer buffer) {
        for (TableBucket target : buffer.buckets()) {
            IndexBatch batch;
            while ((batch = buffer.claimFirstReady(target)) != null) {
                assertThat(buffer.acknowledgeClaim(batch)).isTrue();
                batch.window().onBatchAcked();
            }
        }
    }

    private static final class QueuedExecutor implements Executor {
        private final Queue<Runnable> queued = new ArrayDeque<>();

        @Override
        public void execute(Runnable runnable) {
            queued.add(runnable);
        }

        private void runNext() {
            assertThat(queued).isNotEmpty();
            queued.remove().run();
        }
    }

    private static final class Source implements AutoCloseable {
        private final FairIndexReplicationScheduler scheduler;
        private final int worker;
        private final AtomicInteger reads = new AtomicInteger();
        private final AtomicLong highWatermark = new AtomicLong(16L);
        private final IndexReplicator replicator;

        private Source(
                FairIndexReplicationScheduler scheduler,
                int worker,
                TableBucket bucket,
                IndexSendBuffer buffer,
                ManualClock clock,
                IndexSpec... specs)
                throws Exception {
            this(scheduler, worker, bucket, buffer, clock, null, specs);
        }

        private Source(
                FairIndexReplicationScheduler scheduler,
                int worker,
                TableBucket bucket,
                IndexSendBuffer buffer,
                ManualClock clock,
                QueuedExecutor remote,
                IndexSpec... specs)
                throws Exception {
            this.scheduler = scheduler;
            this.worker = worker;
            List<LogRecordBatch> batches = new ArrayList<>();
            AtomicInteger totalBytes = new AtomicInteger();
            for (int offset = 0; offset < 16; offset += 2) {
                MemoryLogRecords records =
                        createBasicMemoryLogRecords(
                                RowType.of(DataTypes.INT()),
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
                batches.add(records.batches().iterator().next());
                totalBytes.addAndGet(records.sizeInBytes());
            }
            LogRecords records =
                    new LogRecords() {
                        @Override
                        public int sizeInBytes() {
                            return totalBytes.get();
                        }

                        @Override
                        public Iterable<LogRecordBatch> batches() {
                            return batches;
                        }
                    };
            IndexSourceReader.SourceLog log =
                    new IndexSourceReader.SourceLog() {
                        @Override
                        public TableBucket tableBucket() {
                            return bucket;
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
                        public long localLogStartOffset() {
                            return remote == null ? 0L : 16L;
                        }

                        @Override
                        public FetchDataInfo read(
                                long offset,
                                int maxBytes,
                                FetchIsolation isolation,
                                boolean minOneMessage) {
                            reads.incrementAndGet();
                            return new FetchDataInfo(records);
                        }
                    };
            IndexSourceReader.RemoteFetcherFactory remoteFactory =
                    remote == null
                            ? null
                            : () ->
                                    new IndexSourceReader.RemoteFetcher() {
                                        @Override
                                        public Iterable<LogRecordBatch> fetch(
                                                long start, long localStart) {
                                            reads.incrementAndGet();
                                            return batches;
                                        }

                                        @Override
                                        public void close() {}
                                    };
            LogRecordReadContext context =
                    LogRecordReadContext.createIndexedReadContext(
                            RowType.of(DataTypes.INT()), 1, null);
            replicator =
                    new IndexReplicator(
                            new IndexSourceReader(
                                    log,
                                    remoteFactory,
                                    remote == null ? Runnable::run : remote,
                                    context),
                            Arrays.asList(specs),
                            buffer,
                            context,
                            0L,
                            1,
                            (sync, all) -> {},
                            new ExponentialBackoff(10, 2, 100, 0),
                            clock,
                            null);
            replicator.setScheduler(scheduler);
            scheduler.register(worker, replicator);
        }

        @Override
        public void close() {
            scheduler.unregister(worker, replicator);
            replicator.close();
        }
    }
}
