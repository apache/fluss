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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.server.index.TestingIndexSourceLog.RemoteSessions;
import org.apache.fluss.testutils.ListLogRecords;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.apache.fluss.server.index.TestingIndexEncoders.encoder;
import static org.apache.fluss.server.index.TestingIndexEncoders.fanout;
import static org.apache.fluss.server.index.TestingIndexSourceLog.LOCAL_WAL_ONLY;
import static org.apache.fluss.server.index.TestingIndexSourceLog.sourceRecords;
import static org.assertj.core.api.Assertions.assertThat;

class IndexReplicationSchedulerTest {

    private final ManualClock clock = new ManualClock();
    private IndexReplicationScheduler scheduler;
    private IndexSendBuffer buffer;

    @Test
    void inputAdmissionRotatesTablesAcrossWorkersAndReleasesOnClose() throws Exception {
        scheduler = new IndexReplicationScheduler(1);
        buffer = new IndexSendBuffer();
        ManuallyTriggeredScheduledExecutorService io =
                new ManuallyTriggeredScheduledExecutorService();
        try (Source first = new Source(0, new TableBucket(1L, 0), io, encoder("a", 10L));
                Source second = new Source(1, new TableBucket(1L, 1), io, encoder("b", 11L));
                Source third = new Source(2, new TableBucket(1L, 2), io, encoder("c", 12L));
                Source otherTable = new Source(3, new TableBucket(2L, 0), io, encoder("d", 13L))) {
            assertThat(scheduler.pollWorker(first.worker)).isTrue();
            assertThat(scheduler.pollWorker(second.worker)).isFalse();
            assertThat(scheduler.pollWorker(third.worker)).isFalse();
            assertThat(scheduler.pollWorker(otherTable.worker)).isFalse();
            assertThat(io.numQueuedRunnables()).isEqualTo(1);
            // Cancelling a read gives its slot to the next source, not to a fresh arrival.
            first.close();
            io.trigger();
            assertThat(scheduler.pollWorker(second.worker)).isTrue();
            assertThat(scheduler.pollWorker(third.worker)).isFalse();
            io.trigger();
            assertThat(scheduler.pollWorker(second.worker)).isTrue();
            // The next table gets a turn before the remaining source in the larger table.
            assertThat(scheduler.pollWorker(third.worker)).isFalse();
            assertThat(scheduler.pollWorker(otherTable.worker)).isTrue();
            io.trigger();
            assertThat(scheduler.pollWorker(otherTable.worker)).isTrue();
            assertThat(scheduler.pollWorker(third.worker)).isTrue();
            io.trigger();
            assertThat(scheduler.pollWorker(third.worker)).isTrue();
            drain(buffer);
            assertThat(second.replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(otherTable.replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(third.replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(first.reads).hasValue(0);
        }
        assertThat(buffer.pendingBytes()).isZero();
    }

    @Test
    void capacityRetryKeepsInputAdmissionUntilItsBuildFinishes() throws Exception {
        scheduler = new IndexReplicationScheduler(1);
        buffer = new IndexSendBuffer(8192);
        ManuallyTriggeredScheduledExecutorService io =
                new ManuallyTriggeredScheduledExecutorService();
        try (IndexSendBuffer.Reservation blocker = buffer.newReservation();
                Source owner = new Source(0, new TableBucket(1L, 0), io, encoder("a", 10L));
                Source other = new Source(1, new TableBucket(2L, 0), io, encoder("b", 11L))) {
            assertThat(scheduler.pollWorker(owner.worker)).isTrue();
            blocker.allocatePage(8192);
            io.trigger();
            assertThat(scheduler.pollWorker(owner.worker)).isTrue();
            assertThat(owner.replicator.hasPendingRead()).isFalse();
            assertThat(scheduler.pollWorker(other.worker)).isFalse();
            blocker.close();
            clock.advanceTime(50, TimeUnit.MILLISECONDS);
            assertThat(scheduler.pollWorker(owner.worker)).isTrue();
            io.trigger();
            assertThat(scheduler.pollWorker(owner.worker)).isTrue();
            // Input closes at publication; the slot is not held until the network ACK.
            assertThat(scheduler.pollWorker(other.worker)).isTrue();
            io.trigger();
            acknowledge(buffer, 10L, 0);
            assertThat(scheduler.pollWorker(other.worker)).isTrue();
            acknowledge(buffer, 11L, 0);
            assertThat(owner.replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(other.replicator.getAllIndexPushedOffset()).isEqualTo(2L);
        }
        assertThat(buffer.pendingBytes()).isZero();
    }

    @Test
    void serviceOnAnotherWorkerDoesNotConsumeLocalTablePosition() throws Exception {
        scheduler = new IndexReplicationScheduler(8);
        buffer = new IndexSendBuffer();
        try (Source firstA = new Source(0, new TableBucket(1L, 0), encoder("a0", 10L));
                Source secondA = new Source(1, new TableBucket(1L, 1), encoder("a1", 11L));
                Source firstB = new Source(1, new TableBucket(2L, 0), encoder("b0", 12L));
                Source secondB = new Source(1, new TableBucket(2L, 1), encoder("b1", 13L))) {
            for (int round = 0; round < 6; round++) {
                assertThat(scheduler.pollWorker(0)).isTrue();
                drain(buffer);
                assertThat(scheduler.pollWorker(1)).isTrue();
                drain(buffer);
            }
            assertThat(firstA.reads).hasValue(6);
            assertThat(secondA.reads.get()).isEqualTo(firstB.reads.get() + secondB.reads.get());
            assertThat(firstB.reads.get()).isPositive();
            assertThat(secondB.reads.get()).isPositive();
        }
        assertThat(buffer.pendingBytes()).isZero();
    }

    @Test
    void syncLookaheadChecksStartWithoutTruncatingEndAndIncludesBlockedMinimum() throws Exception {
        scheduler = new IndexReplicationScheduler(8);
        buffer = new IndexSendBuffer();
        try (Source source =
                new Source(0, new TableBucket(1L, 0), encoder("fast", 10L), encoder("slow", 11L))) {
            source.highWatermark.set(2);
            assertThat(scheduler.pollWorker(0)).isTrue();
            acknowledge(buffer, 10L, 0);
            source.highWatermark.set(6);
            assertThat(scheduler.pollWorker(0)).isTrue();
            IndexBatch ahead = buffer.claimFirstReady(new TableBucket(10L, 0));
            assertThat(ahead).isNotNull();
            assertThat(ahead.sourceEndOffset()).isEqualTo(4L);
            assertThat(buffer.completeClaim(ahead)).isTrue();
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
        scheduler = new IndexReplicationScheduler(8);
        buffer = new IndexSendBuffer();
        AtomicBoolean broken = new AtomicBoolean(true);
        IndexReplicationEncoder delegate = encoder("recovering", 11L);
        IndexReplicationEncoder recovering =
                encoder(
                        "recovering",
                        11L,
                        IndexVisibility.ASYNC,
                        row -> {
                            if (broken.get()) {
                                throw new IllegalStateException("index needs repair");
                            }
                            return delegate.encodeDataEntry(row);
                        },
                        delegate::encodeProgress);
        try (Source source =
                new Source(
                        0,
                        new TableBucket(1L, 0),
                        encoder("sync", 10L),
                        recovering,
                        encoder("healthyAsync", 12L, IndexVisibility.ASYNC))) {
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
            assertThat(buffer.completeClaim(async)).isTrue();
            async.window().onBatchAcked();
            assertThat(scheduler.pollWorker(0)).isFalse();
            acknowledge(buffer, 10L, 0);
            broken.set(false);
            clock.advanceTime(10, TimeUnit.MILLISECONDS);
            for (int attempt = 0; attempt < 6 && source.replicator.hasFailure(); attempt++) {
                scheduler.pollWorker(0);
                drain(buffer);
            }
            assertThat(source.replicator.hasFailure()).isFalse();
            assertThat(source.replicator.getAllIndexPushedOffset()).isGreaterThanOrEqualTo(2L);
            assertThat(source.replicator.getSyncIndexPushedOffset()).isGreaterThanOrEqualTo(4L);
        }
        assertThat(buffer.pendingBytes()).isZero();
    }

    @Test
    void capacityHandoffIsByTableAndReadyOwnerProtectsBeforePagesAreFree() throws Exception {
        scheduler = new IndexReplicationScheduler(8);
        buffer = new IndexSendBuffer(8192);
        ManuallyTriggeredScheduledExecutorService io =
                new ManuallyTriggeredScheduledExecutorService();
        try (IndexSendBuffer.Reservation blocker = buffer.newReservation();
                Source first =
                        new Source(
                                0,
                                new TableBucket(1L, 0),
                                io,
                                fanout("a0", 10L),
                                fanout("a1", 11L),
                                fanout("a2", 12L));
                Source sameTable = new Source(2, new TableBucket(1L, 1), io, fanout("a3", 14L));
                Source second = new Source(1, new TableBucket(2L, 0), io, fanout("b", 13L))) {
            first.highWatermark.set(2);
            sameTable.highWatermark.set(2);
            second.highWatermark.set(2);
            blocker.allocatePage(4096);
            submitAndComplete(first, io);
            assertThat(scheduler.pollWorker(first.worker)).isTrue();
            submitAndComplete(sameTable, io);
            assertThat(scheduler.pollWorker(sameTable.worker)).isTrue();
            submitAndComplete(second, io);
            assertThat(scheduler.pollWorker(second.worker)).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(4096L);
            clock.advanceTime(50, TimeUnit.MILLISECONDS);
            submitAndComplete(first, io);
            submitAndComplete(sameTable, io);
            submitAndComplete(second, io);
            blocker.close();
            assertThat(scheduler.pollWorker(first.worker)).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(8192L);
            int sameTableReads = sameTable.reads.get();
            int secondReads = second.reads.get();
            assertThat(scheduler.pollWorker(second.worker)).isFalse();
            assertThat(second.replicator.hasPendingRead()).isTrue();
            drain(buffer);
            assertThat(scheduler.pollWorker(sameTable.worker)).isFalse();
            assertThat(sameTable.replicator.hasPendingRead()).isTrue();
            assertThat(sameTable.reads).hasValue(sameTableReads);
            assertThat(scheduler.pollWorker(second.worker)).isTrue();
            assertThat(second.reads).hasValue(secondReads);
            assertThat(buffer.buckets())
                    .containsExactlyInAnyOrder(new TableBucket(13L, 1), new TableBucket(13L, 2));
            drain(buffer);
            assertThat(second.replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(first.replicator.getAllIndexPushedOffset()).isZero();
            assertThat(sameTable.replicator.getAllIndexPushedOffset()).isZero();
            assertThat(first.replicator.hasFailure()).isFalse();
            assertThat(sameTable.replicator.hasFailure()).isFalse();
            assertThat(second.replicator.hasFailure()).isFalse();
        }
        assertThat(buffer.pendingBytes()).isZero();
    }

    @Test
    void ordinaryPublicationDoesNotConsumeCapacityOwnersTableTurn() throws Exception {
        scheduler = new IndexReplicationScheduler(8);
        buffer = new IndexSendBuffer(4096);
        ManuallyTriggeredScheduledExecutorService io =
                new ManuallyTriggeredScheduledExecutorService();
        AtomicBoolean activateOwner = new AtomicBoolean(true);
        IndexReplicationEncoder delegate = encoder("a0", 10L);
        try (IndexSendBuffer.Reservation blocker = buffer.newReservation();
                Source owner = new Source(2, new TableBucket(3L, 0), io, encoder("c", 13L));
                Source first =
                        new Source(
                                0,
                                new TableBucket(1L, 0),
                                io,
                                encoder(
                                        "a0",
                                        10L,
                                        IndexVisibility.SYNC,
                                        delegate::encodeDataEntry,
                                        (target, end) -> {
                                            if (activateOwner.compareAndSet(true, false)) {
                                                assertThat(scheduler.pollWorker(owner.worker))
                                                        .isFalse();
                                                assertThat(owner.replicator.hasPendingRead())
                                                        .isTrue();
                                            }
                                            return delegate.encodeProgress(target, end);
                                        }));
                Source sameTable = new Source(0, new TableBucket(1L, 1), io, encoder("a1", 11L));
                Source second = new Source(1, new TableBucket(2L, 0), io, encoder("b", 12L));
                Source idle = new Source(3, new TableBucket(4L, 0), encoder("idle", 14L))) {
            first.highWatermark.set(2);
            sameTable.highWatermark.set(2);
            second.highWatermark.set(2);
            owner.highWatermark.set(2);
            idle.highWatermark.set(0);
            assertThat(scheduler.pollWorker(first.worker)).isTrue();
            assertThat(scheduler.pollWorker(sameTable.worker)).isTrue();
            assertThat(scheduler.pollWorker(second.worker)).isTrue();
            assertThat(scheduler.pollWorker(owner.worker)).isTrue();
            blocker.allocatePage(4096);
            for (Source source : Arrays.asList(first, sameTable, second, owner)) {
                io.trigger();
                assertThat(scheduler.pollWorker(source.worker)).isTrue();
            }
            assertThat(buffer.pendingBytes()).isEqualTo(4096L);
            blocker.close();
            clock.advanceTime(50, TimeUnit.MILLISECONDS);
            assertThat(scheduler.pollWorker(owner.worker)).isTrue();
            assertThat(scheduler.pollWorker(first.worker)).isTrue();
            assertThat(scheduler.pollWorker(sameTable.worker)).isTrue();
            assertThat(scheduler.pollWorker(second.worker)).isTrue();
            io.trigger();
            assertThat(scheduler.pollWorker(3)).isFalse();
            io.trigger();
            io.trigger();
            io.trigger();
            assertThat(scheduler.pollWorker(first.worker)).isTrue();
            assertThat(activateOwner).isFalse();
            acknowledge(buffer, 10L, 0);
            assertThat(scheduler.pollWorker(owner.worker)).isTrue();
            assertThat(scheduler.pollWorker(sameTable.worker)).isFalse();
            assertThat(sameTable.replicator.hasPendingRead()).isTrue();
            acknowledge(buffer, 13L, 0);
            assertThat(scheduler.pollWorker(sameTable.worker)).isTrue();
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
        scheduler = new IndexReplicationScheduler(8);
        buffer = new IndexSendBuffer(8192);
        ManuallyTriggeredScheduledExecutorService io =
                new ManuallyTriggeredScheduledExecutorService();
        AtomicBoolean fail = new AtomicBoolean();
        IndexReplicationEncoder delegate = fanout("owner", 10L);
        IndexReplicationEncoder owner =
                encoder(
                        "owner",
                        10L,
                        IndexVisibility.SYNC,
                        delegate::encodeDataEntry,
                        (target, end) -> {
                            if (fail.get()) {
                                throw new AssertionError("window construction interrupted");
                            }
                            return delegate.encodeProgress(target, end);
                        });
        try (IndexSendBuffer.Reservation blocker = buffer.newReservation();
                Source first = new Source(0, new TableBucket(1L, 0), io, owner);
                Source other = new Source(1, new TableBucket(2L, 0), encoder("other", 11L))) {
            first.highWatermark.set(2);
            other.highWatermark.set(4);
            blocker.allocatePage(4096);
            submitAndComplete(first, io);
            assertThat(scheduler.pollWorker(first.worker)).isTrue();
            clock.advanceTime(50, TimeUnit.MILLISECONDS);
            assertThat(scheduler.pollWorker(first.worker)).isTrue();
            assertThat(scheduler.pollWorker(first.worker)).isFalse();
            assertThat(scheduler.pollWorker(other.worker)).isTrue();
            acknowledge(buffer, 11L, 0);
            blocker.close();
            io.trigger();
            fail.set(true);
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(buffer.pendingBytes()).isZero();
            assertThat(scheduler.pollWorker(other.worker)).isTrue();
            acknowledge(buffer, 11L, 0);
            assertThat(other.replicator.getAllIndexPushedOffset()).isEqualTo(4L);
        }
        assertThat(buffer.pendingBytes()).isZero();
    }

    @Test
    void oldUnregisterCannotRemoveReplacementForSameBucket() throws Exception {
        scheduler = new IndexReplicationScheduler(8);
        buffer = new IndexSendBuffer();
        TableBucket bucket = new TableBucket(1L, 0);
        try (Source previous = new Source(0, bucket, encoder("old", 10L));
                Source replacement = new Source(0, bucket, encoder("new", 11L))) {
            scheduler.unregister(0, previous.replicator);
            assertThat(scheduler.pollWorker(0)).isTrue();
            assertThat(previous.reads).hasValue(0);
            assertThat(replacement.reads).hasValue(1);
            assertThat(buffer.buckets()).containsExactly(new TableBucket(11L, 0));
            drain(buffer);
            assertThat(replacement.replicator.getAllIndexPushedOffset()).isEqualTo(2L);
        }
    }

    private void submitAndComplete(Source source, ManuallyTriggeredScheduledExecutorService io) {
        assertThat(scheduler.pollWorker(source.worker)).isTrue();
        io.trigger();
    }

    private static void acknowledge(IndexSendBuffer buffer, long table, int bucket) {
        IndexBatch batch = buffer.claimFirstReady(new TableBucket(table, bucket));
        assertThat(batch).isNotNull();
        assertThat(buffer.completeClaim(batch)).isTrue();
        batch.window().onBatchAcked();
    }

    private static void drain(IndexSendBuffer buffer) {
        for (TableBucket target : buffer.buckets()) {
            IndexBatch batch;
            while ((batch = buffer.claimFirstReady(target)) != null) {
                assertThat(buffer.completeClaim(batch)).isTrue();
                batch.window().onBatchAcked();
            }
        }
    }

    private final class Source implements AutoCloseable {
        private final IndexReplicationScheduler scheduler;
        private final int worker;
        private final AtomicInteger reads = new AtomicInteger();
        private final AtomicLong highWatermark = new AtomicLong(16L);
        private final IndexReplicator replicator;

        private Source(int worker, TableBucket bucket, IndexReplicationEncoder... encoders)
                throws Exception {
            this(worker, bucket, null, encoders);
        }

        private Source(
                int worker,
                TableBucket bucket,
                ManuallyTriggeredScheduledExecutorService remote,
                IndexReplicationEncoder... encoders)
                throws Exception {
            this.scheduler = IndexReplicationSchedulerTest.this.scheduler;
            this.worker = worker;
            List<MemoryLogRecords> batches = new ArrayList<>();
            for (int offset = 0; offset < 16; offset += 2) {
                batches.add(sourceRecords(RowType.of(DataTypes.INT()), offset));
            }
            LogRecords records = new ListLogRecords(batches);
            IndexSourceReader.SourceLog log =
                    new TestingIndexSourceLog(
                            bucket,
                            highWatermark::get,
                            remote == null ? 0L : 16L,
                            ignored -> records,
                            reads);
            Supplier<IndexSourceReader.RemoteFetcher> remoteFactory =
                    remote == null
                            ? LOCAL_WAL_ONLY
                            : new RemoteSessions(
                                    offset -> {
                                        reads.incrementAndGet();
                                        return records.batches();
                                    });
            LogRecordReadContext context =
                    LogRecordReadContext.createIndexedReadContext(
                            RowType.of(DataTypes.INT()), 1, null);
            replicator =
                    new IndexReplicator(
                            new IndexSourceReader(
                                    log, remoteFactory, remote == null ? Runnable::run : remote),
                            Arrays.asList(encoders),
                            buffer,
                            context,
                            0L,
                            1,
                            (sync, all) -> {},
                            scheduler,
                            () -> {},
                            new ExponentialBackoff(10, 2, 100, 0),
                            clock,
                            null,
                            ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE.defaultValue().getBytes());
            scheduler.register(worker, replicator);
        }

        @Override
        public void close() {
            scheduler.unregister(worker, replicator);
            replicator.close();
        }
    }
}
