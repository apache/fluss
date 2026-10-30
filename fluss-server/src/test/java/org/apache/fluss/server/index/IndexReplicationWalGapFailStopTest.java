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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;

/**
 * Covers replication-time fail-stop on a source WAL gap: when the pushed offset has fallen below
 * the local log start and the raw remote WAL cannot cover the missing range, replication must stop
 * for good rather than skip ahead. A transient remote read failure over the same gap must stay
 * retryable, since giving up on it would take the index offline over a blip.
 */
class IndexReplicationWalGapFailStopTest {

    private static final TableBucket SOURCE_BUCKET = new TableBucket(1L, 0);
    private static final long HIGH_WATERMARK = 10L;
    private static final long LOCAL_LOG_START = 5L;
    private static final long PUSHED_OFFSET = 0L;

    @Test
    void testRemoteWalShortOfLocalHandoffStopsReplicationForGood() throws Exception {
        AtomicInteger remoteFetches = new AtomicInteger();
        AtomicReference<Throwable> reportedFailure = new AtomicReference<>();
        IndexSendBuffer sendBuffer = new IndexSendBuffer();

        // The remote WAL yields nothing for [0, 5), so the range is unrecoverable from either side.
        IndexSourceReader.RemoteFetcherFactory emptyRemoteWal =
                remoteFetcher(
                        remoteFetches,
                        (startOffset, localLogStartOffset) -> Collections.emptyList());

        try (IndexReplicator replicator = replicator(emptyRemoteWal, sendBuffer, reportedFailure)) {
            assertThatThrownBy(replicator::poll)
                    .isInstanceOf(IndexSourceWalCorruptionException.class)
                    .hasMessageContaining(
                            "remote WAL ended at expected offset "
                                    + PUSHED_OFFSET
                                    + " before local handoff "
                                    + LOCAL_LOG_START);
            assertThat(reportedFailure.get()).isInstanceOf(IndexSourceWalCorruptionException.class);
            assertThat(remoteFetches).hasValue(1);

            // Fail-stop, not fail-once: later polls must neither throw again nor re-read, so no
            // successor window can be derived from a later offset.
            assertThat(replicator.poll()).isFalse();
            assertThat(replicator.poll()).isFalse();
            assertThat(remoteFetches).hasValue(1);

            // The gap must not be papered over by advancing past it.
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(PUSHED_OFFSET);
            assertThat(replicator.getSyncIndexPushedOffset()).isEqualTo(PUSHED_OFFSET);
            assertThat(sendBuffer.buckets()).isEmpty();
        }
    }

    /**
     * Control for the test above: the same gap read failing transiently must keep being retried, so
     * the terminal transition is attributable to the corruption verdict rather than to any failure
     * on the remote path.
     */
    @Test
    void testTransientRemoteReadFailureKeepsReplicationRetryable() throws Exception {
        AtomicInteger remoteFetches = new AtomicInteger();
        AtomicReference<Throwable> reportedFailure = new AtomicReference<>();
        IndexSendBuffer sendBuffer = new IndexSendBuffer();

        IndexSourceReader.RemoteFetcherFactory unavailableRemoteWal =
                remoteFetcher(
                        remoteFetches,
                        (startOffset, localLogStartOffset) -> {
                            throw new IOException("remote storage temporarily unavailable");
                        });

        try (IndexReplicator replicator =
                replicator(unavailableRemoteWal, sendBuffer, reportedFailure)) {
            assertThat(replicator.poll()).isFalse();
            assertThat(remoteFetches).hasValue(1);

            assertThat(replicator.poll()).isFalse();
            assertThat(remoteFetches).hasValue(2);

            assertThat(reportedFailure.get()).isNull();
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(PUSHED_OFFSET);
            assertThat(sendBuffer.buckets()).isEmpty();
        }
    }

    private static IndexReplicator replicator(
            IndexSourceReader.RemoteFetcherFactory remoteFetcherFactory,
            IndexSendBuffer sendBuffer,
            AtomicReference<Throwable> reportedFailure) {
        LogRecordReadContext readContext = mock(LogRecordReadContext.class);
        IndexSourceReader sourceReader =
                new IndexSourceReader(
                        gappedSourceLog(), remoteFetcherFactory, Runnable::run, readContext);
        return new IndexReplicator(
                sourceReader,
                Collections.singletonList(syncIndexSpec()),
                sendBuffer,
                readContext,
                PUSHED_OFFSET,
                1024,
                1,
                (syncOffset, allOffset) -> {},
                (ignored, failure) -> reportedFailure.set(failure));
    }

    /** A source log whose early segments are gone, so {@code [0, 5)} is only reachable remotely. */
    private static IndexSourceReader.SourceLog gappedSourceLog() {
        return new IndexSourceReader.SourceLog() {
            @Override
            public TableBucket tableBucket() {
                return SOURCE_BUCKET;
            }

            @Override
            public long highWatermark() {
                return HIGH_WATERMARK;
            }

            @Override
            public long logStartOffset() {
                return LOCAL_LOG_START;
            }

            @Override
            public FetchDataInfo read(
                    long offset, int maxBytes, FetchIsolation isolation, boolean minOneMessage) {
                return fail(
                        "local read at offset "
                                + offset
                                + " must not be attempted below local log start "
                                + LOCAL_LOG_START);
            }
        };
    }

    private static IndexSourceReader.RemoteFetcherFactory remoteFetcher(
            AtomicInteger fetchCount, RemoteRangeFetch fetch) {
        return () ->
                new IndexSourceReader.RemoteFetcher() {
                    @Override
                    public Iterable<LogRecordBatch> fetch(
                            long startOffset, long localLogStartOffset) throws Exception {
                        fetchCount.incrementAndGet();
                        return fetch.apply(startOffset, localLogStartOffset);
                    }

                    @Override
                    public void close() {}
                };
    }

    @FunctionalInterface
    private interface RemoteRangeFetch {
        Iterable<LogRecordBatch> apply(long startOffset, long localLogStartOffset) throws Exception;
    }

    private static IndexSpec syncIndexSpec() {
        CompactedRowWriter writer = new CompactedRowWriter(1);
        writer.writeByte((byte) 1);
        CompactedRow value = new CompactedRow(new DataType[] {DataTypes.TINYINT()});
        value.pointTo(writer.segment(), 0, writer.position());
        return new IndexSpec(
                "idx_b",
                IndexVisibility.SYNC,
                2L,
                1,
                KvFormat.COMPACTED,
                new int[] {0},
                ignored ->
                        fail("no index entry may be encoded from a WAL range that cannot be read"),
                (sourceBucket, targetBucket, sourceEndOffset) ->
                        new IndexSpec.IndexEntry("idx_b-progress".getBytes(), value, targetBucket));
    }
}
