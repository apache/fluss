/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.replica;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.remote.RemoteLogFetchInfo;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.protocol.FetchLogReadPreference;
import org.apache.fluss.rpc.util.PredicateMessageUtils;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.log.FilterInfo;
import org.apache.fluss.server.replica.delay.DelayedTableBucketKey;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics;
import static org.apache.fluss.record.TestData.ANOTHER_DATA1;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.server.log.FetchIsolation.HIGH_WATERMARK;
import static org.apache.fluss.server.log.LogOffsetMetadata.UNKNOWN_OFFSET_METADATA;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithWriterId;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for bounded fetches. */
class ReplicaManagerBoundedFetchTest extends ReplicaTestBase {

    private static final int MAX_FETCH_BYTES = 1024 * 1024;
    private static final long MAX_WAIT_MS = 10_000L;
    private static final RowType ROW_TYPE_WITH_FIELD_IDS = DATA1_SCHEMA.getRowType();

    @Test
    void testCompletedBoundedFetchReturnsImmediatelyBelowMinBytes() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());

        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                fetch(boundedParams(1024), Collections.singletonMap(tb, bounded(tb, 0, 0)));

        assertThat(future.isDone()).isTrue();
        FetchLogResultForBucket result = future.get().get(tb);
        assertThat(result.succeeded()).isTrue();
        assertThat(result.recordsOrEmpty().sizeInBytes()).isZero();
        assertThat(result.getFilteredEndOffset()).isZero();
        assertThat(result.getLogStartOffset()).isZero();
        assertThat(replicaManager.getDelayedFetchLogManager().numDelayed()).isZero();
    }

    @Test
    void testCompletedBoundedFetchWithNonZeroOffsetReturnsImmediately() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        append(tb, genMemoryLogRecordsByObject(DATA1));

        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                fetch(boundedParams(1024), Collections.singletonMap(tb, bounded(tb, 5, 5)));

        assertThat(future.isDone()).isTrue();
        FetchLogResultForBucket result = future.get().get(tb);
        assertThat(result.succeeded()).isTrue();
        assertThat(result.recordsOrEmpty().sizeInBytes()).isZero();
        assertThat(result.getFilteredEndOffset()).isEqualTo(5L);
    }

    @ParameterizedTest
    @CsvSource({"0, 100", "1, 100", "0, -2", "1, -2"})
    void testUnboundedFetchPreservesLegacyBehavior(short apiVersion, long fetchOffset)
            throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        Replica replica = replicaManager.getReplicaOrException(tb);
        replica.truncateFullyAndStartAt(100L);

        MemoryLogRecords records = genMemoryLogRecordsByObject(DATA1);
        append(tb, records);
        FetchParams params = new FetchParams(-1, MAX_FETCH_BYTES);
        params.setApiVersion(apiVersion);
        // No stopping offset: this is an ordinary unbounded fetch.
        FetchReqInfo request = new FetchReqInfo(DATA1_TABLE_ID, fetchOffset, MAX_FETCH_BYTES);
        FetchLogResultForBucket result =
                fetch(params, Collections.singletonMap(tb, request))
                        .get(5, TimeUnit.SECONDS)
                        .get(tb);

        assertThat(result.succeeded()).isTrue();
        assertThat(result.fetchFromRemote()).isFalse();
        assertThat(result.recordsOrEmpty().sizeInBytes()).isPositive();
        // Neither v0 nor v1 should introduce a bounded completion marker.
        assertThat(result.hasFilteredEndOffset()).isFalse();
        // The internal sentinel remains unchanged for an unbounded fetch.
        assertThat(result.getLogStartOffset()).isEqualTo(-1L);
    }

    @Test
    void testBoundedFetchRejectsV0Request() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        FetchParams params = new FetchParams(-1, MAX_FETCH_BYTES);

        FetchLogResultForBucket result =
                fetch(params, Collections.singletonMap(tb, bounded(tb, 0, 0))).get().get(tb);

        assertThat(result.failed()).isTrue();
        assertThat(result.getErrorMessage()).contains("Stopping offsets require a v1");
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    void testBoundedFetchRejectsFollowerRequest(short version) throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());

        FetchParams params = new FetchParams(TABLET_SERVER_ID, MAX_FETCH_BYTES);
        params.setApiVersion(version);

        FetchLogResultForBucket result =
                fetch(params, Collections.singletonMap(tb, bounded(tb, 0, 0))).get().get(tb);

        assertThat(result.failed()).isTrue();
        assertThat(result.getErrorMessage()).contains("Stopping offsets require a v1");
    }

    @ParameterizedTest
    @CsvSource({"0, -1", "-1, 10", "-3, 10"})
    void testBoundedFetchRejectsInvalidOffsets(long start, long stop) throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        FetchLogResultForBucket result =
                fetch(boundedParams(0), Collections.singletonMap(tb, bounded(tb, start, stop)))
                        .get()
                        .get(tb);

        assertThat(result.failed()).isTrue();
        assertThat(result.getErrorMessage()).contains("nonnegative bounds");
    }

    @Test
    void testFilteredBoundedFetchCompletesWithoutRecords() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        // Every batch is rejected by the a > 5 predicate.
        append(tb, recordsWithStatistics(0, 1, 2, 3));

        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                fetch(filteredParams(1024), Collections.singletonMap(tb, bounded(tb, 0, 3)));

        assertThat(future.isDone()).isTrue();
        FetchLogResultForBucket result = future.get().get(tb);
        assertThat(result.succeeded()).isTrue();
        assertThat(result.recordsOrEmpty().sizeInBytes()).isZero();
        assertThat(result.getFilteredEndOffset()).isEqualTo(3L);
    }

    @Test
    void testCompletedBoundedBytesSatisfyInitialMinBytes() throws Exception {
        TableBucket completed = new TableBucket(DATA1_TABLE_ID, 1);
        TableBucket idle = new TableBucket(DATA1_TABLE_ID, 2);
        makeLogTableAsLeader(completed.getBucket());
        makeLogTableAsLeader(idle.getBucket());
        appendCompletedFilteredRange(completed);

        Map<TableBucket, FetchReqInfo> request = new HashMap<>();
        request.put(completed, bounded(completed, 0, 6));
        request.put(idle, bounded(idle, 0, 100));

        FetchLogResultForBucket initialCompleted =
                replicaManager
                        .readFromLog(filteredParams(0), request, null)
                        .get(completed)
                        .getFetchLogResultForBucket();
        assertThat(initialCompleted.getFilteredEndOffset()).isEqualTo(6L);
        int completedBytes = initialCompleted.recordsOrEmpty().sizeInBytes();
        assertThat(completedBytes).isGreaterThan(0);

        // The completed bucket already meets minBytes, even though the other is idle.
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                fetch(filteredParams(completedBytes), request);

        assertThat(future.isDone()).isTrue();
        assertThat(future.get()).containsKeys(completed, idle);
        assertThat(future.get().get(completed).recordsOrEmpty().sizeInBytes())
                .isEqualTo(completedBytes);
        assertThat(future.get().get(completed).getFilteredEndOffset()).isEqualTo(6L);
    }

    @Test
    void testDelayedFetchPreservesCompletedBoundedRecords() throws Exception {
        TableBucket completed = new TableBucket(DATA1_TABLE_ID, 1);
        TableBucket pending = new TableBucket(DATA1_TABLE_ID, 2);
        makeLogTableAsLeader(completed.getBucket());
        makeLogTableAsLeader(pending.getBucket());
        appendCompletedFilteredRange(completed);
        append(
                pending,
                genMemoryLogRecordsWithWriterId(
                        Arrays.asList(
                                new Object[] {7, "v"},
                                new Object[] {8, "v"},
                                new Object[] {9, "v"}),
                        200L,
                        0,
                        0L));

        Map<TableBucket, FetchReqInfo> request = new HashMap<>();
        request.put(completed, bounded(completed, 0, 6));
        request.put(pending, bounded(pending, 0, 100));
        FetchParams params = filteredParams(0);
        Map<TableBucket, ReplicaManager.LogReadResult> initial =
                replicaManager.readFromLog(params, request, null);

        FetchLogResultForBucket completedResult =
                initial.get(completed).getFetchLogResultForBucket();
        FetchLogResultForBucket pendingResult = initial.get(pending).getFetchLogResultForBucket();

        assertThat(completedResult.succeeded()).isTrue();
        assertThat(completedResult.getFilteredEndOffset()).isEqualTo(6L);
        assertThat(pendingResult.succeeded()).isTrue();

        int completedBytes = completedResult.recordsOrEmpty().sizeInBytes();
        int initialPendingBytes = pendingResult.recordsOrEmpty().sizeInBytes();

        assertThat(completedBytes).isGreaterThan(0);
        assertThat(initialPendingBytes).isGreaterThan(0);

        // The initial response is one byte short of minBytes.
        int minBytes = completedBytes + initialPendingBytes + 1;
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                fetch(filteredParams(minBytes), request);

        // Neither bucket alone nor both initial results satisfy minBytes.
        assertThat(future).isNotDone();
        MemoryLogRecords additionalRecords =
                genMemoryLogRecordsWithWriterId(
                        Collections.singletonList(new Object[] {10, "v"}), 200L, 1, 3L);

        assertThat(additionalRecords.sizeInBytes()).isPositive();
        assertThat(additionalRecords.sizeInBytes()).isLessThan(completedBytes);

        append(pending, additionalRecords);

        assertThat(replicaManager.getReplicaOrException(pending).getLocalLogEndOffset())
                .isEqualTo(4L);

        // Verify the exact response bytes after B receives the additional batch.
        Map<TableBucket, ReplicaManager.LogReadResult> updated =
                replicaManager.readFromLog(filteredParams(0), request, null);

        FetchLogResultForBucket updatedPending = updated.get(pending).getFetchLogResultForBucket();

        int pendingBytesAfterAppend = updatedPending.recordsOrEmpty().sizeInBytes();

        // Crucial regression condition:
        // B alone cannot satisfy minBytes, but A + B can.
        assertThat(pendingBytesAfterAppend).isLessThan(minBytes);
        assertThat(completedBytes + pendingBytesAfterAppend).isGreaterThanOrEqualTo(minBytes);

        // maxWaitMs is 10 seconds. Completion within 5 seconds must be caused by the minBytes
        // condition, not by expiration.
        Map<TableBucket, FetchLogResultForBucket> result = future.get(5, TimeUnit.SECONDS);

        assertThat(result).containsOnlyKeys(completed, pending);

        FetchLogResultForBucket actualCompleted = result.get(completed);
        FetchLogResultForBucket actualPending = result.get(pending);

        assertThat(actualCompleted.succeeded()).isTrue();
        assertThat(actualCompleted.getFilteredEndOffset()).isEqualTo(6L);
        assertThat(actualCompleted.recordsOrEmpty().sizeInBytes()).isEqualTo(completedBytes);

        assertThat(actualPending.succeeded()).isTrue();
        assertThat(actualPending.recordsOrEmpty().sizeInBytes()).isEqualTo(pendingBytesAfterAppend);

        // The final response includes the completed bounded bucket's records.
        assertThat(
                        actualCompleted.recordsOrEmpty().sizeInBytes()
                                + actualPending.recordsOrEmpty().sizeInBytes())
                .isGreaterThanOrEqualTo(minBytes);
    }

    @Test
    void testDelayedFetchReReadsCompletedBucketAfterLogTruncation() throws Exception {
        TableBucket completed = new TableBucket(DATA1_TABLE_ID, 1);
        TableBucket pending = new TableBucket(DATA1_TABLE_ID, 2);
        makeLogTableAsLeader(completed.getBucket());
        makeLogTableAsLeader(pending.getBucket());
        appendCompletedFilteredRange(completed);
        Map<TableBucket, FetchReqInfo> request = new HashMap<>();
        request.put(completed, bounded(completed, 0L, 6L));
        request.put(pending, bounded(pending, 0L, 100L));
        FetchLogResultForBucket initialCompleted =
                replicaManager
                        .readFromLog(filteredParams(0), request, null)
                        .get(completed)
                        .getFetchLogResultForBucket();

        assertThat(initialCompleted.succeeded()).isTrue();
        assertThat(initialCompleted.getFilteredEndOffset()).isEqualTo(6L);

        int completedBytes = initialCompleted.recordsOrEmpty().sizeInBytes();
        assertThat(completedBytes).isPositive();

        // A has completed, but B still needs data.
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                fetch(filteredParams(completedBytes + 1), request);

        assertThat(future).isNotDone();
        assertThat(replicaManager.getDelayedFetchLogManager().numDelayed()).isEqualTo(1);
        // Replace A's local log while the fetch is delayed.
        // The original file-backed records must not be returned afterward.
        Replica replica = replicaManager.getReplicaOrException(completed);
        replica.truncateFullyAndStartAt(100L);

        assertThat(replica.getLocalLogEndOffset()).isEqualTo(100L);

        // Wake the delayed fetch by appending records to B.
        append(
                pending,
                genMemoryLogRecordsWithWriterId(
                        Arrays.asList(
                                new Object[] {7, "v"},
                                new Object[] {8, "v"},
                                new Object[] {9, "v"}),
                        200L,
                        0,
                        0L));

        Map<TableBucket, FetchLogResultForBucket> result = future.get(5, TimeUnit.SECONDS);

        assertThat(result).containsOnlyKeys(completed, pending);

        // A must be re-read. Offset 0 is no longer available after truncation.
        FetchLogResultForBucket completedResult = result.get(completed);
        assertThat(completedResult.failed()).isTrue();
        assertThat(completedResult.recordsOrEmpty().sizeInBytes()).isZero();

        // B must still be returned successfully.
        FetchLogResultForBucket pendingResult = result.get(pending);
        assertThat(pendingResult.succeeded()).isTrue();
        assertThat(pendingResult.recordsOrEmpty().sizeInBytes()).isPositive();
    }

    @Test
    void testCompletedEmptyBucketDoesNotDelayRemoteFetch() throws Exception {
        TableBucket completed = new TableBucket(DATA1_TABLE_ID, 1);
        TableBucket remote = new TableBucket(DATA1_TABLE_ID, 2);

        Map<TableBucket, FetchReqInfo> request = new HashMap<>();
        request.put(completed, bounded(completed, 0L, 0L));
        request.put(remote, bounded(remote, 0L, 100L));

        // A has completed its bounded range without returning records.
        FetchLogResultForBucket completedResult = FetchLogResultForBucket.empty(completed, 0L, 0L);

        // B has already obtained remote fetch information.
        RemoteLogFetchInfo remoteInfo =
                new RemoteLogFetchInfo("unused-remote-directory", null, Collections.emptyList(), 0);
        FetchLogResultForBucket remoteResult =
                FetchLogResultForBucket.remote(remote, remoteInfo, 100L);

        Map<TableBucket, ReplicaManager.LogReadResult> logReadResults = new HashMap<>();
        logReadResults.put(
                completed,
                new ReplicaManager.LogReadResult(completedResult, UNKNOWN_OFFSET_METADATA));
        logReadResults.put(
                remote, new ReplicaManager.LogReadResult(remoteResult, UNKNOWN_OFFSET_METADATA));

        FetchParams params = boundedParams(1024);
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                new CompletableFuture<>();

        // Test the real delayed-fetch decision with controlled initial results.
        replicaManager.maybeAddDelayedFetchLog(
                params, request, logReadResults, null, future::complete);

        // No unfinished local bucket remains, so the response must be immediate.
        assertThat(future).isDone();

        Map<TableBucket, FetchLogResultForBucket> result = future.get();
        assertThat(result).containsOnlyKeys(completed, remote);

        assertThat(result.get(completed).succeeded()).isTrue();
        assertThat(result.get(completed).recordsOrEmpty().sizeInBytes()).isZero();
        assertThat(result.get(completed).getFilteredEndOffset()).isZero();

        assertThat(result.get(remote).succeeded()).isTrue();
        assertThat(result.get(remote).fetchFromRemote()).isTrue();
        assertThat(result.get(remote).remoteLogFetchInfo()).isSameAs(remoteInfo);

        assertThat(replicaManager.getDelayedFetchLogManager().numDelayed()).isZero();
    }

    @Test
    void testUnfilteredBoundedReadLeavesExactTruncationToClient() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        MemoryLogRecords batch = genMemoryLogRecordsByObject(DATA1);
        assertThat(batch.batches().iterator().next().nextLogOffset()).isGreaterThan(5L);
        append(tb, batch);

        // The server may return an entire batch crossing stop=5; the scanner
        // performs the exact record-level [start, stop) truncation.
        FetchLogResultForBucket result =
                fetch(boundedParams(0), Collections.singletonMap(tb, bounded(tb, 0, 5)))
                        .get()
                        .get(tb);

        assertThat(result.succeeded()).isTrue();
        assertThat(result.recordsOrEmpty().sizeInBytes()).isGreaterThan(0);
        assertThat(result.hasFilteredEndOffset()).isFalse();
    }

    @Test
    void testEarliestStartAtOrPastStopCompletesImmediately() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());

        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                fetch(
                        boundedParams(1024),
                        Collections.singletonMap(
                                tb, bounded(tb, FetchParams.FETCH_FROM_EARLIEST_OFFSET, 0)));

        assertThat(future.isDone()).isTrue();
        FetchLogResultForBucket result = future.get().get(tb);
        assertThat(result.succeeded()).isTrue();
        assertThat(result.getLogStartOffset()).isZero();
        assertThat(result.getFilteredEndOffset()).isZero();
    }

    @Test
    void testCompletedBoundedFetchRejectsMissingBucket() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        TableBucket missing = new TableBucket(DATA1_TABLE_ID, 99);
        FetchLogResultForBucket result =
                fetch(
                                boundedParams(1024),
                                Collections.singletonMap(missing, bounded(missing, 10L, 10L)))
                        .get(5, TimeUnit.SECONDS)
                        .get(missing);

        // A completed range must not mask an invalid bucket.
        assertThat(result).isNotNull();
        assertThat(result.failed()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(FetchLogReadPreference.class)
    void testCompletedBoundedFetchWithReadPreference(FetchLogReadPreference preference)
            throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        FetchParams params =
                new FetchParams(-1, true, MAX_FETCH_BYTES, 1024, MAX_WAIT_MS, null, preference);
        params.setApiVersion((short) 1);
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                fetch(
                        params,
                        Collections.singletonMap(
                                tb, bounded(tb, FetchParams.FETCH_FROM_EARLIEST_OFFSET, 0L)));
        assertThat(future).isDone();

        FetchLogResultForBucket result = future.get().get(tb);
        assertThat(result.succeeded()).isTrue();
        assertThat(result.recordsOrEmpty().sizeInBytes()).isZero();
        assertThat(result.getFilteredEndOffset()).isZero();
    }

    @ParameterizedTest(name = "boundedPending={0}, initiallyHasRecords={1}")
    @CsvSource({"true, true", "false, true", "false, false"})
    void testCompletedBucketDoesNotFinishPendingFetch(
            boolean boundedPending, boolean initiallyHasRecords) throws Exception {
        TableBucket completed = new TableBucket(DATA1_TABLE_ID, 1);
        TableBucket pending = new TableBucket(DATA1_TABLE_ID, 2);
        makeLogTableAsLeader(completed.getBucket());
        makeLogTableAsLeader(pending.getBucket());

        int initialBytes = 0;
        if (initiallyHasRecords) {
            MemoryLogRecords firstBatch = genMemoryLogRecordsByObject(DATA1);
            append(pending, firstBatch);
            initialBytes = firstBatch.sizeInBytes();
        }

        Map<TableBucket, FetchReqInfo> request = new HashMap<>();
        request.put(completed, bounded(completed, 0L, 0L));
        request.put(
                pending,
                boundedPending
                        ? bounded(pending, 0L, 100L)
                        : new FetchReqInfo(DATA1_TABLE_ID, 0L, MAX_FETCH_BYTES));

        // A is completed, but B has not satisfied minBytes.
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                fetch(boundedParams(initialBytes + 1), request);

        assertThat(future).isNotDone();

        // Additional records in B allow the delayed request to complete.
        append(pending, genMemoryLogRecordsByObject(initiallyHasRecords ? ANOTHER_DATA1 : DATA1));

        Map<TableBucket, FetchLogResultForBucket> result = future.get(5, TimeUnit.SECONDS);

        assertThat(result).containsOnlyKeys(completed, pending);
        assertThat(result.get(completed).succeeded()).isTrue();
        assertThat(result.get(completed).getFilteredEndOffset()).isZero();
        assertThat(result.get(pending).succeeded()).isTrue();
        assertThat(result.get(pending).recordsOrEmpty().sizeInBytes()).isGreaterThan(initialBytes);
    }

    @Test
    void testDelayedFetchCompletesWhenBoundedStopIsReachedBelowMinBytes() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        Map<TableBucket, FetchReqInfo> request = Collections.singletonMap(tb, bounded(tb, 0L, 3L));
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                fetch(boundedParams(MAX_FETCH_BYTES), request);
        assertThat(future).isNotDone();

        MemoryLogRecords records =
                genMemoryLogRecordsWithWriterId(
                        Arrays.asList(
                                new Object[] {7, "v"},
                                new Object[] {8, "v"},
                                new Object[] {9, "v"}),
                        200L,
                        0,
                        0L);

        assertThat(records.sizeInBytes()).isLessThan(MAX_FETCH_BYTES);
        append(tb, records);
        // Reaching stop must complete the request even below minBytes.
        Map<TableBucket, FetchLogResultForBucket> result = future.get(5, TimeUnit.SECONDS);

        assertThat(result.get(tb).succeeded()).isTrue();
        assertThat(result.get(tb).recordsOrEmpty().sizeInBytes()).isGreaterThan(0);
    }

    @Test
    void testDelayedFetchWaitsWhenOnlyOneBoundedBucketReachesStop() throws Exception {
        TableBucket first = new TableBucket(DATA1_TABLE_ID, 1);
        TableBucket second = new TableBucket(DATA1_TABLE_ID, 2);

        makeLogTableAsLeader(first.getBucket());
        makeLogTableAsLeader(second.getBucket());

        Map<TableBucket, FetchReqInfo> request = new HashMap<>();
        request.put(first, bounded(first, 0L, 3L));
        request.put(second, bounded(second, 0L, 10L));

        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                fetch(boundedParams(MAX_FETCH_BYTES), request);
        assertThat(future).isNotDone();
        append(
                first,
                genMemoryLogRecordsWithWriterId(
                        Arrays.asList(
                                new Object[] {7, "v"},
                                new Object[] {8, "v"},
                                new Object[] {9, "v"}),
                        200L,
                        0,
                        0L));

        assertThat(replicaManager.getReplicaOrException(first).getLocalLogEndOffset())
                .isEqualTo(3L);
        // First reached its stop, but second is still waiting.
        assertThat(future).isNotDone();
        // Completing the other bounded range must release the delayed request.
        append(
                second,
                genMemoryLogRecordsWithWriterId(
                        Arrays.asList(
                                new Object[] {7, "v"},
                                new Object[] {8, "v"},
                                new Object[] {9, "v"},
                                new Object[] {10, "v"},
                                new Object[] {11, "v"},
                                new Object[] {12, "v"},
                                new Object[] {13, "v"},
                                new Object[] {14, "v"},
                                new Object[] {15, "v"},
                                new Object[] {16, "v"}),
                        201L,
                        0,
                        0L));

        assertThat(replicaManager.getReplicaOrException(second).getLocalLogEndOffset())
                .isEqualTo(10L);

        Map<TableBucket, FetchLogResultForBucket> result = future.get(5, TimeUnit.SECONDS);

        assertThat(result).containsOnlyKeys(first, second);
        assertThat(result.get(first).succeeded()).isTrue();
        assertThat(result.get(second).succeeded()).isTrue();
    }

    @Test
    void testBoundedDelayedFetchWaitsForHighWatermark() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        Replica replica = replicaManager.getReplicaOrException(tb);
        // Append three records so LEO reaches stopping offset 3.
        MemoryLogRecords records =
                genMemoryLogRecordsWithWriterId(
                        Arrays.asList(
                                new Object[] {7, "v"},
                                new Object[] {8, "v"},
                                new Object[] {9, "v"}),
                        200L,
                        0,
                        0L);

        assertThat(records.sizeInBytes()).isLessThan(MAX_FETCH_BYTES);
        append(tb, records);
        assertThat(replica.getLocalLogEndOffset()).isEqualTo(3L);

        // LEO=3, but only offsets below HW=2 are visible to client fetches.
        replica.getLogTablet().updateHighWatermark(2L);
        assertThat(replica.getLogHighWatermark()).isEqualTo(2L);
        FetchParams params = boundedParams(MAX_FETCH_BYTES);
        assertThat(params.isolation()).isEqualTo(HIGH_WATERMARK);
        Map<TableBucket, FetchReqInfo> request = Collections.singletonMap(tb, bounded(tb, 0L, 3L));
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                fetch(params, request);

        // Even though LEO has reached stop=3, HW has not.
        // The fetch must remain delayed because minBytes is not satisfied.
        assertThat(future).isNotDone();
        // Trigger an explicit delayed-fetch check with HW still at 2.
        // This must not complete the operation.
        int completed =
                replicaManager
                        .getDelayedFetchLogManager()
                        .checkAndComplete(new DelayedTableBucketKey(tb));

        assertThat(completed).isZero();
        assertThat(future).isNotDone();
        // Make the full bounded range visible: HW reaches stop=3.
        replica.getLogTablet().updateHighWatermark(3L);
        assertThat(replica.getLogHighWatermark()).isEqualTo(3L);
        // Manually trigger the delayed check. A direct HW update is not
        // required to wake DelayedFetchLog automatically.
        completed =
                replicaManager
                        .getDelayedFetchLogManager()
                        .checkAndComplete(new DelayedTableBucketKey(tb));
        // All available bounded records are now visible, even though
        // their total size remains below minBytes.
        assertThat(completed).isEqualTo(1);
        Map<TableBucket, FetchLogResultForBucket> result = future.get(5, TimeUnit.SECONDS);
        assertThat(result).containsOnlyKeys(tb);
        FetchLogResultForBucket bucketResult = result.get(tb);
        assertThat(bucketResult.succeeded()).isTrue();
        assertThat(bucketResult.getHighWatermark()).isEqualTo(3L);
        assertThat(bucketResult.recordsOrEmpty().sizeInBytes()).isGreaterThan(0);
        assertThat(bucketResult.recordsOrEmpty().sizeInBytes()).isLessThan(MAX_FETCH_BYTES);
    }

    private static FetchParams boundedParams(int minBytes) {
        FetchParams params = new FetchParams(-1, MAX_FETCH_BYTES, minBytes, MAX_WAIT_MS);
        params.setApiVersion((short) 1);
        return params;
    }

    private static FetchParams filteredParams(int minBytes) {
        Predicate filter = new PredicateBuilder(ROW_TYPE_WITH_FIELD_IDS).greaterThan(0, 5);
        FilterInfo filterInfo =
                new FilterInfo(
                        PredicateMessageUtils.toPbPredicate(filter, ROW_TYPE_WITH_FIELD_IDS),
                        DEFAULT_SCHEMA_ID);
        FetchParams params =
                new FetchParams(
                        -1,
                        true,
                        MAX_FETCH_BYTES,
                        minBytes,
                        MAX_WAIT_MS,
                        Collections.singletonMap(DATA1_TABLE_ID, filterInfo));
        params.setApiVersion((short) 1);
        return params;
    }

    private static FetchReqInfo bounded(TableBucket tb, long start, long stop) {
        return new FetchReqInfo(tb.getTableId(), start, MAX_FETCH_BYTES, null, stop);
    }

    private CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> fetch(
            FetchParams params, Map<TableBucket, FetchReqInfo> request) {
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(params, request, null, future::complete);
        return future;
    }

    private void append(TableBucket tb, MemoryLogRecords records) throws Exception {
        CompletableFuture<List<ProduceLogResultForBucket>> future = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20_000, 1, Collections.singletonMap(tb, records), null, future::complete);
        List<ProduceLogResultForBucket> results = future.get(5, TimeUnit.SECONDS);
        assertThat(results).hasSize(1);
        assertThat(results.get(0).succeeded()).isTrue();
    }

    private void appendCompletedFilteredRange(TableBucket tb) throws Exception {
        // The first batch is retained, followed by a batch rejected by the filter.
        MemoryLogRecords retainedRecords =
                genMemoryLogRecordsWithWriterId(
                        Arrays.asList(
                                new Object[] {7, "v"},
                                new Object[] {8, "v"},
                                new Object[] {9, "v"}),
                        100L,
                        0,
                        0L);

        append(tb, retainedRecords);
        assertThat(replicaManager.getReplicaOrException(tb).getLocalLogEndOffset()).isEqualTo(3L);
        append(tb, recordsWithStatistics(3, 1, 2, 3));
        assertThat(replicaManager.getReplicaOrException(tb).getLocalLogEndOffset()).isEqualTo(6L);
    }

    private static MemoryLogRecords recordsWithStatistics(
            long baseOffset, int first, int second, int third) throws Exception {
        List<Object[]> rows =
                Arrays.asList(
                        new Object[] {first, "v"},
                        new Object[] {second, "v"},
                        new Object[] {third, "v"});

        return createLogRecordsWithStatistics(
                rows, ROW_TYPE_WITH_FIELD_IDS, baseOffset, DEFAULT_SCHEMA_ID);
    }
}
