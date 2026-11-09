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

package org.apache.fluss.server.replica.delay;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.log.LogOffsetMetadata;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.replica.delay.DelayedFetchLog.FetchBucketStatus;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.testutils.DataTestUtils.assertLogRecordsEquals;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DelayedFetchLog}. */
public class DelayedFetchLogTest extends ReplicaTestBase {

    @Test
    void testCompleteDelayedFetchLog() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());

        FetchLogResultForBucket preFetchResultForBucket =
                FetchLogResultForBucket.records(tb, MemoryLogRecords.EMPTY, 0L, -1L, -1L);
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> delayedResponse =
                new CompletableFuture<>();
        DelayedFetchLog delayedFetchLog =
                createDelayedFetchLogRequest(
                        tb,
                        100,
                        Duration.ofMinutes(3).toMillis(), // max wait ms large enough.
                        new FetchBucketStatus(
                                new FetchReqInfo(150001L, 0L, Integer.MAX_VALUE),
                                new LogOffsetMetadata(0L, 0L, 0),
                                preFetchResultForBucket),
                        delayedResponse::complete);

        DelayedOperationManager<DelayedFetchLog> delayedFetchLogManager =
                replicaManager.getDelayedFetchLogManager();
        DelayedTableBucketKey delayedTableBucketKey = new DelayedTableBucketKey(tb);
        boolean completed =
                delayedFetchLogManager.tryCompleteElseWatch(
                        delayedFetchLog, Collections.singletonList(delayedTableBucketKey));
        assertThat(completed).isFalse();
        assertThat(delayedFetchLogManager.numDelayed()).isEqualTo(1);
        assertThat(delayedFetchLogManager.watched()).isEqualTo(1);

        int numComplete = delayedFetchLogManager.checkAndComplete(delayedTableBucketKey);
        assertThat(numComplete).isEqualTo(0);
        assertThat(delayedFetchLogManager.numDelayed()).isEqualTo(1);
        assertThat(delayedFetchLogManager.watched()).isEqualTo(1);

        // Appending data completes the fetch without an additional completion trigger.
        assertThat(delayedResponse.isDone()).isFalse();
        CompletableFuture<List<ProduceLogResultForBucket>> future = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000,
                -1,
                Collections.singletonMap(tb, genMemoryLogRecordsByObject(DATA1)),
                null,
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 0, 10L));
        assertThat(delayedResponse.isDone()).isTrue();
        assertThat(delayedFetchLogManager.numDelayed()).isEqualTo(0);
        assertThat(delayedFetchLogManager.watched()).isEqualTo(0);

        Map<TableBucket, FetchLogResultForBucket> result = delayedResponse.get();
        assertThat(result.containsKey(tb)).isTrue();
        FetchLogResultForBucket resultForBucket = result.get(tb);
        assertThat(resultForBucket.getHighWatermark()).isEqualTo(10L);
        assertLogRecordsEquals(DATA1_ROW_TYPE, resultForBucket.records(), DATA1);
    }

    @Test
    void testSuccessfulBucketCompletesWhenAnotherBucketAppendFails() throws Exception {
        TableBucket successfulBucket = new TableBucket(DATA1_TABLE_ID, 1);
        TableBucket failedBucket = new TableBucket(DATA1_TABLE_ID, 2);
        makeLogTableAsLeader(successfulBucket.getBucket());
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> delayedResponse =
                watchDelayedFetch(successfulBucket);

        Map<TableBucket, MemoryLogRecords> entries = new HashMap<>();
        entries.put(successfulBucket, genMemoryLogRecordsByObject(DATA1));
        entries.put(failedBucket, genMemoryLogRecordsByObject(DATA1));
        CompletableFuture<List<ProduceLogResultForBucket>> produceResponse =
                new CompletableFuture<>();

        replicaManager.appendRecordsToLog(20000, 1, entries, null, produceResponse::complete);

        List<ProduceLogResultForBucket> produceResults = produceResponse.get();
        assertThat(produceResults).hasSize(2);
        assertThat(produceResults)
                .filteredOn(result -> result.getTableBucket().equals(successfulBucket))
                .hasSize(1)
                .allSatisfy(result -> assertThat(result.succeeded()).isTrue());
        assertThat(produceResults)
                .filteredOn(result -> result.getTableBucket().equals(failedBucket))
                .hasSize(1)
                .allSatisfy(result -> assertThat(result.failed()).isTrue());
        assertThat(delayedResponse).isDone();
        assertThat(replicaManager.getDelayedFetchLogManager().numDelayed()).isZero();
    }

    @Test
    void testAppendCompletesDelayedFetchesForMultipleBuckets() throws Exception {
        TableBucket firstBucket = new TableBucket(DATA1_TABLE_ID, 1);
        TableBucket secondBucket = new TableBucket(DATA1_TABLE_ID, 2);
        makeLogTableAsLeader(firstBucket.getBucket());
        makeLogTableAsLeader(secondBucket.getBucket());
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> firstResponse =
                watchDelayedFetch(firstBucket);
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> secondResponse =
                watchDelayedFetch(secondBucket);

        Map<TableBucket, MemoryLogRecords> entries = new HashMap<>();
        entries.put(firstBucket, genMemoryLogRecordsByObject(DATA1));
        entries.put(secondBucket, genMemoryLogRecordsByObject(DATA1));
        CompletableFuture<List<ProduceLogResultForBucket>> produceResponse =
                new CompletableFuture<>();

        replicaManager.appendRecordsToLog(20000, 1, entries, null, produceResponse::complete);

        assertThat(produceResponse.get())
                .hasSize(2)
                .allSatisfy(result -> assertThat(result.succeeded()).isTrue());
        assertThat(firstResponse).isDone();
        assertThat(secondResponse).isDone();
        assertThat(replicaManager.getDelayedFetchLogManager().numDelayed()).isZero();
    }

    @Test
    void testDelayedFollowerFetchesWithAllAcks() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 1);
        int firstFollowerId = 2;
        int secondFollowerId = 3;
        List<Integer> replicas = Arrays.asList(TABLET_SERVER_ID, firstFollowerId, secondFollowerId);
        makeLogTableAsLeader(tableBucket, replicas, replicas, false);
        Replica leader = replicaManager.getReplicaOrException(tableBucket);
        long logEndOffset = DATA1.size();

        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> firstFollowerFetch =
                fetchLog(tableBucket, firstFollowerId, 0L, 1);
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> secondFollowerFetch =
                fetchLog(tableBucket, secondFollowerId, 0L, 1);
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> consumerFetch =
                fetchLog(tableBucket, -1, 0L, 1);
        assertThat(firstFollowerFetch).isNotDone();
        assertThat(secondFollowerFetch).isNotDone();
        assertThat(consumerFetch).isNotDone();
        assertThat(replicaManager.getDelayedFetchLogManager().numDelayed()).isEqualTo(3);

        CompletableFuture<List<ProduceLogResultForBucket>> produceResponse =
                new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                (int) Duration.ofMinutes(3).toMillis(),
                -1,
                Collections.singletonMap(tableBucket, genMemoryLogRecordsByObject(DATA1)),
                null,
                produceResponse::complete);

        assertThat(leader.getLogTablet().localLogEndOffset()).isEqualTo(logEndOffset);
        assertThat(leader.getLogHighWatermark()).isZero();
        assertThat(produceResponse).isNotDone();
        assertThat(consumerFetch).isNotDone();
        assertThat(replicaManager.getDelayedWriteManager().numDelayed()).isOne();

        // Followers read up to LEO, while the consumer and acks=all produce must wait for HW.
        assertFetchedRecords(firstFollowerFetch, tableBucket, 0L);
        assertFetchedRecords(secondFollowerFetch, tableBucket, 0L);
        assertThat(leader.getLogHighWatermark()).isZero();
        assertThat(produceResponse).isNotDone();
        assertThat(consumerFetch).isNotDone();
        assertThat(replicaManager.getDelayedFetchLogManager().numDelayed()).isOne();

        // Fetching from the new LEO acknowledges the records returned by the previous fetch.
        acknowledgeFetchedRecords(tableBucket, firstFollowerId, logEndOffset);
        assertThat(leader.getLogHighWatermark()).isZero();
        assertThat(produceResponse).isNotDone();
        assertThat(consumerFetch).isNotDone();

        acknowledgeFetchedRecords(tableBucket, secondFollowerId, logEndOffset);
        assertThat(leader.getLogHighWatermark()).isEqualTo(logEndOffset);
        assertThat(produceResponse).isDone();
        assertThat(produceResponse.join())
                .containsExactly(new ProduceLogResultForBucket(tableBucket, 0L, logEndOffset));
        assertFetchedRecords(consumerFetch, tableBucket, logEndOffset);
        assertThat(replicaManager.getDelayedWriteManager().numDelayed()).isZero();
        assertThat(replicaManager.getDelayedFetchLogManager().numDelayed()).isZero();
    }

    @Test
    void testFetchCompletionFailureDoesNotFailProduceOrOtherBuckets() throws Exception {
        TableBucket firstBucket = new TableBucket(DATA1_TABLE_ID, 1);
        TableBucket secondBucket = new TableBucket(DATA1_TABLE_ID, 2);
        int followerId = 2;
        List<Integer> replicas = Arrays.asList(TABLET_SERVER_ID, followerId);
        Map<TableBucket, MemoryLogRecords> entries = new HashMap<>();
        Map<TableBucket, FetchLogResultForBucket> fetchResults = new HashMap<>();
        AtomicInteger completedFetches = new AtomicInteger();
        AtomicInteger pendingWritesAtFirstFetch = new AtomicInteger(-1);
        for (TableBucket tableBucket : Arrays.asList(firstBucket, secondBucket)) {
            makeLogTableAsLeader(tableBucket, replicas, replicas, false);
            entries.put(tableBucket, genMemoryLogRecordsByObject(DATA1));
            fetchLog(
                    tableBucket,
                    followerId,
                    0L,
                    1,
                    results -> {
                        fetchResults.putAll(results);
                        // Fail the first callback regardless of bucket iteration order.
                        if (completedFetches.getAndIncrement() == 0) {
                            pendingWritesAtFirstFetch.set(
                                    replicaManager.getDelayedWriteManager().numDelayed());
                            throw new IllegalStateException("Expected fetch callback failure");
                        }
                    });
        }
        assertThat(completedFetches).hasValue(0);
        assertThat(replicaManager.getDelayedFetchLogManager().numDelayed()).isEqualTo(2);
        CompletableFuture<List<ProduceLogResultForBucket>> produceResponse =
                new CompletableFuture<>();

        replicaManager.appendRecordsToLog(
                (int) Duration.ofMinutes(3).toMillis(),
                -1,
                entries,
                null,
                produceResponse::complete);

        assertThat(completedFetches).hasValue(2);
        assertThat(pendingWritesAtFirstFetch).hasValue(1);
        assertThat(fetchResults).containsOnlyKeys(firstBucket, secondBucket);
        for (FetchLogResultForBucket result : fetchResults.values()) {
            assertThat(result.succeeded()).isTrue();
            assertThat(result.getHighWatermark()).isZero();
            assertLogRecordsEquals(DATA1_ROW_TYPE, result.records(), DATA1);
        }
        assertThat(produceResponse).isNotDone();
        assertThat(replicaManager.getDelayedWriteManager().numDelayed()).isOne();
        assertThat(replicaManager.getDelayedFetchLogManager().numDelayed()).isZero();

        acknowledgeFetchedRecords(firstBucket, followerId, DATA1.size());
        assertThat(produceResponse).isNotDone();
        acknowledgeFetchedRecords(secondBucket, followerId, DATA1.size());
        assertThat(produceResponse).isDone();
        assertThat(produceResponse.join())
                .containsExactlyInAnyOrder(
                        new ProduceLogResultForBucket(firstBucket, 0L, DATA1.size()),
                        new ProduceLogResultForBucket(secondBucket, 0L, DATA1.size()));
        assertThat(replicaManager.getDelayedWriteManager().numDelayed()).isZero();
    }

    @Test
    void testDelayFetchLogTimeout() {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());

        FetchLogResultForBucket preFetchResultForBucket =
                FetchLogResultForBucket.records(tb, MemoryLogRecords.EMPTY, 0L, -1L, -1L);
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> delayedResponse =
                new CompletableFuture<>();
        DelayedFetchLog delayedFetchLog =
                createDelayedFetchLogRequest(
                        tb,
                        100,
                        1000, // wait time is small enough.
                        new FetchBucketStatus(
                                new FetchReqInfo(150001L, 0L, Integer.MAX_VALUE),
                                new LogOffsetMetadata(0L, 0L, 0),
                                preFetchResultForBucket),
                        delayedResponse::complete);

        DelayedOperationManager<DelayedFetchLog> delayedFetchLogManager =
                replicaManager.getDelayedFetchLogManager();
        DelayedTableBucketKey delayedTableBucketKey = new DelayedTableBucketKey(tb);
        boolean completed =
                delayedFetchLogManager.tryCompleteElseWatch(
                        delayedFetchLog, Collections.singletonList(delayedTableBucketKey));
        assertThat(completed).isFalse();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    delayedFetchLogManager.checkAndComplete(delayedTableBucketKey);
                    assertThat(delayedFetchLogManager.numDelayed()).isEqualTo(0);
                    assertThat(delayedFetchLogManager.watched()).isEqualTo(0);

                    assertThat(delayedResponse.isDone()).isTrue();
                    Map<TableBucket, FetchLogResultForBucket> result = delayedResponse.get();
                    assertThat(result.containsKey(tb)).isTrue();
                    FetchLogResultForBucket resultForBucket = result.get(tb);
                    assertThat(resultForBucket.getHighWatermark()).isEqualTo(0L);
                    assertThat(resultForBucket.recordsOrEmpty()).isEqualTo(MemoryLogRecords.EMPTY);
                });
    }

    private CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> fetchLog(
            TableBucket tableBucket, int replicaId, long fetchOffset, int minFetchBytes) {
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> response =
                new CompletableFuture<>();
        fetchLog(tableBucket, replicaId, fetchOffset, minFetchBytes, response::complete);
        return response;
    }

    private void fetchLog(
            TableBucket tableBucket,
            int replicaId,
            long fetchOffset,
            int minFetchBytes,
            Consumer<Map<TableBucket, FetchLogResultForBucket>> responseCallback) {
        replicaManager.fetchLogRecords(
                new FetchParams(
                        replicaId,
                        Integer.MAX_VALUE,
                        minFetchBytes,
                        Duration.ofMinutes(3).toMillis()),
                Collections.singletonMap(
                        tableBucket,
                        new FetchReqInfo(tableBucket.getTableId(), fetchOffset, Integer.MAX_VALUE)),
                null,
                responseCallback);
    }

    private void acknowledgeFetchedRecords(TableBucket tableBucket, int followerId, long offset) {
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> response =
                fetchLog(tableBucket, followerId, offset, 0);
        assertThat(response).isDone();
        FetchLogResultForBucket result = response.join().get(tableBucket);
        assertThat(result.succeeded()).isTrue();
        assertThat(result.recordsOrEmpty().sizeInBytes()).isZero();
    }

    private void assertFetchedRecords(
            CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> response,
            TableBucket tableBucket,
            long expectedHighWatermark)
            throws Exception {
        assertThat(response).isDone();
        assertThat(response.join()).containsOnlyKeys(tableBucket);
        FetchLogResultForBucket result = response.join().get(tableBucket);
        assertThat(result.succeeded()).isTrue();
        assertThat(result.getHighWatermark()).isEqualTo(expectedHighWatermark);
        assertLogRecordsEquals(DATA1_ROW_TYPE, result.records(), DATA1);
    }

    private DelayedFetchLog createDelayedFetchLogRequest(
            TableBucket tb,
            int minFetchSize,
            long maxWaitMs,
            FetchBucketStatus prevFetchBucketStatus,
            Consumer<Map<TableBucket, FetchLogResultForBucket>> responseCallback) {
        FetchParams fetchParams = new FetchParams(-1, Integer.MAX_VALUE, minFetchSize, maxWaitMs);
        return new DelayedFetchLog(
                fetchParams,
                replicaManager,
                Collections.singletonMap(tb, prevFetchBucketStatus),
                responseCallback,
                TestingMetricGroups.TABLET_SERVER_METRICS,
                null);
    }

    private CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> watchDelayedFetch(
            TableBucket tableBucket) {
        FetchLogResultForBucket previousResult =
                FetchLogResultForBucket.records(tableBucket, MemoryLogRecords.EMPTY, 0L, -1L, -1L);
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> response =
                new CompletableFuture<>();
        DelayedFetchLog delayedFetchLog =
                createDelayedFetchLogRequest(
                        tableBucket,
                        1,
                        Duration.ofMinutes(3).toMillis(),
                        new FetchBucketStatus(
                                new FetchReqInfo(150001L, 0L, Integer.MAX_VALUE),
                                new LogOffsetMetadata(0L, 0L, 0),
                                previousResult),
                        response::complete);
        replicaManager
                .getDelayedFetchLogManager()
                .tryCompleteElseWatch(
                        delayedFetchLog,
                        Collections.singletonList(new DelayedTableBucketKey(tableBucket)));
        return response;
    }
}
