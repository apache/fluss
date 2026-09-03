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
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.replica.delay.DelayedFetchLog.FetchBucketStatus;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
                new FetchLogResultForBucket(tb, MemoryLogRecords.EMPTY, 0L);
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

        // Appending data enqueues completion, but does not run it under the append call.
        assertThat(delayedResponse.isDone()).isFalse();
        CompletableFuture<List<ProduceLogResultForBucket>> future = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000,
                -1,
                Collections.singletonMap(tb, genMemoryLogRecordsByObject(DATA1)),
                null,
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 0, 10L));
        assertThat(delayedResponse.isDone()).isFalse();

        replicaManager.tryCompleteActions();
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
        assertThat(delayedResponse).isNotDone();

        replicaManager.tryCompleteActions();

        assertThat(delayedResponse).isDone();
        assertThat(replicaManager.getDelayedFetchLogManager().numDelayed()).isZero();
    }

    @Test
    void testDrainCompletesDelayedFetchesForMultipleBuckets() throws Exception {
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
        assertThat(firstResponse).isNotDone();
        assertThat(secondResponse).isNotDone();

        replicaManager.tryCompleteActions();

        assertThat(firstResponse).isDone();
        assertThat(secondResponse).isDone();
        assertThat(replicaManager.getDelayedFetchLogManager().numDelayed()).isZero();
    }

    @Test
    void testDelayFetchLogTimeout() {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());

        FetchLogResultForBucket preFetchResultForBucket =
                new FetchLogResultForBucket(tb, MemoryLogRecords.EMPTY, 0L);
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
                new FetchLogResultForBucket(tableBucket, MemoryLogRecords.EMPTY, 0L);
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
