/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.replica.delay;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;
import com.alibaba.fluss.rpc.entity.ProduceLogResultForBucket;
import com.alibaba.fluss.server.entity.FetchData;
import com.alibaba.fluss.server.log.FetchParams;
import com.alibaba.fluss.server.log.LogOffsetMetadata;
import com.alibaba.fluss.server.metrics.group.TestingMetricGroups;
import com.alibaba.fluss.server.replica.ReplicaTestBase;
import com.alibaba.fluss.server.replica.delay.DelayedFetchLog.FetchBucketStatus;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.testutils.DataTestUtils.assertLogRecordsEquals;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
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
                                new FetchData(150001L, 0L, Integer.MAX_VALUE),
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

        // write data.
        assertThat(delayedResponse.isDone()).isFalse();
        CompletableFuture<List<ProduceLogResultForBucket>> future = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000,
                -1,
                Collections.singletonMap(tb, genMemoryLogRecordsByObject(DATA1)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 0, 10L));

        assertThat(delayedFetchLogManager.numDelayed()).isEqualTo(0);
        assertThat(delayedFetchLogManager.watched()).isEqualTo(0);

        Map<TableBucket, FetchLogResultForBucket> result = delayedResponse.get();
        assertThat(result.containsKey(tb)).isTrue();
        FetchLogResultForBucket resultForBucket = result.get(tb);
        assertThat(resultForBucket.getHighWatermark()).isEqualTo(10L);
        assertLogRecordsEquals(DATA1_ROW_TYPE, resultForBucket.records(), DATA1);
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
                                new FetchData(150001L, 0L, Integer.MAX_VALUE),
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
                TestingMetricGroups.TABLET_SERVER_METRICS);
    }
}
