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

package com.alibaba.fluss.server.replica.delay;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.entity.ProduceLogResultForBucket;
import com.alibaba.fluss.rpc.protocol.Errors;
import com.alibaba.fluss.server.log.LogAppendInfo;
import com.alibaba.fluss.server.metrics.group.TestingMetricGroups;
import com.alibaba.fluss.server.replica.Replica;
import com.alibaba.fluss.server.replica.ReplicaTestBase;
import com.alibaba.fluss.server.replica.delay.DelayedWrite.DelayedBucketStatus;
import com.alibaba.fluss.server.replica.delay.DelayedWrite.DelayedWriteMetadata;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DelayedWrite}. */
final class DelayedWriteTest extends ReplicaTestBase {

    @Test
    void testCompleteDelayWrite() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        Replica replica = replicaManager.getReplicaOrException(tb);

        // try to append records directly into logTablet instead of replica.appendRecordsToLeader
        // because we want to increase the highWatermark by ourselves.
        DelayedWrite<?> delayedWrite =
                createDelayedWrite(
                        replica,
                        tb,
                        10000,
                        resultList -> {
                            assertThat(resultList.size()).isEqualTo(1);
                            assertThat(resultList.get(0).getWriteLogEndOffset()).isEqualTo(10);
                        });

        DelayedOperationManager<DelayedWrite<?>> delayedWriteManager =
                replicaManager.getDelayedWriteManager();
        DelayedTableBucketKey delayedTableBucketKey = new DelayedTableBucketKey(tb);
        boolean completed =
                delayedWriteManager.tryCompleteElseWatch(
                        delayedWrite, Collections.singletonList(delayedTableBucketKey));
        assertThat(completed).isFalse();
        assertThat(delayedWriteManager.numDelayed()).isEqualTo(1);
        assertThat(delayedWriteManager.watched()).isEqualTo(1);

        int numComplete = delayedWriteManager.checkAndComplete(delayedTableBucketKey);
        assertThat(numComplete).isEqualTo(0);
        assertThat(delayedWriteManager.numDelayed()).isEqualTo(1);
        assertThat(delayedWriteManager.watched()).isEqualTo(1);

        // update highWatermark to log end offset.
        replica.getLogTablet().updateHighWatermark(10L);
        numComplete = delayedWriteManager.checkAndComplete(delayedTableBucketKey);
        assertThat(numComplete).isEqualTo(1);
        assertThat(delayedWriteManager.numDelayed()).isEqualTo(0);
        assertThat(delayedWriteManager.watched()).isEqualTo(0);
    }

    @Test
    void testDelayedWriteKey() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        Replica replica = replicaManager.getReplicaOrException(tb);

        DelayedTableBucketKey delayedTableBucketKey = new DelayedTableBucketKey(tb);
        assertThat(delayedTableBucketKey.getTableBucket()).isEqualTo(tb);
        DelayedOperationManager<DelayedWrite<?>> delayedWriteManager =
                replicaManager.getDelayedWriteManager();

        DelayedWrite<?> delayedWrite =
                createDelayedWrite(
                        replica,
                        tb,
                        10000,
                        resultList -> {
                            assertThat(resultList.size()).isEqualTo(1);
                            assertThat(resultList.get(0).getWriteLogEndOffset()).isEqualTo(10);
                        });
        // Directly watch for the delayedWrite.
        delayedWriteManager.watchForOperation(delayedTableBucketKey, delayedWrite);
        assertThat(delayedWriteManager.watched()).isEqualTo(1);

        // new a delayedWriteKey point to the same tableBucket. It's import to new a delayedWriteKey
        // because when we want to check and complete the operation in ReplicaManager/Replica,
        // we will always make a new delayedWriteKey.
        DelayedTableBucketKey newDelayedTableBucketKey = new DelayedTableBucketKey(tb);
        int numComplete = delayedWriteManager.checkAndComplete(newDelayedTableBucketKey);
        assertThat(numComplete).isEqualTo(0);
        assertThat(delayedWriteManager.watched()).isEqualTo(1);

        // update highWatermark to log end offset.
        replica.getLogTablet().updateHighWatermark(10L);
        numComplete = delayedWriteManager.checkAndComplete(newDelayedTableBucketKey);
        assertThat(numComplete).isEqualTo(1);
        assertThat(delayedWriteManager.watched()).isEqualTo(0);
    }

    @Test
    void testDelayWriteTimeOut() throws Exception {
        int delayMs = 3000;
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        Replica replica = replicaManager.getReplicaOrException(tb);

        DelayedWrite<?> delayedWrite =
                createDelayedWrite(
                        replica,
                        tb,
                        delayMs,
                        resultList -> {
                            assertThat(resultList.size()).isEqualTo(1);
                            assertThat(resultList.get(0).getErrorCode())
                                    .isEqualTo(Errors.REQUEST_TIME_OUT.code());
                        });

        DelayedOperationManager<DelayedWrite<?>> delayedWriteManager =
                replicaManager.getDelayedWriteManager();
        DelayedTableBucketKey delayedTableBucketKey = new DelayedTableBucketKey(tb);
        boolean completed =
                delayedWriteManager.tryCompleteElseWatch(
                        delayedWrite, Collections.singletonList(delayedTableBucketKey));
        assertThat(completed).isFalse();
        assertThat(delayedWriteManager.numDelayed()).isEqualTo(1);
        assertThat(delayedWriteManager.watched()).isEqualTo(1);

        int numComplete = delayedWriteManager.checkAndComplete(delayedTableBucketKey);
        assertThat(numComplete).isEqualTo(0);
        assertThat(delayedWriteManager.numDelayed()).isEqualTo(1);
        assertThat(delayedWriteManager.watched()).isEqualTo(1);

        // make sure the delayedWrite timeout.
        retry(
                Duration.ofMillis(delayMs + 3000),
                () -> assertThat(delayedWriteManager.numDelayed()).isEqualTo(0));

        assertThat(delayedWriteManager.watched()).isEqualTo(1);
    }

    private DelayedWrite<ProduceLogResultForBucket> createDelayedWrite(
            Replica replica,
            TableBucket tb,
            int delayMs,
            Consumer<List<ProduceLogResultForBucket>> callback)
            throws Exception {
        LogAppendInfo appendInfo =
                replica.getLogTablet().appendAsLeader(genMemoryLogRecordsByObject(DATA1));
        ProduceLogResultForBucket appendResult =
                new ProduceLogResultForBucket(
                        tb, appendInfo.firstOffset(), appendInfo.lastOffset() + 1);
        Map<TableBucket, DelayedBucketStatus<ProduceLogResultForBucket>> bucketStatusMap =
                Collections.singletonMap(tb, new DelayedBucketStatus<>(10, appendResult));

        return new DelayedWrite<>(
                delayMs,
                new DelayedWriteMetadata<>(-1, bucketStatusMap),
                replicaManager,
                callback,
                TestingMetricGroups.TABLET_SERVER_METRICS);
    }
}
