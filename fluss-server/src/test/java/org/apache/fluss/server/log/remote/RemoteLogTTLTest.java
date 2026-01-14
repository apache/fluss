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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.log.LogTablet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for remote log ttl in {@link RemoteLogManager}. */
final class RemoteLogTTLTest extends RemoteLogTestBase {

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRemoteLogTTL(boolean partitionTable) throws Exception {
        TableBucket tb;
        if (partitionTable) {
            tb = new TableBucket(DATA1_TABLE_ID, 0L, 0);
        } else {
            tb = new TableBucket(DATA1_TABLE_ID, 0);
        }
        // Need to make leader by ReplicaManager.
        makeLogTableAsLeader(tb, partitionTable);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();
        // enable data lake
        logTablet.updateIsDataLakeEnabled(true);
        // add 5 segments, so 4 segments will be uploaded to remote (exclude active segment)
        // segment offsets: [0,10), [10,20), [20,30), [30,40), [40,50) active
        addMultiSegmentsToLogTablet(logTablet, 5);
        // run RLMTask to copy local log segment to remote and commit snapshot.
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        RemoteLogTablet remoteLog = remoteLogManager.remoteLogTablet(tb);
        assertThat(remoteLog.relevantRemoteLogSegments(0L).size()).isEqualTo(4);
        assertThat(remoteLog.allRemoteLogSegments().size()).isEqualTo(4);
        assertThat(remoteLog.getRemoteLogStartOffset()).isEqualTo(0L);
        assertThat(remoteLog.getRemoteLogEndOffset()).hasValue(40L);

        // advance time past TTL (7 days)
        manualClock.advanceTime(Duration.ofDays(7).plusHours(1));

        // since data lake is enabled and no data has been tiered to data lake,
        // the expired segments should not be deleted.
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        assertThat(remoteLog.allRemoteLogSegments()).hasSize(4);
        assertThat(remoteLog.getRemoteLogStartOffset()).isEqualTo(0L);

        // set lake log end offset to 20, meaning only the first 2 segments
        // ([0,10) and [10,20)) have been tiered to lake
        logTablet.updateLakeLogEndOffset(20L);

        // trigger RLMTask to delete expired segments
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        // only segments with remoteLogEndOffset <= 20 should be deleted (first 2 segments)
        // remaining segments: [20,30) and [30,40)
        assertThat(remoteLog.allRemoteLogSegments()).hasSize(2);
        assertThat(remoteLog.getRemoteLogStartOffset()).isEqualTo(20L);
        assertThat(remoteLog.getRemoteLogEndOffset()).hasValue(40L);
        // verify remaining segments have the expected offsets
        assertThat(remoteLog.allRemoteLogSegments())
                .allSatisfy(
                        segment ->
                                assertThat(segment.remoteLogStartOffset())
                                        .isGreaterThanOrEqualTo(20L));

        // now advance lake log end offset to include all remaining segments
        logTablet.updateLakeLogEndOffset(40L);
        // trigger again, remaining expired segments should now be deleted
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        assertThat(remoteLog.allRemoteLogSegments()).isEmpty();
        assertThat(remoteLog.getRemoteLogStartOffset()).isEqualTo(Long.MAX_VALUE);

        // Fetch records from remote.
        // mock to update remote log end offset and remote log start offset as
        // NotifyRemoteLogOffsetsRequest do.
        logTablet.updateRemoteLogStartOffset(40L);
        logTablet.updateRemoteLogEndOffset(40L);
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                new FetchParams(-1, Integer.MAX_VALUE),
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 0L, 1024 * 1024)),
                null,
                future::complete);
        Map<TableBucket, FetchLogResultForBucket> result = future.get();
        assertThat(result.size()).isEqualTo(1);
        FetchLogResultForBucket resultForBucket = result.get(tb);
        assertThat(resultForBucket.getErrorCode())
                .isEqualTo(Errors.LOG_OFFSET_OUT_OF_RANGE_EXCEPTION.code());
    }
}
