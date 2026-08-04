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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests TTL-based cleanup of inactive local segments retained by tiered storage. */
final class TieredLocalSegmentTTLTest extends RemoteLogTestBase {

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        Map<String, String> properties = new HashMap<>();
        properties.put(ConfigOptions.TABLE_LOG_TTL.key(), "2h");
        properties.put(ConfigOptions.TABLE_LOG_LOCAL_TTL.key(), "1h");
        registerTableInZkClient(
                DATA1_TABLE_PATH,
                DATA1_SCHEMA,
                DATA1_TABLE_ID,
                Collections.emptyList(),
                properties);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testInactiveTieredLocalSegmentRemovedAfterTtl(boolean partitionTable) throws Exception {
        TableBucket tb =
                partitionTable
                        ? new TableBucket(DATA1_TABLE_ID, 0L, 0)
                        : new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, partitionTable);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();

        addMultiSegmentsToLogTablet(logTablet, 5);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        RemoteLogTablet remoteLog = remoteLogManager.remoteLogTablet(tb);
        logManager.cleanupExpiredLocalLogSegments();

        // The configured number of local segments is retained before their TTL expires.
        assertThat(logTablet.getSegments()).hasSize(2);
        assertThat(logTablet.localLogStartOffset()).isEqualTo(30L);
        assertThat(remoteLog.allRemoteLogSegments()).hasSize(4);

        manualClock.advanceTime(Duration.ofMinutes(90));
        // Run local retention without updating the remote manifest or uploading another segment.
        logManager.cleanupExpiredLocalLogSegments();

        // The inactive segment is expired and deleted, while the active segment is retained.
        assertThat(remoteLog.allRemoteLogSegments()).hasSize(4);
        assertThat(logTablet.getSegments()).hasSize(1);
        assertThat(logTablet.localLogStartOffset()).isEqualTo(40L);
        assertThat(logTablet.activeLogSegment().getBaseOffset()).isEqualTo(40L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testExpiredLocalSegmentsRetainedWithoutRemoteLogEndOffset(boolean partitionTable)
            throws Exception {
        TableBucket tb =
                partitionTable
                        ? new TableBucket(DATA1_TABLE_ID, 0L, 0)
                        : new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, partitionTable);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();

        addMultiSegmentsToLogTablet(logTablet, 5);
        assertThat(remoteLogManager.remoteLogTablet(tb).getRemoteLogEndOffset()).isEmpty();

        manualClock.advanceTime(Duration.ofMinutes(90));
        logManager.cleanupExpiredLocalLogSegments();

        assertThat(logTablet.getSegments()).hasSize(5);
        assertThat(logTablet.localLogStartOffset()).isZero();
        assertThat(logTablet.activeLogSegment().getBaseOffset()).isEqualTo(40L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTtlCleanupBoundedByRemoteLogEndOffset(boolean partitionTable) throws Exception {
        TableBucket tb =
                partitionTable
                        ? new TableBucket(DATA1_TABLE_ID, 0L, 0)
                        : new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, partitionTable);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();

        addMultiSegmentsToLogTablet(logTablet, 5);
        logTablet.updateTieredLogLocalSegments(5);
        logTablet.updateRemoteLogEndOffset(20L);

        manualClock.advanceTime(Duration.ofMinutes(90));
        logManager.cleanupExpiredLocalLogSegments();

        assertThat(logTablet.getSegments()).hasSize(3);
        assertThat(logTablet.localLogStartOffset()).isEqualTo(20L);
        assertThat(logTablet.activeLogSegment().getBaseOffset()).isEqualTo(40L);
    }

    @Test
    void testFollowerRestoresHighestCopiedOffset() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        zkClient.upsertRemoteLogManifestHandle(
                tableBucket,
                new RemoteLogManifestHandle(new FsPath("test:///remote-log-manifest"), 40L));
        makeLeaderAndFollower(
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tableBucket,
                                Arrays.asList(TABLET_SERVER_ID, 2),
                                new LeaderAndIsr(
                                        LeaderAndIsr.NO_LEADER,
                                        0,
                                        Collections.singletonList(TABLET_SERVER_ID),
                                        Collections.emptyList(),
                                        0,
                                        0))));
        LogTablet logTablet = replicaManager.getReplicaOrException(tableBucket).getLogTablet();

        addMultiSegmentsToLogTablet(logTablet, 5);
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());
        manualClock.advanceTime(Duration.ofMinutes(90));
        logManager.cleanupExpiredLocalLogSegments();

        assertThat(logTablet.getSegments()).hasSize(1);
        assertThat(logTablet.localLogStartOffset()).isEqualTo(40L);
    }
}
