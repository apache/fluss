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

package org.apache.fluss.server.log;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collections;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests local log TTL cleanup when remote log tiering is disabled. */
final class LocalSegmentTTLTest extends ReplicaTestBase {

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        registerTableInZkClient(
                DATA1_TABLE_PATH,
                DATA1_SCHEMA,
                DATA1_TABLE_ID,
                Collections.emptyList(),
                Collections.singletonMap(ConfigOptions.TABLE_LOG_TTL.key(), "1h"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testExpiredActiveSegmentCleaned(boolean partitionTable) throws Exception {
        TableBucket tb =
                partitionTable
                        ? new TableBucket(DATA1_TABLE_ID, 0L, 0)
                        : new TableBucket(DATA1_TABLE_ID, 0);
        conf.set(ConfigOptions.LOG_RETENTION_ROLL_ACTIVE_SEGMENT_ENABLED, true);
        logManager.reconfigure(conf);
        makeLogTableAsLeader(tb, partitionTable);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();

        assertThatThrownBy(() -> remoteLogManager.remoteLogTablet(tb))
                .isInstanceOf(IllegalStateException.class);
        addMultiSegmentsToLogTablet(logTablet, 1);
        logManager.cleanupExpiredLocalLogSegments();

        assertThat(logTablet.getSegments()).hasSize(1);
        assertThat(logTablet.localLogStartOffset()).isEqualTo(0L);
        assertThat(logTablet.activeLogSegment().getBaseOffset()).isEqualTo(0L);

        manualClock.advanceTime(Duration.ofHours(2));
        logManager.cleanupExpiredLocalLogSegments();

        assertThat(logTablet.getSegments()).hasSize(2);
        assertThat(logTablet.localLogStartOffset()).isEqualTo(0L);
        assertThat(logTablet.activeLogSegment().getBaseOffset()).isEqualTo(10L);

        logManager.cleanupExpiredLocalLogSegments();

        assertThat(logTablet.getSegments()).hasSize(1);
        assertThat(logTablet.localLogStartOffset()).isEqualTo(10L);
        assertThat(logTablet.activeLogSegment().getBaseOffset()).isEqualTo(10L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testExpiredActiveSegmentNotRolledByDefault(boolean partitionTable) throws Exception {
        TableBucket tb =
                partitionTable
                        ? new TableBucket(DATA1_TABLE_ID, 0L, 0)
                        : new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, partitionTable);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();

        addMultiSegmentsToLogTablet(logTablet, 5);
        manualClock.advanceTime(Duration.ofHours(2));

        logManager.cleanupExpiredLocalLogSegments();

        assertThat(logTablet.getSegments()).hasSize(1);
        assertThat(logTablet.localLogStartOffset()).isEqualTo(40L);
        assertThat(logTablet.activeLogSegment().getBaseOffset()).isEqualTo(40L);
    }

    @Test
    void testExpiredSegmentsDeletedUsingHighWatermarkWhenRemoteLogDisabled() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        LogTablet logTablet = replicaManager.getReplicaOrException(tableBucket).getLogTablet();

        addMultiSegmentsToLogTablet(logTablet, 5);
        manualClock.advanceTime(Duration.ofMinutes(90));
        logManager.cleanupExpiredLocalLogSegments();

        assertThat(logTablet.getSegments()).hasSize(1);
        assertThat(logTablet.localLogStartOffset()).isEqualTo(40L);
    }

    @Test
    void testLogTtlRemainsEffectiveWhenRemoteLogDisabled() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        LogTablet logTablet = replicaManager.getReplicaOrException(tableBucket).getLogTablet();

        logTablet.updateLogTtls(Duration.ofHours(2).toMillis(), Duration.ofMinutes(30).toMillis());

        assertThat(logTablet.getEffectiveLocalLogTtlMs()).isEqualTo(Duration.ofHours(2).toMillis());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testUpdateLogTtlMsAffectsLocalSegmentCleanup(boolean partitionTable) throws Exception {
        TableBucket tb =
                partitionTable
                        ? new TableBucket(DATA1_TABLE_ID, 0L, 0)
                        : new TableBucket(DATA1_TABLE_ID, 0);
        conf.set(ConfigOptions.LOG_RETENTION_ROLL_ACTIVE_SEGMENT_ENABLED, true);
        logManager.reconfigure(conf);
        makeLogTableAsLeader(tb, partitionTable);

        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();

        // Remote log is disabled in this test base, so updateLogTtls must still reach LogTablet.
        assertThatThrownBy(() -> remoteLogManager.remoteLogTablet(tb))
                .isInstanceOf(IllegalStateException.class);

        addMultiSegmentsToLogTablet(logTablet, 1);

        // Advance 30min — within the 1h TTL, segment is not expired.
        manualClock.advanceTime(Duration.ofMinutes(30));
        logManager.cleanupExpiredLocalLogSegments();
        assertThat(logTablet.getSegments()).hasSize(1);

        // Shrink TTL to 1ms; the segment should now be expired.
        replica.updateLogTtls(1L, 1L);
        logManager.cleanupExpiredLocalLogSegments();
        // The expired active segment is rolled, creating a new empty segment at offset 10.
        assertThat(logTablet.getSegments()).hasSize(2);
        assertThat(logTablet.activeLogSegment().getBaseOffset()).isEqualTo(10L);

        // Second pass deletes the now-inactive expired segment.
        logManager.cleanupExpiredLocalLogSegments();
        assertThat(logTablet.getSegments()).hasSize(1);
        assertThat(logTablet.localLogStartOffset()).isEqualTo(10L);

        // Disable expiration; the remaining segment must survive even after a long time.
        replica.updateLogTtls(-1L, -1L);
        manualClock.advanceTime(Duration.ofDays(365));
        logManager.cleanupExpiredLocalLogSegments();
        assertThat(logTablet.getSegments()).hasSize(1);
    }
}
