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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.log.LogTablet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that among the last {@code table.log.tiered.local-segments} local segments, the inactive
 * ones can still be removed once {@link ConfigOptions#TABLE_LOG_TTL} has passed (while the default
 * long TTL would keep them until offset-based cleanup allows it).
 */
final class TieredLocalSegmentTtlTest extends RemoteLogTestBase {

    @Override
    public Configuration getServerConf() {
        Configuration conf = super.getServerConf();
        conf.set(ConfigOptions.TABLE_LOG_TTL, Duration.ofMillis(50));
        return conf;
    }

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testInactiveTieredLocalSegmentRemovedAfterTtl(boolean partitionTable) throws Exception {
        TableBucket tb =
                partitionTable
                        ? new TableBucket(DATA1_TABLE_ID, 0L, 0)
                        : new TableBucket(DATA1_TABLE_ID, 0);

        makeKvTableAsLeader(tb, DATA1_TABLE_PATH_PK, INITIAL_LEADER_EPOCH, partitionTable);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();

        addMultiSegmentsToLogTablet(logTablet, 5);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        logTablet.updateRemoteLogEndOffset(40L);
        assertThat(logTablet.getSegments()).hasSize(5);

        logTablet.updateMinRetainOffset(33L);
        assertThat(logTablet.getSegments()).hasSize(2);

        // Below the inactive tier-retained segment's TTL: cleanup runs but segment stays.
        logTablet.updateMinRetainOffset(34L);
        assertThat(logTablet.getSegments()).hasSize(2);

        manualClock.advanceTime(Duration.ofMillis(200));

        logTablet.updateMinRetainOffset(35L);
        assertThat(logTablet.getSegments()).hasSize(1);
    }
}
