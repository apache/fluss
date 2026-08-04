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
import org.apache.fluss.server.replica.ReplicaTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.assertj.core.api.Assertions.assertThat;

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
}
