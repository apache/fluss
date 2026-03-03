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
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.remote.RemoteLogSegment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LogTieringTask} max upload segments per task limit. */
class RemoteLogMaxUploadSegmentsTest extends RemoteLogTestBase {

    @Override
    public Configuration getServerConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.LOG_INDEX_INTERVAL_SIZE, MemorySize.parse("1b"));
        conf.set(ConfigOptions.REMOTE_LOG_INDEX_FILE_CACHE_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.REMOTE_FS_WRITE_BUFFER_SIZE, MemorySize.parse("10b"));
        // Use default value (5) for REMOTE_LOG_TASK_MAX_UPLOAD_SEGMENTS.
        return conf;
    }

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMaxUploadSegmentsPerTaskLimit(boolean partitionTable) throws Exception {
        // Default maxUploadSegmentsPerTask is 5, so with 10 segments (9 candidates),
        // only 5 should be uploaded per task execution.
        TableBucket tb = makeTableBucket(partitionTable);
        makeLogTableAsLeader(tb, partitionTable);
        addMultiSegmentsToLogTablet(replicaManager.getReplicaOrException(tb).getLogTablet(), 10);
        // 10 segments total, 9 candidates (1 active segment excluded).

        // First tiering task execution - should upload only 5 segments.
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        RemoteLogTablet remoteLog = remoteLogManager.remoteLogTablet(tb);
        List<RemoteLogSegment> manifestSegments = remoteLog.allRemoteLogSegments();
        assertThat(manifestSegments).hasSize(5);
        assertThat(remoteLog.getRemoteLogStartOffset()).isEqualTo(0L);
        assertThat(remoteLog.getRemoteLogEndOffset()).hasValue(50L);

        // Second tiering task execution - should upload the remaining 4 segments.
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        manifestSegments = remoteLog.allRemoteLogSegments();
        assertThat(manifestSegments).hasSize(9);
        assertThat(remoteLog.getRemoteLogEndOffset()).hasValue(90L);
        // Verify remote storage has all 9 segment files.
        assertThat(listRemoteLogFiles(tb))
                .isEqualTo(
                        manifestSegments.stream()
                                .map(s -> s.remoteLogSegmentId().toString())
                                .collect(Collectors.toSet()));
    }

    private TableBucket makeTableBucket(boolean partitionTable) {
        if (partitionTable) {
            return new TableBucket(DATA1_TABLE_ID, 0L, 0);
        } else {
            return new TableBucket(DATA1_TABLE_ID, 0);
        }
    }
}
