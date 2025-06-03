/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.replica;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.server.coordinator.TestCoordinatorGateway;
import com.alibaba.fluss.server.entity.NotifyLeaderAndIsrData;
import com.alibaba.fluss.server.log.checkpoint.OffsetCheckpointFile;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.Collections;

import static com.alibaba.fluss.record.TestData.ANOTHER_DATA1;
import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.record.TestData.DATA2_TABLE_ID;
import static com.alibaba.fluss.record.TestData.DATA2_TABLE_PATH;
import static com.alibaba.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static com.alibaba.fluss.server.replica.ReplicaManager.HIGH_WATERMARK_CHECKPOINT_FILE_NAME;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for the high watermark persistence. */
final class HighWatermarkPersistenceTest extends ReplicaTestBase {

    @Test
    void testHighWatermarkPersistenceSingleReplica() throws Exception {
        replicaManager.checkpointHighWatermarks();
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        long highWatermark = highWatermarkFor(tableBucket);
        assertThat(highWatermark).isEqualTo(0L);

        // become leader.
        makeLogTableAsLeader(tableBucket.getBucket());

        // append record to replica and update high watermark to log end offset.
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        replica.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);
        replicaManager.checkpointHighWatermarks();
        highWatermark = highWatermarkFor(tableBucket);
        assertThat(highWatermark).isEqualTo(10L);

        // set the high watermark for local replica.
        replica.getLogTablet().updateHighWatermark(10L);
        replicaManager.checkpointHighWatermarks();
        highWatermark = highWatermarkFor(tableBucket);
        assertThat(highWatermark).isEqualTo(10L);
    }

    @Test
    void testHighWatermarkPersistenceMultipleReplicas() throws Exception {
        replicaManager.checkpointHighWatermarks();
        TableBucket tableBucket0 = new TableBucket(DATA1_TABLE_ID, 0);
        long highWatermark0 = highWatermarkFor(tableBucket0);
        assertThat(highWatermark0).isEqualTo(0L);

        // become leader.
        makeLogTableAsLeader(tableBucket0.getBucket());

        Replica replica0 = replicaManager.getReplicaOrException(tableBucket0);
        replicaManager.checkpointHighWatermarks();
        highWatermark0 = highWatermarkFor(tableBucket0);
        assertThat(highWatermark0).isEqualTo(0L);

        // set the high watermark for local replica.
        replica0.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);
        replica0.getLogTablet().updateHighWatermark(10L);
        replicaManager.checkpointHighWatermarks();
        highWatermark0 = highWatermarkFor(tableBucket0);
        assertThat(highWatermark0).isEqualTo(10L);
        assertThat(replica0.getLogTablet().getHighWatermark()).isEqualTo(10L);

        // add another replica and set highWatermark.
        TableBucket tableBucket1 = new TableBucket(DATA2_TABLE_ID, 0);
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA2_TABLE_PATH),
                                tableBucket1,
                                Collections.singletonList(TABLET_SERVER_ID),
                                new LeaderAndIsr(
                                        TABLET_SERVER_ID,
                                        LeaderAndIsr.INITIAL_LEADER_EPOCH,
                                        Collections.singletonList(TABLET_SERVER_ID),
                                        INITIAL_COORDINATOR_EPOCH,
                                        LeaderAndIsr.INITIAL_BUCKET_EPOCH))),
                result -> {});

        replicaManager.checkpointHighWatermarks();
        long highWatermark1 = highWatermarkFor(tableBucket1);
        assertThat(highWatermark1).isEqualTo(0);

        // set the highWatermark for local replica
        Replica replica1 = replicaManager.getReplicaOrException(tableBucket1);
        replica1.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);
        replica1.getLogTablet().updateHighWatermark(10L);
        assertThat(replica1.getLogTablet().getHighWatermark()).isEqualTo(10L);

        // change the highWatermark for t1.
        replica0.appendRecordsToLeader(genMemoryLogRecordsByObject(ANOTHER_DATA1), 1);
        assertThat(replica0.getLogTablet().getHighWatermark()).isEqualTo(20L);
        replicaManager.checkpointHighWatermarks();

        // verify the highWatermark for t1 and t2.
        highWatermark0 = highWatermarkFor(tableBucket0);
        assertThat(highWatermark0).isEqualTo(20L);
        highWatermark1 = highWatermarkFor(tableBucket1);
        assertThat(highWatermark1).isEqualTo(10L);
    }

    @Test
    void testHighWatermarkPersistenceThread() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        long highWatermark = highWatermarkFor(tableBucket);
        assertThat(highWatermark).isEqualTo(0L);

        // become leader.
        makeLogTableAsLeader(tableBucket.getBucket());

        // append record to replica and update high watermark to log end offset.
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        replica.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);

        retry(
                Duration.ofMinutes(1),
                () -> assertThat(highWatermarkFor(tableBucket)).isEqualTo(10L));
    }

    @Test
    void testReplicaManagerShutDownAndCheckpointHighWatermarks() throws Exception {
        TableBucket tb1 = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb1.getBucket());
        // append record to replica and update high watermark to log end offset.
        Replica replica = replicaManager.getReplicaOrException(tb1);
        replica.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);
        // Wait highWatermark checkpoint thread to checkpoint highWatermark.
        retry(Duration.ofMinutes(1), () -> assertThat(highWatermarkFor(tb1)).isEqualTo(10L));

        // tb2 don't wait highWatermark checkpoint thread to checkpoint highWatermark.
        TableBucket tb2 = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb2.getBucket());
        // append record to replica and update high watermark to log end offset.
        Replica replica2 = replicaManager.getReplicaOrException(tb2);
        replica2.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);

        replicaManager.shutdown();
        assertThat(highWatermarkFor(tb1)).isEqualTo(10L);
        assertThat(highWatermarkFor(tb2)).isEqualTo(10L);

        // 1. replicaManager shutdown after tb1/tb2 become leader.
        replicaManager = buildReplicaManager(new TestCoordinatorGateway());
        replicaManager.startup();
        assertThat(highWatermarkFor(tb1)).isEqualTo(10L);
        assertThat(highWatermarkFor(tb2)).isEqualTo(10L);
        makeLogTableAsLeader(tb1.getBucket());
        makeLogTableAsLeader(tb2.getBucket());
        replica = replicaManager.getReplicaOrException(tb1);
        replica.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);
        replicaManager.shutdown();
        assertThat(highWatermarkFor(tb1)).isEqualTo(20L);
        assertThat(highWatermarkFor(tb2)).isEqualTo(10L);

        // 2. replicaManager shutdown before tb1/tb2 become leader.
        replicaManager = buildReplicaManager(new TestCoordinatorGateway());
        replicaManager.startup();
        replicaManager.shutdown();
        assertThat(highWatermarkFor(tb1)).isEqualTo(20L);
        assertThat(highWatermarkFor(tb2)).isEqualTo(10L);
    }

    private long highWatermarkFor(TableBucket tableBucket) throws Exception {
        return new OffsetCheckpointFile(
                        new File(
                                conf.getString(ConfigOptions.DATA_DIR),
                                HIGH_WATERMARK_CHECKPOINT_FILE_NAME))
                .read()
                .getOrDefault(tableBucket, 0L);
    }
}
