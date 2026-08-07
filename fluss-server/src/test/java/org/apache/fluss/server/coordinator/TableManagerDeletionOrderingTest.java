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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketReplica;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.server.coordinator.event.TestingEventManager;
import org.apache.fluss.server.coordinator.statemachine.ReplicaStateMachine;
import org.apache.fluss.server.coordinator.statemachine.TableBucketStateMachine;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.clock.SystemClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.ReplicaDeletionSuccessful;
import static org.assertj.core.api.Assertions.assertThat;

/** Regression tests for table and partition deletion task ordering. */
class TableManagerDeletionOrderingTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;
    private static ZkEpoch zkEpoch;

    private CoordinatorContext coordinatorContext;
    private TableManager tableManager;
    private TableLifecycleThrottler lifecycleThrottler;
    private RecordingExecutorService recordingExecutor;

    @BeforeAll
    static void baseBeforeAll() throws Exception {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        zkEpoch = zookeeperClient.fenceBecomeCoordinatorLeader("1");
    }

    @BeforeEach
    void beforeEach() {
        recordingExecutor = new RecordingExecutorService();
        initTableManager(recordingExecutor);
    }

    @AfterEach
    void afterEach() {
        if (tableManager != null) {
            tableManager.shutdown();
        }
        if (lifecycleThrottler != null) {
            lifecycleThrottler.close();
        }
    }

    @Test
    void testDeleteTableSubmitsMetadataDeletionBeforeRemoteCleanup() throws Exception {
        long tableId = zookeeperClient.getTableIdAndIncrement();
        TableAssignment assignment = createAssignment();
        zookeeperClient.registerTableAssignment(tableId, assignment);

        coordinatorContext.putTableInfo(
                TableInfo.of(
                        DATA1_TABLE_PATH_PK,
                        tableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR_PK,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
        tableManager.onCreateNewTable(DATA1_TABLE_PATH_PK, tableId, assignment);

        coordinatorContext.queueTableDeletion(Collections.singleton(tableId));
        tableManager.onDeleteTable(tableId);
        for (TableBucketReplica replica : getReplicas(tableId, assignment)) {
            coordinatorContext.putReplicaState(replica, ReplicaDeletionSuccessful);
        }

        tableManager.resumeDeletions();

        assertThat(recordingExecutor.getTaskCount()).isGreaterThan(1);
        assertThat(zookeeperClient.getTableAssignment(tableId)).isPresent();

        recordingExecutor.runNext();

        assertThat(zookeeperClient.getTableAssignment(tableId)).isEmpty();
    }

    @Test
    void testDeletePartitionSubmitsMetadataDeletionBeforeRemoteCleanup() throws Exception {
        long tableId = zookeeperClient.getTableIdAndIncrement();
        zookeeperClient.registerTableAssignment(tableId, TableAssignment.builder().build());

        coordinatorContext.putTableInfo(
                TableInfo.of(
                        DATA1_TABLE_PATH_PK,
                        tableId,
                        0,
                        DATA1_TABLE_DESCRIPTOR_PK,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
        tableManager.onCreateNewTable(
                DATA1_TABLE_PATH_PK, tableId, TableAssignment.builder().build());

        PartitionAssignment partitionAssignment =
                new PartitionAssignment(tableId, createAssignment().getBucketAssignments());
        String partitionName = "2024";
        long partitionId = zookeeperClient.getPartitionIdAndIncrement();
        zookeeperClient.registerPartitionAssignmentAndMetadata(
                partitionId,
                partitionName,
                partitionAssignment,
                DEFAULT_REMOTE_DATA_DIR,
                DATA1_TABLE_PATH_PK,
                tableId);
        tableManager.onCreateNewPartition(
                DATA1_TABLE_PATH_PK, tableId, partitionId, partitionName, partitionAssignment);

        TablePartition tablePartition = new TablePartition(tableId, partitionId);
        coordinatorContext.queuePartitionDeletion(Collections.singleton(tablePartition));
        tableManager.onDeletePartition(tableId, partitionId);
        for (TableBucketReplica replica : getReplicas(tableId, partitionId, partitionAssignment)) {
            coordinatorContext.putReplicaState(replica, ReplicaDeletionSuccessful);
        }

        tableManager.resumeDeletions();

        assertThat(recordingExecutor.getTaskCount()).isGreaterThan(1);
        assertThat(zookeeperClient.getPartitionAssignment(partitionId)).isPresent();

        recordingExecutor.runNext();

        assertThat(zookeeperClient.getPartitionAssignment(partitionId)).isEmpty();
    }

    private void initTableManager(ExecutorService executorService) {
        TestingEventManager testingEventManager = new TestingEventManager();
        coordinatorContext = new CoordinatorContext(zkEpoch);
        TestCoordinatorChannelManager testCoordinatorChannelManager =
                new TestCoordinatorChannelManager();
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.REMOTE_DATA_DIR, "/tmp/fluss/remote-data");
        CoordinatorRequestBatch coordinatorRequestBatch =
                new CoordinatorRequestBatch(
                        testCoordinatorChannelManager, testingEventManager, coordinatorContext);
        ReplicaStateMachine replicaStateMachine =
                new ReplicaStateMachine(
                        coordinatorContext, coordinatorRequestBatch, zookeeperClient);
        TableBucketStateMachine tableBucketStateMachine =
                new TableBucketStateMachine(
                        coordinatorContext, coordinatorRequestBatch, zookeeperClient);
        MetadataManager metadataManager =
                new MetadataManager(
                        zookeeperClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
        lifecycleThrottler =
                new TableLifecycleThrottler(
                        testingEventManager, SystemClock.getInstance(), new Configuration());
        tableManager =
                new TableManager(
                        metadataManager,
                        coordinatorContext,
                        replicaStateMachine,
                        tableBucketStateMachine,
                        new RemoteStorageCleaner(conf, executorService),
                        executorService,
                        lifecycleThrottler);
        tableManager.startup();

        coordinatorContext.setLiveTabletServers(
                CoordinatorTestUtils.createServers(Arrays.asList(0, 1, 2)));
        CoordinatorTestUtils.makeSendLeaderAndStopRequestAlwaysSuccess(
                coordinatorContext, testCoordinatorChannelManager);
    }

    private TableAssignment createAssignment() {
        return TableAssignment.builder()
                .add(0, BucketAssignment.of(0, 1, 2))
                .add(1, BucketAssignment.of(1, 2, 0))
                .add(2, BucketAssignment.of(2, 1, 0))
                .build();
    }

    private Set<TableBucketReplica> getReplicas(long tableId, TableAssignment assignment) {
        return getReplicas(tableId, null, assignment);
    }

    private Set<TableBucketReplica> getReplicas(
            long tableId, Long partitionId, TableAssignment assignment) {
        Set<TableBucketReplica> tableBucketReplicas = new HashSet<>();
        for (int bucketId : assignment.getBuckets()) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
            List<Integer> replicas = assignment.getBucketAssignment(bucketId).getReplicas();
            for (int replica : replicas) {
                tableBucketReplicas.add(new TableBucketReplica(tableBucket, replica));
            }
        }
        return tableBucketReplicas;
    }

    private static final class RecordingExecutorService extends AbstractExecutorService {
        private final List<Runnable> tasks = new ArrayList<>();
        private boolean shutdown;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            List<Runnable> pendingTasks = new ArrayList<>(tasks);
            tasks.clear();
            return pendingTasks;
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return shutdown;
        }

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        private int getTaskCount() {
            return tasks.size();
        }

        private void runNext() {
            tasks.remove(0).run();
        }
    }
}
