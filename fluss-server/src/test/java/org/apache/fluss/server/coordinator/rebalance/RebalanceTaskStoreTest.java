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

package org.apache.fluss.server.coordinator.rebalance;

import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.server.zk.data.RebalanceTaskHandle;
import org.apache.fluss.server.zk.data.RebalanceTaskZNodeData;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.COMPLETED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.REBALANCING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RebalanceTaskStore}. */
class RebalanceTaskStoreTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOOKEEPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zkClient;

    private ZkEpoch zkEpoch;
    private RebalanceTaskStore store;

    @BeforeAll
    static void beforeAll() {
        zkClient = ZOOKEEPER.getCustomExtension().getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void beforeEach() throws Exception {
        zkEpoch = zkClient.fenceBecomeCoordinatorLeader("leader");
        store = new RebalanceTaskStore(zkClient, zkClient.getDefaultRemoteDataDir());
    }

    @AfterEach
    void afterEach() throws Exception {
        ZOOKEEPER.getCustomExtension().cleanupRoot();
        FsPath rebalanceDir = new FsPath(zkClient.getDefaultRemoteDataDir(), "rebalance");
        if (rebalanceDir.getFileSystem().exists(rebalanceDir)) {
            rebalanceDir.getFileSystem().delete(rebalanceDir, true);
        }
    }

    @AfterAll
    static void afterAll() {
        zkClient.close();
    }

    @Test
    void testPersistAndLoadRemoteTask() throws Exception {
        RebalanceTask initialTask = task("rebalance-1", REBALANCING);
        store.persist(initialTask, zkEpoch.getCoordinatorEpochZkVersion(), true);

        RebalanceTaskHandle initialHandle = getRemoteHandle();
        assertThat(initialHandle.getTaskPath().toString())
                .contains("/rebalance/rebalance-1/")
                .endsWith(".task");
        assertThat(initialHandle.getTaskPath().getFileSystem().exists(initialHandle.getTaskPath()))
                .isTrue();
        FsPath orphanFile = new FsPath(initialHandle.getTaskPath().getParent(), "orphan.task");
        try (FSDataOutputStream output =
                orphanFile.getFileSystem().create(orphanFile, FileSystem.WriteMode.NO_OVERWRITE)) {
            output.write(1);
        }
        markAsOld(orphanFile);
        assertThat(store.load()).contains(initialTask);
        store.cleanupOrphanFiles();
        assertThat(orphanFile.getFileSystem().exists(orphanFile)).isFalse();

        RebalanceTask completedTask = task("rebalance-1", COMPLETED);
        store.persist(completedTask, zkEpoch.getCoordinatorEpochZkVersion(), true);

        RebalanceTaskHandle completedHandle = getRemoteHandle();
        assertThat(completedHandle).isNotEqualTo(initialHandle);
        assertThat(initialHandle.getTaskPath().getFileSystem().exists(initialHandle.getTaskPath()))
                .isFalse();
        assertThat(store.load()).contains(completedTask);
    }

    @Test
    void testPersistInlineTaskWhenRemoteStorageIsDisabled() throws Exception {
        RebalanceTask task = task("rebalance-2", REBALANCING);

        store.persist(task, zkEpoch.getCoordinatorEpochZkVersion(), false);

        RebalanceTaskZNodeData data = zkClient.getRebalanceTaskDataWithVersion().get().data();
        assertThat(data.isInline()).isTrue();
        assertThat(data.getInlineTask()).isEqualTo(task);
        assertThat(store.load()).contains(task);
    }

    @Test
    void testCoordinatorEpochFencesRemoteHandleCommit() throws Exception {
        assertThatThrownBy(
                        () ->
                                store.persist(
                                        task("fenced-rebalance", REBALANCING),
                                        zkEpoch.getCoordinatorEpochZkVersion() + 1,
                                        true))
                .isInstanceOf(KeeperException.BadVersionException.class);

        assertThat(zkClient.getRebalanceTaskDataWithVersion()).isEmpty();
    }

    @Test
    void testCleanupOrphanFileWithoutZookeeperHandle() throws Exception {
        FsPath orphan =
                FlussPaths.remoteRebalanceTaskPath(
                        zkClient.getDefaultRemoteDataDir(), "orphan-rebalance", UUID.randomUUID());
        orphan.getFileSystem().mkdirs(orphan.getParent());
        try (FSDataOutputStream output =
                orphan.getFileSystem().create(orphan, FileSystem.WriteMode.NO_OVERWRITE)) {
            output.write(1);
        }
        markAsOld(orphan);

        store.cleanupOrphanFiles();
        assertThat(orphan.getFileSystem().exists(orphan)).isFalse();
    }

    @Test
    void testCleanupRetainsRecentUnreferencedFile() throws Exception {
        FsPath recentFile =
                FlussPaths.remoteRebalanceTaskPath(
                        zkClient.getDefaultRemoteDataDir(),
                        "upload-in-progress",
                        UUID.randomUUID());
        recentFile.getFileSystem().mkdirs(recentFile.getParent());
        try (FSDataOutputStream output =
                recentFile.getFileSystem().create(recentFile, FileSystem.WriteMode.NO_OVERWRITE)) {
            output.write(1);
        }

        store.cleanupOrphanFiles();
        assertThat(recentFile.getFileSystem().exists(recentFile)).isTrue();
    }

    private RebalanceTaskHandle getRemoteHandle() throws Exception {
        RebalanceTaskZNodeData data = zkClient.getRebalanceTaskDataWithVersion().get().data();
        assertThat(data.isInline()).isFalse();
        return data.getRemoteTaskHandle();
    }

    private static void markAsOld(FsPath path) throws Exception {
        Files.setLastModifiedTime(Paths.get(path.toUri()), FileTime.fromMillis(0L));
    }

    private static RebalanceTask task(
            String rebalanceId,
            org.apache.fluss.cluster.rebalance.RebalanceStatus rebalanceStatus) {
        TableBucket tableBucket = new TableBucket(1L, 0);
        Map<TableBucket, RebalancePlanForBucket> plans = new LinkedHashMap<>();
        plans.put(
                tableBucket,
                new RebalancePlanForBucket(
                        tableBucket, 0, 1, Arrays.asList(0, 1, 2), Arrays.asList(1, 2, 3)));
        return new RebalanceTask(rebalanceId, rebalanceStatus, plans);
    }
}
