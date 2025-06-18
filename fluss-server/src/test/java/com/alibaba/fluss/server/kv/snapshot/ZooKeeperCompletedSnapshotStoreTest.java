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

package com.alibaba.fluss.server.kv.snapshot;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperExtension;
import com.alibaba.fluss.server.zk.ZooKeeperUtils;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;
import com.alibaba.fluss.utils.concurrent.Executors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CompletedSnapshotStore} with {@link ZooKeeperCompletedSnapshotHandleStore}. */
class ZooKeeperCompletedSnapshotStoreTest {

    @RegisterExtension
    public static AllCallbackWrapper<ZooKeeperExtension> zooKeeperExtensionWrapper =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    static ZooKeeperClient zooKeeperClient;

    @BeforeAll
    static void beforeAll() {
        final Configuration configuration = new Configuration();
        configuration.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                zooKeeperExtensionWrapper.getCustomExtension().getConnectString());
        zooKeeperClient =
                ZooKeeperUtils.startZookeeperClient(configuration, NOPErrorHandler.INSTANCE);
    }

    @AfterAll
    static void afterAll() {
        if (zooKeeperClient != null) {
            zooKeeperClient.close();
        }
    }

    /** Tests that subsumed snapshots are discarded. */
    @Test
    void testDiscardingSubsumedSnapshots(@TempDir Path tmpDir) throws Exception {
        SharedKvFileRegistry sharedKvFileRegistry = new SharedKvFileRegistry();
        final CompletedSnapshotStore snapshotStore =
                createZooKeeperSnapshotStore(zooKeeperClient, sharedKvFileRegistry);
        TableBucket tableBucket = new TableBucket(1, 1);
        FsPath snapshotPath = FsPath.fromLocalFile(tmpDir.toFile());
        SnapshotsCleanerTest.TestCompletedSnapshot snapshot1 =
                SnapshotsCleanerTest.createSnapshot(tableBucket, 0, snapshotPath);

        snapshotStore.add(snapshot1);
        assertThat(snapshotStore.getAllSnapshots()).containsExactly(snapshot1);

        final SnapshotsCleanerTest.TestCompletedSnapshot snapshot2 =
                SnapshotsCleanerTest.createSnapshot(tableBucket, 1, snapshotPath);
        snapshotStore.add(snapshot2);
        final List<CompletedSnapshot> allSnapshots = snapshotStore.getAllSnapshots();
        assertThat(allSnapshots).containsExactly(snapshot2);

        // verify that the subsumed snapshot is discarded
        SnapshotsCleanerTest.verifySnapshotDiscarded(snapshot1);
    }

    /**
     * Tests that the snapshot does not exist in the store when we fail to add it into the store
     * (i.e., there exists an exception thrown by the method).
     */
    @Test
    void testAddSnapshotWithFailedRemove(@TempDir Path tmpDir) throws Exception {
        final Configuration configuration = new Configuration();
        configuration.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                zooKeeperExtensionWrapper.getCustomExtension().getConnectString());

        SharedKvFileRegistry sharedKvFileRegistry = new SharedKvFileRegistry();
        TableBucket tableBucket = new TableBucket(1, 2);
        try (ZooKeeperClient zooKeeperClient =
                ZooKeeperUtils.startZookeeperClient(configuration, NOPErrorHandler.INSTANCE)) {
            final CompletedSnapshotStore store =
                    createZooKeeperSnapshotStore(zooKeeperClient, sharedKvFileRegistry);

            CountDownLatch discardAttempted = new CountDownLatch(1);
            for (long i = 0; i < 2; ++i) {
                FsPath snapshotPath = FsPath.fromLocalFile(tmpDir.toFile());
                CompletedSnapshot snapshotToAdd =
                        SnapshotsCleanerTest.createSnapshot(tableBucket, i, snapshotPath);
                // shouldn't fail despite the exception
                store.addSnapshotAndSubsumeOldestOne(
                        snapshotToAdd,
                        new SnapshotsCleaner(),
                        () -> {
                            discardAttempted.countDown();
                            throw new RuntimeException();
                        });
            }
            discardAttempted.await();
        }
    }

    @Nonnull
    private CompletedSnapshotStore createZooKeeperSnapshotStore(
            ZooKeeperClient zooKeeperClient, SharedKvFileRegistry sharedKvFileRegistry) {
        ZooKeeperCompletedSnapshotHandleStore snapshotsInZooKeeper =
                new ZooKeeperCompletedSnapshotHandleStore(zooKeeperClient);
        return new CompletedSnapshotStore(
                1,
                sharedKvFileRegistry,
                Collections.emptyList(),
                snapshotsInZooKeeper,
                Executors.directExecutor());
    }
}
