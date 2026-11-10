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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.ZooKeeperUtils;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.concurrent.Executors;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
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
        configuration.set(ConfigOptions.REMOTE_DATA_DIR, DEFAULT_REMOTE_DATA_DIR);
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

    @Test
    void testCleanupFailureDoesNotFailNewSnapshotCommit(@TempDir Path tmpDir) throws Exception {
        CompletedSnapshotStore store =
                createZooKeeperSnapshotStore(zooKeeperClient, new SharedKvFileRegistry());
        TableBucket bucket = new TableBucket(1, 2);
        AtomicInteger discardAttempts = new AtomicInteger();
        CompletedSnapshot old =
                new CompletedSnapshot(
                        bucket,
                        0,
                        FsPath.fromLocalFile(tmpDir.resolve("0").toFile()),
                        KvSnapshotHandle.create(
                                Collections.emptyList(), Collections.emptyList(), 0L)) {
                    @Override
                    public CompletableFuture<Void> discardAsync(Executor executor) {
                        discardAttempts.incrementAndGet();
                        return FutureUtils.completedExceptionally(
                                new IOException("Snapshot deletion failed"));
                    }
                };
        store.add(old);
        CompletedSnapshot next =
                SnapshotsCleanerTest.createSnapshot(
                        bucket, 1, FsPath.fromLocalFile(tmpDir.resolve("1").toFile()));

        store.add(next);

        assertThat(discardAttempts).hasValue(1);
        assertThat(store.getAllSnapshots()).containsExactly(next);
        assertThat(new ZooKeeperCompletedSnapshotHandleStore(zooKeeperClient).get(bucket, 1))
                .isPresent();
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
                Executors.directExecutor(),
                (consumeKvSnapshotForBucket) -> false); // only retain the latest snapshot.
    }
}
