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
import org.apache.fluss.server.kv.KvSnapshotResource;
import org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.ZooKeeperUtils;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.concurrent.Executors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.testutils.KvTestUtils.mockCompletedSnapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

        snapshotStore.add(snapshot1, null, 0);
        assertThat(snapshotStore.getAllSnapshots()).containsExactly(snapshot1);

        final SnapshotsCleanerTest.TestCompletedSnapshot snapshot2 =
                SnapshotsCleanerTest.createSnapshot(tableBucket, 1, snapshotPath);
        snapshotStore.add(snapshot2, null, 0);
        final List<CompletedSnapshot> allSnapshots = snapshotStore.getAllSnapshots();
        assertThat(allSnapshots).containsExactly(snapshot2);

        // verify that the subsumed snapshot is discarded
        SnapshotsCleanerTest.verifySnapshotDiscarded(snapshot1);
    }

    @Test
    void testAddSnapshotWithFailedDiscard(@TempDir Path tmpDir) throws Exception {
        TableBucket bucket = new TableBucket(1L, 2);
        CompletedSnapshotStore store =
                createZooKeeperSnapshotStore(zooKeeperClient, new SharedKvFileRegistry());
        CompletableFuture<Void> discard = new CompletableFuture<>();
        CompletedSnapshot first =
                new CompletedSnapshot(
                        bucket,
                        0L,
                        new FsPath(tmpDir.resolve("snapshot-0").toUri()),
                        KvSnapshotHandle.create(
                                Collections.emptyList(), Collections.emptyList(), 0)) {
                    @Override
                    public CompletableFuture<Void> discardAsync(Executor executor) {
                        discard.completeExceptionally(new IOException("Snapshot discard failed."));
                        return discard;
                    }
                };
        CompletedSnapshot second = mockCompletedSnapshot(tmpDir, bucket, 1L);
        store.add(first, null, 0);
        store.add(second, null, 0);
        assertThat(discard).isCompletedExceptionally();
        assertThat(store.getAllSnapshots()).containsExactly(second);
        assertThat(zooKeeperClient.getTableBucketSnapshot(bucket, 0L)).isEmpty();
    }

    @Test
    void testRecoverySelectionAppearingDuringLegacyLookupCarriesItsIdentity(@TempDir Path root)
            throws Exception {
        TableBucket bucket = new TableBucket(3L, 0);
        CompletedSnapshot selected = mockCompletedSnapshot(root, bucket, 2L);
        createZooKeeperSnapshotStore(zooKeeperClient, new SharedKvFileRegistry())
                .add(selected, null, 0);
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.REMOTE_DATA_DIR, root.toUri().toString());
        ZooKeeperClient raced =
                new ZooKeeperClient(
                        new CuratorFrameworkWithUnhandledErrorListener(
                                zooKeeperClient.getCuratorClient(), (message, error) -> {}),
                        conf) {
                    @Override
                    public Optional<BucketSnapshot> getTableBucketLatestSnapshot(
                            TableBucket requested) throws Exception {
                        zooKeeperClient.setRecoverySnapshot(
                                bucket,
                                new BucketSnapshot(
                                        2L, 0L, selected.getMetadataFilePath().toString()));
                        return super.getTableBucketLatestSnapshot(requested);
                    }
                };
        ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        KvSnapshotResource resource = KvSnapshotResource.create(1, conf, executor);
        try {
            CompletedSnapshot loaded =
                    DefaultSnapshotContext.create(
                                    raced,
                                    (snapshot,
                                            coordinatorEpoch,
                                            leaderEpoch,
                                            previousSnapshotId) -> {},
                                    resource,
                                    conf)
                            .getLatestCompletedSnapshotProvider()
                            .apply(bucket);
            assertThat(loaded.getSnapshotID()).isEqualTo(2L);
            assertThat(loaded.getRecoverySnapshotId()).isEqualTo(2L);
        } finally {
            resource.close();
            executor.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSelectedSharedFilesSurviveOutsideRetentionWindow(
            boolean responseLost, @TempDir Path root) throws Exception {
        TableBucket bucket = new TableBucket(2L, responseLost ? 1 : 0);
        zooKeeperClient
                .getCuratorClient()
                .create()
                .creatingParentsIfNeeded()
                .forPath(
                        ZkData.LeaderAndIsrZNode.path(bucket),
                        ZkData.LeaderAndIsrZNode.encode(new LeaderAndIsr(0, 0)));
        Path shared = root.resolve("shared.sst");
        Files.write(shared, new byte[] {1});
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.REMOTE_DATA_DIR, root.toUri().toString());
        ZooKeeperClient publisher =
                new ZooKeeperClient(
                        new CuratorFrameworkWithUnhandledErrorListener(
                                zooKeeperClient.getCuratorClient(), (message, error) -> {}),
                        conf) {
                    @Override
                    public void setRecoverySnapshot(TableBucket bucket, BucketSnapshot snapshot)
                            throws Exception {
                        super.setRecoverySnapshot(bucket, snapshot);
                        if (responseLost) {
                            throw new KeeperException.ConnectionLossException();
                        }
                    }
                };
        CompletedSnapshotStore store =
                new CompletedSnapshotStore(
                        2,
                        new SharedKvFileRegistry(Runnable::run),
                        Collections.emptyList(),
                        new ZooKeeperCompletedSnapshotHandleStore(publisher),
                        Runnable::run,
                        snapshot -> false);
        store.add(snapshot(root, bucket, 1L, shared), null, 0);
        store.add(snapshot(root, bucket, 3L, null), null, 0);
        CompletedSnapshot selected = snapshot(root, bucket, 2L, shared);
        if (responseLost) {
            assertThatThrownBy(() -> store.setRecoverySnapshot(selected))
                    .isInstanceOf(KeeperException.ConnectionLossException.class);
        } else {
            store.add(selected, null, 0);
            store.setRecoverySnapshot(selected);
        }
        store.add(snapshot(root, bucket, 4L, null), null, 0);
        store.add(snapshot(root, bucket, 5L, null), null, 0);
        assertThat(Files.exists(shared)).isTrue();
        assertThat(zooKeeperClient.getTableBucketLatestSnapshot(bucket).get().getSnapshotId())
                .isEqualTo(2L);
        assertThat(store.getActiveSnapshotIds()).contains(2L, 4L, 5L);
        store.add(snapshot(root, bucket, 6L, null), 2L, 0);
        assertThat(Files.exists(shared)).isFalse();
        assertThat(zooKeeperClient.getTableBucketLatestSnapshot(bucket).get().getSnapshotId())
                .isEqualTo(6L);
    }

    private CompletedSnapshot snapshot(Path root, TableBucket bucket, long id, Path shared) {
        return new CompletedSnapshot(
                bucket,
                id,
                new FsPath(root.resolve("snapshot-" + id).toUri()),
                KvSnapshotHandle.create(
                        shared == null
                                ? Collections.emptyList()
                                : Collections.singletonList(
                                        KvFileHandleAndLocalPath.of(
                                                new KvFileHandle(shared.toUri().toString(), 1L),
                                                "000001.sst")),
                        Collections.emptyList(),
                        0));
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
