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
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Recovery selection is authoritative even when snapshot registration completes out of order. */
class RecoverySnapshotTest {

    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZOOKEEPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final AtomicLong TABLE_IDS = new AtomicLong(1000);
    private static ZooKeeperClient client;
    private TableBucket bucket;

    @TempDir Path temporaryDirectory;

    @Test
    void testOldLeaderCommitCannotInvalidateNewRecoveryContext() throws Exception {
        client.getCuratorClient()
                .create()
                .forPath(
                        ZkData.LeaderAndIsrZNode.path(bucket),
                        ZkData.LeaderAndIsrZNode.encode(
                                new LeaderAndIsr(
                                        1,
                                        0,
                                        Collections.singletonList(1),
                                        Collections.emptyList(),
                                        0,
                                        0)));
        client.setRecoverySnapshot(bucket, new BucketSnapshot(4L, 30L, "file:/source-4"));
        AtomicInteger reads = new AtomicInteger();
        Configuration conf = snapshotConfiguration();
        ZooKeeperClient delayed =
                new ZooKeeperClient(sharedCurator(), conf) {
                    @Override
                    public java.util.Optional<BucketSnapshot> getTableBucketSnapshot(
                            TableBucket requested, long id) throws Exception {
                        if (requested.equals(bucket) && id == 5L && reads.incrementAndGet() == 2) {
                            client.getCuratorClient()
                                    .setData()
                                    .forPath(
                                            ZkData.LeaderAndIsrZNode.path(bucket),
                                            ZkData.LeaderAndIsrZNode.encode(
                                                    new LeaderAndIsr(
                                                            2,
                                                            1,
                                                            Collections.singletonList(2),
                                                            Collections.emptyList(),
                                                            0,
                                                            1)));
                        }
                        return super.getTableBucketSnapshot(requested, id);
                    }
                };
        CompletedSnapshotStore store =
                new CompletedSnapshotStore(
                        2,
                        new SharedKvFileRegistry(Runnable::run),
                        Collections.emptyList(),
                        new ZooKeeperCompletedSnapshotHandleStore(delayed),
                        Runnable::run,
                        snapshot -> false);
        CompletedSnapshot old = snapshot(temporaryDirectory, 5L, 40L, null);
        Throwable failure = catchThrowable(() -> store.add(old, 4L, 0));
        store.add(snapshot(temporaryDirectory, 6L, 50L, null), 4L, 1);
        assertThat(client.getRecoveryBucketSnapshot(bucket).get().getSnapshotId()).isEqualTo(6L);
        assertThat(failure).isInstanceOf(KeeperException.BadVersionException.class);
        assertThat(client.getTableBucketSnapshot(bucket, 5L)).isEmpty();
    }

    @Test
    void testRecoverySelectionAppearingDuringLegacyLookupCarriesItsIdentity() throws Exception {
        CompletedSnapshot selected = snapshot(temporaryDirectory, 2L, 20L, null);
        Path metadata = Paths.get(selected.getMetadataFilePath().toUri());
        Files.createDirectories(metadata.getParent());
        Files.write(metadata, CompletedSnapshotJsonSerde.toJson(selected));
        Configuration conf = snapshotConfiguration();
        ZooKeeperClient raced =
                new ZooKeeperClient(sharedCurator(), conf) {
                    @Override
                    public java.util.Optional<BucketSnapshot> getTableBucketLatestSnapshot(
                            TableBucket requested) throws Exception {
                        client.setRecoverySnapshot(
                                bucket,
                                new BucketSnapshot(
                                        2L, 20L, selected.getMetadataFilePath().toString()));
                        return super.getTableBucketLatestSnapshot(requested);
                    }
                };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        KvSnapshotResource resource = KvSnapshotResource.create(1, conf, executor);
        try {
            DefaultSnapshotContext context =
                    DefaultSnapshotContext.create(
                            raced, (snapshot, cc, leader, recoveryId) -> {}, resource, conf);
            CompletedSnapshot loaded = context.getLatestCompletedSnapshotProvider().apply(bucket);
            assertThat(loaded.getSnapshotID()).isEqualTo(2L);
            assertThat(loaded.getRecoverySnapshotId()).isEqualTo(2L);
        } finally {
            resource.close();
            executor.shutdownNow();
        }
    }

    @BeforeAll
    static void start() {
        client = ZOOKEEPER.getCustomExtension().getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void setup() throws Exception {
        bucket = new TableBucket(TABLE_IDS.incrementAndGet(), 0);
        client.registerTableAssignment(
                bucket.getTableId(),
                TableAssignment.builder().add(0, BucketAssignment.of(1)).build());
        registerSnapshot(bucket, 1L, 10L);
        registerSnapshot(bucket, 3L, 20L);
    }

    @AfterAll
    static void close() {
        client.close();
    }

    @Test
    void testSelectedSourceOverridesMaximumId() throws Exception {
        select("{\"version\":1,\"snapshot_id\":1}");
        assertThat(client.getTableBucketLatestSnapshot(bucket).get().getSnapshotId()).isEqualTo(1L);
        assertThat(
                        client.getTableLatestBucketSnapshot(bucket.getTableId())
                                .get(0)
                                .get()
                                .getSnapshotId())
                .isEqualTo(1L);
    }

    @Test
    void testMissingSelectedReferenceDoesNotFallBack() throws Exception {
        select("{\"version\":1,\"snapshot_id\":2}");
        assertThatThrownBy(() -> client.getTableBucketLatestSnapshot(bucket))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> client.getTableLatestBucketSnapshot(bucket.getTableId()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testUnsupportedSelectionFormatDoesNotFallBack() throws Exception {
        select("{\"version\":99,\"snapshot_id\":1}");
        assertThatThrownBy(() -> client.getTableBucketLatestSnapshot(bucket))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private void select(String json) throws Exception {
        client.getCuratorClient()
                .create()
                .forPath(
                        ZkData.BucketIdZNode.path(bucket) + "/recovery_snapshot",
                        json.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void testOriginalContextAdvancesContinuouslyAndRejectsStaleSnapshot() throws Exception {
        client.setRecoverySnapshot(bucket, new BucketSnapshot(4L, 30L, "file:/snapshot-4"));
        BucketSnapshot next = new BucketSnapshot(5L, 40L, "file:/snapshot-5");
        client.registerTableBucketSnapshot(bucket, next, 4L, null);
        assertThat(client.getRecoveryBucketSnapshot(bucket)).contains(next);
        BucketSnapshot equalOffset = new BucketSnapshot(6L, 40L, "file:/snapshot-6");
        client.registerTableBucketSnapshot(bucket, equalOffset, 5L, null);
        assertThat(client.getRecoveryBucketSnapshot(bucket)).contains(equalOffset);
        assertThatThrownBy(
                        () ->
                                client.registerTableBucketSnapshot(
                                        bucket,
                                        new BucketSnapshot(7L, 50L, "file:/snapshot-7"),
                                        4L,
                                        null))
                .isInstanceOf(KeeperException.BadVersionException.class);
        assertThat(client.getTableBucketSnapshot(bucket, 7L)).isEmpty();
        client.registerTableBucketSnapshot(
                bucket, new BucketSnapshot(8L, 60L, "file:/snapshot-8"), null, null);
        assertThat(client.getRecoveryBucketSnapshot(bucket)).contains(equalOffset);
    }

    @Test
    void testSelectionCannotMoveRecoveryOffsetBackwards() throws Exception {
        assertThatThrownBy(
                        () ->
                                client.setRecoverySnapshot(
                                        bucket, new BucketSnapshot(4L, 19L, "file:/snapshot-4")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(client.getTableBucketSnapshot(bucket, 4L)).isEmpty();
        assertThat(client.getRecoverySnapshotSelection(bucket)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSelectedSharedFilesSurviveOutsideRetentionWindow(boolean responseLost)
            throws Exception {
        bucket = new TableBucket(TABLE_IDS.incrementAndGet(), 0);
        client.getCuratorClient()
                .create()
                .creatingParentsIfNeeded()
                .forPath(ZkData.BucketIdZNode.path(bucket));
        Path root = temporaryDirectory;
        Path shared = root.resolve("shared.sst");
        Files.write(shared, new byte[] {1});
        ZooKeeperClient publisher =
                new ZooKeeperClient(sharedCurator(), snapshotConfiguration()) {
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
        store.add(snapshot(root, 1L, 10L, shared), null, null);
        store.add(snapshot(root, 3L, 20L, null), null, null);
        CompletedSnapshot selected = snapshot(root, 2L, 20L, shared);
        if (responseLost) {
            assertThatThrownBy(() -> store.setRecoverySnapshot(selected))
                    .isInstanceOf(KeeperException.ConnectionLossException.class);
        } else {
            store.add(selected, null, null);
            store.setRecoverySnapshot(selected);
        }
        store.add(snapshot(root, 4L, 30L, null), null, null);
        store.add(snapshot(root, 5L, 40L, null), null, null);

        assertThat(Files.exists(shared)).isTrue();
        assertThat(client.getTableBucketLatestSnapshot(bucket).get().getSnapshotId()).isEqualTo(2L);
        assertThat(store.getActiveSnapshotIds()).contains(2L, 4L, 5L);

        store.add(snapshot(root, 6L, 40L, null), 2L, null);
        assertThat(Files.exists(shared)).isFalse();
        assertThat(client.getTableBucketLatestSnapshot(bucket).get().getSnapshotId()).isEqualTo(6L);
    }

    private Configuration snapshotConfiguration() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.REMOTE_DATA_DIR, temporaryDirectory.toUri().toString());
        return conf;
    }

    private CuratorFrameworkWithUnhandledErrorListener sharedCurator() {
        return new CuratorFrameworkWithUnhandledErrorListener(
                client.getCuratorClient(), (message, error) -> {});
    }

    private void registerSnapshot(TableBucket target, long id, long offset) throws Exception {
        Path directory =
                temporaryDirectory
                        .resolve("references")
                        .resolve(Integer.toString(target.getBucket()))
                        .resolve(Long.toString(id));
        Files.createDirectories(directory);
        CompletedSnapshot snapshot =
                new CompletedSnapshot(
                        target,
                        id,
                        new FsPath(directory.toUri()),
                        KvSnapshotHandle.create(
                                Collections.emptyList(), Collections.emptyList(), 0),
                        offset,
                        0L,
                        null);
        Files.write(
                Paths.get(snapshot.getMetadataFilePath().toUri()),
                CompletedSnapshotJsonSerde.toJson(snapshot));
        client.registerTableBucketSnapshot(
                target, new BucketSnapshot(id, offset, snapshot.getMetadataFilePath().toString()));
    }

    private CompletedSnapshot snapshot(Path root, long id, long offset, Path shared) {
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
                        0),
                offset,
                0L,
                null);
    }
}
