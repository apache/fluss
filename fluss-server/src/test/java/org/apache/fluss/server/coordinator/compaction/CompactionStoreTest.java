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

package org.apache.fluss.server.coordinator.compaction;

import org.apache.fluss.cluster.compaction.CompactionInfo;
import org.apache.fluss.cluster.compaction.CompactionStatus;
import org.apache.fluss.cluster.compaction.CompactionType;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.local.LocalFileSystem;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Persistence and recovery tests using real ZooKeeper and local files. */
class CompactionStoreTest {
    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZK =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @TempDir Path directory;
    private ZooKeeperClient zk;
    private CompactionStore store;
    private int epoch;
    private final TableBucket bucket = new TableBucket(1, 2L, 0);

    @BeforeEach
    void setup() throws Exception {
        zk = ZK.getCustomExtension().getZooKeeperClient(NOPErrorHandler.INSTANCE);
        epoch = zk.fenceBecomeCoordinatorLeader("test").getCoordinatorEpochZkVersion();
        store = new CompactionStore(zk, directory.toString());
    }

    @AfterEach
    void cleanup() {
        ZK.getCustomExtension().cleanupRoot();
    }

    @Test
    void testCreateAndPublishAreEpochFenced() throws Exception {
        int active = zk.getCompactionActive().zkVersion();
        zk.fenceBecomeCoordinatorLeader("replacement");
        assertThatThrownBy(
                        () ->
                                store.create(
                                        task("one", CompactionStatus.PENDING, null, null),
                                        active,
                                        epoch))
                .isInstanceOf(KeeperException.BadVersionException.class);
        assertThat(store.list()).isEmpty();
        epoch = zk.fenceBecomeCoordinatorLeader("next").getCoordinatorEpochZkVersion();
        store.create(task("one", CompactionStatus.PENDING, null, null), active, epoch);
        int occupied = zk.getCompactionActive().zkVersion();
        zk.fenceBecomeCoordinatorLeader("replacement");
        assertThatThrownBy(
                        () ->
                                store.publish(
                                        task("one", CompactionStatus.FAILED, null, null),
                                        0,
                                        occupied,
                                        epoch))
                .isInstanceOf(KeeperException.BadVersionException.class);
        assertThat(store.load(zk.getCompactionActive().data()).info().status())
                .isEqualTo(CompactionStatus.PENDING);
    }

    @Test
    void testUnpublishedFilesAreNotRecovered() throws Exception {
        store.create(task("one", CompactionStatus.PENDING, null, null), 0, epoch);
        FsPath plan =
                store.writePlan(
                        "one",
                        new CompactionTask.Plan(
                                Arrays.asList(bucket, new TableBucket(2, 0)),
                                Collections.singletonMap("compression", "lz4")));
        FsPath progress = store.writeProgress("one", null, Collections.singletonMap(bucket, 11L));
        store.publish(task("one", CompactionStatus.RUNNING, plan, progress), 0, 1, epoch);
        store.writeProgress("one", progress, Collections.singletonMap(bucket, 99L));
        CompactionStore recovered = new CompactionStore(zk, directory.toString());
        CompactionTask loaded = recovered.load("one");
        Map<TableBucket, Long> snapshots =
                recovered.readProgress(loaded.progressHandle(), loaded.succeeded());
        assertThat(snapshots).containsEntry(bucket, 11L).doesNotContainValue(99L);
        assertThat(recovered.readPlan(loaded.planHandle()).buckets())
                .containsExactly(bucket, new TableBucket(2, 0));
        assertThat(recovered.readPlan(loaded.planHandle()).configuration())
                .containsEntry("compression", "lz4");
        assertThat(loaded.deadline()).isEqualTo(12345L);
    }

    @Test
    void testTerminalPublicationClearsActiveAtomically() throws Exception {
        store.create(task("one", CompactionStatus.PENDING, null, null), 0, epoch);
        assertThatThrownBy(
                        () ->
                                store.publish(
                                        task("one", CompactionStatus.FAILED, null, null),
                                        1,
                                        1,
                                        epoch))
                .isInstanceOf(KeeperException.BadVersionException.class);
        assertThat(zk.getCompactionActive().data()).isEqualTo("one");
        store.publish(task("one", CompactionStatus.FAILED, null, null), 0, 1, epoch);
        assertThat(zk.getCompactionActive().data()).isEmpty();
        assertThat(store.load("one").info().status()).isEqualTo(CompactionStatus.FAILED);
        store.create(task("two", CompactionStatus.PENDING, null, null), 2, epoch);
        assertThat(zk.getCompactionActive().data()).isEqualTo("two");
    }

    @Test
    void testCloseFailureDoesNotPublishAndAllowsTerminalRelease() throws Exception {
        CompactionStore failing =
                new CompactionStore(
                        zk,
                        directory.toString(),
                        new LocalFileSystem() {
                            @Override
                            public FSDataOutputStream create(FsPath path, WriteMode mode)
                                    throws IOException {
                                FSDataOutputStream delegate = super.create(path, mode);
                                return new FSDataOutputStream() {
                                    @Override
                                    public void write(int b) throws IOException {
                                        delegate.write(b);
                                    }

                                    @Override
                                    public long getPos() throws IOException {
                                        return delegate.getPos();
                                    }

                                    @Override
                                    public void flush() throws IOException {
                                        delegate.flush();
                                    }

                                    @Override
                                    public void close() throws IOException {
                                        delegate.close();
                                        throw new IOException("close failed");
                                    }
                                };
                            }
                        },
                        1024,
                        1024,
                        8192);
        failing.create(task("one", CompactionStatus.PENDING, null, null), 0, epoch);
        assertThatThrownBy(
                        () ->
                                failing.writePlan(
                                        "one",
                                        new CompactionTask.Plan(
                                                Collections.singletonList(bucket),
                                                Collections.emptyMap())))
                .hasMessageContaining("close failed");
        assertThat(store.load("one").version()).isZero();
        assertThat(store.load("one").planHandle()).isNull();
        failing.publish(task("one", CompactionStatus.FAILED, null, null), 0, 1, epoch);
        assertThat(zk.getCompactionActive().data()).isEmpty();
    }

    @Test
    void testChunkedPlanAndPublishedProgressChain() throws Exception {
        store =
                new CompactionStore(
                        zk, directory.toString(), new LocalFileSystem(), 1024, 1024, 1024 * 1024);
        store.create(task("one", CompactionStatus.PENDING, null, null), 0, epoch);
        List<TableBucket> buckets = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            buckets.add(new TableBucket(10, 20L, i));
        }
        FsPath plan =
                store.writePlan("one", new CompactionTask.Plan(buckets, Collections.emptyMap()));
        assertThat(store.readPlan(plan).buckets()).containsExactlyElementsOf(buckets);
        try (Stream<Path> files = Files.list(directory.resolve("compactions/one"))) {
            assertThat(files.count()).isGreaterThan(2);
        }
        FsPath first = store.writeProgress("one", null, Collections.singletonMap(bucket, 55L));
        store.publish(task("one", CompactionStatus.RUNNING, plan, first), 0, 1, epoch);
        FsPath second =
                store.writeProgress(
                        "one", first, Collections.singletonMap(new TableBucket(2, 0), 77L));
        store.publish(task("one", CompactionStatus.RUNNING, plan, second, 2), 1, 2, epoch);
        Map<TableBucket, Long> recovered =
                store.readProgress(store.load("one").progressHandle(), 2);
        assertThat(recovered)
                .hasSize(2)
                .containsEntry(bucket, 55L)
                .containsEntry(new TableBucket(2, 0), 77L);
    }

    @Test
    void testProgressRecoveryRejectsCorruptPublishedChain() throws Exception {
        FsPath first = store.writeProgress("one", null, Collections.singletonMap(bucket, 11L));
        assertThatThrownBy(() -> store.readProgress(first, 0)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> store.readProgress(first, 2)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> store.readProgress(null, 1)).isInstanceOf(IOException.class);
        FsPath duplicate = store.writeProgress("one", first, Collections.singletonMap(bucket, 12L));
        assertThatThrownBy(() -> store.readProgress(duplicate, 2))
                .hasMessageContaining("Duplicate");
        Files.write(
                java.nio.file.Paths.get(first.toUri().getPath()),
                CompactionTaskJsonSerde.encode(
                        CompactionTaskJsonSerde.progress(
                                first, Collections.singletonMap(bucket, 11L))));
        assertThatThrownBy(() -> store.readProgress(first, 2)).hasMessageContaining("Cyclic");
        Files.write(
                java.nio.file.Paths.get(first.toUri().getPath()),
                CompactionTaskJsonSerde.encode(
                        CompactionTaskJsonSerde.progress(null, Collections.emptyMap())));
        assertThatThrownBy(() -> store.readProgress(first, 1)).hasMessageContaining("Empty");
    }

    @Test
    void testOversizedBlockAndPlanAreRejectedBeforeWriting() throws Exception {
        store =
                new CompactionStore(
                        zk, directory.toString(), new LocalFileSystem(), 1024, 128, 1024 * 1024);
        store.create(task("one", CompactionStatus.PENDING, null, null), 0, epoch);
        assertThatThrownBy(
                        () ->
                                store.writePlan(
                                        "one",
                                        new CompactionTask.Plan(
                                                Collections.singletonList(bucket),
                                                Collections.singletonMap(
                                                        "setting",
                                                        String.join(
                                                                "",
                                                                Collections.nCopies(200, "大"))))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("block budget");
        assertThat(directory.resolve("compactions/one")).doesNotExist();
        CompactionStore smallPlan =
                new CompactionStore(
                        zk, directory.toString(), new LocalFileSystem(), 1024, 1024, 100);
        assertThatThrownBy(
                        () ->
                                smallPlan.writePlan(
                                        "one",
                                        new CompactionTask.Plan(
                                                Collections.singletonList(bucket),
                                                Collections.emptyMap())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("plan budget");
        assertThat(directory.resolve("compactions/one")).doesNotExist();
        assertThat(store.load("one").version()).isZero();
    }

    @Test
    void testOversizedRecordRejectsBeforeCreate() throws Exception {
        CompactionStore smallRecord =
                new CompactionStore(
                        zk, directory.toString(), new LocalFileSystem(), 64, 1024, 8192);
        assertThatThrownBy(
                        () ->
                                smallRecord.create(
                                        task("one", CompactionStatus.PENDING, null, null),
                                        0,
                                        epoch))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("record budget");
        assertThat(store.list()).isEmpty();
        assertThat(zk.getCompactionActive().data()).isEmpty();
    }

    @Test
    void testScopeAndTerminalSummarySurviveReload() throws Exception {
        CompactionInfo info =
                new CompactionInfo(
                        "scope",
                        CompactionType.MIGRATE,
                        CompactionStatus.PENDING,
                        10L,
                        null,
                        new TablePath("db", "table"),
                        "partition");
        store.create(
                new CompactionTask(info, 100L, 3L, 0L, 0L, 0L, 0L, 1L, null, null, null, -1),
                0,
                epoch);
        assertThat(store.load("scope").info()).isEqualTo(info);
        CompactionInfo terminal =
                new CompactionInfo(
                        "scope",
                        CompactionType.MIGRATE,
                        CompactionStatus.FAILED,
                        10L,
                        null,
                        new TablePath("db", "table"),
                        "partition");
        store.publish(
                new CompactionTask(
                        terminal, 100L, 3L, 1L, 1L, 1L, 80L, 80L, null, null, "failed snapshot", 0),
                0,
                1,
                epoch);
        CompactionTask recovered = new CompactionStore(zk, directory.toString()).load("scope");
        assertThat(recovered.info()).isEqualTo(terminal);
        assertThat(recovered.deadline()).isEqualTo(100L);
        assertThat(recovered.total()).isEqualTo(3L);
        assertThat(recovered.succeeded()).isEqualTo(1L);
        assertThat(recovered.failed()).isEqualTo(1L);
        assertThat(recovered.cancelled()).isEqualTo(1L);
        assertThat(recovered.finishTime()).isEqualTo(80L);
        assertThat(recovered.persistedTime()).isEqualTo(80L);
        assertThat(recovered.firstError()).isEqualTo("failed snapshot");
        assertThat(recovered.version()).isEqualTo(1);
    }

    @Test
    void testUnpublishedFilesSurviveRecoveryUntilRetentionExpires() throws Exception {
        store =
                new CompactionStore(
                        zk, directory.toString(), new LocalFileSystem(), 1024, 1024, 700);
        store.create(task("one", CompactionStatus.PENDING, null, null), 0, epoch);
        Map<TableBucket, Long> update = Collections.singletonMap(bucket, 5L);
        FsPath orphan = store.writeProgress("one", null, update);
        CompactionStore recovered =
                new CompactionStore(
                        zk, directory.toString(), new LocalFileSystem(), 1024, 1024, 700);
        assertThat(recovered.load("one").progressHandle()).isNull();
        assertThat(recovered.readProgress(recovered.load("one").progressHandle(), 0)).isEmpty();
        assertThat(new LocalFileSystem().exists(orphan)).isTrue();
        recovered.publish(task("one", CompactionStatus.FAILED, null, null), 0, 1, epoch);
        assertThat(recovered.cleanup(100, epoch)).isZero();
        assertThat(new LocalFileSystem().exists(orphan)).isTrue();
        assertThat(recovered.cleanup(101, epoch)).isEqualTo(1);
        assertThat(new LocalFileSystem().exists(orphan)).isFalse();
    }

    @Test
    void testBoundedCleanupAlsoRemovesLateOrphanDirectories() throws Exception {
        for (int i = 0; i < 34; i++) {
            String id = "task-" + i;
            int active = zk.getCompactionActive().zkVersion();
            store.create(task(id, CompactionStatus.PENDING, null, null), active, epoch);
            store.writePlan(
                    id,
                    new CompactionTask.Plan(
                            Collections.singletonList(bucket), Collections.emptyMap()));
            store.publish(task(id, CompactionStatus.FAILED, null, null), 0, active + 1, epoch);
        }
        assertThat(store.cleanup(101, epoch)).isEqualTo(32);
        assertThat(store.list()).hasSize(2);
        assertThat(store.cleanup(101, epoch)).isEqualTo(2);
        assertThat(store.list()).isEmpty();
        Path orphan = directory.resolve("compactions").resolve("task-0");
        Files.createDirectories(orphan);
        Files.write(orphan.resolve("late.progress"), new byte[] {1});
        assertThat(store.cleanup(1, epoch)).isZero();
        assertThat(Files.exists(orphan)).isTrue();
        Files.setLastModifiedTime(orphan, java.nio.file.attribute.FileTime.fromMillis(5));
        assertThat(store.cleanup(101, epoch)).isEqualTo(1);
        assertThat(Files.exists(orphan)).isFalse();
        assertThat(store.cleanup(101, epoch)).isZero();
    }

    @Test
    void testCleanupPreservesTaskOnRemoteFailureAndHonorsEpoch() throws Exception {
        store.create(task("one", CompactionStatus.PENDING, null, null), 0, epoch);
        store.writePlan(
                "one",
                new CompactionTask.Plan(Collections.singletonList(bucket), Collections.emptyMap()));
        store.publish(task("one", CompactionStatus.FAILED, null, null), 0, 1, epoch);
        CompactionStore failing =
                new CompactionStore(
                        zk,
                        directory.toString(),
                        new LocalFileSystem() {
                            @Override
                            public boolean delete(FsPath path, boolean recursive)
                                    throws IOException {
                                throw new IOException("delete failed");
                            }
                        },
                        1024,
                        1024,
                        8192);
        assertThatThrownBy(() -> failing.cleanup(101, epoch)).hasMessageContaining("delete failed");
        assertThat(store.list()).hasSize(1);
        zk.fenceBecomeCoordinatorLeader("replacement");
        assertThatThrownBy(() -> store.cleanup(101, epoch))
                .isInstanceOf(KeeperException.BadVersionException.class);
        assertThat(Files.exists(directory.resolve("compactions").resolve("one"))).isTrue();
    }

    @Test
    void testCleanupRotatesPastPersistentFailuresToTasksAndOrphans() throws Exception {
        for (int i = 0; i < 36; i++) {
            String id = "task-" + i;
            int active = zk.getCompactionActive().zkVersion();
            store.create(task(id, CompactionStatus.PENDING, null, null), active, epoch);
            store.writePlan(
                    id,
                    new CompactionTask.Plan(
                            Collections.singletonList(bucket), Collections.emptyMap()));
            store.publish(task(id, CompactionStatus.FAILED, null, null), 0, active + 1, epoch);
        }
        java.util.Set<String> failures = new java.util.HashSet<>();
        List<CompactionTask> ordered = store.list();
        for (int i = 0; i < 34; i++) {
            failures.add(ordered.get(i).info().operationId());
        }
        Path orphan = directory.resolve("compactions").resolve("orphan-later");
        Files.createDirectories(orphan);
        Files.write(orphan.resolve("late.progress"), new byte[] {1});
        Files.setLastModifiedTime(orphan, java.nio.file.attribute.FileTime.fromMillis(5));
        List<String> attempts = new ArrayList<>();
        CompactionStore failing =
                new CompactionStore(
                        zk,
                        directory.toString(),
                        new LocalFileSystem() {
                            @Override
                            public boolean delete(FsPath path, boolean recursive)
                                    throws IOException {
                                attempts.add(path.getName());
                                if (failures.contains(path.getName())) {
                                    throw new IOException("persistent failure");
                                }
                                return super.delete(path, recursive);
                            }
                        },
                        64 * 1024,
                        256 * 1024,
                        512L * 1024 * 1024);
        for (int round = 0; round < 2; round++) {
            attempts.clear();
            assertThatThrownBy(() -> failing.cleanup(101, epoch))
                    .hasMessageContaining("persistent failure");
            assertThat(attempts).hasSize(32);
        }
        assertThat(store.list())
                .extracting(t -> t.info().operationId())
                .containsExactlyInAnyOrderElementsOf(failures);
        assertThat(Files.exists(orphan)).isFalse();
    }

    @Test
    void testSuccessAggregateIsPublishedAtomicallyWithProgress() throws Exception {
        store.create(task("aggregate", CompactionStatus.PENDING, null, null), 0, epoch);
        FsPath plan =
                store.writePlan(
                        "aggregate",
                        new CompactionTask.Plan(
                                Arrays.asList(bucket, new TableBucket(2, 0)),
                                Collections.emptyMap()));
        FsPath unpublished =
                store.writeProgress("aggregate", null, Collections.singletonMap(bucket, 55L));
        CompactionInfo info =
                new CompactionInfo(
                        "aggregate",
                        CompactionType.OPTIMIZE,
                        CompactionStatus.RUNNING,
                        1L,
                        null,
                        null,
                        null);
        store.publish(
                new CompactionTask(info, 12345L, 2, 0, 0, 0, 0, 2, plan, null, null, 1),
                0,
                1,
                epoch);
        FsPath completion =
                store.writeProgress("aggregate", null, Collections.singletonMap(bucket, 11L));
        CompactionTask before = store.load("aggregate");
        assertThat(before.succeeded()).isZero();
        assertThat(before.progressHandle()).isNull();
        store.publish(
                new CompactionTask(info, 12345L, 2, 1, 0, 0, 0, 3, plan, completion, null, 2),
                1,
                2,
                epoch);
        CompactionTask after = store.load("aggregate");
        assertThat(after.succeeded()).isEqualTo(1);
        assertThat(after.progressHandle()).isEqualTo(completion);
        assertThatThrownBy(
                        () ->
                                new CompactionTask(
                                        info, 12345L, 2, 3, 0, 0, 0, 3, plan, completion, null, 2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new CompactionTask(
                                        info, 12345L, 2, 1, 2, 0, 0, 3, plan, completion, null, 2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(store.load("aggregate").progressHandle()).isEqualTo(completion);
    }

    private CompactionTask task(String id, CompactionStatus status, FsPath plan, FsPath progress) {
        return task(id, status, plan, progress, progress == null ? 0 : 1);
    }

    private CompactionTask task(
            String id, CompactionStatus status, FsPath plan, FsPath progress, long successes) {
        return new CompactionTask(
                new CompactionInfo(id, CompactionType.OPTIMIZE, status, 1L, null, null, null),
                12345L,
                2L,
                successes,
                0L,
                0L,
                status == CompactionStatus.FAILED ? 100L : 0L,
                status == CompactionStatus.FAILED ? 100L : 1L,
                plan,
                progress,
                null,
                -1);
    }
}
