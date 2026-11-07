/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.tablet;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.exception.InvalidConsumerGroupIdException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsResponseBucket;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.SystemTables;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.tablet.consumer.group.ConsumerOffsetSchema;
import org.apache.fluss.server.tablet.consumer.group.GroupManager;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TabletService}. */
class TabletServiceTest extends ReplicaTestBase {

    private static final long OFFSETS_TABLE_ID = 100L;
    private static final String GROUP_ID = "ordered-commits";
    private static final TableBucket SOURCE_BUCKET = new TableBucket(1L, 0);
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(10);

    private LakeCatalogDynamicLoader lakeCatalogLoader;
    private MetadataManager metadataManager;
    private GroupManager groupManager;

    @BeforeEach
    void createMetadataManager() {
        lakeCatalogLoader = new LakeCatalogDynamicLoader(conf, null, true);
        metadataManager = new MetadataManager(zkClient, conf, lakeCatalogLoader);
        groupManager =
                GroupManager.from(conf, TABLET_SERVER_ID, replicaManager, serverMetadataCache);
    }

    @AfterEach
    void closeLakeCatalogLoader() throws Exception {
        if (lakeCatalogLoader != null) {
            lakeCatalogLoader.close();
        }
    }

    @ParameterizedTest
    @CsvSource({"false,empty-commit", "true,empty-commit", "false,''", "true,''"})
    void testEmptyCommitDoesNotInitializeSystemTable(boolean includeEmptyTable, String groupId)
            throws Exception {
        assertThat(zkClient.tableExist(SystemTables.CONSUMER_OFFSETS)).isFalse();
        ManuallyTriggeredScheduledExecutorService executor =
                new ManuallyTriggeredScheduledExecutorService();
        TabletService service = createService(executor);
        CommitConsumerOffsetsRequest request =
                new CommitConsumerOffsetsRequest().setGroupId(groupId);
        if (includeEmptyTable) {
            request.addTable().setTableId(Long.MAX_VALUE);
        }

        CompletableFuture<CommitConsumerOffsetsResponse> response =
                service.commitConsumerOffsets(request);

        assertThat(executor.numQueuedRunnables()).isZero();
        assertThat(executor.getActiveScheduledTasks()).isEmpty();
        assertThat(response)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(value -> assertThat(value.getTablesList()).isEmpty());
        assertThat(zkClient.tableExist(SystemTables.CONSUMER_OFFSETS)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testInvalidGroupIdDoesNotInitializeSystemTable(boolean tableExists) throws Exception {
        if (tableExists) {
            registerOffsetsTable(true);
        }
        ManuallyTriggeredScheduledExecutorService executor =
                new ManuallyTriggeredScheduledExecutorService();
        TabletService service = createService(executor);

        CompletableFuture<CommitConsumerOffsetsResponse> response =
                service.commitConsumerOffsets(newCommitRequest("", 10L));

        assertThat(response).isCompletedExceptionally();
        assertThatThrownBy(() -> response.get(WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .cause()
                .isInstanceOf(InvalidConsumerGroupIdException.class);
        assertThat(executor.numQueuedRunnables()).isZero();
        assertThat(executor.getActiveScheduledTasks()).isEmpty();
        assertThat(zkClient.tableExist(SystemTables.CONSUMER_OFFSETS)).isEqualTo(tableExists);
    }

    @ParameterizedTest
    @CsvSource({"false,10,20", "true,10,20", "false,20,10", "true,20,10"})
    void testConsumerOffsetCommitsPreserveSubmissionOrder(
            boolean initialized, long firstOffset, long secondOffset) throws Exception {
        registerOffsetsTable(true);
        ReorderingExecutor executor = new ReorderingExecutor();
        TabletService service = createService(executor);
        if (initialized) {
            initializeOffsetCommits(service, executor);
        }

        CompletableFuture<CommitConsumerOffsetsResponse> first =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, firstOffset));
        CompletableFuture<CommitConsumerOffsetsResponse> second =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, secondOffset));

        // An unresolved initialization future can enqueue its dependents in reverse order;
        // after initialization, reverse execution simulates a shared pool reordering writes.
        finishTasks(executor, initialized, first, second);
        assertCommitResult(first, Errors.NONE);
        assertCommitResult(second, Errors.NONE);
        assertStoredOffset(GROUP_ID, secondOffset);
    }

    @ParameterizedTest
    @CsvSource({"false,10,20", "true,10,20", "false,20,10", "true,20,10"})
    void testConsumerOffsetAppendsDoNotWaitForReplicationAcknowledgement(
            boolean initialized, long firstOffset, long secondOffset) throws Exception {
        conf.set(ConfigOptions.OFFSET_COMMIT_TIMEOUT, Duration.ofMinutes(1));
        groupManager =
                GroupManager.from(conf, TABLET_SERVER_ID, replicaManager, serverMetadataCache);
        registerOffsetsTable(true);
        ReorderingExecutor executor = new ReorderingExecutor();
        TabletService service = createService(executor);
        if (initialized) {
            initializeOffsetCommits(service, executor);
        }

        TableBucket bucket =
                new TableBucket(
                        OFFSETS_TABLE_ID,
                        groupManager.bucketFor(GROUP_ID, TEST_ROUTING_BUCKET_COUNT));
        Replica replica = replicaManager.getReplicaOrException(bucket);
        int leaderEpoch = replica.getLeaderEpoch();
        List<Integer> replicas = Arrays.asList(TABLET_SERVER_ID, TABLET_SERVER_ID + 1);
        makeKvTableAsLeader(
                bucket, SystemTables.CONSUMER_OFFSETS, replicas, replicas, leaderEpoch, false);
        assertThat(replica.getIsr()).containsExactlyElementsOf(replicas);
        long initialLogEnd = replica.getLogTablet().localLogEndOffset();
        // Report the follower as caught up, then withhold fetches to delay acknowledgements.
        // The test clock does not advance, so the follower remains in the ISR.
        FetchParams followerFetch = new FetchParams(TABLET_SERVER_ID + 1, Integer.MAX_VALUE);
        followerFetch.setCurrentFetch(
                OFFSETS_TABLE_ID, initialLogEnd, Integer.MAX_VALUE, null, null, null, null);
        replica.fetchRecords(followerFetch);

        CompletableFuture<CommitConsumerOffsetsResponse> first =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, firstOffset));
        CompletableFuture<CommitConsumerOffsetsResponse> second =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, secondOffset));
        if (!initialized) {
            executor.trigger();
        }
        executor.triggerLast();
        long firstLogEnd = replica.getLogTablet().localLogEndOffset();
        assertThat(firstLogEnd).isGreaterThan(initialLogEnd);
        assertThat(first).isNotDone();

        while (executor.numQueuedRunnables() > 0) {
            executor.triggerLast();
        }
        assertThat(replica.getLogTablet().localLogEndOffset())
                .as("The second commit must append while the first is awaiting replication")
                .isGreaterThan(firstLogEnd);
        assertThat(first).isNotDone();
        assertThat(second).isNotDone();

        followerFetch.setCurrentFetch(
                OFFSETS_TABLE_ID,
                replica.getLogTablet().localLogEndOffset(),
                Integer.MAX_VALUE,
                null,
                null,
                null,
                null);
        replica.fetchRecords(followerFetch);
        assertCommitResult(first, Errors.NONE);
        assertCommitResult(second, Errors.NONE);
        assertStoredOffset(GROUP_ID, secondOffset);
    }

    @Test
    void testCompletedCommitDoesNotDetachQueuedCommits() throws Exception {
        registerOffsetsTable(true);
        ReorderingExecutor executor = new ReorderingExecutor();
        TabletService service = createService(executor);
        initializeOffsetCommits(service, executor);

        CompletableFuture<CommitConsumerOffsetsResponse> first =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 10L));
        CompletableFuture<CommitConsumerOffsetsResponse> second =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 20L));
        executor.trigger();
        assertCommitResult(first, Errors.NONE);
        CompletableFuture<CommitConsumerOffsetsResponse> third =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 30L));

        finishTasks(executor, true, second, third);
        assertCommitResult(second, Errors.NONE);
        assertCommitResult(third, Errors.NONE);
        assertStoredOffset(GROUP_ID, 30L);
    }

    @Test
    void testDifferentConsumerGroupsCanCommitIndependently() throws Exception {
        registerOffsetsTable(true);
        ReorderingExecutor executor = new ReorderingExecutor();
        TabletService service = createService(executor);
        initializeOffsetCommits(service, executor);

        CompletableFuture<CommitConsumerOffsetsResponse> first =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 10L));
        CompletableFuture<CommitConsumerOffsetsResponse> second =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 20L));
        CompletableFuture<CommitConsumerOffsetsResponse> other =
                service.commitConsumerOffsets(newCommitRequest("other-group", 30L));
        executor.triggerLast();
        assertCommitResult(other, Errors.NONE);
        assertThat(first).isNotDone();
        assertThat(second).isNotDone();

        finishTasks(executor, true, first, second);
        assertCommitResult(first, Errors.NONE);
        assertCommitResult(second, Errors.NONE);
        assertStoredOffset(GROUP_ID, 20L);
        assertStoredOffset("other-group", 30L);
    }

    @Test
    void testInitializationFailureDoesNotFailQueuedCommit() throws Exception {
        registerOffsetsTable(false);
        ReorderingExecutor executor = new ReorderingExecutor();
        TabletService service = createService(executor);
        CompletableFuture<CommitConsumerOffsetsResponse> first =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 10L));
        CompletableFuture<CommitConsumerOffsetsResponse> second =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 20L));
        executor.trigger();
        assertThatThrownBy(() -> first.get(WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .cause()
                .isInstanceOf(ConsumerGroupOwnerNotAvailableException.class);

        publishOffsetsSchemaAndCreateReplicas();
        finishTasks(executor, false, second);
        assertCommitResult(second, Errors.NONE);
        assertStoredOffset(GROUP_ID, 20L);
    }

    @Test
    void testCommitErrorCompletesResponseAndDoesNotBlockQueuedCommit() throws Exception {
        registerOffsetsTable(false);
        zkClient.registerFirstSchema(
                SystemTables.CONSUMER_OFFSETS, ConsumerOffsetSchema.getTableSchema());
        LinkageError failure = new LinkageError("Failed to resolve offset bucket metadata.");
        TabletServerMetadataCache failingMetadataCache =
                new TabletServerMetadataCache(metadataManager) {
                    @Override
                    public Optional<BucketMetadata> getCachedBucketMetadata(
                            TablePath tablePath, int bucketId) {
                        throw failure;
                    }
                };
        groupManager =
                GroupManager.from(conf, TABLET_SERVER_ID, replicaManager, failingMetadataCache);
        ReorderingExecutor executor = new ReorderingExecutor();
        TabletService service = createService(executor);
        CompletableFuture<CommitConsumerOffsetsResponse> first =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 10L));
        CompletableFuture<CommitConsumerOffsetsResponse> second =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 20L));
        executor.trigger();
        executor.trigger();

        assertThat(first).isCompletedExceptionally();
        assertThatThrownBy(() -> first.get(WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .cause()
                .isSameAs(failure);
        assertThat(second).isNotDone();

        makeKvTableAsLeader(
                OFFSETS_TABLE_ID,
                SystemTables.CONSUMER_OFFSETS,
                groupManager.bucketFor(GROUP_ID, TEST_ROUTING_BUCKET_COUNT));
        finishTasks(executor, false, second);
        assertCommitResult(second, Errors.NONE);
        assertStoredOffset(GROUP_ID, 20L);
    }

    @Test
    void testExecutorRejectionDoesNotBlockQueuedCommit() throws Exception {
        registerOffsetsTable(true);
        ReorderingExecutor executor = new ReorderingExecutor();
        TabletService service = createService(executor);
        CompletableFuture<CommitConsumerOffsetsResponse> first =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 10L));
        CompletableFuture<CommitConsumerOffsetsResponse> second =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 20L));
        executor.rejectNextSubmission();
        executor.trigger();

        finishTasks(executor, false, first, second);
        assertThatThrownBy(() -> first.get(WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .rootCause()
                .isInstanceOf(RejectedExecutionException.class);
        assertCommitResult(second, Errors.NONE);
        assertStoredOffset(GROUP_ID, 20L);
    }

    @Test
    void testWriteFailureDoesNotBlockQueuedCommit() throws Exception {
        registerOffsetsTable(true);
        ReorderingExecutor executor = new ReorderingExecutor();
        TabletService service = createService(executor);
        initializeOffsetCommits(service, executor);
        conf.set(ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO, 0.85);
        conf.set(ConfigOptions.SERVER_DATA_DISK_WRITE_RECOVER_RATIO, 0.80);
        localDiskManager.reconfigure(conf);
        replicaManager.getDiskUsageMonitor().update(0.99);

        CompletableFuture<CommitConsumerOffsetsResponse> first =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 10L));
        CompletableFuture<CommitConsumerOffsetsResponse> second =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 20L));
        executor.trigger();
        assertCommitResult(first, Errors.DISK_WRITE_LOCKED);
        replicaManager.getDiskUsageMonitor().update(0.50);

        finishTasks(executor, false, second);
        assertCommitResult(second, Errors.NONE);
        assertStoredOffset(GROUP_ID, 20L);
    }

    @Test
    void testCommitConsumerOffsetsPreservesStorageBackpressure() throws Exception {
        conf.set(ConfigOptions.KV_WRITE_BUFFER_SIZE, MemorySize.parse("1kb"));
        conf.set(ConfigOptions.OFFSET_METADATA_MAX_BYTES, MemorySize.parse("64kb"));
        groupManager =
                GroupManager.from(conf, TABLET_SERVER_ID, replicaManager, serverMetadataCache);
        registerOffsetsTable(true);
        ReorderingExecutor executor = new ReorderingExecutor();
        TabletService service = createService(executor);

        // Valid offset metadata can still produce a batch exceeding the KV flush budget.
        char[] metadata = new char[64 * 1024];
        Arrays.fill(metadata, 'x');
        CommitConsumerOffsetsRequest request = newCommitRequest(GROUP_ID, 10L);
        request.getTableAt(0).getBucketAt(0).setMetadata(new String(metadata));
        CompletableFuture<CommitConsumerOffsetsResponse> rejected =
                service.commitConsumerOffsets(request);
        finishTasks(executor, false, rejected);
        assertCommitResult(rejected, Errors.STORAGE_BACKPRESSURE_EXCEPTION);

        CompletableFuture<CommitConsumerOffsetsResponse> next =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 20L));
        finishTasks(executor, false, next);
        assertCommitResult(next, Errors.NONE);
        assertStoredOffset(GROUP_ID, 20L);
    }

    @Test
    void testCancelledResponseDoesNotReorderCommits() throws Exception {
        registerOffsetsTable(true);
        ReorderingExecutor executor = new ReorderingExecutor();
        TabletService service = createService(executor);
        CompletableFuture<CommitConsumerOffsetsResponse> first =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 10L));
        CompletableFuture<CommitConsumerOffsetsResponse> second =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 20L));
        assertThat(first.cancel(false)).isTrue();

        finishTasks(executor, false, second);
        assertCommitResult(second, Errors.NONE);
        assertStoredOffset(GROUP_ID, 20L);
    }

    private TabletService createService(ExecutorService executor) throws Exception {
        TabletService service =
                new TabletService(
                        TABLET_SERVER_ID,
                        new FsPath(conf.get(ConfigOptions.REMOTE_DATA_DIR)).getFileSystem(),
                        zkClient,
                        replicaManager,
                        serverMetadataCache,
                        metadataManager,
                        null,
                        null,
                        executor,
                        executor,
                        scannerManager,
                        testCoordinatorGateway,
                        "CLIENT",
                        groupManager);
        service.setCurrentSession(
                new Session(
                        (short) 0,
                        "CLIENT",
                        false,
                        InetAddress.getLoopbackAddress(),
                        FlussPrincipal.ANONYMOUS));
        return service;
    }

    private void registerOffsetsTable(boolean publishSchema) throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(ConsumerOffsetSchema.getTableSchema())
                        .logFormat(LogFormat.COMPACTED)
                        .distributedBy(
                                TEST_ROUTING_BUCKET_COUNT,
                                ConsumerOffsetSchema.getBucketKeyColumn())
                        .build();
        zkClient.registerTable(
                SystemTables.CONSUMER_OFFSETS,
                TableRegistration.newTable(
                        OFFSETS_TABLE_ID, conf.get(ConfigOptions.REMOTE_DATA_DIR), descriptor));
        if (publishSchema) {
            publishOffsetsSchemaAndCreateReplicas();
        }
    }

    private void publishOffsetsSchemaAndCreateReplicas() throws Exception {
        zkClient.registerFirstSchema(
                SystemTables.CONSUMER_OFFSETS, ConsumerOffsetSchema.getTableSchema());
        for (int bucket = 0; bucket < TEST_ROUTING_BUCKET_COUNT; bucket++) {
            makeKvTableAsLeader(OFFSETS_TABLE_ID, SystemTables.CONSUMER_OFFSETS, bucket);
        }
    }

    private static CommitConsumerOffsetsRequest newCommitRequest(String groupId, long offset) {
        CommitConsumerOffsetsRequest request =
                new CommitConsumerOffsetsRequest().setGroupId(groupId);
        request.addTable()
                .setTableId(SOURCE_BUCKET.getTableId())
                .addBucket()
                .setBucketId(SOURCE_BUCKET.getBucket())
                .setOffset(offset)
                .setMetadata("offset-" + offset);
        return request;
    }

    private static void initializeOffsetCommits(
            TabletService service, ReorderingExecutor executor) {
        CompletableFuture<CommitConsumerOffsetsResponse> initialized =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 0L));
        finishTasks(executor, false, initialized);
        assertCommitResult(initialized, Errors.NONE);
    }

    private static void finishTasks(
            ReorderingExecutor executor, boolean reverse, CompletableFuture<?>... responses) {
        retry(
                WAIT_TIMEOUT,
                () -> {
                    while (executor.numQueuedRunnables() > 0) {
                        if (reverse) {
                            executor.triggerLast();
                        } else {
                            executor.trigger();
                        }
                    }
                    for (CompletableFuture<?> response : responses) {
                        assertThat(response.isDone()).isTrue();
                    }
                });
    }

    private static void assertCommitResult(
            CompletableFuture<CommitConsumerOffsetsResponse> response, Errors expectedError) {
        assertThat(response)
                .succeedsWithin(WAIT_TIMEOUT)
                .satisfies(
                        value -> {
                            assertThat(value.getTablesList()).hasSize(1);
                            assertThat(value.getTableAt(0).getBucketsList()).hasSize(1);
                            PbCommitConsumerOffsetsResponseBucket bucket =
                                    value.getTableAt(0).getBucketAt(0);
                            assertThat(
                                            bucket.hasErrorCode()
                                                    ? Errors.forCode(bucket.getErrorCode())
                                                    : Errors.NONE)
                                    .isEqualTo(expectedError);
                        });
    }

    private void assertStoredOffset(String groupId, long expectedOffset) throws Exception {
        TableBucket offsetsBucket =
                new TableBucket(
                        OFFSETS_TABLE_ID,
                        groupManager.bucketFor(groupId, TEST_ROUTING_BUCKET_COUNT));
        byte[] key =
                ConsumerOffsetSchema.encodeOffsetKey(
                        ConsumerOffsetSchema.createOffsetKeyEncoder(),
                        groupId,
                        SOURCE_BUCKET.getTableId(),
                        SOURCE_BUCKET.getPartitionId(),
                        SOURCE_BUCKET.getBucket());
        ValueDecoder decoder =
                new ValueDecoder(
                        new TestingSchemaGetter(
                                zkClient.getCurrentSchemaId(SystemTables.CONSUMER_OFFSETS),
                                ConsumerOffsetSchema.getTableSchema()),
                        KvFormat.COMPACTED);
        retry(
                WAIT_TIMEOUT,
                () -> {
                    CompletableFuture<Map<TableBucket, LookupResultForBucket>> lookup =
                            new CompletableFuture<>();
                    replicaManager.lookups(
                            Collections.singletonMap(offsetsBucket, Collections.singletonList(key)),
                            ApiKeys.LOOKUP.highestSupportedVersion,
                            lookup::complete);
                    LookupResultForBucket result =
                            lookup.get(WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                    .get(offsetsBucket);
                    assertThat(result.failed()).isFalse();
                    assertThat(result.lookupValues()).hasSize(1);
                    assertThat(result.lookupValues().get(0)).isNotNull();
                    InternalRow row =
                            decoder.decodeValue(result.lookupValues().get(0).toByteArray()).row;
                    assertThat(row.getLong(5)).isEqualTo(expectedOffset);
                    assertThat(row.getString(6).toString()).isEqualTo("offset-" + expectedOffset);
                });
    }

    private static final class ReorderingExecutor
            extends ManuallyTriggeredScheduledExecutorService {
        private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
        private boolean rejectNext;

        @Override
        public synchronized void execute(Runnable command) {
            if (rejectNext) {
                rejectNext = false;
                throw new RejectedExecutionException("Rejected commit task for testing.");
            }
            tasks.addLast(command);
        }

        @Override
        public synchronized int numQueuedRunnables() {
            return tasks.size();
        }

        @Override
        public void trigger() {
            Runnable task;
            synchronized (this) {
                task = tasks.removeFirst();
            }
            task.run();
        }

        void triggerLast() {
            Runnable task;
            synchronized (this) {
                task = tasks.removeLast();
            }
            task.run();
        }

        synchronized void rejectNextSubmission() {
            rejectNext = true;
        }
    }
}
