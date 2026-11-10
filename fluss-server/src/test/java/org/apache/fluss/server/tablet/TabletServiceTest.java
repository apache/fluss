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
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsResponse;
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
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.tablet.consumer.group.ConsumerOffsetSchema;
import org.apache.fluss.server.tablet.consumer.group.ConsumerOffsetTableConfig;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TabletService}. */
class TabletServiceTest extends ReplicaTestBase {

    private static final long OFFSETS_TABLE_ID = 100L;
    private static final String GROUP_ID = "worker-commits";
    private static final TableBucket SOURCE_BUCKET = new TableBucket(1L, 0);
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(10);

    private LakeCatalogDynamicLoader lakeCatalogLoader;
    private MetadataManager metadataManager;
    private final ManuallyTriggeredScheduledExecutorService executor =
            new ManuallyTriggeredScheduledExecutorService();

    @BeforeEach
    void createMetadataManager() {
        lakeCatalogLoader = new LakeCatalogDynamicLoader(conf, null, true);
        metadataManager = new MetadataManager(zkClient, conf, lakeCatalogLoader);
    }

    @AfterEach
    void closeLakeCatalogLoader() throws Exception {
        if (lakeCatalogLoader != null) {
            lakeCatalogLoader.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testInvalidGroupIdTakesPrecedence(boolean empty) throws Exception {
        TabletService service = createService();
        CommitConsumerOffsetsRequest request =
                empty
                        ? new CommitConsumerOffsetsRequest().setGroupId("")
                        : newCommitRequest("", 10L);
        if (!empty) {
            request.getTableAt(0).getBucketAt(0).setPartitionId(-1L);
        }
        assertThatThrownBy(() -> service.commitConsumerOffsets(request).join())
                .cause()
                .isInstanceOf(InvalidConsumerGroupIdException.class);
        assertThat(executor.numQueuedRunnables()).isZero();
    }

    @Test
    void testCommitWhileFindIsInitializing() throws Exception {
        registerOffsetsTable();
        TabletService service = createService();
        CompletableFuture<?> find =
                service.findConsumerGroupOwner(
                        new FindConsumerGroupOwnerRequest().setGroupId(GROUP_ID));
        CommitConsumerOffsetsRequest request = newCommitRequest(GROUP_ID, 10L);
        request.getTableAt(0).getBucketAt(0).setPartitionId(-1L);

        assertThat(find).isNotDone();
        assertThat(executor.numQueuedRunnables()).isOne();
        CompletableFuture<?> unavailable = service.commitConsumerOffsets(request);
        assertThat(unavailable).isCompletedExceptionally();
        assertThatThrownBy(unavailable::join)
                .cause()
                .isInstanceOf(ConsumerGroupOwnerNotAvailableException.class);
        assertThat(executor.numQueuedRunnables()).isOne();

        executor.triggerAll();
        assertThat(find).isCompleted();
        assertThatThrownBy(() -> service.commitConsumerOffsets(request).join())
                .cause()
                .isInstanceOf(InvalidPartitionException.class);
        assertCommitResult(
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 10L)), Errors.NONE);
        assertThat(executor.numQueuedRunnables()).isZero();
    }

    @Test
    void testConsumerOffsetAppendsDoNotWaitForReplicationAcknowledgement() throws Exception {
        conf.set(ConfigOptions.OFFSET_COMMIT_TIMEOUT, Duration.ofMinutes(1));
        registerOffsetsTable();
        TabletService service = createInitializedService();
        TableBucket bucket = new TableBucket(OFFSETS_TABLE_ID, 0);
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
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 20L));
        long firstLogEnd = replica.getLogTablet().localLogEndOffset();
        assertThat(firstLogEnd).isGreaterThan(initialLogEnd);
        assertThat(first).isNotDone();

        CompletableFuture<CommitConsumerOffsetsResponse> second =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 10L));
        assertThat(executor.numQueuedRunnables()).isZero();
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
        assertStoredOffset(GROUP_ID, 10L);
    }

    @Test
    void testGetConsumerOffsetsAfterFindOwnerUsesInitializedMetadata() throws Exception {
        registerOffsetsTable();
        TabletService service = createService();

        CompletableFuture<?> find =
                service.findConsumerGroupOwner(
                        new FindConsumerGroupOwnerRequest().setGroupId(GROUP_ID));
        assertThat(find).isNotDone();
        executor.triggerAll();
        assertThat(find).isCompleted();

        GetConsumerOffsetsRequest request =
                new GetConsumerOffsetsRequest().setGroupId(GROUP_ID).setAllOffsets(true);
        CompletableFuture<GetConsumerOffsetsResponse> response =
                service.getConsumerOffsets(request);
        executor.triggerAll();

        assertThat(response).isCompleted();
        assertThat(zkClient.tableExist(SystemTables.CONSUMER_OFFSETS)).isTrue();
    }

    @Test
    void testCommitBeforeFindDoesNotScheduleInitialization() throws Exception {
        TabletService service = createService();
        CompletableFuture<CommitConsumerOffsetsResponse> response =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 10L));

        assertThat(executor.numQueuedRunnables()).isZero();
        assertThat(response).isCompletedExceptionally();
        assertThatThrownBy(response::join)
                .cause()
                .isInstanceOf(ConsumerGroupOwnerNotAvailableException.class);
        assertThat(zkClient.tableExist(SystemTables.CONSUMER_OFFSETS)).isFalse();
    }

    @Test
    void testDuplicateBucketValidatesOnlyLastOffset() throws Exception {
        conf.set(ConfigOptions.OFFSET_METADATA_MAX_BYTES, new MemorySize(9));
        registerOffsetsTable();
        TabletService service = createInitializedService();
        CommitConsumerOffsetsRequest request = newCommitRequest(GROUP_ID, 30L);
        request.getTableAt(0).getBucketAt(0).setMetadata("metadata too large");
        request.addTable()
                .setTableId(SOURCE_BUCKET.getTableId())
                .addBucket()
                .setBucketId(SOURCE_BUCKET.getBucket())
                .setOffset(20L)
                .setMetadata("offset-20");

        assertCommitResult(service.commitConsumerOffsets(request), Errors.NONE);
        assertStoredOffset(GROUP_ID, 20L);

        request.getTableAt(0).getBucketAt(0).setMetadata("offset-30");
        request.getTableAt(1).getBucketAt(0).setMetadata("metadata too large");
        assertCommitResult(
                service.commitConsumerOffsets(request), Errors.CONSUMER_OFFSET_METADATA_TOO_LARGE);
        assertStoredOffset(GROUP_ID, 20L);
    }

    @Test
    void testCommitConsumerOffsetsPreservesStorageBackpressure() throws Exception {
        conf.set(ConfigOptions.KV_WRITE_BUFFER_SIZE, MemorySize.parse("1kb"));
        conf.set(ConfigOptions.OFFSET_METADATA_MAX_BYTES, MemorySize.parse("64kb"));
        registerOffsetsTable();
        TabletService service = createInitializedService();

        // Valid offset metadata can still produce a batch exceeding the KV flush budget.
        char[] metadata = new char[64 * 1024];
        Arrays.fill(metadata, 'x');
        CommitConsumerOffsetsRequest request = newCommitRequest(GROUP_ID, 10L);
        request.getTableAt(0).getBucketAt(0).setMetadata(new String(metadata));
        CompletableFuture<CommitConsumerOffsetsResponse> rejected =
                service.commitConsumerOffsets(request);
        assertCommitResult(rejected, Errors.STORAGE_BACKPRESSURE_EXCEPTION);

        CompletableFuture<CommitConsumerOffsetsResponse> next =
                service.commitConsumerOffsets(newCommitRequest(GROUP_ID, 20L));
        assertCommitResult(next, Errors.NONE);
        assertStoredOffset(GROUP_ID, 20L);
    }

    private TabletService createInitializedService() throws Exception {
        TabletService service = createService();
        CompletableFuture<?> find =
                service.findConsumerGroupOwner(
                        new FindConsumerGroupOwnerRequest().setGroupId(GROUP_ID));
        executor.triggerAll();
        assertThat(find).isCompleted();
        return service;
    }

    private TabletService createService() throws Exception {
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
                        ConsumerOffsetTableConfig.from(conf));
        service.setCurrentSession(
                new Session(
                        (short) 0,
                        "CLIENT",
                        false,
                        InetAddress.getLoopbackAddress(),
                        FlussPrincipal.ANONYMOUS));
        return service;
    }

    private void registerOffsetsTable() throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(ConsumerOffsetSchema.getTableSchema())
                        .logFormat(LogFormat.COMPACTED)
                        .distributedBy(1, ConsumerOffsetSchema.getBucketKeyColumn())
                        .build();
        zkClient.registerTable(
                SystemTables.CONSUMER_OFFSETS,
                TableRegistration.newTable(
                        OFFSETS_TABLE_ID, conf.get(ConfigOptions.REMOTE_DATA_DIR), descriptor));
        zkClient.registerFirstSchema(
                SystemTables.CONSUMER_OFFSETS, ConsumerOffsetSchema.getTableSchema());
        makeKvTableAsLeader(OFFSETS_TABLE_ID, SystemTables.CONSUMER_OFFSETS, 0);
        serverMetadataCache.updateTableMetadata(
                new TableMetadata(
                        metadataManager.getTable(SystemTables.CONSUMER_OFFSETS),
                        Collections.singletonList(
                                new BucketMetadata(
                                        0,
                                        TABLET_SERVER_ID,
                                        0,
                                        Collections.singletonList(TABLET_SERVER_ID)))));
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
        TableBucket offsetsBucket = new TableBucket(OFFSETS_TABLE_ID, 0);
        byte[] key =
                new ConsumerOffsetSchema()
                        .encodeOffsetKey(
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
}
