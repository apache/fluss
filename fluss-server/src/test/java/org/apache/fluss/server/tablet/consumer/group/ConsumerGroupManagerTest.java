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

package org.apache.fluss.server.tablet.consumer.group;

import org.apache.fluss.exception.InvalidConsumerGroupIdException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.SystemTables;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataProvider;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager.NoneReplica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.ZkVersion;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.NO_LEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for consumer-group routing and local ownership checks. */
class ConsumerGroupManagerTest extends ReplicaTestBase {

    private static final String GROUP_ID = "ownership-test";
    private static final long OFFSETS_TABLE_ID = 100L;
    private static final int OTHER_SERVER_ID = TABLET_SERVER_ID + 1;
    private static final TableBucket SOURCE_BUCKET = new TableBucket(1L, 0);

    private LakeCatalogDynamicLoader lakeCatalogLoader;
    private ConsumerGroupManager groupManager;
    private TableInfo offsetsTableInfo;
    private TableBucket offsetsTableBucket;

    @BeforeEach
    void createConsumerGroupManager() throws Exception {
        registerTableInZkClient(
                SystemTables.CONSUMER_OFFSETS,
                ConsumerOffsetSchema.getTableSchema(),
                OFFSETS_TABLE_ID,
                Collections.singletonList(ConsumerOffsetSchema.getBucketKeyColumn()),
                Collections.emptyMap());
        lakeCatalogLoader = new LakeCatalogDynamicLoader(conf, null, true);
        MetadataManager metadataManager = new MetadataManager(zkClient, conf, lakeCatalogLoader);
        offsetsTableInfo = metadataManager.getTable(SystemTables.CONSUMER_OFFSETS);
        groupManager =
                new ConsumerGroupManager(
                        TABLET_SERVER_ID,
                        replicaManager,
                        ConsumerOffsetTableConfig.from(conf),
                        zkClient,
                        metadataManager,
                        serverMetadataCache,
                        new TabletServerMetadataProvider(
                                zkClient, metadataManager, serverMetadataCache),
                        testCoordinatorGateway,
                        Runnable::run);
        groupManager.ensureTableAsync().join();
        offsetsTableBucket =
                new TableBucket(
                        OFFSETS_TABLE_ID,
                        new ConsumerOffsetSchema().bucketFor(GROUP_ID, TEST_ROUTING_BUCKET_COUNT));
        // Keep ZooKeeper independent of the cache to detect synchronous owner lookups.
        zkClient.registerLeaderAndIsr(
                offsetsTableBucket,
                new LeaderAndIsr(OTHER_SERVER_ID, INITIAL_COORDINATOR_EPOCH),
                ZkVersion.MATCH_ANY_VERSION.getVersion());
    }

    @AfterEach
    void closeLakeCatalogLoader() throws Exception {
        if (lakeCatalogLoader != null) {
            lakeCatalogLoader.close();
        }
    }

    @Test
    void testInvalidGroupId() {
        assertThatThrownBy(() -> groupManager.validateGroupId(null))
                .isInstanceOf(InvalidConsumerGroupIdException.class);
        assertThatThrownBy(() -> groupManager.validateGroupId(""))
                .isInstanceOf(InvalidConsumerGroupIdException.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMissingCachedBucketReturnsOwnerUnavailable(boolean tableKnown) throws Exception {
        if (tableKnown) {
            serverMetadataCache.updateTableMetadata(
                    new TableMetadata(offsetsTableInfo, Collections.emptyList()));
        }
        assertCommitResult(Errors.CONSUMER_GROUP_OWNER_NOT_AVAILABLE);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(ints = {NO_LEADER})
    void testMissingCachedLeaderReturnsOwnerUnavailable(@Nullable Integer leaderId)
            throws Exception {
        cacheLeader(leaderId);
        assertCommitResult(Errors.CONSUMER_GROUP_OWNER_NOT_AVAILABLE);
    }

    @Test
    void testCachedLocalOwnerWithoutReplicaReturnsLoading() throws Exception {
        cacheLeader(TABLET_SERVER_ID);
        assertThat(replicaManager.getReplica(offsetsTableBucket)).isInstanceOf(NoneReplica.class);
        assertCommitResult(Errors.CONSUMER_GROUP_OWNER_LOADING);
    }

    @Test
    void testCachedRemoteOwnerOverridesZooKeeperOwner() throws Exception {
        cacheLeader(OTHER_SERVER_ID);
        zkClient.updateLeaderAndIsr(
                offsetsTableBucket,
                new LeaderAndIsr(TABLET_SERVER_ID, INITIAL_COORDINATOR_EPOCH),
                ZkVersion.MATCH_ANY_VERSION.getVersion());
        assertCommitResult(Errors.NOT_CONSUMER_GROUP_OWNER);
    }

    @Test
    void testOnlineLeaderAcceptsCommitWithStaleCachedOwner() throws Exception {
        makeKvTableAsLeader(
                OFFSETS_TABLE_ID, SystemTables.CONSUMER_OFFSETS, offsetsTableBucket.getBucket());
        cacheLeader(OTHER_SERVER_ID);
        assertCommitResult(Errors.NONE);
        assertThat(
                        replicaManager
                                .getReplicaOrException(offsetsTableBucket)
                                .getLogTablet()
                                .localLogEndOffset())
                .isPositive();
    }

    @Test
    void testFollowerRejectsCommitWithStaleCachedLocalOwner() throws Exception {
        makeKvTableAsLeader(
                OFFSETS_TABLE_ID, SystemTables.CONSUMER_OFFSETS, offsetsTableBucket.getBucket());
        cacheLeader(TABLET_SERVER_ID);
        Replica replica = replicaManager.getReplicaOrException(offsetsTableBucket);
        replica.makeFollower(
                new NotifyLeaderAndIsrData(
                        PhysicalTablePath.of(SystemTables.CONSUMER_OFFSETS),
                        offsetsTableBucket,
                        Arrays.asList(TABLET_SERVER_ID, OTHER_SERVER_ID),
                        new LeaderAndIsr(
                                OTHER_SERVER_ID,
                                INITIAL_LEADER_EPOCH + 1,
                                Collections.singletonList(OTHER_SERVER_ID),
                                Collections.emptyList(),
                                INITIAL_COORDINATOR_EPOCH,
                                INITIAL_LEADER_EPOCH + 1),
                        TEST_ROUTING_BUCKET_COUNT,
                        0L));
        assertThat(replica.isLeader()).isFalse();
        long logEndOffset = replica.getLogTablet().localLogEndOffset();

        assertCommitResult(Errors.CONSUMER_GROUP_OWNER_LOADING);
        cacheLeader(OTHER_SERVER_ID);
        assertCommitResult(Errors.NOT_CONSUMER_GROUP_OWNER);
        assertThat(replica.getLogTablet().localLogEndOffset()).isEqualTo(logEndOffset);
    }

    private void cacheLeader(@Nullable Integer leaderId) {
        serverMetadataCache.updateTableMetadata(
                new TableMetadata(
                        offsetsTableInfo,
                        Collections.singletonList(
                                new BucketMetadata(
                                        offsetsTableBucket.getBucket(),
                                        leaderId,
                                        INITIAL_LEADER_EPOCH,
                                        Arrays.asList(TABLET_SERVER_ID, OTHER_SERVER_ID)))));
    }

    private void assertCommitResult(Errors expectedError) throws Exception {
        if (expectedError != Errors.NONE) {
            assertThatThrownBy(() -> groupManager.validateGroupId(GROUP_ID))
                    .isInstanceOf(expectedError.exception().getClass());
            return;
        }
        CompletableFuture<Map<TableBucket, Errors>> result =
                groupManager.handleCommitConsumerOffsets(
                        GROUP_ID,
                        groupManager.validateGroupId(GROUP_ID),
                        Collections.singletonMap(
                                SOURCE_BUCKET, new ConsumerOffsetAndMetadata(10L, "")));
        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(10))
                .isEqualTo(Collections.singletonMap(SOURCE_BUCKET, expectedError));
    }
}
