/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

package org.apache.fluss.client.admin;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.exception.ConsumerGroupOwnerLoadingException;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.server.metadata.SystemTables;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.utils.ExceptionUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests for the consumer group Admin APIs.
 *
 * <p>These tests verify the end-to-end functionality of:
 *
 * <ul>
 *   <li>{@link Admin#alterConsumerGroupOffsets(String, Map)}
 *   <li>{@link Admin#listConsumerGroupOffsets(String)}
 *   <li>{@link Admin#listConsumerGroups()}
 *   <li>{@link Admin#deleteConsumerGroupOffsets(String, java.util.Set)}
 *   <li>{@link Admin#deleteConsumerGroups(java.util.Collection)}
 * </ul>
 */
class ConsumerGroupAdminITCase extends ClientToServerITCaseBase {

    private static final TableBucket BUCKET_0 = new TableBucket(1L, null, 0);
    private static final TableBucket BUCKET_1 = new TableBucket(1L, null, 1);

    @BeforeEach
    void waitForConsumerOffsetsTable() throws Exception {
        ServerNode discoveryNode = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().get(0);
        TabletServerGateway gateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(discoveryNode.id());
        FindConsumerGroupOwnerRequest request =
                new FindConsumerGroupOwnerRequest().setGroupId("consumer-group-admin-setup");

        retry(
                Duration.ofMinutes(1),
                () -> {
                    try {
                        assertThat(
                                        gateway.findConsumerGroupOwner(request)
                                                .get(10, TimeUnit.SECONDS)
                                                .hasOwner())
                                .isTrue();
                        assertThat(
                                        FLUSS_CLUSTER_EXTENSION
                                                .getZooKeeperClient()
                                                .getTable(SystemTables.CONSUMER_OFFSETS))
                                .isPresent();
                    } catch (ExecutionException e) {
                        Throwable cause = ExceptionUtils.stripExecutionException(e);
                        if (cause instanceof ConsumerGroupOwnerNotAvailableException
                                || cause instanceof ConsumerGroupOwnerLoadingException) {
                            throw new AssertionError(cause);
                        }
                        throw e;
                    } catch (TimeoutException e) {
                        throw new AssertionError(e);
                    }
                });

        TableRegistration offsetsTable =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getTable(SystemTables.CONSUMER_OFFSETS)
                        .get();
        assertThat(offsetsTable.bucketCount).isEqualTo(1);
        waitAllReplicasReady(offsetsTable.tableId, offsetsTable.bucketCount);
    }

    @Test
    void testAlterAndListConsumerGroupOffsets() throws Exception {
        String groupId = uniqueGroupId("alter-list");
        Map<TableBucket, OffsetMetadata> offsets = new HashMap<>();
        offsets.put(BUCKET_0, new OffsetMetadata(10L, "checkpoint"));
        offsets.put(BUCKET_1, new OffsetMetadata(20L, ""));
        AlterConsumerGroupOffsetsResult result = admin.alterConsumerGroupOffsets(groupId, offsets);
        result.all().get();
        assertThat(result.bucketResult(BUCKET_0).get()).isNull();
        assertThat(result.bucketResult(BUCKET_1).get()).isNull();

        // List all committed offsets.
        Map<TableBucket, OffsetMetadata> listed = admin.listConsumerGroupOffsets(groupId).get();
        assertThat(listed)
                .containsEntry(BUCKET_0, new OffsetMetadata(10L, "checkpoint"))
                .containsEntry(BUCKET_1, new OffsetMetadata(20L, ""));

        assertThat(admin.listConsumerGroupOffsets(groupId, Collections.emptySet()).get()).isEmpty();

        // List only the first bucket.
        Map<TableBucket, OffsetMetadata> selected =
                admin.listConsumerGroupOffsets(groupId, Collections.singleton(BUCKET_0)).get();
        assertThat(selected).containsOnlyKeys(BUCKET_0);
        assertThat(selected.get(BUCKET_0).getMetadata()).isEqualTo("checkpoint");
    }

    @Test
    void testNegativeOffsetResetIsRejectedAndZeroIsAccepted() throws Exception {
        String groupId = uniqueGroupId("reset-offset");
        OffsetMetadata committed = new OffsetMetadata(42L, "checkpoint");
        admin.alterConsumerGroupOffsets(groupId, Collections.singletonMap(BUCKET_0, committed))
                .all()
                .get();
        assertThat(admin.listConsumerGroupOffsets(groupId).get())
                .containsEntry(BUCKET_0, committed);

        assertThatThrownBy(() -> new OffsetMetadata(-1L, "checkpoint"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(admin.listConsumerGroupOffsets(groupId).get())
                .containsEntry(BUCKET_0, committed);

        OffsetMetadata zero = new OffsetMetadata(0L);
        admin.alterConsumerGroupOffsets(groupId, Collections.singletonMap(BUCKET_0, zero))
                .all()
                .get();
        assertThat(admin.listConsumerGroupOffsets(groupId).get()).containsEntry(BUCKET_0, zero);
    }

    @Test
    void testListConsumerGroups() throws Exception {
        String groupId = uniqueGroupId("list-groups");
        admin.alterConsumerGroupOffsets(
                        groupId, Collections.singletonMap(BUCKET_0, new OffsetMetadata(1L)))
                .all()
                .get();

        assertThat(admin.listConsumerGroups().get()).contains(groupId);
    }

    @Test
    void testAlterOffsetsWithPartitionedBuckets() throws Exception {
        String groupId = uniqueGroupId("partitioned");
        Map<TableBucket, OffsetMetadata> offsets = new HashMap<>();
        offsets.put(new TableBucket(2L, 10L, 0), new OffsetMetadata(200L, "p10"));
        offsets.put(new TableBucket(2L, 20L, 1), new OffsetMetadata(301L, "p20"));
        admin.alterConsumerGroupOffsets(groupId, offsets).all().get();

        Map<TableBucket, OffsetMetadata> listed = admin.listConsumerGroupOffsets(groupId).get();
        assertThat(listed)
                .containsEntry(new TableBucket(2L, 10L, 0), new OffsetMetadata(200L, "p10"))
                .containsEntry(new TableBucket(2L, 20L, 1), new OffsetMetadata(301L, "p20"));
    }

    @Test
    void testDeleteConsumerGroupOffsets() throws Exception {
        String groupId = uniqueGroupId("delete-offsets");
        Map<TableBucket, OffsetMetadata> offsets = new HashMap<>();
        offsets.put(BUCKET_0, new OffsetMetadata(10L));
        offsets.put(BUCKET_1, new OffsetMetadata(20L));
        admin.alterConsumerGroupOffsets(groupId, offsets).all().get();

        DeleteConsumerGroupOffsetsResult result =
                admin.deleteConsumerGroupOffsets(groupId, Collections.singleton(BUCKET_0));
        result.all().get();

        Map<TableBucket, OffsetMetadata> listed = admin.listConsumerGroupOffsets(groupId).get();
        assertThat(listed).containsOnlyKeys(BUCKET_1);
        assertThat(listed.get(BUCKET_1).getOffset()).isEqualTo(20L);
    }

    @Test
    void testDeleteConsumerGroups() throws Exception {
        String groupId = uniqueGroupId("delete-groups");
        admin.alterConsumerGroupOffsets(
                        groupId, Collections.singletonMap(BUCKET_0, new OffsetMetadata(10L)))
                .all()
                .get();
        assertThat(admin.listConsumerGroupOffsets(groupId).get()).isNotEmpty();

        admin.deleteConsumerGroups(Collections.singleton(groupId)).all().get();

        // A deleted group lists no offsets and no longer shows up in group listings.
        assertThat(admin.listConsumerGroupOffsets(groupId).get()).isEmpty();
        assertThat(admin.listConsumerGroups().get()).doesNotContain(groupId);
    }

    @Test
    void testDeleteNonExistentGroupIsIdempotent() {
        String groupId = uniqueGroupId("missing");
        DeleteConsumerGroupsResult result =
                admin.deleteConsumerGroups(Collections.singleton(groupId));
        // Deleting an unknown group succeeds; the operation is idempotent.
        assertThat(result.deletedGroup(groupId)).succeedsWithin(Duration.ofSeconds(30));
        assertThat(result.all()).succeedsWithin(Duration.ofSeconds(30));
    }

    private static String uniqueGroupId(String prefix) {
        return prefix + "-group-" + System.nanoTime();
    }
}
