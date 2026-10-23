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

import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.tablet.consumer.group.ConsumerOffsetSchema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
        // Keep the tests independent of system-table creation and assignment timing.
        long tableId =
                createTable(
                        TablePath.of("sys", "consumer_offsets"),
                        TableDescriptor.builder()
                                .schema(ConsumerOffsetSchema.getTableSchema())
                                .logFormat(LogFormat.COMPACTED)
                                .distributedBy(1, ConsumerOffsetSchema.getBucketKeyColumn())
                                .build(),
                        true);
        waitAllReplicasReady(tableId, 1);
    }

    @Test
    void testAlterAndListConsumerGroupOffsets() throws Exception {
        String groupId = uniqueGroupId("alter-list");
        Map<TableBucket, OffsetMetadata> offsets = new HashMap<>();
        offsets.put(BUCKET_0, new OffsetMetadata(10L, "checkpoint"));
        offsets.put(BUCKET_1, new OffsetMetadata(20L, ""));
        AlterConsumerGroupOffsetsResult result = admin.alterConsumerGroupOffsets(groupId, offsets);
        result.all().get();
        assertThat(result.bucketResult(BUCKET_0).get().isSuccess()).isTrue();
        assertThat(result.bucketResult(BUCKET_0).get().newOffset()).isEqualTo(10L);
        assertThat(result.bucketResult(BUCKET_1).get().isSuccess()).isTrue();

        // List all committed offsets.
        Map<TableBucket, OffsetMetadata> listed = admin.listConsumerGroupOffsets(groupId).get();
        assertThat(listed)
                .containsEntry(BUCKET_0, new OffsetMetadata(10L, "checkpoint"))
                .containsEntry(BUCKET_1, new OffsetMetadata(20L, ""));

        // List only the first bucket.
        Map<TableBucket, OffsetMetadata> selected =
                admin.listConsumerGroupOffsets(groupId, Collections.singleton(BUCKET_0)).get();
        assertThat(selected).containsOnlyKeys(BUCKET_0);
        assertThat(selected.get(BUCKET_0).getMetadata()).isEqualTo("checkpoint");
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
