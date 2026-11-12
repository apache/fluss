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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.GroupId;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.metadata.SystemTables;
import org.apache.fluss.server.metadata.TabletServerMetadataProvider;
import org.apache.fluss.server.replica.ReplicaTestBase;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/** Tests consumer-offset commit failures using real replicas. */
class ConsumerGroupManagerCommitFailureTest extends ReplicaTestBase {

    @Test
    void testWriteCompletionExceptionReturnsGroupOwnerError() {
        TableBucket bucket = new TableBucket(1L, 0);
        Map<TableBucket, Errors> results = new HashMap<>();
        results.put(bucket, Errors.NONE);
        PutKvResultForBucket writeResult =
                new PutKvResultForBucket(bucket, ApiError.NONE) {
                    @Override
                    public ApiError getError() {
                        throw Errors.NOT_LEADER_OR_FOLLOWER.exception();
                    }
                };

        assertThat(
                        ConsumerGroupManager.handleWriteCompletion(
                                GroupId.of("group"),
                                results,
                                Collections.singletonList(writeResult)))
                .containsEntry(bucket, Errors.NOT_CONSUMER_GROUP_OWNER);
    }

    @Test
    void testSynchronousWriteFailurePreservesValidationErrors() throws Exception {
        long offsetsTableId = 100L;
        registerTableInZkClient(
                SystemTables.CONSUMER_OFFSETS,
                ConsumerOffsetSchema.getTableSchema(),
                offsetsTableId,
                Collections.singletonList(ConsumerOffsetSchema.getBucketKeyColumn()),
                Collections.emptyMap());
        conf.set(ConfigOptions.OFFSET_METADATA_MAX_BYTES, new MemorySize(5));

        try (LakeCatalogDynamicLoader lakeCatalogLoader =
                new LakeCatalogDynamicLoader(conf, null, true)) {
            MetadataManager metadataManager =
                    new MetadataManager(zkClient, conf, lakeCatalogLoader);
            ConsumerGroupManager groupManager =
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
            GroupId groupId = GroupId.of("group-with-locked-disk");
            int bucketId = new ConsumerOffsetSchema().bucketFor(groupId, TEST_ROUTING_BUCKET_COUNT);
            makeKvTableAsLeader(offsetsTableId, SystemTables.CONSUMER_OFFSETS, bucketId);

            conf.set(ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO, 0.85);
            conf.set(ConfigOptions.SERVER_DATA_DISK_WRITE_RECOVER_RATIO, 0.80);
            localDiskManager.reconfigure(conf);
            replicaManager.getDiskUsageMonitor().update(0.99);
            assertThat(replicaManager.isDiskWriteLocked()).isTrue();

            TableBucket validBucket = new TableBucket(1L, 0);
            TableBucket invalidBucket = new TableBucket(1L, 1);
            Map<TableBucket, ConsumerOffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(validBucket, new ConsumerOffsetAndMetadata(10L, "valid"));
            offsets.put(invalidBucket, new ConsumerOffsetAndMetadata(20L, "too-large"));
            groupManager.ensureTableAsync().join();
            Map<TableBucket, Errors> result =
                    groupManager
                            .handleCommitConsumerOffsets(
                                    groupId, groupManager.validateGroupId(groupId), offsets)
                            .get();

            assertThat(result)
                    .containsOnly(
                            entry(validBucket, Errors.DISK_WRITE_LOCKED),
                            entry(invalidBucket, Errors.CONSUMER_OFFSET_METADATA_TOO_LARGE));
        }
    }
}
