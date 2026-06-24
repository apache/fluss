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

package org.apache.fluss.server.metadata;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropPartitionRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for {@link PartitionNegativeCache} integration with metadata request processing. */
class PartitionNegativeCacheITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    private static CoordinatorGateway coordinatorGateway;

    @BeforeAll
    static void setup() {
        coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
    }

    @Test
    void testNegativeCacheForDeletedPartition() throws Exception {
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        // Create a partitioned table with auto-partition disabled.
        TablePath tablePath = TablePath.of("test_db_neg_cache", "partitioned_table");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(1)
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, false)
                        .build();
        createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);

        // Create a partition and get its ID.
        String partitionName = "p_to_delete";
        long partitionId =
                createPartition(
                        FLUSS_CLUSTER_EXTENSION,
                        tablePath,
                        new PartitionSpec(Collections.singletonMap("b", partitionName)),
                        false);

        PartitionNegativeCache negativeCache =
                FLUSS_CLUSTER_EXTENSION
                        .getCoordinatorServer()
                        .getCoordinatorService()
                        .getPartitionNegativeCache();

        // A stale negative-cache entry must not hide a partition that exists in metadata cache.
        MetadataRequest existingPartitionRequest = new MetadataRequest();
        existingPartitionRequest.addPartitionsId(partitionId);
        existingPartitionRequest
                .addTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        negativeCache.markNonExistent(partitionId);
        retry(
                Duration.ofMinutes(1),
                () -> coordinatorGateway.metadata(existingPartitionRequest).get());
        negativeCache.clear();

        // Drop the partition.
        coordinatorGateway
                .dropPartition(
                        newDropPartitionRequest(
                                tablePath,
                                new PartitionSpec(Collections.singletonMap("b", partitionName)),
                                false))
                .get();

        // Wait until the partition is actually deleted from ZK (metadata cache update).
        retry(
                Duration.ofMinutes(1),
                () -> {
                    // Request metadata with the deleted partition ID - should throw.
                    MetadataRequest request = new MetadataRequest();
                    request.addPartitionsId(partitionId);
                    request.addTablePath()
                            .setDatabaseName(tablePath.getDatabaseName())
                            .setTableName(tablePath.getTableName());
                    assertThatThrownBy(() -> coordinatorGateway.metadata(request).get())
                            .cause()
                            .isInstanceOf(PartitionNotExistException.class);
                    assertThat(negativeCache.isKnownNonExistent(partitionId)).isTrue();
                });

        // At this point, the partition ID should be in the negative cache.
        // Verify the negative cache is populated on the coordinator.
        assertThat(negativeCache.isKnownNonExistent(partitionId)).isTrue();

        // Second request should also fail (served from negative cache, no ZK query).
        MetadataRequest secondRequest = new MetadataRequest();
        secondRequest.addPartitionsId(partitionId);
        secondRequest
                .addTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        assertThatThrownBy(() -> coordinatorGateway.metadata(secondRequest).get())
                .cause()
                .isInstanceOf(PartitionNotExistException.class);

        // Negative cache should still have the entry.
        assertThat(negativeCache.isKnownNonExistent(partitionId)).isTrue();
    }
}
