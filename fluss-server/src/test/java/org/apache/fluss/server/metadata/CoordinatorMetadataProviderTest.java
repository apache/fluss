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

import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.assertj.core.api.Assertions.assertThat;

class CoordinatorMetadataProviderTest {

    @Test
    void testPartialAssignmentDoesNotOverrideRegisteredPartitionCount() {
        CoordinatorContext context = new CoordinatorContext(ZkEpoch.INITIAL_EPOCH);
        TableInfo tableInfo =
                TableInfo.of(
                        DATA1_TABLE_INFO.getTablePath(),
                        DATA1_TABLE_INFO.getTableId(),
                        0,
                        DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                        DATA1_TABLE_INFO.getRemoteDataDir(),
                        0,
                        0);
        long tableId = tableInfo.getTableId();
        PhysicalTablePath path = PhysicalTablePath.of(tableInfo.getTablePath(), "p");
        context.putTableInfo(tableInfo);
        context.putTablePath(tableId, path.getTablePath());
        context.putPartition(
                12,
                path,
                new PartitionInfo(
                        12,
                        ResolvedPartitionSpec.fromPartitionName(tableInfo.getPartitionKeys(), "p"),
                        tableInfo.getRemoteDataDir(),
                        3));
        TablePartition partition = new TablePartition(tableId, 12L);
        context.putPartitionBucketCount(partition, 3);
        context.updateBucketReplicaAssignment(
                new TableBucket(tableId, 12L, 0), Collections.singletonList(1));
        CoordinatorMetadataProvider provider = new CoordinatorMetadataProvider(null, null, context);
        assertThat(provider.getPartitionMetadataFromCache(path)).isEmpty();
        for (int bucket = 1; bucket < 3; bucket++) {
            context.updateBucketReplicaAssignment(
                    new TableBucket(tableId, 12L, bucket), Collections.singletonList(1));
        }
        assertThat(provider.getPartitionMetadataFromCache(path))
                .hasValueSatisfying(
                        metadata -> {
                            assertThat(metadata.getBucketCount()).isEqualTo(3);
                            assertThat(metadata.getBucketMetadataList()).hasSize(3);
                        });
        context.removePartition(partition);
        assertThat(context.getPartitionBucketCount(partition)).isNull();
    }

    @Test
    void testBuildsAuthoritativeBucketMetadataFromSingleLeaderAndIsrState() {
        long tableId = 10L;
        CoordinatorContext context = new CoordinatorContext(ZkEpoch.INITIAL_EPOCH);
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(1, 2, Arrays.asList(1, 2), Collections.emptyList(), 3, 4);
        context.putBucketLeaderAndIsr(new TableBucket(tableId, 0), leaderAndIsr);
        Map<Integer, List<Integer>> assignment = new LinkedHashMap<>();
        assignment.put(0, Arrays.asList(1, 2, 3));
        assignment.put(1, Arrays.asList(2, 3, 4));

        List<BucketMetadata> metadata =
                CoordinatorMetadataProvider.getBucketMetadataFromContext(
                        context, tableId, null, assignment);

        assertThat(metadata)
                .containsExactly(
                        new BucketMetadata(
                                0,
                                leaderAndIsr.leader(),
                                leaderAndIsr.leaderEpoch(),
                                Arrays.asList(1, 2, 3),
                                leaderAndIsr.isr(),
                                leaderAndIsr.bucketEpoch()),
                        new BucketMetadata(
                                1,
                                null,
                                null,
                                Arrays.asList(2, 3, 4),
                                Collections.emptyList(),
                                BucketMetadata.NO_LEADER_ISR_STATE_EPOCH));
    }
}
