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

package org.apache.fluss.client.metadata;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises the public capture entry against the ordinary Java client test cluster. */
class UnionReadBoundaryPlannerITCase extends ClientToServerITCaseBase {

    @Test
    void testCapturePartitionScope() throws Exception {
        TablePath path = TablePath.of("union_read_boundary", "orders");
        long tableId =
                createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .column("region", DataTypes.STRING())
                                                .primaryKey("id", "region")
                                                .build())
                                .distributedBy(2, "id")
                                .partitionedBy("region")
                                .build(),
                        false);
        try {
            for (String region : Arrays.asList("US", "EU")) {
                admin.createPartition(
                                path,
                                new PartitionSpec(Collections.singletonMap("region", region)),
                                false)
                        .get();
            }
            for (PartitionInfo partition : admin.listPartitionInfos(path).get()) {
                for (int bucket = 0; bucket < partition.getBucketCount(); bucket++) {
                    FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(
                            new TableBucket(tableId, partition.getPartitionId(), bucket));
                }
            }
            UnionReadContext all = UnionReadBoundaryPlanner.captureReadContext(admin, path);
            UnionReadContext scoped =
                    UnionReadBoundaryPlanner.captureReadContext(
                            admin,
                            path,
                            new PartitionSpec(Collections.singletonMap("region", "US")));
            assertThat(all.getLogRanges()).hasSize(4);
            assertThat(scoped.getLakeFormat()).isEmpty();
            assertThat(scoped.getTableId()).isEqualTo(tableId);
            assertThat(scoped.getLogRanges()).hasSize(2);
            assertThat(scoped.getPartitionFilter().get("region")).isEqualTo("US");
            for (UnionReadContext.LogRange range : scoped.getLogRanges()) {
                assertThat(range.getPartitionValues().get("region")).isEqualTo("US");
                assertThat(range.getStartOffset()).isEqualTo(range.getEarliestOffset());
                assertThat(range.getStopOffset()).isZero();
            }
        } finally {
            admin.dropTable(path, false).get();
        }
    }
}
