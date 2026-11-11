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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.tiering.FlussTableLakeSnapshotCommitter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.rpc.messages.PbUnionReadContext;
import org.apache.fluss.rpc.messages.PbUnionReadLogRange;
import org.apache.fluss.rpc.util.PredicateMessageUtils;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.apache.fluss.testutils.DataTestUtils.row;
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
            PbUnionReadContext all = UnionReadBoundaryPlanner.captureReadContext(admin, path);
            PbUnionReadContext scoped =
                    UnionReadBoundaryPlanner.captureReadContext(
                            admin,
                            path,
                            new PartitionSpec(Collections.singletonMap("region", "US")));
            assertThat(all.getLogRangesList()).hasSize(4);
            assertThat(scoped.hasLake()).isFalse();
            assertThat(scoped.getTableId()).isEqualTo(tableId);
            assertThat(scoped.getLogRangesList()).hasSize(2);
            assertThat(
                            PredicateMessageUtils.toPredicate(
                                    scoped.getPartitionScope(),
                                    admin.getTableInfo(path).get().getRowType()))
                    .isEqualTo(
                            PredicateBuilder.partition(
                                    Collections.singletonMap("region", "US"),
                                    admin.getTableInfo(path).get().getRowType()));
            for (PbUnionReadLogRange range : scoped.getLogRangesList()) {
                assertThat(range.getPartitionValuesList()).containsExactly("US");
                assertThat(range.getStartOffset()).isEqualTo(range.getEarliestOffset());
                assertThat(range.getStopOffset()).isZero();
            }
        } finally {
            admin.dropTable(path, false).get();
        }
    }

    @Test
    void testCaptureReadableSnapshotAndLogTail() throws Exception {
        TablePath path = TablePath.of("union_read_boundary_lake", "events");
        long tableId =
                createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                                .distributedBy(1)
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                                .build(),
                        false);
        TableBucket bucket = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(bucket);
        try (Table table = conn.getTable(path);
                FlussTableLakeSnapshotCommitter committer =
                        new FlussTableLakeSnapshotCommitter(clientConf)) {
            AppendWriter writer = table.newAppend().createWriter();
            writer.append(row(1)).get();
            writer.append(row(2)).get();
            PbUnionReadContext untiered = UnionReadBoundaryPlanner.captureReadContext(admin, path);
            assertThat(untiered.getLake().hasSnapshotId()).isFalse();
            assertThat(untiered.getLogRangeAt(0).getBaseline()).isZero();
            assertThat(untiered.getLogRangeAt(0).getStopOffset()).isEqualTo(2);

            committer.open();
            Map<TableBucket, Long> tiered = Collections.singletonMap(bucket, 2L);
            String tieredPath = committer.prepareLakeSnapshot(tableId, path, tiered);
            String readablePath =
                    committer.prepareLakeSnapshot(
                            tableId, path, Collections.singletonMap(bucket, 1L));
            committer.commit(
                    tableId, 42L, tieredPath, readablePath, tiered, Collections.emptyMap(), null);
            PbUnionReadContext captured = UnionReadBoundaryPlanner.captureReadContext(admin, path);
            assertThat(captured.getLake().getSnapshotId()).isEqualTo(42);
            assertThat(captured.getLogRangeAt(0).getBaseline()).isEqualTo(1);
            assertThat(captured.getLogRangeAt(0).getStartOffset()).isEqualTo(1);
            assertThat(captured.getLogRangeAt(0).getStopOffset()).isEqualTo(2);
            writer.append(row(3)).get();
            assertThat(
                            UnionReadBoundaryPlanner.captureReadContext(admin, path)
                                    .getLogRangeAt(0)
                                    .getStopOffset())
                    .isEqualTo(3);
        } finally {
            admin.dropTable(path, false).get();
        }
    }
}
