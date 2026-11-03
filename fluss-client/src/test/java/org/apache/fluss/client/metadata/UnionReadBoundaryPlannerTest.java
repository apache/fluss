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

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests capture sequencing and metadata races, through the structured boundary API. */
class UnionReadBoundaryPlannerTest {

    @Test
    void testScopedCaptureUsesSnapshotBeforeOffsetsAndFrozenSchema() throws Exception {
        Admin admin = mock(Admin.class);
        TableInfo table = table(true);
        TableInfo evolved = table(true, 7, 3);
        when(admin.getTableInfo(table.getTablePath()))
                .thenReturn(
                        CompletableFuture.completedFuture(table),
                        CompletableFuture.completedFuture(table),
                        CompletableFuture.completedFuture(evolved));
        PartitionInfo partition = partition(9, "US", 1);
        PartitionSpec scope = new PartitionSpec(Collections.singletonMap("region", "US"));
        when(admin.listPartitionInfos(table.getTablePath(), scope))
                .thenReturn(
                        CompletableFuture.completedFuture(Collections.singletonList(partition)));
        CompletableFuture<LakeSnapshot> snapshot = new CompletableFuture<>();
        when(admin.getReadableLakeSnapshot(table.getTablePath())).thenReturn(snapshot);
        when(admin.listOffsets(
                        eq(table.getTablePath()),
                        eq(partition.getPartitionName()),
                        eq(Collections.singletonList(0)),
                        isA(OffsetSpec.EarliestSpec.class)))
                .thenReturn(offsets(1, 8));
        when(admin.listOffsets(
                        eq(table.getTablePath()),
                        eq(partition.getPartitionName()),
                        eq(Collections.singletonList(0)),
                        isA(OffsetSpec.LatestSpec.class)))
                .thenReturn(offsets(1, 20));

        InOrder order = inOrder(admin);
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(
                            () ->
                                    UnionReadBoundaryPlanner.captureReadContext(
                                            admin, table.getTablePath(), scope))
                    .isInstanceOf(InterruptedException.class);
        } finally {
            Thread.interrupted();
        }
        order.verify(admin).getTableInfo(table.getTablePath());
        order.verify(admin).getReadableLakeSnapshot(table.getTablePath());

        snapshot.complete(
                new LakeSnapshot(42, Collections.singletonMap(new TableBucket(7, 9L, 0), 12L)));
        UnionReadContext context =
                UnionReadBoundaryPlanner.captureReadContext(admin, table.getTablePath(), scope);
        assertThat(context.getSchemaId()).isEqualTo(2);
        assertThat(context.getPartitionFilter().get("region")).isEqualTo("US");
        assertThat(context.getLogRanges()).hasSize(1);
        assertThat(context.getLogRanges().get(0).getStartOffset()).isEqualTo(12);
        assertThat(context.getLogRanges().get(0).getStopOffset()).isEqualTo(20);
        table.getProperties().setString("table.datalake.table-name", "changed");
        assertThat(context.getPartitionFilter()).containsEntry("region", "US");
        assertThat(context.getLakeTablePath()).contains(TablePath.of("lake_db", "orders_lake"));
        order.verify(admin).getTableInfo(table.getTablePath());
        order.verify(admin).getReadableLakeSnapshot(table.getTablePath());
        order.verify(admin).listPartitionInfos(table.getTablePath(), scope);
        order.verify(admin)
                .listOffsets(
                        eq(table.getTablePath()),
                        eq(partition.getPartitionName()),
                        anyList(),
                        isA(OffsetSpec.EarliestSpec.class));
        order.verify(admin)
                .listOffsets(
                        eq(table.getTablePath()),
                        eq(partition.getPartitionName()),
                        anyList(),
                        isA(OffsetSpec.LatestSpec.class));
        order.verify(admin).listPartitionInfos(table.getTablePath(), scope);
        order.verify(admin).getTableInfo(table.getTablePath());

        assertThatThrownBy(
                        () ->
                                UnionReadBoundaryPlanner.captureReadContext(
                                        admin,
                                        table.getTablePath(),
                                        new PartitionSpec(Collections.singletonMap("amount", "1"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partition keys");
    }

    @Test
    void testNoLakeBaselineAndRecreatedTable() throws Exception {
        for (boolean lakeEnabled : new boolean[] {false, true}) {
            Admin admin = mock(Admin.class);
            TableInfo table = table(false);
            table.getProperties()
                    .setString("table.datalake.enabled", Boolean.toString(lakeEnabled));
            when(admin.getTableInfo(table.getTablePath()))
                    .thenReturn(CompletableFuture.completedFuture(table));
            when(admin.getReadableLakeSnapshot(table.getTablePath()))
                    .thenReturn(failed(new LakeTableSnapshotNotExistException("not tiered yet")));
            when(admin.listOffsets(
                            eq(table.getTablePath()),
                            anyList(),
                            isA(OffsetSpec.EarliestSpec.class)))
                    .thenReturn(offsets(2, 8));
            when(admin.listOffsets(
                            eq(table.getTablePath()), anyList(), isA(OffsetSpec.LatestSpec.class)))
                    .thenReturn(offsets(2, 20));
            UnionReadContext context =
                    UnionReadBoundaryPlanner.captureReadContext(admin, table.getTablePath());
            assertThat(context.getLogRanges().get(0).getStartOffset()).isEqualTo(8);
            assertThat(context.getLogRanges().get(0).getLakeSnapshotOffset()).isEmpty();
            if (lakeEnabled) {
                assertThat(context.getLakeSnapshotId()).isEmpty();
            } else {
                assertThat(context.getLakeFormat()).isEmpty();
                verify(admin, never()).getReadableLakeSnapshot(any());
            }
            TableInfo recreated = table(false, 8, 2);
            when(admin.getTableInfo(table.getTablePath()))
                    .thenReturn(
                            CompletableFuture.completedFuture(table),
                            CompletableFuture.completedFuture(recreated));
            assertThatThrownBy(
                            () ->
                                    UnionReadBoundaryPlanner.captureReadContext(
                                            admin, table.getTablePath()))
                    .hasMessage("Table identity changed during boundary capture.");
        }
    }

    @Test
    void testPartitionRecheckSkipsOnlyConfirmedRemoval() throws Exception {
        // Both successful and failed lookups may have crossed a same-name recreation.
        for (boolean lookupFails : new boolean[] {false, true}) {
            for (int recheck : new int[] {0, 1, 2}) {
                Admin admin = mock(Admin.class);
                TableInfo table = table(true);
                table.getProperties().setString("table.datalake.enabled", "false");
                when(admin.getTableInfo(table.getTablePath()))
                        .thenReturn(CompletableFuture.completedFuture(table));
                PartitionInfo original = partition(9, "US", 1);
                CompletableFuture<java.util.List<PartitionInfo>> live =
                        recheck == 2
                                ? failed(new IllegalStateException("recheck failed"))
                                : CompletableFuture.completedFuture(
                                        Collections.singletonList(
                                                recheck == 0 ? partition(10, "US", 1) : original));
                when(admin.listPartitionInfos(eq(table.getTablePath()), any(PartitionSpec.class)))
                        .thenReturn(
                                CompletableFuture.completedFuture(
                                        Collections.singletonList(original)),
                                live);
                when(admin.listOffsets(
                                eq(table.getTablePath()),
                                eq(original.getPartitionName()),
                                anyList(),
                                isA(OffsetSpec.EarliestSpec.class)))
                        .thenAnswer(
                                call -> {
                                    if (lookupFails) {
                                        throw new IllegalStateException("lookup failed");
                                    }
                                    return offsets(1, 8);
                                });
                when(admin.listOffsets(
                                eq(table.getTablePath()),
                                eq(original.getPartitionName()),
                                anyList(),
                                isA(OffsetSpec.LatestSpec.class)))
                        .thenReturn(offsets(1, 20));
                if (recheck == 0 || (!lookupFails && recheck == 1)) {
                    UnionReadContext context =
                            UnionReadBoundaryPlanner.captureReadContext(
                                    admin, table.getTablePath());
                    assertThat(context.getLogRanges()).hasSize(recheck == 0 ? 0 : 1);
                } else {
                    assertThatThrownBy(
                                    () ->
                                            UnionReadBoundaryPlanner.captureReadContext(
                                                    admin, table.getTablePath()))
                            .hasStackTraceContaining(
                                    lookupFails ? "lookup failed" : "recheck failed");
                }
            }
        }
    }

    static TableInfo table(boolean partitioned) {
        return table(partitioned, 7, 2);
    }

    static TableInfo table(boolean partitioned, long tableId, int schemaId) {
        Map<String, String> properties = new HashMap<>();
        properties.put("table.datalake.enabled", "true");
        properties.put("table.datalake.format", "paimon");
        properties.put("table.datalake.database-name", "lake_db");
        properties.put("table.datalake.table-name", "orders_lake");
        properties.put("table.datalake.paimon.s3.secret-key", "SECRET");
        properties.put("table.datalake.paimon.warehouse", "s3://private/warehouse");
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("region", DataTypes.STRING())
                        .column("amount", DataTypes.BIGINT());
        if (schemaId > 2) {
            schema.column("added", DataTypes.STRING());
        }
        if (partitioned) {
            schema.primaryKey("id", "region");
        }
        return new TableInfo(
                TablePath.of("fluss", "orders"),
                tableId,
                schemaId,
                schema.build(),
                partitioned ? new ArrayList<>(Collections.singletonList("id")) : new ArrayList<>(),
                partitioned
                        ? new ArrayList<>(Collections.singletonList("region"))
                        : new ArrayList<>(),
                2,
                Configuration.fromMap(properties),
                new Configuration(),
                null,
                null,
                0,
                0);
    }

    static PartitionInfo partition(long id, String value, int bucketCount) {
        return new PartitionInfo(
                id, ResolvedPartitionSpec.fromPartitionValue("region", value), null, bucketCount);
    }

    private static ListOffsetsResult offsets(int buckets, long offset) {
        Map<Integer, CompletableFuture<Long>> offsets = new HashMap<>();
        for (int bucket = 0; bucket < buckets; bucket++) {
            offsets.put(bucket, CompletableFuture.completedFuture(offset));
        }
        return new ListOffsetsResult(offsets);
    }

    private static <T> CompletableFuture<T> failed(Throwable error) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(error);
        return future;
    }
}
