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
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.metadata.UnionReadContextEncoderTest.partition;
import static org.apache.fluss.client.metadata.UnionReadContextEncoderTest.table;
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

/** Tests capture sequencing and metadata races, independently of JSON encoding details. */
class UnionReadBoundaryPlannerTest {

    @Test
    void testScopedCaptureUsesSnapshotBeforeOffsetsAndFrozenSchema() throws Exception {
        Admin admin = mock(Admin.class);
        TableInfo table = table(true);
        TableInfo evolved = table(true, 7, 3);
        when(admin.getTableInfo(table.getTablePath()))
                .thenReturn(
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

        CompletableFuture<byte[]> result =
                UnionReadBoundaryPlanner.prepare(admin, table.getTablePath(), scope);
        verify(admin, never()).listPartitionInfos(any(), any(PartitionSpec.class));
        snapshot.complete(
                new LakeSnapshot(42, Collections.singletonMap(new TableBucket(7, 9L, 0), 12L)));
        JsonNode context = new ObjectMapper().readTree(result.get());
        assertThat(context.get("schema_id").intValue()).isEqualTo(2);
        assertThat(context.at("/partition_filter/region").asText()).isEqualTo("US");
        assertThat(context.get("log_ranges")).hasSize(1);
        assertThat(context.at("/log_ranges/0/start_offset").longValue()).isEqualTo(12);
        assertThat(context.at("/log_ranges/0/stop_offset").longValue()).isEqualTo(20);
        InOrder order = inOrder(admin);
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
            JsonNode context =
                    new ObjectMapper()
                            .readTree(
                                    UnionReadBoundaryPlanner.prepare(admin, table.getTablePath())
                                            .get());
            assertThat(context.at("/log_ranges/0/start_offset").longValue()).isEqualTo(8);
            assertThat(context.at("/log_ranges/0/baseline/kind").asText()).isEqualTo("none");
            if (!lakeEnabled) {
                verify(admin, never()).getReadableLakeSnapshot(any());
            }
            TableInfo recreated = table(false, 8, 2);
            when(admin.getTableInfo(table.getTablePath()))
                    .thenReturn(
                            CompletableFuture.completedFuture(table),
                            CompletableFuture.completedFuture(recreated));
            assertThatThrownBy(
                            () ->
                                    UnionReadBoundaryPlanner.prepare(admin, table.getTablePath())
                                            .join())
                    .hasRootCauseMessage("Table identity changed during boundary capture.");
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
                CompletableFuture<byte[]> result =
                        UnionReadBoundaryPlanner.prepare(admin, table.getTablePath());
                if (recheck == 0 || (!lookupFails && recheck == 1)) {
                    JsonNode context = new ObjectMapper().readTree(result.get());
                    assertThat(context.get("log_ranges")).hasSize(recheck == 0 ? 0 : 1);
                } else {
                    assertThatThrownBy(result::join)
                            .hasRootCauseMessage(lookupFails ? "lookup failed" : "recheck failed");
                }
            }
        }
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
