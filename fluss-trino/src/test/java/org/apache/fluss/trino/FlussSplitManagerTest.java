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

package org.apache.fluss.trino;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.concurrent.FutureUtils;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.TableNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.fluss.trino.TestingFlussMetadata.metadataAccess;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests batch offset planning against the Admin boundary. */
final class FlussSplitManagerTest {
    private static final TablePath PATH = TablePath.of("Sales", "Users");
    private static final List<Integer> BUCKETS = Arrays.asList(0, 1, 2);
    private static final FlussTableHandle HANDLE =
            new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3, 3, 0);
    private final Admin admin = mock(Admin.class);
    private final FlussSplitManager manager = new FlussSplitManager(metadataAccess(admin));

    @BeforeEach
    void setUp() {
        when(admin.getTableInfo(PATH)).thenReturn(completedFuture(logTable(42, 3, 3, 0)));
        when(admin.listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.EarliestSpec.class)))
                .thenReturn(offsets(5, 8, 12));
        when(admin.listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.LatestSpec.class)))
                .thenReturn(offsets(10, 8, 20));
    }

    @Test
    void testPlansNonEmptyRangesWithTwoBatchCalls() throws Exception {
        List<ConnectorSplit> splits = plan();
        assertThat(splits).hasSize(2);
        assertRange(splits.get(0), 0, 5, 10);
        assertRange(splits.get(1), 2, 12, 20);
        InOrder calls = inOrder(admin);
        calls.verify(admin).getTableInfo(PATH);
        calls.verify(admin).listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.EarliestSpec.class));
        calls.verify(admin).listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.LatestSpec.class));
        calls.verify(admin).getTableInfo(PATH);
        calls.verifyNoMoreInteractions();
    }

    @Test
    void testWaitsForEveryEarliestOffsetBeforeRequestingLatest() throws Exception {
        CompletableFuture<Long> lastBucket = new CompletableFuture<>();
        Map<Integer, CompletableFuture<Long>> starts = new HashMap<>();
        starts.put(0, completedFuture(5L));
        starts.put(1, completedFuture(8L));
        starts.put(2, lastBucket);
        CompletableFuture<Void> requested = new CompletableFuture<>();
        when(admin.listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.EarliestSpec.class)))
                .thenAnswer(
                        invocation -> {
                            requested.complete(null);
                            return new ListOffsetsResult(starts);
                        });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<List<ConnectorSplit>> result = executor.submit(this::plan);
            requested.get(10, TimeUnit.SECONDS);
            verify(admin, never())
                    .listOffsets(eq(PATH), anyCollection(), isA(OffsetSpec.LatestSpec.class));
            assertThat(result.isDone()).isFalse();
            lastBucket.complete(12L);
            assertThat(result.get(10, TimeUnit.SECONDS)).hasSize(2);
        } finally {
            lastBucket.complete(12L);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testEmptyTableHasNoSplits() throws Exception {
        when(admin.listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.LatestSpec.class)))
                .thenReturn(offsets(5, 8, 12));
        assertThat(plan()).isEmpty();
    }

    @Test
    void testRangesAreNotCachedAcrossScans() throws Exception {
        plan();
        when(admin.listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.LatestSpec.class)))
                .thenReturn(offsets(11, 9, 21));
        List<ConnectorSplit> second = plan();
        assertThat(second).hasSize(3);
        assertRange(second.get(0), 0, 5, 11);
        verify(admin, times(2))
                .listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.EarliestSpec.class));
    }

    @Test
    void testRejectsMissingAndInvalidOffsets() {
        when(admin.listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.LatestSpec.class)))
                .thenReturn(offsets(10, 8));
        assertPlanningFailure("bucket 2");
        when(admin.listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.LatestSpec.class)))
                .thenReturn(offsets(4, 8, 20));
        assertPlanningFailure("bucket 0");
        when(admin.listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.EarliestSpec.class)))
                .thenReturn(offsets(-1, 8, 12));
        assertPlanningFailure("bucket 0");
    }

    @Test
    void testPartialFailureAbortsPlanningAndPreservesCause() {
        RuntimeException failure = new RuntimeException("offset RPC failed");
        Map<Integer, CompletableFuture<Long>> starts = new HashMap<>();
        starts.put(0, completedFuture(5L));
        starts.put(1, FutureUtils.completedExceptionally(failure));
        starts.put(2, completedFuture(12L));
        when(admin.listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.EarliestSpec.class)))
                .thenReturn(new ListOffsetsResult(starts));
        assertThatThrownBy(this::plan).isInstanceOf(TrinoException.class).hasCause(failure);
        verify(admin, never())
                .listOffsets(eq(PATH), anyCollection(), isA(OffsetSpec.LatestSpec.class));
    }

    @Test
    void testDoesNotRequestOffsetsForUnsupportedTables() {
        TableDescriptor.Builder descriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(3);
        when(admin.getTableInfo(PATH))
                .thenReturn(
                        completedFuture(
                                tableInfo(
                                        descriptor
                                                .property("table.datalake.enabled", "true")
                                                .build())));
        assertUnsupportedTableType("Lakehouse");
        when(admin.getTableInfo(PATH))
                .thenReturn(
                        completedFuture(
                                tableInfo(
                                        TableDescriptor.builder()
                                                .schema(
                                                        Schema.newBuilder()
                                                                .column("id", DataTypes.INT())
                                                                .primaryKey("id")
                                                                .build())
                                                .distributedBy(3)
                                                .build())));
        assertUnsupportedTableType("primary key");
        when(admin.getTableInfo(PATH))
                .thenReturn(
                        completedFuture(
                                tableInfo(
                                        TableDescriptor.builder()
                                                .schema(
                                                        Schema.newBuilder()
                                                                .column("id", DataTypes.INT())
                                                                .build())
                                                .partitionedBy("id")
                                                .distributedBy(3)
                                                .build())));
        assertUnsupportedTableType("partitioned");
        verify(admin, never())
                .listOffsets(any(TablePath.class), anyCollection(), any(OffsetSpec.class));
    }

    @Test
    void testRejectsChangedIdentityBeforeOffsets() {
        for (TableInfo changed : changedTables()) {
            when(admin.getTableInfo(PATH)).thenReturn(completedFuture(changed));
            assertTableChanged("changed during query planning");
        }
        verify(admin, never())
                .listOffsets(any(TablePath.class), anyCollection(), any(OffsetSpec.class));
    }

    @Test
    void testRejectsChangedIdentityAfterOffsets() {
        for (TableInfo changed : changedTables()) {
            when(admin.getTableInfo(PATH))
                    .thenReturn(completedFuture(logTable(42, 3, 3, 0)), completedFuture(changed));
            assertTableChanged("changed during query planning");
        }
    }

    @Test
    void testReportsTableDroppedDuringOffsetPlanning() {
        when(admin.listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.EarliestSpec.class)))
                .thenThrow(new org.apache.fluss.exception.TableNotExistException("dropped"));
        assertThatThrownBy(this::plan).isInstanceOf(TableNotFoundException.class);
    }

    private List<ConnectorSplit> plan() throws Exception {
        try (ConnectorSplitSource source =
                manager.getSplits(
                        FlussTransactionHandle.INSTANCE,
                        mock(ConnectorSession.class),
                        HANDLE,
                        Collections.emptySet(),
                        Constraint.alwaysTrue())) {
            List<ConnectorSplit> splits =
                    source.getNextBatch(100, DynamicFilterSnapshot.EMPTY).get(10, TimeUnit.SECONDS);
            assertThat(source.isFinished()).isTrue();
            return splits;
        }
    }

    private void assertPlanningFailure(String message) {
        assertThatThrownBy(this::plan)
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        failure ->
                                assertThat(failure.getErrorCode())
                                        .isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()))
                .hasMessageContaining(message);
    }

    private void assertUnsupportedTableType(String message) {
        assertThatThrownBy(this::plan)
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        failure ->
                                assertThat(failure.getErrorCode())
                                        .isEqualTo(UNSUPPORTED_TABLE_TYPE.toErrorCode()))
                .hasMessageContaining(message);
    }

    private void assertTableChanged(String message) {
        assertThatThrownBy(this::plan)
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        failure ->
                                assertThat(failure.getErrorCode())
                                        .isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining(message);
    }

    private static void assertRange(ConnectorSplit split, int bucket, long start, long stop) {
        assertThat(split).isInstanceOf(FlussSplit.class);
        FlussSplit range = (FlussSplit) split;
        assertThat(range.getBucketId()).isEqualTo(bucket);
        assertThat(range.getStartOffset()).isEqualTo(start);
        assertThat(range.getStoppingOffset()).isEqualTo(stop);
    }

    private static ListOffsetsResult offsets(long... offsets) {
        Map<Integer, CompletableFuture<Long>> futures = new HashMap<>();
        for (int bucket = 0; bucket < offsets.length; bucket++) {
            futures.put(bucket, completedFuture(offsets[bucket]));
        }
        return new ListOffsetsResult(futures);
    }

    private static List<TableInfo> changedTables() {
        return Arrays.asList(
                logTable(43, 3, 3, 0),
                logTable(42, 4, 3, 0),
                logTable(42, 3, 4, 0),
                logTable(42, 3, 3, 1));
    }

    private static TableInfo tableInfo(TableDescriptor descriptor) {
        return TableInfo.of(PATH, 42, 3, descriptor, null, 0, 0);
    }

    private static TableInfo logTable(long tableId, int schemaId, int buckets, long epoch) {
        return TableInfo.of(
                PATH,
                tableId,
                schemaId,
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(buckets)
                        .build(),
                null,
                0,
                0,
                epoch);
    }
}
