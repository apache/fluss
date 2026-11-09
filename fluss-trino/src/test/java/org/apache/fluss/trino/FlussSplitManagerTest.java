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

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.concurrent.FutureUtils;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.fluss.trino.FlussErrorCode.FLUSS_METADATA_CHANGED;
import static org.apache.fluss.trino.FlussErrorCode.FLUSS_SPLIT_ERROR;
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
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/** Tests batch offset planning against the Admin boundary. */
final class FlussSplitManagerTest {
    private static final TablePath PATH = TablePath.of("Sales", "Users");
    private static final List<Integer> BUCKETS = Arrays.asList(0, 1, 2);
    private static final FlussTableHandle HANDLE =
            new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3, TupleDomain.all());
    private final Admin admin = mock(Admin.class);
    private final FlussMetadataAccess access = metadataAccess(admin);
    private final FlussSplitManager manager =
            new FlussSplitManager(access, new FlussSplitPlanner(access));

    @BeforeEach
    void setUp() {
        when(admin.getTableInfo(PATH)).thenReturn(completedFuture(logTable(42, 3, 3, 0)));
        when(admin.listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.EarliestSpec.class)))
                .thenReturn(offsets(5, 8, 12));
        when(admin.listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.LatestSpec.class)))
                .thenReturn(offsets(10, 8, 20));
    }

    @AfterEach
    void verifyNoBucketDiscovery() {
        verify(admin, never()).describeBuckets(any(TablePath.class));
        verify(admin, never()).describeBuckets(any(TablePath.class), any(PartitionSpec.class));
    }

    @Test
    void testPrimaryKeyPlanningDoesNotDescribeBuckets() throws Exception {
        when(admin.getTableInfo(PATH)).thenReturn(completedFuture(primaryKeyTable()));
        assertThat(plan()).hasSize(3);
        verify(admin, never()).listPartitionInfos(any(TablePath.class));
        verify(admin, never()).listPartitionInfos(any(TablePath.class), any(PartitionSpec.class));
    }

    @Test
    void testNoneConstraintSkipsAllMetadataAndScanPlanning() throws Exception {
        assertThat(planWithHandle(HANDLE.withConstraint(TupleDomain.none()))).isEmpty();
        verifyNoInteractions(admin);
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
        assertPlanningFailure("bucketId=2");
        when(admin.listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.LatestSpec.class)))
                .thenReturn(offsets(4, 8, 20));
        assertThatThrownBy(this::plan)
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        failure ->
                                assertThat(failure.getErrorCode())
                                        .isEqualTo(FLUSS_SPLIT_ERROR.toErrorCode()))
                .hasMessageContaining("bucketId=0");
        when(admin.listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.EarliestSpec.class)))
                .thenReturn(offsets(-1, 8, 12));
        assertPlanningFailure("bucketId=0");
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
        verify(admin, never())
                .listOffsets(any(TablePath.class), anyCollection(), any(OffsetSpec.class));
    }

    @Test
    void testPlansEveryPrimaryKeyBucketWithoutOffsetsOrStatistics() throws Exception {
        when(admin.getTableInfo(PATH)).thenReturn(completedFuture(primaryKeyTable()));
        List<ConnectorSplit> splits = plan();
        assertThat(splits).hasSize(3);
        for (int bucket = 0; bucket < 3; bucket++) {
            FlussSplit split = (FlussSplit) splits.get(bucket);
            assertThat(split.getScanType()).isEqualTo(FlussScanType.KV);
            assertThat(split.getBucket().getBucketId()).isEqualTo(bucket);
            assertThat(split.getLogRange()).isEmpty();
        }
        InOrder calls = inOrder(admin);
        calls.verify(admin).getTableInfo(PATH);
        calls.verifyNoMoreInteractions();
    }

    @Test
    void testRejectsChangedIdentityBeforeOffsets() {
        for (TableInfo changed : changedTables()) {
            when(admin.getTableInfo(PATH)).thenReturn(completedFuture(changed));
            assertTableChanged("changed");
        }
        verify(admin, never())
                .listOffsets(any(TablePath.class), anyCollection(), any(OffsetSpec.class));
    }

    @Test
    void testPartitionedLogPlanningGroupsOffsetsByPartition() throws Exception {
        givenPartitionedLogTable();
        List<ConnectorSplit> splits = plan();

        assertThat(splits).hasSize(2);
        assertRange(splits.get(0), 0, 2, 4);
        assertRange(splits.get(1), 0, 8, 11);

        assertThat(((FlussSplit) splits.get(0)).getBucket().getPartitionId()).contains(10L);
        assertThat(((FlussSplit) splits.get(1)).getBucket().getPartitionId()).contains(20L);

        verify(admin, never()).listOffsets(eq(PATH), anyCollection(), any(OffsetSpec.class));
    }

    @Test
    void testPartitionDroppedBeforeOffsetLookup() throws Exception {
        givenPartitionedLogTable();

        when(admin.listOffsets(
                        eq(PATH),
                        eq("20"),
                        eq(Collections.singletonList(0)),
                        isA(OffsetSpec.EarliestSpec.class)))
                .thenThrow(new PartitionNotExistException("Partition expired"));

        List<ConnectorSplit> splits = plan();

        assertThat(splits).hasSize(1);
        assertRange(splits.get(0), 0, 2, 4);
        assertThat(((FlussSplit) splits.get(0)).getBucket().getPartitionId()).contains(10L);
        verify(admin, never())
                .listOffsets(eq(PATH), eq("20"), anyCollection(), isA(OffsetSpec.LatestSpec.class));
    }

    @Test
    void testPartitionDroppedDuringOffsetLookup() throws Exception {
        givenPartitionedLogTable();

        when(admin.listOffsets(
                        eq(PATH),
                        eq("20"),
                        eq(Collections.singletonList(0)),
                        isA(OffsetSpec.EarliestSpec.class)))
                .thenReturn(failedOffsets(new PartitionNotExistException("Partition expired")));

        List<ConnectorSplit> splits = plan();

        assertThat(splits).hasSize(1);
        assertRange(splits.get(0), 0, 2, 4);
        assertThat(((FlussSplit) splits.get(0)).getBucket().getPartitionId()).contains(10L);
    }

    @Test
    void testPartitionDroppedDuringLatestOffsetLookup() throws Exception {
        givenPartitionedLogTable();
        when(admin.listOffsets(
                        eq(PATH),
                        eq("20"),
                        eq(Collections.singletonList(0)),
                        isA(OffsetSpec.LatestSpec.class)))
                .thenReturn(failedOffsets(new PartitionNotExistException("Partition expired")));
        List<ConnectorSplit> splits = plan();
        assertThat(splits).hasSize(1);
        assertRange(splits.get(0), 0, 2, 4);
        assertThat(((FlussSplit) splits.get(0)).getBucket().getPartitionId()).contains(10L);
        verify(admin).listPartitionInfos(PATH);
    }

    @Test
    void testMissingPartitionOffsetIsNotTreatedAsExpiration() {
        givenPartitionedLogTable();
        when(admin.listOffsets(
                        eq(PATH),
                        eq("10"),
                        eq(Arrays.asList(0, 1)),
                        isA(OffsetSpec.EarliestSpec.class)))
                .thenReturn(offsets(2));
        assertPlanningFailure("bucketId=1");
    }

    @Test
    void testPartitionDiscoveryOccursOnceDuringPlanning() throws Exception {
        givenPartitionedLogTable();
        when(admin.listPartitionInfos(PATH))
                .thenReturn(
                        completedFuture(
                                Arrays.asList(partition(10L, "10", 2), partition(20L, "20", 1))),
                        completedFuture(Collections.singletonList(partition(30L, "10", 4))));
        List<ConnectorSplit> splits = plan();
        assertThat(splits).hasSize(2);
        assertThat(((FlussSplit) splits.get(0)).getBucket().getPartitionId()).contains(10L);
        assertThat(((FlussSplit) splits.get(1)).getBucket().getPartitionId()).contains(20L);
        verify(admin).listPartitionInfos(PATH);
    }

    @Test
    void testPartitionedOffsetLookupFailure() {
        givenPartitionedLogTable();

        RuntimeException failure = new RuntimeException("Offset RPC failed");

        when(admin.listOffsets(
                        eq(PATH),
                        eq("20"),
                        eq(Collections.singletonList(0)),
                        isA(OffsetSpec.EarliestSpec.class)))
                .thenReturn(failedOffsets(failure));

        assertThatThrownBy(this::plan)
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        exception ->
                                assertThat(exception.getErrorCode())
                                        .isEqualTo(FLUSS_SPLIT_ERROR.toErrorCode()))
                .hasCause(failure);

        verify(admin, never())
                .listOffsets(
                        eq(PATH),
                        eq("20"),
                        eq(Collections.singletonList(0)),
                        isA(OffsetSpec.LatestSpec.class));
    }

    @Test
    void testPartitionedPrimaryKeyAndNoPartitions() throws Exception {
        when(admin.getTableInfo(PATH)).thenReturn(completedFuture(partitionedTable(true)));
        when(admin.listPartitionInfos(PATH))
                .thenReturn(
                        completedFuture(
                                Arrays.asList(partition(10L, "10", 1), partition(20L, "20", 1))));
        List<ConnectorSplit> splits = plan();
        assertThat(splits).hasSize(2);
        assertThat(((FlussSplit) splits.get(0)).getScanType()).isEqualTo(FlussScanType.KV);
        assertThat(((FlussSplit) splits.get(0)).getBucket().getPartitionId()).contains(10L);
        assertThat(((FlussSplit) splits.get(1)).getBucket().getPartitionId()).contains(20L);
        when(admin.listPartitionInfos(PATH)).thenReturn(completedFuture(Collections.emptyList()));
        assertThat(plan()).isEmpty();
        verify(admin, never())
                .listOffsets(any(TablePath.class), anyCollection(), any(OffsetSpec.class));
        verify(admin, never())
                .listOffsets(
                        any(TablePath.class),
                        any(String.class),
                        anyCollection(),
                        any(OffsetSpec.class));
    }

    @Test
    void testPartitionPruningForLogTable() throws Exception {
        when(admin.getTableInfo(PATH)).thenReturn(completedFuture(stringPartitionedTable(false)));

        PartitionSpec spec = new PartitionSpec(Collections.singletonMap("region", "east"));

        when(admin.listPartitionInfos(PATH, spec))
                .thenReturn(completedFuture(Arrays.asList(partition(10L, "east", 2))));

        when(admin.listOffsets(
                        eq(PATH),
                        eq("east"),
                        eq(Arrays.asList(0, 1)),
                        isA(OffsetSpec.EarliestSpec.class)))
                .thenReturn(offsets(2, 5));

        when(admin.listOffsets(
                        eq(PATH),
                        eq("east"),
                        eq(Arrays.asList(0, 1)),
                        isA(OffsetSpec.LatestSpec.class)))
                .thenReturn(offsets(4, 5));

        List<ConnectorSplit> splits =
                planWithHandle(
                        withRegionConstraint(Domain.singleValue(VARCHAR, utf8Slice("east"))));

        assertThat(splits).hasSize(1);
        assertRange(splits.get(0), 0, 2, 4);
        assertThat(((FlussSplit) splits.get(0)).getBucket().getPartitionId()).contains(10L);

        verify(admin).listPartitionInfos(PATH, spec);
        verify(admin, never()).listPartitionInfos(PATH);
    }

    @Test
    void testPartitionPruningForPrimaryKeyTable() throws Exception {
        TableInfo info =
                tableInfo(
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .column("region", DataTypes.STRING())
                                                .column("category", DataTypes.STRING())
                                                .primaryKey("id", "region", "category")
                                                .build())
                                .partitionedBy("region", "category")
                                .distributedBy(1, "id")
                                .build());

        when(admin.getTableInfo(PATH)).thenReturn(completedFuture(info));

        PartitionSpec spec = new PartitionSpec(Collections.singletonMap("region", "east"));
        when(admin.listPartitionInfos(PATH, spec))
                .thenReturn(
                        completedFuture(
                                Arrays.asList(
                                        partition(10L, "east$A", 1), partition(20L, "east$B", 1))));

        Map<FlussColumnHandle, Domain> domains = new LinkedHashMap<>();
        domains.put(
                new FlussColumnHandle("region", 1), Domain.singleValue(VARCHAR, utf8Slice("east")));
        domains.put(
                new FlussColumnHandle("category", 2),
                Domain.multipleValues(VARCHAR, Arrays.asList(utf8Slice("A"), utf8Slice("B"))));

        FlussTableHandle handle = HANDLE.withConstraint(TupleDomain.withColumnDomains(domains));

        List<ConnectorSplit> splits = planWithHandle(handle);

        assertThat(splits).hasSize(2);
        assertThat(splits)
                .allSatisfy(
                        split -> {
                            FlussSplit flussSplit = (FlussSplit) split;
                            assertThat(flussSplit.getScanType()).isEqualTo(FlussScanType.KV);
                            assertThat(flussSplit.getBucket().getPartitionId()).isPresent();
                        });

        verify(admin).listPartitionInfos(PATH, spec);
        verify(admin, never()).listPartitionInfos(PATH);
    }

    @Test
    void testUnsupportedPartitionDomainsFallBackToFullScan() throws Exception {
        when(admin.getTableInfo(PATH)).thenReturn(completedFuture(stringPartitionedTable(true)));

        when(admin.listPartitionInfos(PATH))
                .thenReturn(
                        completedFuture(
                                Arrays.asList(
                                        partition(10L, "east", 1), partition(20L, "west", 1))));

        List<Domain> domains =
                Arrays.asList(
                        Domain.create(
                                ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("east"))),
                                false),
                        Domain.multipleValues(
                                VARCHAR, Arrays.asList(utf8Slice("east"), utf8Slice("west"))),
                        Domain.onlyNull(VARCHAR),
                        Domain.singleValue(VARCHAR, utf8Slice("east"), true));

        for (Domain domain : domains) {
            assertThat(planWithHandle(withRegionConstraint(domain))).hasSize(2);
        }

        verify(admin, times(domains.size())).listPartitionInfos(PATH);
        verify(admin, never()).listPartitionInfos(eq(PATH), any(PartitionSpec.class));
    }

    @Test
    void testNonPartitionNonBucketColumnDoesNotPrune() throws Exception {
        TableInfo table =
                tableInfo(
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .column("region", DataTypes.STRING())
                                                .column("payload", DataTypes.INT())
                                                .primaryKey("id", "region")
                                                .build())
                                .partitionedBy("region")
                                .distributedBy(2, "id")
                                .build());
        when(admin.getTableInfo(PATH)).thenReturn(completedFuture(table));

        when(admin.listPartitionInfos(PATH))
                .thenReturn(
                        completedFuture(
                                Arrays.asList(
                                        partition(10L, "east", 1), partition(20L, "west", 1))));

        TupleDomain<FlussColumnHandle> constraint =
                TupleDomain.withColumnDomains(
                        Collections.singletonMap(
                                new FlussColumnHandle("payload", 2),
                                Domain.singleValue(INTEGER, 7L)));

        List<ConnectorSplit> splits = planWithHandle(HANDLE.withConstraint(constraint));

        assertThat(splits).hasSize(2);

        verify(admin).listPartitionInfos(PATH);
        verify(admin, never()).listPartitionInfos(eq(PATH), any(PartitionSpec.class));
    }

    @Test
    void testNoMatchingLogPartitionSkipsOffsetPlanning() throws Exception {
        when(admin.getTableInfo(PATH)).thenReturn(completedFuture(stringPartitionedTable(false)));

        PartitionSpec spec = new PartitionSpec(Collections.singletonMap("region", "missing"));

        when(admin.listPartitionInfos(PATH, spec))
                .thenReturn(completedFuture(Collections.emptyList()));

        List<ConnectorSplit> splits =
                planWithHandle(
                        withRegionConstraint(Domain.singleValue(VARCHAR, utf8Slice("missing"))));

        assertThat(splits).isEmpty();

        verify(admin).getTableInfo(PATH);
        verify(admin).listPartitionInfos(PATH, spec);
        verifyNoMoreInteractions(admin);
    }

    @Test
    void testReportsTableDroppedDuringOffsetPlanning() {
        when(admin.listOffsets(eq(PATH), eq(BUCKETS), isA(OffsetSpec.EarliestSpec.class)))
                .thenThrow(new org.apache.fluss.exception.TableNotExistException("dropped"));
        assertThatThrownBy(this::plan).isInstanceOf(TableNotFoundException.class);
    }

    @ParameterizedTest
    @CsvSource({"false,3", "true,3", "false,1000000000", "true,1000000000"})
    void testNonPartitionedBucketPruning(boolean primaryKey, int bucketCount) throws Exception {
        Schema.Builder schema = Schema.newBuilder().column("id", DataTypes.INT());
        if (primaryKey) {
            schema.primaryKey("id");
        }
        TableInfo table =
                tableInfo(
                        TableDescriptor.builder()
                                .schema(schema.build())
                                .distributedBy(bucketCount, "id")
                                .build());
        when(admin.getTableInfo(PATH)).thenReturn(completedFuture(table));
        int selected = bucketForId(table, 7, bucketCount);
        List<Integer> ids = Collections.singletonList(selected);
        if (!primaryKey) {
            when(admin.listOffsets(eq(PATH), eq(ids), isA(OffsetSpec.EarliestSpec.class)))
                    .thenReturn(singleOffset(selected, 2));
            when(admin.listOffsets(eq(PATH), eq(ids), isA(OffsetSpec.LatestSpec.class)))
                    .thenReturn(singleOffset(selected, 9));
        }
        FlussTableHandle handle =
                HANDLE.withConstraint(
                        TupleDomain.withColumnDomains(
                                Collections.singletonMap(
                                        new FlussColumnHandle("id", 0),
                                        Domain.singleValue(INTEGER, 7L))));
        List<ConnectorSplit> splits = planWithHandle(handle);
        assertThat(splits).hasSize(1);
        assertThat(((FlussSplit) splits.get(0)).getBucket().getBucketId()).isEqualTo(selected);
        if (primaryKey) {
            verify(admin, never()).listOffsets(eq(PATH), anyCollection(), any(OffsetSpec.class));
        } else {
            assertRange(splits.get(0), selected, 2, 9);
            verify(admin).listOffsets(eq(PATH), eq(ids), isA(OffsetSpec.EarliestSpec.class));
            verify(admin).listOffsets(eq(PATH), eq(ids), isA(OffsetSpec.LatestSpec.class));
            verify(admin, never()).listOffsets(eq(PATH), eq(BUCKETS), any(OffsetSpec.class));
        }
        verify(admin, never()).listPartitionInfos(PATH);
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testPartitionAndBucketPruning(boolean primaryKey, boolean restrictPartition)
            throws Exception {
        TableInfo table = stringPartitionedTable(primaryKey);
        when(admin.getTableInfo(PATH)).thenReturn(completedFuture(table));
        List<PartitionInfo> partitions = new ArrayList<>();
        partitions.add(
                new PartitionInfo(
                        10, ResolvedPartitionSpec.fromPartitionValue("region", "east"), null, 4));
        if (!restrictPartition) {
            partitions.add(
                    new PartitionInfo(
                            20,
                            ResolvedPartitionSpec.fromPartitionValue("region", "west"),
                            null,
                            2));
        }
        PartitionSpec spec = new PartitionSpec(Collections.singletonMap("region", "east"));
        Map<FlussColumnHandle, Domain> domains = new HashMap<>();
        domains.put(new FlussColumnHandle("id", 0), Domain.singleValue(INTEGER, 7L));
        if (restrictPartition) {
            domains.put(
                    new FlussColumnHandle("region", 1),
                    Domain.singleValue(VARCHAR, utf8Slice("east")));
            when(admin.listPartitionInfos(PATH, spec)).thenReturn(completedFuture(partitions));
        } else {
            when(admin.listPartitionInfos(PATH)).thenReturn(completedFuture(partitions));
        }
        for (PartitionInfo partition : partitions) {
            int selected = bucketForId(table, 7, partition.getBucketCount());
            List<Integer> ids = Collections.singletonList(selected);
            if (!primaryKey) {
                when(admin.listOffsets(
                                eq(PATH),
                                eq(partition.getPartitionName()),
                                eq(ids),
                                isA(OffsetSpec.EarliestSpec.class)))
                        .thenReturn(singleOffset(selected, 3));
                when(admin.listOffsets(
                                eq(PATH),
                                eq(partition.getPartitionName()),
                                eq(ids),
                                isA(OffsetSpec.LatestSpec.class)))
                        .thenReturn(singleOffset(selected, 11));
            }
        }
        List<ConnectorSplit> splits =
                planWithHandle(HANDLE.withConstraint(TupleDomain.withColumnDomains(domains)));
        assertThat(splits).hasSize(partitions.size());
        for (int i = 0; i < partitions.size(); i++) {
            PartitionInfo partition = partitions.get(i);
            FlussSplit split = (FlussSplit) splits.get(i);
            int selected = bucketForId(table, 7, partition.getBucketCount());
            assertThat(split.getBucket().getPartitionId()).contains(partition.getPartitionId());
            assertThat(split.getBucket().getBucketId()).isEqualTo(selected);
            if (!primaryKey) {
                assertRange(split, selected, 3, 11);
                verify(admin)
                        .listOffsets(
                                eq(PATH),
                                eq(partition.getPartitionName()),
                                eq(Collections.singletonList(selected)),
                                isA(OffsetSpec.EarliestSpec.class));
                verify(admin)
                        .listOffsets(
                                eq(PATH),
                                eq(partition.getPartitionName()),
                                eq(Collections.singletonList(selected)),
                                isA(OffsetSpec.LatestSpec.class));
            }
        }
        if (primaryKey) {
            verify(admin, never())
                    .listOffsets(
                            eq(PATH), any(String.class), anyCollection(), any(OffsetSpec.class));
        }
        if (restrictPartition) {
            verify(admin).listPartitionInfos(PATH, spec);
            verify(admin, never()).listPartitionInfos(PATH);
        } else {
            verify(admin).listPartitionInfos(PATH);
        }
    }

    @Test
    void testIncompleteCompositeBucketKeyEnumeratesPartitionBuckets() throws Exception {
        TableInfo table =
                tableInfo(
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .column("salt", DataTypes.INT())
                                                .column("region", DataTypes.STRING())
                                                .primaryKey("id", "salt", "region")
                                                .build())
                                .partitionedBy("region")
                                .distributedBy(4, "id", "salt")
                                .build());

        when(admin.getTableInfo(PATH)).thenReturn(completedFuture(table));
        when(admin.listPartitionInfos(PATH))
                .thenReturn(
                        completedFuture(
                                Arrays.asList(
                                        partition(10L, "east", 4), partition(20L, "west", 4))));

        // id is constrained, but salt is not.
        // The physical bucket key (id, salt) is incomplete.
        FlussTableHandle handle =
                HANDLE.withConstraint(
                        TupleDomain.withColumnDomains(
                                Collections.singletonMap(
                                        new FlussColumnHandle("id", 0),
                                        Domain.singleValue(INTEGER, 7L))));

        List<ConnectorSplit> splits = planWithHandle(handle);
        assertThat(splits).hasSize(8);
        assertThat(splits)
                .allSatisfy(
                        split -> {
                            FlussSplit flussSplit = (FlussSplit) split;
                            assertThat(flussSplit.getScanType()).isEqualTo(FlussScanType.KV);
                        });
        assertThat(
                        splits.stream()
                                .map(
                                        split ->
                                                ((FlussSplit) split)
                                                        .getBucket()
                                                        .getPartitionId()
                                                        .orElseThrow(
                                                                () ->
                                                                        new AssertionError(
                                                                                "Expected a partitioned bucket")))
                                .distinct())
                .containsExactlyInAnyOrder(10L, 20L);
        verify(admin).listPartitionInfos(PATH);
        verify(admin, never()).listPartitionInfos(eq(PATH), any(PartitionSpec.class));
        verify(admin, never()).listOffsets(eq(PATH), anyCollection(), any(OffsetSpec.class));
        verify(admin, never())
                .listOffsets(eq(PATH), any(String.class), anyCollection(), any(OffsetSpec.class));
    }

    private static ListOffsetsResult singleOffset(int bucket, long offset) {
        return new ListOffsetsResult(Collections.singletonMap(bucket, completedFuture(offset)));
    }

    private static int bucketForId(TableInfo table, int value, int count) {
        byte[] key =
                KeyEncoder.ofBucketKeyEncoder(table.getRowType(), table.getBucketKeys(), null)
                        .encodeKey(GenericRow.of(value));
        return BucketingFunction.of(null).bucketing(key, count);
    }

    private List<ConnectorSplit> plan() throws Exception {
        return planWithHandle(HANDLE);
    }

    private List<ConnectorSplit> planWithHandle(FlussTableHandle handle) throws Exception {
        try (ConnectorSplitSource source =
                manager.getSplits(
                        FlussTransactionHandle.INSTANCE,
                        mock(ConnectorSession.class),
                        handle,
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
                                        .isEqualTo(FLUSS_METADATA_CHANGED.toErrorCode()))
                .hasMessageContaining(message);
    }

    private static void assertRange(ConnectorSplit split, int bucket, long start, long stop) {
        assertThat(split).isInstanceOf(FlussSplit.class);
        FlussSplit range = (FlussSplit) split;
        assertThat(range.getScanType()).isEqualTo(FlussScanType.LOG);
        assertThat(range.getBucket().getBucketId()).isEqualTo(bucket);
        assertThat(range.getRequiredLogRange().getStartOffset()).isEqualTo(start);
        assertThat(range.getRequiredLogRange().getStoppingOffset()).isEqualTo(stop);
    }

    private static ListOffsetsResult offsets(long... offsets) {
        Map<Integer, CompletableFuture<Long>> futures = new HashMap<>();
        for (int bucket = 0; bucket < offsets.length; bucket++) {
            futures.put(bucket, completedFuture(offsets[bucket]));
        }
        return new ListOffsetsResult(futures);
    }

    private static ListOffsetsResult failedOffsets(RuntimeException failure) {
        Map<Integer, CompletableFuture<Long>> futures = new HashMap<>();
        futures.put(0, FutureUtils.completedExceptionally(failure));
        return new ListOffsetsResult(futures);
    }

    private static List<TableInfo> changedTables() {
        return Arrays.asList(logTable(43, 3, 3, 0), logTable(42, 4, 3, 0));
    }

    private void givenPartitionedLogTable() {
        when(admin.getTableInfo(PATH)).thenReturn(completedFuture(partitionedTable(false)));
        when(admin.listPartitionInfos(PATH))
                .thenReturn(
                        completedFuture(
                                Arrays.asList(partition(10L, "10", 2), partition(20L, "20", 1))));

        when(admin.listOffsets(
                        eq(PATH),
                        eq("10"),
                        eq(Arrays.asList(0, 1)),
                        isA(OffsetSpec.EarliestSpec.class)))
                .thenReturn(offsets(2, 5));
        when(admin.listOffsets(
                        eq(PATH),
                        eq("10"),
                        eq(Arrays.asList(0, 1)),
                        isA(OffsetSpec.LatestSpec.class)))
                .thenReturn(offsets(4, 5));

        when(admin.listOffsets(
                        eq(PATH),
                        eq("20"),
                        eq(Collections.singletonList(0)),
                        isA(OffsetSpec.EarliestSpec.class)))
                .thenReturn(offsets(8));
        when(admin.listOffsets(
                        eq(PATH),
                        eq("20"),
                        eq(Collections.singletonList(0)),
                        isA(OffsetSpec.LatestSpec.class)))
                .thenReturn(offsets(11));
    }

    private static PartitionInfo partition(long id, String name, int count) {
        ResolvedPartitionSpec spec =
                name.contains("$")
                        ? ResolvedPartitionSpec.fromPartitionName(
                                Arrays.asList("region", "category"), name)
                        : ResolvedPartitionSpec.fromPartitionValue("region", name);
        return new PartitionInfo(id, spec, null, count);
    }

    private static TableInfo partitionedTable(boolean primaryKey) {
        Schema.Builder schema =
                Schema.newBuilder().column("id", DataTypes.INT()).column("region", DataTypes.INT());
        if (primaryKey) {
            schema.primaryKey("id", "region");
        }
        return tableInfo(
                TableDescriptor.builder()
                        .schema(schema.build())
                        .partitionedBy("region")
                        .distributedBy(3)
                        .build());
    }

    private static TableInfo primaryKeyTable() {
        return tableInfo(
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .primaryKey("id")
                                        .build())
                        .distributedBy(3)
                        .build());
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

    private static TableInfo stringPartitionedTable(boolean primaryKey) {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("region", DataTypes.STRING());

        if (primaryKey) {
            schema.primaryKey("id", "region");
        }

        return tableInfo(
                TableDescriptor.builder()
                        .schema(schema.build())
                        .partitionedBy("region")
                        .distributedBy(2, "id")
                        .build());
    }

    private static TableInfo twoKeyPartitionedTable(boolean primaryKey) {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("region", DataTypes.STRING())
                        .column("dt", DataTypes.STRING());
        if (primaryKey) {
            schema.primaryKey("id", "region", "dt");
        }
        return tableInfo(
                TableDescriptor.builder()
                        .schema(schema.build())
                        .partitionedBy("region", "dt")
                        .distributedBy(2, "id")
                        .build());
    }

    private static FlussTableHandle withRegionConstraint(Domain domain) {
        TupleDomain<FlussColumnHandle> constraint =
                TupleDomain.withColumnDomains(
                        Collections.singletonMap(new FlussColumnHandle("region", 1), domain));

        return HANDLE.withConstraint(constraint);
    }
}
