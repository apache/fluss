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

import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.table.BucketCalculator;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.concurrent.FutureUtils;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.fluss.trino.FlussErrorCode.FLUSS_METADATA_CHANGED;
import static org.apache.fluss.trino.FlussErrorCode.FLUSS_SPLIT_ERROR;
import static org.apache.fluss.trino.TestingFlussMetadata.tableInfo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests physical split planning with real catalog metadata and log offsets. */
final class FlussSplitManagerTest extends FlussTrinoTestBase {
    private TablePath tablePath;
    private TableInfo info;
    private FlussTableHandle handle;

    @BeforeEach
    void resetState() {
        // Deletion notifications can outlive a test; do not reuse its physical table path.
        tablePath =
                TablePath.of(
                        "Sales_split_" + UUID.randomUUID().toString().replace("-", ""), "Users");
        info = null;
        handle = null;
    }

    @Test
    void testNoneConstraintSkipsAllMetadataAndScanPlanning() throws Exception {
        clients.close();
        FlussTableHandle missing = tableHandle(logTable(42, 1, 3, 0));
        assertThat(planWithHandle(missing.withConstraint(TupleDomain.none()))).isEmpty();
    }

    @Test
    void testPlansNonEmptyRanges() throws Exception {
        create(logTable(42, 1, 3, 0));
        appendToBucket(0, 5);
        appendToBucket(2, 8);
        List<ConnectorSplit> splits = plan();
        assertThat(splits).hasSize(2);
        assertRange(splits.get(0), 0, 0, 5);
        assertRange(splits.get(1), 2, 0, 8);
    }

    @Test
    void testWaitsForEveryEarliestOffsetBeforeRequestingLatest() throws Exception {
        create(logTable(42, 1, 3, 0));
        appendToBucket(0, 1);
        CompletableFuture<Long> lastBucket = new CompletableFuture<>();
        CompletableFuture<Void> requested = new CompletableFuture<>();
        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath path, Collection<Integer> buckets, OffsetSpec spec) {
                        if (spec instanceof OffsetSpec.LatestSpec) {
                            assertThat(lastBucket).isDone();
                            return super.listOffsets(path, buckets, spec);
                        }
                        ListOffsetsResult real = super.listOffsets(path, buckets, spec);
                        Map<Integer, CompletableFuture<Long>> starts = new HashMap<>();
                        for (int bucket : buckets) {
                            starts.put(
                                    bucket, bucket == 2 ? lastBucket : real.bucketResult(bucket));
                        }
                        requested.complete(null);
                        return new ListOffsetsResult(starts);
                    }
                });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<List<ConnectorSplit>> result = executor.submit(this::plan);
            requested.get(10, TimeUnit.SECONDS);
            assertThat(result.isDone()).isFalse();
            lastBucket.complete(0L);
            assertThat(result.get(30, TimeUnit.SECONDS)).hasSize(1);
        } finally {
            lastBucket.complete(0L);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testEmptyTableHasNoSplits() throws Exception {
        create(logTable(42, 1, 3, 0));
        assertThat(plan()).isEmpty();
    }

    @Test
    void testRangesAreNotCachedAcrossScans() throws Exception {
        create(logTable(42, 1, 3, 0));
        appendToBucket(0, 1);
        List<ConnectorSplit> first = plan();
        appendToBucket(0, 1);
        appendToBucket(1, 1);
        List<ConnectorSplit> second = plan();
        assertThat(first).hasSize(1);
        assertRange(first.get(0), 0, 0, 1);
        assertThat(second).hasSize(2);
        assertRange(second.get(0), 0, 0, 2);
        assertRange(second.get(1), 1, 0, 1);
    }

    @Test
    void testRejectsMissingAndInvalidOffsets() throws Exception {
        create(logTable(42, 1, 3, 0));
        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath path, Collection<Integer> buckets, OffsetSpec spec) {
                        return spec instanceof OffsetSpec.LatestSpec
                                ? offsets(0, 0)
                                : super.listOffsets(path, buckets, spec);
                    }
                });
        assertPlanningFailure("bucketId=2");
        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath path, Collection<Integer> buckets, OffsetSpec spec) {
                        return spec instanceof OffsetSpec.EarliestSpec
                                ? offsets(1, 0, 0)
                                : super.listOffsets(path, buckets, spec);
                    }
                });
        assertThatThrownBy(this::plan)
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        failure ->
                                assertThat(failure.getErrorCode())
                                        .isEqualTo(FLUSS_SPLIT_ERROR.toErrorCode()))
                .hasMessageContaining("bucketId=0");
        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath path, Collection<Integer> buckets, OffsetSpec spec) {
                        return offsets(-1, 0, 0);
                    }
                });
        assertPlanningFailure("bucketId=0");
    }

    @Test
    void testPartialFailureAbortsPlanningAndPreservesCause() throws Exception {
        create(logTable(42, 1, 3, 0));
        RuntimeException failure = new IllegalStateException("offset RPC failed");
        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath path, Collection<Integer> buckets, OffsetSpec spec) {
                        assertThat(spec).isInstanceOf(OffsetSpec.EarliestSpec.class);
                        ListOffsetsResult real = super.listOffsets(path, buckets, spec);
                        Map<Integer, CompletableFuture<Long>> starts = new HashMap<>();
                        starts.put(0, real.bucketResult(0));
                        starts.put(1, FutureUtils.completedExceptionally(failure));
                        starts.put(2, real.bucketResult(2));
                        return new ListOffsetsResult(starts);
                    }
                });
        assertThatThrownBy(this::plan).isInstanceOf(TrinoException.class).hasCause(failure);
    }

    @Test
    void testDoesNotRequestOffsetsForUnsupportedTables() throws Exception {
        create(
                tableInfo(
                        TableDescriptor.builder()
                                .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                                .distributedBy(3)
                                .property("table.datalake.enabled", "true")
                                .build()));
        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath path, Collection<Integer> buckets, OffsetSpec spec) {
                        throw new AssertionError(
                                "Unsupported tables must be rejected before offset lookup");
                    }
                });
        assertUnsupportedTableType("Lakehouse");
    }

    @Test
    void testPlansEveryPrimaryKeyBucketWithoutOffsetsOrStatistics() throws Exception {
        create(primaryKeyTable());

        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath path, Collection<Integer> buckets, OffsetSpec spec) {
                        throw new AssertionError("PK split planning must not request log offsets");
                    }
                });

        List<ConnectorSplit> splits = plan();

        assertThat(splits).hasSize(3);
        for (int bucket = 0; bucket < 3; bucket++) {
            FlussSplit split = (FlussSplit) splits.get(bucket);
            assertThat(split.getScanType()).isEqualTo(FlussScanType.KV);
            assertThat(split.getBucket().getBucketId()).isEqualTo(bucket);
            assertThat(split.getLogRange()).isEmpty();
        }
    }

    @Test
    void testRejectsChangedSchemaBeforeOffsets() throws Exception {
        create(logTable(42, 1, 3, 0));
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "extra",
                                        DataTypes.INT(),
                                        null,
                                        TableChange.ColumnPosition.last())),
                        false)
                .get(30, TimeUnit.SECONDS);
        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath path, Collection<Integer> buckets, OffsetSpec spec) {
                        throw new AssertionError("Changed schema must be rejected before offsets");
                    }
                });
        assertTableChanged("schema changed");
    }

    @Test
    void testRejectsChangedTableIdBeforeOffsets() throws Exception {
        create(logTable(42, 1, 3, 0));
        TableInfo recreated =
                TableInfo.of(
                        tablePath,
                        info.getTableId() + 1,
                        info.getSchemaId(),
                        info.toTableDescriptor(),
                        info.getRemoteDataDir(),
                        info.getCreatedTime(),
                        info.getModifiedTime(),
                        info.getBucketCountEpoch());
        // Exercise the connector's identity check without racing server-side DROP/recreate.
        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public CompletableFuture<TableInfo> getTableInfo(TablePath path) {
                        assertThat(path).isEqualTo(tablePath);
                        return completedFuture(recreated);
                    }

                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath path, Collection<Integer> buckets, OffsetSpec spec) {
                        throw new AssertionError(
                                "Changed table ID must be rejected before offsets");
                    }
                });
        assertTableChanged("table changed");
    }

    @Test
    void testPartitionedLogPlanningGroupsOffsetsByPartition() throws Exception {
        givenPartitionedLogTable();
        List<ConnectorSplit> splits = plan();
        assertThat(splits).hasSize(2);
        assertThat(splits).allSatisfy(split -> assertRange(split, bucketForId(info, 7, 2), 0, 1));
        assertThat(splits)
                .extracting(split -> ((FlussSplit) split).getBucket().getPartitionId().get())
                .containsExactlyInAnyOrderElementsOf(
                        CLUSTER.waitUntilPartitionsCreated(tablePath, 2).values());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionDroppedBeforeOffsetLookup(boolean latest) throws Exception {
        givenPartitionedLogTable();
        PartitionSpec expired = new PartitionSpec(Collections.singletonMap("region", "20"));
        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath path,
                            String partition,
                            Collection<Integer> buckets,
                            OffsetSpec spec) {
                        if (partition.equals("20")
                                && (spec instanceof OffsetSpec.LatestSpec) == latest) {
                            admin.dropPartition(path, expired, false)
                                    .orTimeout(30, TimeUnit.SECONDS)
                                    .join();

                            throw new PartitionNotExistException(
                                    "Partition '" + partition + "' was dropped");
                        }
                        return super.listOffsets(path, partition, buckets, spec);
                    }
                });
        List<ConnectorSplit> splits = plan();
        assertThat(splits).hasSize(1);
        assertRange(splits.get(0), bucketForId(info, 7, 2), 0, 1);
        assertThat(((FlussSplit) splits.get(0)).getBucket().getPartitionId())
                .contains(CLUSTER.waitUntilPartitionsCreated(tablePath, 1).get("10"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionDroppedDuringOffsetLookup(boolean latest) throws Exception {
        givenPartitionedLogTable();
        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath path,
                            String partition,
                            Collection<Integer> buckets,
                            OffsetSpec spec) {
                        if (partition.equals("20")
                                && (spec instanceof OffsetSpec.LatestSpec) == latest) {
                            return failedOffsets(
                                    new PartitionNotExistException("Partition expired"));
                        }
                        return super.listOffsets(path, partition, buckets, spec);
                    }
                });
        List<ConnectorSplit> splits = plan();
        assertThat(splits).hasSize(1);
        assertRange(splits.get(0), bucketForId(info, 7, 2), 0, 1);
    }

    @Test
    void testMissingPartitionOffsetIsNotTreatedAsExpiration() throws Exception {
        givenPartitionedLogTable();
        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath path,
                            String partition,
                            Collection<Integer> buckets,
                            OffsetSpec spec) {
                        return partition.equals("10")
                                ? offsets(0)
                                : super.listOffsets(path, partition, buckets, spec);
                    }
                });
        assertPlanningFailure("bucketId=1");
    }

    @Test
    void testPartitionDiscoveryOccursOnceDuringPlanning() throws Exception {
        givenPartitionedLogTable();

        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public CompletableFuture<List<PartitionInfo>> listPartitionInfos(
                            TablePath path) {
                        return super.listPartitionInfos(path)
                                .thenCompose(
                                        partitions ->
                                                admin.createPartition(
                                                                path,
                                                                new PartitionSpec(
                                                                        Collections.singletonMap(
                                                                                "region", "30")),
                                                                false)
                                                        .thenApply(ignored -> partitions));
                    }
                });

        assertThat(plan()).hasSize(2);
        assertThat(admin.listPartitionInfos(tablePath).get(30, TimeUnit.SECONDS)).hasSize(3);
    }

    @Test
    void testPartitionedOffsetLookupFailure() throws Exception {
        givenPartitionedLogTable();
        RuntimeException failure = new IllegalStateException("Offset RPC failed");
        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath path,
                            String partition,
                            Collection<Integer> buckets,
                            OffsetSpec spec) {
                        assertThat(spec).isInstanceOf(OffsetSpec.EarliestSpec.class);
                        return partition.equals("20")
                                ? failedOffsets(failure)
                                : super.listOffsets(path, partition, buckets, spec);
                    }
                });
        assertThatThrownBy(this::plan)
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode())
                                        .isEqualTo(FLUSS_SPLIT_ERROR.toErrorCode()))
                .hasCause(failure);
    }

    @Test
    void testPartitionedPrimaryKeyAndNoPartitions() throws Exception {
        create(partitionedTable(true));
        assertThat(plan()).isEmpty();
        createPartition("region", "10");
        createPartition("region", "20");
        assertThat(plan())
                .hasSize(4)
                .allSatisfy(
                        split -> {
                            assertThat(((FlussSplit) split).getScanType())
                                    .isEqualTo(FlussScanType.KV);
                            assertThat(((FlussSplit) split).getBucket().getPartitionId())
                                    .isPresent();
                        });
    }

    @Test
    void testPartitionPruningForLogTable() throws Exception {
        create(stringPartitionedTable(false));
        createPartition("region", "east");
        createPartition("region", "west");
        append(
                GenericRow.of(7, BinaryString.fromString("east")),
                GenericRow.of(7, BinaryString.fromString("west")));
        List<ConnectorSplit> splits =
                planWithHandle(
                        withRegionConstraint(Domain.singleValue(VARCHAR, utf8Slice("east"))));
        assertThat(splits).hasSize(1);
        assertRange(splits.get(0), bucketForId(info, 7, 2), 0, 1);
        assertThat(((FlussSplit) splits.get(0)).getBucket().getPartitionId())
                .contains(CLUSTER.waitUntilPartitionsCreated(tablePath, 2).get("east"));
    }

    @Test
    void testPartitionPruningForPrimaryKeyTable() throws Exception {
        create(
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
                                .build()));
        for (String region : Arrays.asList("east", "west")) {
            for (String category : Arrays.asList("A", "B")) {
                Map<String, String> values = new LinkedHashMap<>();
                values.put("region", region);
                values.put("category", category);
                admin.createPartition(tablePath, new PartitionSpec(values), false)
                        .get(30, TimeUnit.SECONDS);
            }
        }
        Map<FlussColumnHandle, Domain> domains = new LinkedHashMap<>();
        domains.put(
                new FlussColumnHandle("region", 1), Domain.singleValue(VARCHAR, utf8Slice("east")));
        domains.put(
                new FlussColumnHandle("category", 2),
                Domain.multipleValues(VARCHAR, Arrays.asList(utf8Slice("A"), utf8Slice("C"))));
        List<ConnectorSplit> splits =
                planWithHandle(handle.withConstraint(TupleDomain.withColumnDomains(domains)));
        Map<String, Long> partitions = CLUSTER.waitUntilPartitionsCreated(tablePath, 4);
        assertThat(splits)
                .extracting(split -> ((FlussSplit) split).getBucket().getPartitionId().get())
                .containsExactly(partitions.get("east$A"));
    }

    @Test
    void testPartitionRangeAndInSelectPhysicalPartitions() throws Exception {
        create(stringPartitionedTable(true));
        createPartition("region", "east");
        createPartition("region", "west");
        createPartition("region", "south");
        Map<String, Long> partitions = CLUSTER.waitUntilPartitionsCreated(tablePath, 3);
        long west = partitions.get("west");
        long south = partitions.get("south");
        for (Domain domain :
                Arrays.asList(
                        Domain.create(
                                ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("east"))),
                                false),
                        Domain.multipleValues(
                                VARCHAR, Arrays.asList(utf8Slice("south"), utf8Slice("west"))))) {
            assertThat(planWithHandle(withRegionConstraint(domain)))
                    .extracting(split -> ((FlussSplit) split).getBucket().getPartitionId().get())
                    .containsExactlyInAnyOrder(west, west, south, south);
        }
    }

    @Test
    void testUnsupportedPartitionDomainsFallBackToFullScan() throws Exception {
        create(stringPartitionedTable(true));
        createPartition("region", "east");
        createPartition("region", "west");
        for (Domain domain :
                Arrays.asList(
                        Domain.onlyNull(VARCHAR),
                        Domain.singleValue(VARCHAR, utf8Slice("east"), true))) {
            assertThat(planWithHandle(withRegionConstraint(domain))).hasSize(4);
        }
    }

    @Test
    void testNonPartitionNonBucketColumnDoesNotPrune() throws Exception {
        create(
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
                                .build()));
        createPartition("region", "east");
        createPartition("region", "west");
        assertThat(
                        planWithHandle(
                                handle.withConstraint(
                                        TupleDomain.withColumnDomains(
                                                Collections.singletonMap(
                                                        new FlussColumnHandle("payload", 2),
                                                        Domain.singleValue(INTEGER, 7L))))))
                .hasSize(4);
    }

    @Test
    void testNoMatchingLogPartitionSkipsOffsetPlanning() throws Exception {
        create(stringPartitionedTable(false));
        createPartition("region", "east");
        append(GenericRow.of(7, BinaryString.fromString("east")));

        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath path,
                            String partition,
                            Collection<Integer> buckets,
                            OffsetSpec spec) {
                        throw new AssertionError("No matching partition must skip offset lookup");
                    }
                });

        assertThat(
                        planWithHandle(
                                withRegionConstraint(
                                        Domain.create(
                                                ValueSet.ofRanges(
                                                        Range.lessThan(VARCHAR, utf8Slice("east"))),
                                                false))))
                .isEmpty();
    }

    @Test
    void testReportsTableDroppedDuringOffsetPlanning() throws Exception {
        create(logTable(42, 1, 3, 0));

        useAdmin(
                new TestingAdmin(testMetadataUpdater(), sharedRpcClient()) {
                    @Override
                    public ListOffsetsResult listOffsets(
                            TablePath path, Collection<Integer> buckets, OffsetSpec spec) {
                        admin.dropTable(path, false).orTimeout(30, TimeUnit.SECONDS).join();
                        return super.listOffsets(path, buckets, spec);
                    }
                });

        assertThatThrownBy(this::plan)
                .isInstanceOf(TableNotFoundException.class)
                .hasRootCauseInstanceOf(TableNotExistException.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNonPartitionedBucketPruning(boolean primaryKey) throws Exception {
        Schema.Builder schema = Schema.newBuilder().column("id", DataTypes.INT());
        if (primaryKey) {
            schema.primaryKey("id");
        }
        create(
                tableInfo(
                        TableDescriptor.builder()
                                .schema(schema.build())
                                .distributedBy(3, "id")
                                .build()));
        if (!primaryKey) {
            append(GenericRow.of(7));
        }
        List<ConnectorSplit> splits =
                planWithHandle(
                        handle.withConstraint(
                                TupleDomain.withColumnDomains(
                                        Collections.singletonMap(
                                                new FlussColumnHandle("id", 0),
                                                Domain.singleValue(INTEGER, 7L)))));
        assertThat(splits).hasSize(1);
        int selected = bucketForId(info, 7, 3);
        assertThat(((FlussSplit) splits.get(0)).getBucket().getBucketId()).isEqualTo(selected);
        if (!primaryKey) {
            assertRange(splits.get(0), selected, 0, 1);
        }
    }

    @Test
    void testLargeBucketCountPruningDoesNotEnumerateBuckets() {
        TableInfo table =
                tableInfo(
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .primaryKey("id")
                                                .build())
                                .distributedBy(1_000_000_000, "id")
                                .build());
        FlussTableHandle constrained =
                tableHandle(table)
                        .withConstraint(
                                TupleDomain.withColumnDomains(
                                        Collections.singletonMap(
                                                new FlussColumnHandle("id", 0),
                                                Domain.singleValue(INTEGER, 7L))));
        assertThat(new FlussSplitPlanner(access).plan(constrained, table))
                .singleElement()
                .satisfies(
                        split ->
                                assertThat(split.getBucket().getBucketId())
                                        .isEqualTo(bucketForId(table, 7, table.getNumBuckets())));
    }

    @Test
    void testBucketInOverCombinationLimitPlansAllBuckets() {
        TableInfo table =
                tableInfo(
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .primaryKey("id")
                                                .build())
                                .distributedBy(4, "id")
                                .build());
        // Use a key list whose hashes only hit one bucket, so fallback is distinguishable.
        BucketCalculator calculator = BucketCalculator.forBucketKeyRow(table);
        List<Long> values =
                LongStream.range(0, 4096)
                        .filter(
                                value ->
                                        calculator.getBucketId(
                                                        calculator.encodeKey(
                                                                GenericRow.of((int) value)),
                                                        4)
                                                == 0)
                        .limit(257)
                        .boxed()
                        .collect(Collectors.toList());
        assertThat(values).hasSize(257);
        FlussTableHandle constrained =
                tableHandle(table)
                        .withConstraint(
                                TupleDomain.withColumnDomains(
                                        Collections.singletonMap(
                                                new FlussColumnHandle("id", 0),
                                                Domain.multipleValues(INTEGER, values))));
        assertThat(new FlussSplitPlanner(access).plan(constrained, table))
                .extracting(split -> split.getBucket().getBucketId())
                .containsExactly(0, 1, 2, 3);
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testPartitionAndBucketPruning(boolean primaryKey, boolean restrictPartition)
            throws Exception {
        create(stringPartitionedTable(primaryKey));
        createPartition("region", "west");
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(TableChange.modifyBucketCount(4)),
                        false)
                .get(30, TimeUnit.SECONDS);
        createPartition("region", "east");
        if (!primaryKey) {
            append(
                    GenericRow.of(7, BinaryString.fromString("east")),
                    GenericRow.of(7, BinaryString.fromString("west")));
        }
        Map<FlussColumnHandle, Domain> domains = new HashMap<>();
        domains.put(new FlussColumnHandle("id", 0), Domain.singleValue(INTEGER, 7L));
        if (restrictPartition) {
            domains.put(
                    new FlussColumnHandle("region", 1),
                    Domain.singleValue(VARCHAR, utf8Slice("east")));
        }
        List<ConnectorSplit> splits =
                planWithHandle(handle.withConstraint(TupleDomain.withColumnDomains(domains)));
        List<PartitionInfo> partitions =
                admin.listPartitionInfos(tablePath).get(30, TimeUnit.SECONDS);
        assertThat(splits).hasSize(restrictPartition ? 1 : 2);
        for (ConnectorSplit split : splits) {
            FlussSplit flussSplit = (FlussSplit) split;
            PartitionInfo partition =
                    partitions.stream()
                            .filter(
                                    p ->
                                            flussSplit.getBucket().getPartitionId().get()
                                                    == p.getPartitionId())
                            .findFirst()
                            .get();
            if (restrictPartition) {
                assertThat(partition.getPartitionName()).isEqualTo("east");
            }
            int selected = bucketForId(info, 7, partition.getBucketCount());
            assertThat(flussSplit.getBucket().getBucketId()).isEqualTo(selected);
            if (!primaryKey) {
                assertRange(split, selected, 0, 1);
            }
        }
    }

    @Test
    void testIncompleteCompositeBucketKeyEnumeratesPartitionBuckets() throws Exception {
        create(
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
                                .build()));
        createPartition("region", "east");
        createPartition("region", "west");
        assertThat(
                        planWithHandle(
                                handle.withConstraint(
                                        TupleDomain.withColumnDomains(
                                                Collections.singletonMap(
                                                        new FlussColumnHandle("id", 0),
                                                        Domain.singleValue(INTEGER, 7L))))))
                .hasSize(8)
                .allSatisfy(
                        split ->
                                assertThat(((FlussSplit) split).getScanType())
                                        .isEqualTo(FlussScanType.KV));
    }

    private void create(TableInfo template) throws Exception {
        info = createTable(tablePath, template.toTableDescriptor());
        handle = tableHandle(info);
    }

    private void createPartition(String key, String value) throws Exception {
        admin.createPartition(
                        tablePath, new PartitionSpec(Collections.singletonMap(key, value)), false)
                .get(30, TimeUnit.SECONDS);
    }

    private void append(InternalRow... rows) throws Exception {
        if (info.isPartitioned()) {
            for (PartitionInfo partition :
                    admin.listPartitionInfos(tablePath).get(30, TimeUnit.SECONDS)) {
                for (int bucket = 0; bucket < partition.getBucketCount(); bucket++) {
                    CLUSTER.waitAndGetLeader(
                            new TableBucket(info.getTableId(), partition.getPartitionId(), bucket));
                }
            }
        }
        try (Table table = dataConnection().getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (InternalRow row : rows) {
                writer.append(row).get(30, TimeUnit.SECONDS);
            }
        }
    }

    private void appendToBucket(int bucket, int count) throws Exception {
        int value = 0;
        while (bucketForId(info, value, info.getNumBuckets()) != bucket) {
            value++;
        }
        InternalRow[] rows = new InternalRow[count];
        Arrays.fill(rows, GenericRow.of(value));
        append(rows);
    }

    private void givenPartitionedLogTable() throws Exception {
        create(partitionedTable(false));
        createPartition("region", "10");
        createPartition("region", "20");
        append(GenericRow.of(7, 10), GenericRow.of(7, 20));
    }

    private List<ConnectorSplit> plan() throws Exception {
        return planWithHandle(handle);
    }

    private List<ConnectorSplit> planWithHandle(FlussTableHandle table) throws Exception {
        FlussSplitManager manager = new FlussSplitManager(access, new FlussSplitPlanner(access));
        try (ConnectorSplitSource source =
                manager.getSplits(
                        FlussTransactionHandle.INSTANCE,
                        SESSION,
                        table,
                        Collections.emptySet(),
                        Constraint.alwaysTrue())) {
            List<ConnectorSplit> splits =
                    source.getNextBatch(100, DynamicFilterSnapshot.EMPTY).get(30, TimeUnit.SECONDS);
            assertThat(source.isFinished()).isTrue();
            return splits;
        }
    }

    private FlussTableHandle withRegionConstraint(Domain domain) {
        return handle.withConstraint(
                TupleDomain.withColumnDomains(
                        Collections.singletonMap(new FlussColumnHandle("region", 1), domain)));
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
                        .distributedBy(2, "id")
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

    private TableInfo logTable(long tableId, int schemaId, int buckets, long epoch) {
        return TableInfo.of(
                tablePath,
                tableId,
                schemaId,
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(buckets, "id")
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

    private static int bucketForId(TableInfo table, int value, int count) {
        BucketCalculator calculator = BucketCalculator.forBucketKeyRow(table);
        return calculator.getBucketId(calculator.encodeKey(GenericRow.of(value)), count);
    }
}
