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

package org.apache.fluss.flink.tiering.source.split;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.util.FlinkRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link TieringSplitGenerator}. */
class TieringSplitGeneratorTest {

    private static final TablePath TABLE_PATH = TablePath.of("test", "tiering_partition_deletion");
    private static final long TABLE_ID = 1L;
    private static final long DELETED_PARTITION_ID = 1L;
    private static final long HEALTHY_PARTITION_ID = 2L;
    private static final String DELETED_PARTITION = "20261019";
    private static final String HEALTHY_PARTITION = "20261020";
    private static final List<Integer> BUCKETS = Collections.singletonList(0);

    private Admin admin;
    private TieringSplitGenerator generator;

    @BeforeEach
    void setUp() {
        admin = mock(Admin.class);
        generator = new TieringSplitGenerator(admin);
        when(admin.getLatestLakeSnapshot(TABLE_PATH))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new LakeSnapshot(0L, Collections.emptyMap())));
        when(admin.listPartitionInfos(TABLE_PATH, true))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                Arrays.asList(
                                        partition(DELETED_PARTITION_ID, DELETED_PARTITION),
                                        partition(HEALTHY_PARTITION_ID, HEALTHY_PARTITION))));
        when(admin.listOffsets(
                        eq(TABLE_PATH), anyString(), eq(BUCKETS), isA(OffsetSpec.LatestSpec.class)))
                .thenReturn(offsetsResult(CompletableFuture.completedFuture(10L)));
        when(admin.getLatestKvSnapshots(TABLE_PATH, DELETED_PARTITION))
                .thenReturn(emptySnapshots(DELETED_PARTITION_ID));
        when(admin.getLatestKvSnapshots(TABLE_PATH, HEALTHY_PARTITION))
                .thenReturn(emptySnapshots(HEALTHY_PARTITION_ID));
    }

    @ParameterizedTest(name = "primaryKey={0}, asynchronousFailure={1}")
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    void testSkipPartitionDeletedBeforeLatestOffsets(
            boolean primaryKey, boolean asynchronousFailure) throws Exception {
        // The list already returned both partitions. The next Admin RPC observes deletion.
        PartitionNotExistException failure = new PartitionNotExistException("partition deleted");
        if (asynchronousFailure) {
            // Also covers the exception shape of an exceptional ListOffsetsResult. This boundary
            // case does not assert that every server reports partition deletion in this form.
            when(admin.listOffsets(
                            eq(TABLE_PATH),
                            eq(DELETED_PARTITION),
                            eq(BUCKETS),
                            isA(OffsetSpec.LatestSpec.class)))
                    .thenReturn(offsetsResult(failedFuture(failure)));
        } else {
            // FlussAdmin.listOffsets synchronously refreshes the partition metadata and can throw
            // PartitionNotExistException before constructing ListOffsetsResult.
            doThrow(failure)
                    .when(admin)
                    .listOffsets(
                            eq(TABLE_PATH),
                            eq(DELETED_PARTITION),
                            eq(BUCKETS),
                            isA(OffsetSpec.LatestSpec.class));
        }

        List<TieringSplit> splits = generator.generateTableSplits(tableInfo(primaryKey));

        assertThat(splits).hasSize(1);
        assertHealthySplit(splits.get(0));
        verify(admin).listPartitionInfos(TABLE_PATH, true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testGenerateSplitsWhenListedPartitionsStillExist(boolean primaryKey) throws Exception {
        List<TieringSplit> splits = generator.generateTableSplits(tableInfo(primaryKey));

        assertThat(splits)
                .extracting(TieringSplit::getTableBucket)
                .containsExactlyInAnyOrder(
                        new TableBucket(TABLE_ID, DELETED_PARTITION_ID, 0),
                        new TableBucket(TABLE_ID, HEALTHY_PARTITION_ID, 0));
        assertThat(splits)
                .filteredOn(split -> HEALTHY_PARTITION.equals(split.getPartitionName()))
                .singleElement()
                .satisfies(TieringSplitGeneratorTest::assertHealthySplit);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPropagateUnrelatedLatestOffsetsFailure(boolean primaryKey) {
        NetworkException failure = new NetworkException("coordinator disconnected");
        when(admin.listOffsets(
                        eq(TABLE_PATH),
                        eq(DELETED_PARTITION),
                        eq(BUCKETS),
                        isA(OffsetSpec.LatestSpec.class)))
                .thenReturn(offsetsResult(failedFuture(failure)));

        assertThatThrownBy(() -> generator.generateTableSplits(tableInfo(primaryKey)))
                .isInstanceOf(FlussRuntimeException.class)
                .hasRootCause(failure);
    }

    @Test
    void testPropagateUnrelatedKvSnapshotFailure() {
        NetworkException failure = new NetworkException("coordinator disconnected");
        when(admin.getLatestKvSnapshots(TABLE_PATH, DELETED_PARTITION))
                .thenReturn(failedFuture(failure));

        assertThatThrownBy(() -> generator.generateTableSplits(tableInfo(true)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasRootCause(failure);
    }

    private static void assertHealthySplit(TieringSplit split) {
        assertThat(split.getTableBucket())
                .isEqualTo(new TableBucket(TABLE_ID, HEALTHY_PARTITION_ID, 0));
        assertThat(split.getPartitionName()).isEqualTo(HEALTHY_PARTITION);
        assertThat(split).isInstanceOf(TieringLogSplit.class);
        TieringLogSplit logSplit = (TieringLogSplit) split;
        assertThat(logSplit.getStartingOffset()).isEqualTo(EARLIEST_OFFSET);
        assertThat(logSplit.getStoppingOffset()).isEqualTo(10L);
    }

    private static PartitionInfo partition(long partitionId, String partitionName) {
        return new PartitionInfo(
                partitionId,
                ResolvedPartitionSpec.fromPartitionValue("dt", partitionName),
                null,
                1);
    }

    private static TableInfo tableInfo(boolean primaryKey) {
        Schema.Builder schema =
                Schema.newBuilder().column("id", DataTypes.INT()).column("dt", DataTypes.STRING());
        if (primaryKey) {
            schema.primaryKey("id", "dt");
        }
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema.build())
                        .partitionedBy("dt")
                        .distributedBy(1, "id")
                        .build();
        return TableInfo.of(TABLE_PATH, TABLE_ID, 0, descriptor, "file:/repro", 0L, 0L);
    }

    private static ListOffsetsResult offsetsResult(CompletableFuture<Long> bucketOffset) {
        return new ListOffsetsResult(Collections.singletonMap(0, bucketOffset));
    }

    private static CompletableFuture<KvSnapshots> emptySnapshots(long partitionId) {
        return CompletableFuture.completedFuture(
                new KvSnapshots(
                        TABLE_ID, partitionId, Collections.emptyMap(), Collections.emptyMap()));
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable failure) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(failure);
        return future;
    }
}
