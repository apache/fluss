/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link IcebergSplitPlanner}. */
class IcebergSplitPlannerTest extends IcebergSourceTestBase {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLogTablePlan(boolean isPartitioned) throws Exception {
        // prepare iceberg log table
        TablePath tablePath =
                TablePath.of(
                        DEFAULT_DB, "log_" + (isPartitioned ? "partitioned_" : "") + DEFAULT_TABLE);
        Schema schema =
                new Schema(
                        optional(1, "c1", Types.IntegerType.get()),
                        optional(2, "c2", Types.StringType.get()),
                        optional(3, "c3", Types.StringType.get()));
        PartitionSpec partitionSpec =
                isPartitioned
                        ? PartitionSpec.builderFor(schema).identity("c2").bucket("c1", 2).build()
                        : PartitionSpec.builderFor(schema).bucket("c1", 2).build();
        createTable(tablePath, schema, partitionSpec);

        // write data
        Table table = getTable(tablePath);
        GenericRecord record1 = createIcebergRecord(schema, 12, "a", "A");
        GenericRecord record2 = createIcebergRecord(schema, 13, "b", "B");

        writeRecord(table, Collections.singletonList(record1), isPartitioned ? "a" : null, 0);
        writeRecord(table, Collections.singletonList(record2), isPartitioned ? "b" : null, 1);

        // refresh table
        table.refresh();
        Snapshot snapshot = table.currentSnapshot();

        LakeSource<IcebergSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        List<IcebergSplit> icebergSplits = lakeSource.createPlanner(snapshot::snapshotId).plan();
        assertThat(icebergSplits.size()).isEqualTo(2);
        // Log table with bucket-aware
        assertThat(icebergSplits.stream().map(IcebergSplit::bucket))
                .containsExactlyInAnyOrder(0, 1);
        if (isPartitioned) {
            assertThat(icebergSplits.stream().map(IcebergSplit::partition))
                    .containsExactlyInAnyOrder(
                            Collections.singletonList("a"), Collections.singletonList("b"));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testBucketUnawareLogTablePlan(boolean isPartitioned) throws Exception {
        // prepare iceberg table which corresponding to a Fluss bucket unaware table log table
        TablePath tablePath =
                TablePath.of(
                        DEFAULT_DB,
                        "log_"
                                + (isPartitioned ? "partitioned_" : "")
                                + DEFAULT_TABLE
                                + "_fluss_bucket");
        Schema schema =
                new Schema(
                        optional(1, "c1", Types.IntegerType.get()),
                        optional(2, "c2", Types.StringType.get()),
                        optional(3, "c3", Types.StringType.get()),
                        // System columns
                        required(14, BUCKET_COLUMN_NAME, Types.IntegerType.get()),
                        required(15, OFFSET_COLUMN_NAME, Types.LongType.get()),
                        required(16, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone()));
        PartitionSpec partitionSpec =
                isPartitioned
                        ? PartitionSpec.builderFor(schema)
                                .identity("c2")
                                .identity(BUCKET_COLUMN_NAME)
                                .build()
                        : PartitionSpec.builderFor(schema).identity(BUCKET_COLUMN_NAME).build();
        createTable(tablePath, schema, partitionSpec);

        // write data
        Table table = getTable(tablePath);
        GenericRecord record1 =
                createIcebergRecord(
                        schema, 12, "a", "A", 0, 100L, OffsetDateTime.now(ZoneOffset.UTC));
        GenericRecord record2 =
                createIcebergRecord(
                        schema, 13, "b", "B", 1, 200L, OffsetDateTime.now(ZoneOffset.UTC));

        writeRecord(table, Collections.singletonList(record1), isPartitioned ? "a" : null, 0);
        writeRecord(table, Collections.singletonList(record2), isPartitioned ? "b" : null, 1);

        // refresh table
        table.refresh();
        Snapshot snapshot = table.currentSnapshot();

        LakeSource<IcebergSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        List<IcebergSplit> icebergSplits = lakeSource.createPlanner(snapshot::snapshotId).plan();
        assertThat(icebergSplits.size()).isEqualTo(2);
        // Log table is not bucket-aware
        assertThat(icebergSplits.get(0).bucket()).isEqualTo(-1);
        assertThat(icebergSplits.get(1).bucket()).isEqualTo(-1);
        if (isPartitioned) {
            assertThat(icebergSplits.stream().map(IcebergSplit::partition))
                    .containsExactlyInAnyOrder(
                            Collections.singletonList("a"), Collections.singletonList("b"));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLogTablePlanKeepsWriteOrderWithinBucket(boolean isPartitioned) throws Exception {
        TablePath tablePath =
                TablePath.of(
                        DEFAULT_DB,
                        "log_write_order_" + (isPartitioned ? "partitioned_" : "") + DEFAULT_TABLE);
        Schema schema =
                new Schema(
                        optional(1, "c1", Types.IntegerType.get()),
                        optional(2, "c2", Types.StringType.get()));
        PartitionSpec partitionSpec =
                isPartitioned
                        ? PartitionSpec.builderFor(schema).identity("c2").bucket("c1", 2).build()
                        : PartitionSpec.builderFor(schema).bucket("c1", 2).build();
        createTable(tablePath, schema, partitionSpec);
        Table table = getTable(tablePath);
        List<String> partitions =
                isPartitioned ? Arrays.asList("p1", "p2") : Collections.singletonList(null);

        // Each tiering round is one Iceberg commit. Interleave commits across partitions and
        // buckets, so the snapshot has one manifest per commit and every bucket has several files.
        Map<String, String> bucketByFile = new HashMap<>();
        Map<String, List<String>> expectedFilesByBucket = new HashMap<>();
        for (int round = 0; round < 5; round++) {
            for (String partition : partitions) {
                for (int bucket = 0; bucket < 2; bucket++) {
                    DataFile dataFile = writeDataFile(table, schema, partition, bucket, round);
                    table.newAppend().appendFile(dataFile).commit();
                    recordFile(bucketByFile, expectedFilesByBucket, partition, bucket, dataFile);
                }
            }
        }
        // One commit holding several files of the same bucket, which share a sequence number. As
        // in a tiering round, they are rolled by a single writer.
        AppendFiles append = table.newAppend();
        for (DataFile dataFile : writeRollingDataFiles(table, schema, partitions.get(0), 0, 3)) {
            append.appendFile(dataFile);
            recordFile(bucketByFile, expectedFilesByBucket, partitions.get(0), 0, dataFile);
        }
        append.commit();
        table.refresh();
        long snapshotId = table.currentSnapshot().snapshotId();

        // Iceberg's planning order varies between runs, so plan several times.
        LakeSource<IcebergSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        for (int run = 0; run < 20; run++) {
            Map<String, List<String>> plannedFilesByBucket = new HashMap<>();
            for (IcebergSplit split : lakeSource.createPlanner(() -> snapshotId).plan()) {
                String file = split.fileScanTask().file().location();
                assertThat(bucketKey(split.partition(), split.bucket()))
                        .isEqualTo(bucketByFile.get(file));
                plannedFilesByBucket
                        .computeIfAbsent(bucketByFile.get(file), k -> new ArrayList<>())
                        .add(file);
            }
            assertThat(plannedFilesByBucket).isEqualTo(expectedFilesByBucket);
        }
    }

    @Test
    void testSortKeepsRolledFilesOfOneCommitInWriteOrder() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "log_rolled_files_" + DEFAULT_TABLE);
        Schema schema =
                new Schema(
                        optional(1, "c1", Types.IntegerType.get()),
                        optional(2, "c2", Types.StringType.get()));
        createTable(tablePath, schema, PartitionSpec.builderFor(schema).bucket("c1", 2).build());
        Table table = getTable(tablePath);
        List<DataFile> dataFiles = writeRollingDataFiles(table, schema, null, 0, 4);
        AppendFiles append = table.newAppend();
        dataFiles.forEach(append::appendFile);
        append.commit();
        table.refresh();

        // The files share a sequence number. Iceberg may list them in any order when the commit
        // spans several manifests, which a shuffle stands in for.
        assertSortRestoresWriteOrder(tablePath, table, null, locationsOf(dataFiles));
    }

    @Test
    void testSortOrdersLegacyTableByOffset() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "log_legacy_offset_" + DEFAULT_TABLE);
        Schema schema =
                new Schema(
                        optional(1, "c1", Types.IntegerType.get()),
                        required(2, OFFSET_COLUMN_NAME, Types.LongType.get()));
        createTable(tablePath, schema, PartitionSpec.builderFor(schema).bucket("c1", 2).build());
        Table table = getTable(tablePath);
        // Files written by separate writers, as compaction does for separate file groups, so
        // neither their sequence number nor their file name tells their order apart.
        List<DataFile> dataFiles = new ArrayList<>();
        AppendFiles append = table.newAppend();
        for (int i = 0; i < 4; i++) {
            try (TaskWriter<Record> taskWriter =
                    TaskWriterFactory.createTaskWriter(table, null, 0)) {
                for (long offset = i * 10L; offset < i * 10L + 3; offset++) {
                    taskWriter.write(createIcebergRecord(schema, 1, offset));
                }
                DataFile[] files = taskWriter.dataFiles();
                assertThat(files).hasSize(1);
                dataFiles.add(files[0]);
                append.appendFile(files[0]);
            }
        }
        append.commit();
        table.refresh();

        assertSortRestoresWriteOrder(
                tablePath,
                table,
                table.schema().findField(OFFSET_COLUMN_NAME).fieldId(),
                locationsOf(dataFiles));
    }

    @Test
    void testOnlyIncludesFilterColumnStats() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_filter_column_stats");
        Schema schema =
                new Schema(
                        optional(1, "c1", Types.IntegerType.get()),
                        optional(2, "c2", Types.StringType.get()),
                        optional(3, "c3", Types.StringType.get()));
        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).bucket("c1", 2).build();
        createTable(tablePath, schema, partitionSpec);

        Table table = getTable(tablePath);
        GenericRecord record = createIcebergRecord(schema, 12, "a", "A");
        writeRecord(table, Collections.singletonList(record), null, 0);
        table.refresh();
        Snapshot snapshot = table.currentSnapshot();

        PredicateBuilder predicateBuilder =
                new PredicateBuilder(RowType.of(new IntType(), new StringType(), new StringType()));
        LakeSource<IcebergSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        lakeSource.withFilters(Collections.singletonList(predicateBuilder.greaterOrEqual(0, 10)));
        List<IcebergSplit> icebergSplits = lakeSource.createPlanner(snapshot::snapshotId).plan();

        assertThat(icebergSplits).hasSize(1);
        assertThat(icebergSplits.get(0).fileScanTask().file().lowerBounds().keySet())
                .containsOnly(1);
    }

    @Test
    void testReferencedColumns() {
        // Unbound predicates expose the referenced column through their named reference.
        assertThat(referencedColumns(Expressions.greaterThanOrEqual("c1", 10))).containsOnly("c1");

        // Compound expressions merge referenced columns and deduplicate repeated columns.
        Expression compoundExpression =
                Expressions.and(
                        Expressions.or(
                                Expressions.greaterThanOrEqual("c1", 10),
                                Expressions.equal("c2", "a")),
                        Expressions.not(Expressions.lessThan("c1", 20)));
        assertThat(referencedColumns(compoundExpression)).containsOnly("c1", "c2");

        // Bound predicates expose the resolved field through their bound reference.
        Schema schema =
                new Schema(
                        optional(1, "c1", Types.IntegerType.get()),
                        optional(2, "c2", Types.StringType.get()));
        Expression boundExpression =
                Expressions.greaterThanOrEqual("c2", "a").bind(schema.asStruct(), true);
        assertThat(referencedColumns(boundExpression)).containsOnly("c2");
    }

    private DataFile writeDataFile(
            Table table, Schema schema, @Nullable String partition, int bucket, int value)
            throws Exception {
        try (TaskWriter<Record> taskWriter =
                TaskWriterFactory.createTaskWriter(table, partition, bucket)) {
            taskWriter.write(createIcebergRecord(schema, value, partition));
            DataFile[] dataFiles = taskWriter.dataFiles();
            assertThat(dataFiles).hasSize(1);
            return dataFiles[0];
        }
    }

    /** Writes {@code numFiles} files of one bucket with a single writer that rolls them. */
    private List<DataFile> writeRollingDataFiles(
            Table table, Schema schema, @Nullable String partition, int bucket, int numFiles)
            throws Exception {
        // The writer checks whether to roll every 1000 rows, so every 1000 rows go to a new file.
        table.updateProperties().set(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "1").commit();
        try (TaskWriter<Record> taskWriter =
                TaskWriterFactory.createTaskWriter(table, partition, bucket)) {
            for (int i = 0; i < numFiles * 1000; i++) {
                taskWriter.write(createIcebergRecord(schema, i, partition));
            }
            DataFile[] dataFiles = taskWriter.dataFiles();
            assertThat(dataFiles).hasSize(numFiles);
            return Arrays.asList(dataFiles);
        } finally {
            table.updateProperties().remove(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES).commit();
        }
    }

    private void assertSortRestoresWriteOrder(
            TablePath tablePath,
            Table table,
            @Nullable Integer offsetFieldId,
            List<String> expectedLocations)
            throws Exception {
        long snapshotId = table.currentSnapshot().snapshotId();
        List<IcebergSplit> splits =
                lakeStorage.createLakeSource(tablePath).createPlanner(() -> snapshotId).plan();
        Random random = new Random(42);
        for (int run = 0; run < 20; run++) {
            List<IcebergSplit> shuffled = new ArrayList<>(splits);
            Collections.shuffle(shuffled, random);
            IcebergSplitPlanner.sortInWriteOrder(shuffled, offsetFieldId);
            assertThat(shuffled)
                    .extracting(split -> split.fileScanTask().file().location())
                    .containsExactlyElementsOf(expectedLocations);
        }
    }

    private static List<String> locationsOf(List<DataFile> dataFiles) {
        List<String> locations = new ArrayList<>();
        dataFiles.forEach(dataFile -> locations.add(dataFile.location()));
        return locations;
    }

    private static void recordFile(
            Map<String, String> bucketByFile,
            Map<String, List<String>> expectedFilesByBucket,
            @Nullable String partition,
            int bucket,
            DataFile dataFile) {
        String bucketKey =
                bucketKey(
                        partition == null
                                ? Collections.emptyList()
                                : Collections.singletonList(partition),
                        bucket);
        bucketByFile.put(dataFile.location(), bucketKey);
        expectedFilesByBucket
                .computeIfAbsent(bucketKey, k -> new ArrayList<>())
                .add(dataFile.location());
    }

    private static String bucketKey(List<String> partition, int bucket) {
        return partition + "/" + bucket;
    }

    private Set<String> referencedColumns(Expression expression) {
        return new IcebergSplitPlanner(
                        new Configuration(),
                        TablePath.of(DEFAULT_DB, "test_referenced_columns"),
                        0,
                        null)
                .referencedColumns(expression);
    }
}
