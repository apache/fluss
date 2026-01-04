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

package org.apache.fluss.lake.lance.tiering;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.lance.LanceConfig;
import org.apache.fluss.lake.lance.utils.LanceArrowUtils;
import org.apache.fluss.lake.lance.utils.LanceDatasetAdapter;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.types.Tuple2;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.WriteParams;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

/** The UT for tiering to Lance via {@link LanceLakeTieringFactory}. */
class LanceTieringTest {
    private @TempDir File tempWarehouseDir;
    private LanceLakeTieringFactory lanceLakeTieringFactory;
    private Configuration configuration;

    @BeforeEach
    void beforeEach() {
        configuration = new Configuration();
        configuration.setString("warehouse", tempWarehouseDir.toString());
        lanceLakeTieringFactory = new LanceLakeTieringFactory(configuration);
    }

    private static Stream<Arguments> tieringWriteArgs() {
        return Stream.of(Arguments.of(false), Arguments.of(true));
    }

    @ParameterizedTest
    @MethodSource("tieringWriteArgs")
    void testTieringWriteTable(boolean isPartitioned) throws Exception {
        int bucketNum = 3;
        TablePath tablePath = TablePath.of("lance", "logTable");
        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("lance.batch_size", "256");
        LanceConfig config =
                LanceConfig.from(
                        configuration.toMap(),
                        customProperties,
                        tablePath.getDatabaseName(),
                        tablePath.getTableName());
        Schema schema = createTable(config);

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(bucketNum)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperties(customProperties)
                        .build();
        TableInfo tableInfo = TableInfo.of(tablePath, 0, 1, descriptor, 1L, 1L);

        List<LanceWriteResult> lanceWriteResults = new ArrayList<>();
        SimpleVersionedSerializer<LanceWriteResult> writeResultSerializer =
                lanceLakeTieringFactory.getWriteResultSerializer();
        SimpleVersionedSerializer<LanceCommittable> committableSerializer =
                lanceLakeTieringFactory.getCommittableSerializer();

        try (LakeCommitter<LanceWriteResult, LanceCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            // should no any missing snapshot
            assertThat(lakeCommitter.getMissingLakeSnapshot(2L)).isNull();
        }

        Map<Tuple2<String, Integer>, List<LogRecord>> recordsByBucket = new HashMap<>();
        Map<Long, String> partitionIdAndName =
                isPartitioned
                        ? new HashMap<Long, String>() {
                            {
                                put(1L, "p1");
                                put(2L, "p2");
                                put(3L, "p3");
                            }
                        }
                        : Collections.singletonMap(null, null);
        // first, write data
        for (int bucket = 0; bucket < bucketNum; bucket++) {
            for (Map.Entry<Long, String> entry : partitionIdAndName.entrySet()) {
                String partition = entry.getValue();
                try (LakeWriter<LanceWriteResult> lakeWriter =
                        createLakeWriter(tablePath, bucket, partition, tableInfo)) {
                    Tuple2<String, Integer> partitionBucket = Tuple2.of(partition, bucket);
                    Tuple2<List<LogRecord>, List<LogRecord>> writeAndExpectRecords =
                            genLogTableRecords(partition, bucket, 10);
                    List<LogRecord> writtenRecords = writeAndExpectRecords.f0;
                    List<LogRecord> expectRecords = writeAndExpectRecords.f1;
                    recordsByBucket.put(partitionBucket, expectRecords);
                    for (LogRecord logRecord : writtenRecords) {
                        lakeWriter.write(logRecord);
                    }
                    // serialize/deserialize writeResult
                    LanceWriteResult lanceWriteResult = lakeWriter.complete();
                    byte[] serialized = writeResultSerializer.serialize(lanceWriteResult);
                    lanceWriteResults.add(
                            writeResultSerializer.deserialize(
                                    writeResultSerializer.getVersion(), serialized));
                }
            }
        }

        // second, commit data
        try (LakeCommitter<LanceWriteResult, LanceCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            // serialize/deserialize committable
            LanceCommittable lanceCommittable = lakeCommitter.toCommittable(lanceWriteResults);
            byte[] serialized = committableSerializer.serialize(lanceCommittable);
            lanceCommittable =
                    committableSerializer.deserialize(
                            committableSerializer.getVersion(), serialized);
            Map<String, String> snapshotProperties =
                    Collections.singletonMap(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, "offsets");
            long snapshot = lakeCommitter.commit(lanceCommittable, snapshotProperties);
            // lance dataset version starts from 1
            assertThat(snapshot).isEqualTo(2);
        }

        try (Dataset dataset =
                Dataset.open(
                        new RootAllocator(),
                        config.getDatasetUri(),
                        LanceConfig.genReadOptionFromConfig(config))) {
            ArrowReader reader = dataset.newScan().scanBatches();
            VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();

            // then, check data
            for (int bucket = 0; bucket < 3; bucket++) {
                for (String partition : partitionIdAndName.values()) {
                    reader.loadNextBatch();
                    Tuple2<String, Integer> partitionBucket = Tuple2.of(partition, bucket);
                    List<LogRecord> expectRecords = recordsByBucket.get(partitionBucket);
                    verifyLogTableRecords(
                            readerRoot, expectRecords, bucket, isPartitioned, partition);
                }
            }
            assertThat(reader.loadNextBatch()).isFalse();
        }

        // then, let's verify getMissingLakeSnapshot works
        try (LakeCommitter<LanceWriteResult, LanceCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            // use snapshot id 1 as the known snapshot id
            CommittedLakeSnapshot committedLakeSnapshot = lakeCommitter.getMissingLakeSnapshot(1L);
            assertThat(committedLakeSnapshot).isNotNull();
            assertThat(committedLakeSnapshot.getLakeSnapshotId()).isEqualTo(2L);

            // use null as the known snapshot id
            CommittedLakeSnapshot committedLakeSnapshot2 =
                    lakeCommitter.getMissingLakeSnapshot(null);
            assertThat(committedLakeSnapshot2).isEqualTo(committedLakeSnapshot);

            // use snapshot id 2 as the known snapshot id
            committedLakeSnapshot = lakeCommitter.getMissingLakeSnapshot(2L);
            // no any missing committed offset since the latest snapshot is 2L
            assertThat(committedLakeSnapshot).isNull();
        }
    }

    private void verifyLogTableRecords(
            VectorSchemaRoot root,
            List<LogRecord> expectRecords,
            int expectBucket,
            boolean isPartitioned,
            @Nullable String partition)
            throws Exception {
        assertThat(root.getRowCount()).isEqualTo(expectRecords.size());
        for (int i = 0; i < expectRecords.size(); i++) {
            LogRecord expectRecord = expectRecords.get(i);
            // check business columns:
            assertThat((int) (root.getVector(0).getObject(i)))
                    .isEqualTo(expectRecord.getRow().getInt(0));
            assertThat(((VarCharVector) root.getVector(1)).getObject(i).toString())
                    .isEqualTo(expectRecord.getRow().getString(1).toString());
            assertThat(((VarCharVector) root.getVector(2)).getObject(i).toString())
                    .isEqualTo(expectRecord.getRow().getString(2).toString());
        }
    }

    private LakeCommitter<LanceWriteResult, LanceCommittable> createLakeCommitter(
            TablePath tablePath, TableInfo tableInfo) throws IOException {
        return lanceLakeTieringFactory.createLakeCommitter(
                new CommitterInitContext() {
                    @Override
                    public TablePath tablePath() {
                        return tablePath;
                    }

                    @Override
                    public TableInfo tableInfo() {
                        return tableInfo;
                    }

                    @Override
                    public Configuration lakeTieringConfig() {
                        return new Configuration();
                    }
                });
    }

    private LakeWriter<LanceWriteResult> createLakeWriter(
            TablePath tablePath, int bucket, @Nullable String partition, TableInfo tableInfo)
            throws IOException {
        return lanceLakeTieringFactory.createLakeWriter(
                new WriterInitContext() {
                    @Override
                    public TablePath tablePath() {
                        return tablePath;
                    }

                    @Override
                    public TableBucket tableBucket() {
                        // don't care about tableId & partitionId
                        return new TableBucket(0, 0L, bucket);
                    }

                    @Nullable
                    @Override
                    public String partition() {
                        return partition;
                    }

                    @Override
                    public TableInfo tableInfo() {
                        return tableInfo;
                    }
                });
    }

    private Tuple2<List<LogRecord>, List<LogRecord>> genLogTableRecords(
            @Nullable String partition, int bucket, int numRecords) {
        List<LogRecord> logRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            GenericRow genericRow;
            if (partition != null) {
                // Partitioned table: include partition field in data
                genericRow = new GenericRow(3); // c1, c2, c3(partition)
                genericRow.setField(0, i);
                genericRow.setField(1, BinaryString.fromString("bucket" + bucket + "_" + i));
                genericRow.setField(2, BinaryString.fromString(partition)); // partition field
            } else {
                // Non-partitioned table
                genericRow = new GenericRow(3);
                genericRow.setField(0, i);
                genericRow.setField(1, BinaryString.fromString("bucket" + bucket + "_" + i));
                genericRow.setField(2, BinaryString.fromString("bucket" + bucket));
            }
            LogRecord logRecord =
                    new GenericRecord(
                            i, System.currentTimeMillis(), ChangeType.APPEND_ONLY, genericRow);
            logRecords.add(logRecord);
        }
        return Tuple2.of(logRecords, logRecords);
    }

    private Schema createTable(LanceConfig config) {
        List<Schema.Column> columns = new ArrayList<>();
        columns.add(new Schema.Column("c1", DataTypes.INT()));
        columns.add(new Schema.Column("c2", DataTypes.STRING()));
        columns.add(new Schema.Column("c3", DataTypes.STRING()));
        Schema.Builder schemaBuilder = Schema.newBuilder().fromColumns(columns);
        Schema schema = schemaBuilder.build();
        WriteParams params = LanceConfig.genWriteParamsFromConfig(config);
        LanceDatasetAdapter.createDataset(
                config.getDatasetUri(), LanceArrowUtils.toArrowSchema(schema.getRowType()), params);

        return schema;
    }

    @Test
    void testArrayTypeInt() throws Exception {
        TablePath tablePath = TablePath.of("lance", "arrayTable");
        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("lance.batch_size", "256");
        LanceConfig config =
                LanceConfig.from(
                        configuration.toMap(),
                        customProperties,
                        tablePath.getDatabaseName(),
                        tablePath.getTableName());

        // Create schema with ARRAY<INT> column
        List<Schema.Column> columns = new ArrayList<>();
        columns.add(new Schema.Column("id", DataTypes.INT()));
        columns.add(new Schema.Column("int_array", DataTypes.ARRAY(DataTypes.INT())));
        Schema schema = Schema.newBuilder().fromColumns(columns).build();

        WriteParams params = LanceConfig.genWriteParamsFromConfig(config);
        LanceDatasetAdapter.createDataset(
                config.getDatasetUri(), LanceArrowUtils.toArrowSchema(schema.getRowType()), params);

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperties(customProperties)
                        .build();
        TableInfo tableInfo = TableInfo.of(tablePath, 0, 1, descriptor, 1L, 1L);

        List<LanceWriteResult> lanceWriteResults = new ArrayList<>();
        Map<TableBucket, Long> tableBucketOffsets = new HashMap<>();

        // Write data with arrays
        try (LakeWriter<LanceWriteResult> lakeWriter =
                createLakeWriter(tablePath, 0, null, tableInfo)) {
            List<LogRecord> logRecords = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                GenericRow row = new GenericRow(2);
                row.setField(0, i);
                // Create array with values [i, i+1, i+2]
                row.setField(1, new GenericArray(new int[] {i, i + 1, i + 2}));
                logRecords.add(
                        new GenericRecord(
                                i, System.currentTimeMillis(), ChangeType.APPEND_ONLY, row));
            }
            tableBucketOffsets.put(new TableBucket(0, null, 0), 10L);
            for (LogRecord logRecord : logRecords) {
                lakeWriter.write(logRecord);
            }
            lanceWriteResults.add(lakeWriter.complete());
        }

        // Commit data
        try (LakeCommitter<LanceWriteResult, LanceCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            LanceCommittable lanceCommittable = lakeCommitter.toCommittable(lanceWriteResults);
            long snapshot =
                    lakeCommitter.commit(
                            lanceCommittable, toBucketOffsetsProperty(tableBucketOffsets));
            assertThat(snapshot).isEqualTo(2);
        }

        // Verify dataset was created successfully
        try (Dataset dataset =
                Dataset.open(
                        new RootAllocator(),
                        config.getDatasetUri(),
                        LanceConfig.genReadOptionFromConfig(config))) {
            assertThat(dataset).isNotNull();
        }
    }

    @Test
    void testArrayTypeString() throws Exception {
        TablePath tablePath = TablePath.of("lance", "stringArrayTable");
        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("lance.batch_size", "256");
        LanceConfig config =
                LanceConfig.from(
                        configuration.toMap(),
                        customProperties,
                        tablePath.getDatabaseName(),
                        tablePath.getTableName());

        // Create schema with ARRAY<STRING> column
        List<Schema.Column> columns = new ArrayList<>();
        columns.add(new Schema.Column("id", DataTypes.INT()));
        columns.add(new Schema.Column("string_array", DataTypes.ARRAY(DataTypes.STRING())));
        Schema schema = Schema.newBuilder().fromColumns(columns).build();

        WriteParams params = LanceConfig.genWriteParamsFromConfig(config);
        LanceDatasetAdapter.createDataset(
                config.getDatasetUri(), LanceArrowUtils.toArrowSchema(schema.getRowType()), params);

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperties(customProperties)
                        .build();
        TableInfo tableInfo = TableInfo.of(tablePath, 0, 1, descriptor, 1L, 1L);

        List<LanceWriteResult> lanceWriteResults = new ArrayList<>();
        Map<TableBucket, Long> tableBucketOffsets = new HashMap<>();

        // Write data with string arrays
        try (LakeWriter<LanceWriteResult> lakeWriter =
                createLakeWriter(tablePath, 0, null, tableInfo)) {
            List<LogRecord> logRecords = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                GenericRow row = new GenericRow(2);
                row.setField(0, i);
                row.setField(
                        1,
                        new GenericArray(
                                new BinaryString[] {
                                    BinaryString.fromString("str_" + i),
                                    BinaryString.fromString("str_" + (i + 1))
                                }));
                logRecords.add(
                        new GenericRecord(
                                i, System.currentTimeMillis(), ChangeType.APPEND_ONLY, row));
            }
            tableBucketOffsets.put(new TableBucket(0, null, 0), 10L);
            for (LogRecord logRecord : logRecords) {
                lakeWriter.write(logRecord);
            }
            lanceWriteResults.add(lakeWriter.complete());
        }

        // Commit data
        try (LakeCommitter<LanceWriteResult, LanceCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            LanceCommittable lanceCommittable = lakeCommitter.toCommittable(lanceWriteResults);
            long snapshot =
                    lakeCommitter.commit(
                            lanceCommittable, toBucketOffsetsProperty(tableBucketOffsets));
            assertThat(snapshot).isEqualTo(2);
        }

        // Verify dataset was created successfully
        try (Dataset dataset =
                Dataset.open(
                        new RootAllocator(),
                        config.getDatasetUri(),
                        LanceConfig.genReadOptionFromConfig(config))) {
            assertThat(dataset).isNotNull();
        }
    }

    @Test
    void testArrayTypeNullable() throws Exception {
        TablePath tablePath = TablePath.of("lance", "nullableArrayTable");
        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("lance.batch_size", "256");
        LanceConfig config =
                LanceConfig.from(
                        configuration.toMap(),
                        customProperties,
                        tablePath.getDatabaseName(),
                        tablePath.getTableName());

        // Create schema with ARRAY<INT> column (elements can be null using Object[] arrays)
        List<Schema.Column> columns = new ArrayList<>();
        columns.add(new Schema.Column("id", DataTypes.INT()));
        columns.add(new Schema.Column("nullable_array", DataTypes.ARRAY(DataTypes.INT())));
        Schema schema = Schema.newBuilder().fromColumns(columns).build();

        WriteParams params = LanceConfig.genWriteParamsFromConfig(config);
        LanceDatasetAdapter.createDataset(
                config.getDatasetUri(), LanceArrowUtils.toArrowSchema(schema.getRowType()), params);

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperties(customProperties)
                        .build();
        TableInfo tableInfo = TableInfo.of(tablePath, 0, 1, descriptor, 1L, 1L);

        List<LanceWriteResult> lanceWriteResults = new ArrayList<>();
        Map<TableBucket, Long> tableBucketOffsets = new HashMap<>();

        // Write data with null arrays and null elements
        try (LakeWriter<LanceWriteResult> lakeWriter =
                createLakeWriter(tablePath, 0, null, tableInfo)) {
            List<LogRecord> logRecords = new ArrayList<>();

            // Row with null array
            GenericRow row1 = new GenericRow(2);
            row1.setField(0, 0);
            row1.setField(1, null);
            logRecords.add(
                    new GenericRecord(0, System.currentTimeMillis(), ChangeType.APPEND_ONLY, row1));

            // Row with empty array
            GenericRow row2 = new GenericRow(2);
            row2.setField(0, 1);
            row2.setField(1, new GenericArray(new Integer[] {}));
            logRecords.add(
                    new GenericRecord(1, System.currentTimeMillis(), ChangeType.APPEND_ONLY, row2));

            // Row with array containing null elements
            GenericRow row3 = new GenericRow(2);
            row3.setField(0, 2);
            row3.setField(1, new GenericArray(new Integer[] {1, null, 3}));
            logRecords.add(
                    new GenericRecord(2, System.currentTimeMillis(), ChangeType.APPEND_ONLY, row3));

            tableBucketOffsets.put(new TableBucket(0, null, 0), 3L);
            for (LogRecord logRecord : logRecords) {
                lakeWriter.write(logRecord);
            }
            lanceWriteResults.add(lakeWriter.complete());
        }

        // Commit data
        try (LakeCommitter<LanceWriteResult, LanceCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            LanceCommittable lanceCommittable = lakeCommitter.toCommittable(lanceWriteResults);
            long snapshot =
                    lakeCommitter.commit(
                            lanceCommittable, toBucketOffsetsProperty(tableBucketOffsets));
            assertThat(snapshot).isEqualTo(2);
        }

        // Verify dataset was created successfully
        try (Dataset dataset =
                Dataset.open(
                        new RootAllocator(),
                        config.getDatasetUri(),
                        LanceConfig.genReadOptionFromConfig(config))) {
            assertThat(dataset).isNotNull();
        }
    }

    @Test
    void testNestedArrayType() throws Exception {
        TablePath tablePath = TablePath.of("lance", "nestedArrayTable");
        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("lance.batch_size", "256");
        LanceConfig config =
                LanceConfig.from(
                        configuration.toMap(),
                        customProperties,
                        tablePath.getDatabaseName(),
                        tablePath.getTableName());

        // Create schema with ARRAY<ARRAY<INT>> column
        List<Schema.Column> columns = new ArrayList<>();
        columns.add(new Schema.Column("id", DataTypes.INT()));
        columns.add(
                new Schema.Column(
                        "nested_array", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()))));
        Schema schema = Schema.newBuilder().fromColumns(columns).build();

        WriteParams params = LanceConfig.genWriteParamsFromConfig(config);
        LanceDatasetAdapter.createDataset(
                config.getDatasetUri(), LanceArrowUtils.toArrowSchema(schema.getRowType()), params);

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperties(customProperties)
                        .build();
        TableInfo tableInfo = TableInfo.of(tablePath, 0, 1, descriptor, 1L, 1L);

        List<LanceWriteResult> lanceWriteResults = new ArrayList<>();
        Map<TableBucket, Long> tableBucketOffsets = new HashMap<>();

        // Write data with nested arrays
        try (LakeWriter<LanceWriteResult> lakeWriter =
                createLakeWriter(tablePath, 0, null, tableInfo)) {
            List<LogRecord> logRecords = new ArrayList<>();
            GenericRow row = new GenericRow(2);
            row.setField(0, 0);

            // Create nested array: [[1, 2], [3, 4, 5], [6]]
            GenericArray inner1 = new GenericArray(new int[] {1, 2});
            GenericArray inner2 = new GenericArray(new int[] {3, 4, 5});
            GenericArray inner3 = new GenericArray(new int[] {6});
            GenericArray outer = new GenericArray(new Object[] {inner1, inner2, inner3});
            row.setField(1, outer);

            logRecords.add(
                    new GenericRecord(0, System.currentTimeMillis(), ChangeType.APPEND_ONLY, row));

            tableBucketOffsets.put(new TableBucket(0, null, 0), 1L);
            for (LogRecord logRecord : logRecords) {
                lakeWriter.write(logRecord);
            }
            lanceWriteResults.add(lakeWriter.complete());
        }

        // Commit data
        try (LakeCommitter<LanceWriteResult, LanceCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            LanceCommittable lanceCommittable = lakeCommitter.toCommittable(lanceWriteResults);
            long snapshot =
                    lakeCommitter.commit(
                            lanceCommittable, toBucketOffsetsProperty(tableBucketOffsets));
            assertThat(snapshot).isEqualTo(2);
        }

        // Verify dataset was created successfully
        try (Dataset dataset =
                Dataset.open(
                        new RootAllocator(),
                        config.getDatasetUri(),
                        LanceConfig.genReadOptionFromConfig(config))) {
            assertThat(dataset).isNotNull();
        }
    }

    @Test
    void testMultiplePrimitiveArrayTypes() throws Exception {
        TablePath tablePath = TablePath.of("lance", "multiArrayTable");
        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("lance.batch_size", "256");
        LanceConfig config =
                LanceConfig.from(
                        configuration.toMap(),
                        customProperties,
                        tablePath.getDatabaseName(),
                        tablePath.getTableName());

        // Create schema with multiple array type columns
        List<Schema.Column> columns = new ArrayList<>();
        columns.add(new Schema.Column("int_array", DataTypes.ARRAY(DataTypes.INT())));
        columns.add(new Schema.Column("bigint_array", DataTypes.ARRAY(DataTypes.BIGINT())));
        columns.add(new Schema.Column("double_array", DataTypes.ARRAY(DataTypes.DOUBLE())));
        columns.add(new Schema.Column("boolean_array", DataTypes.ARRAY(DataTypes.BOOLEAN())));
        columns.add(new Schema.Column("date_array", DataTypes.ARRAY(DataTypes.DATE())));
        Schema schema = Schema.newBuilder().fromColumns(columns).build();

        WriteParams params = LanceConfig.genWriteParamsFromConfig(config);
        LanceDatasetAdapter.createDataset(
                config.getDatasetUri(), LanceArrowUtils.toArrowSchema(schema.getRowType()), params);

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperties(customProperties)
                        .build();
        TableInfo tableInfo = TableInfo.of(tablePath, 0, 1, descriptor, 1L, 1L);

        List<LanceWriteResult> lanceWriteResults = new ArrayList<>();
        Map<TableBucket, Long> tableBucketOffsets = new HashMap<>();

        // Write data with multiple array types
        try (LakeWriter<LanceWriteResult> lakeWriter =
                createLakeWriter(tablePath, 0, null, tableInfo)) {
            List<LogRecord> logRecords = new ArrayList<>();
            GenericRow row = new GenericRow(5);
            row.setField(0, new GenericArray(new int[] {1, 2, 3}));
            row.setField(1, new GenericArray(new long[] {100L, 200L, 300L}));
            row.setField(2, new GenericArray(new double[] {1.1, 2.2, 3.3}));
            row.setField(3, new GenericArray(new boolean[] {true, false, true}));
            row.setField(4, new GenericArray(new Integer[] {18993, 18994, 18995}));

            logRecords.add(
                    new GenericRecord(0, System.currentTimeMillis(), ChangeType.APPEND_ONLY, row));

            tableBucketOffsets.put(new TableBucket(0, null, 0), 1L);
            for (LogRecord logRecord : logRecords) {
                lakeWriter.write(logRecord);
            }
            lanceWriteResults.add(lakeWriter.complete());
        }

        // Commit data
        try (LakeCommitter<LanceWriteResult, LanceCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            LanceCommittable lanceCommittable = lakeCommitter.toCommittable(lanceWriteResults);
            long snapshot =
                    lakeCommitter.commit(
                            lanceCommittable, toBucketOffsetsProperty(tableBucketOffsets));
            assertThat(snapshot).isEqualTo(2);
        }

        // Verify dataset was created successfully
        try (Dataset dataset =
                Dataset.open(
                        new RootAllocator(),
                        config.getDatasetUri(),
                        LanceConfig.genReadOptionFromConfig(config))) {
            assertThat(dataset).isNotNull();
        }
    }
}
