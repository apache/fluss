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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.fluss.flink.tiering.committer.TieringCommitOperator.toBucketOffsetsProperty;
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
                createLakeCommitter(tablePath)) {
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
        List<String> partitionKeys = isPartitioned ? Arrays.asList("c3") : null;
        Map<TableBucket, Long> tableBucketOffsets = new HashMap<>();
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
                    tableBucketOffsets.put(new TableBucket(0, entry.getKey(), bucket), 10L);
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
                createLakeCommitter(tablePath)) {
            // serialize/deserialize committable
            LanceCommittable lanceCommittable = lakeCommitter.toCommittable(lanceWriteResults);
            byte[] serialized = committableSerializer.serialize(lanceCommittable);
            lanceCommittable =
                    committableSerializer.deserialize(
                            committableSerializer.getVersion(), serialized);
            long snapshot =
                    lakeCommitter.commit(
                            lanceCommittable,
                            toBucketOffsetsProperty(
                                    tableBucketOffsets, partitionIdAndName, partitionKeys));
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
                createLakeCommitter(tablePath)) {
            // use snapshot id 1 as the known snapshot id
            CommittedLakeSnapshot committedLakeSnapshot = lakeCommitter.getMissingLakeSnapshot(1L);
            assertThat(committedLakeSnapshot).isNotNull();
            Map<Tuple2<Long, Integer>, Long> offsets = committedLakeSnapshot.getLogEndOffsets();
            for (int bucket = 0; bucket < 3; bucket++) {
                for (Long partitionId : partitionIdAndName.keySet()) {
                    // we only write 10 records, so expected log offset should be 10
                    assertThat(offsets.get(Tuple2.of(partitionId, bucket))).isEqualTo(10);
                }
            }
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
            TablePath tablePath) throws IOException {
        return lanceLakeTieringFactory.createLakeCommitter(() -> tablePath);
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

    private Schema createTable(LanceConfig config) throws Exception {
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
}
