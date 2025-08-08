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

package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.committer.CommittedLakeSnapshot;
import com.alibaba.fluss.lake.committer.LakeCommitter;
import com.alibaba.fluss.lake.lance.LanceConfig;
import com.alibaba.fluss.lake.lance.utils.LanceArrowUtils;
import com.alibaba.fluss.lake.lance.utils.LanceDatasetAdapter;
import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.lake.writer.LakeWriter;
import com.alibaba.fluss.lake.writer.WriterInitContext;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.GenericRecord;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.utils.types.Tuple2;

import com.lancedb.lance.WriteParams;
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

import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/** The UT for tiering to Lance via {@link LanceLakeTieringFactory}. */
public class LanceTieringTest {
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
        return Stream.of(Arguments.of(false));
    }

    @ParameterizedTest
    @MethodSource("tieringWriteArgs")
    void testTieringWriteTable(boolean isPartitioned) throws Exception {
        int bucketNum = 3;
        TablePath tablePath = TablePath.of("lance", "logTable");
        LanceConfig config =
                LanceConfig.from(
                        configuration.toMap(),
                        tablePath.getDatabaseName(),
                        tablePath.getTableName());
        Schema schema = createTable(tablePath, isPartitioned, null, config);

        List<LanceWriteResult> lanceWriteResults = new ArrayList<>();
        SimpleVersionedSerializer<LanceWriteResult> writeResultSerializer =
                lanceLakeTieringFactory.getWriteResultSerializer();
        SimpleVersionedSerializer<LanceCommittable> committableSerializer =
                lanceLakeTieringFactory.getCommittableSerializer();

        try (LakeCommitter<LanceWriteResult, LanceCommittable> lakeCommitter =
                createLakeCommitter(tablePath)) {
            // should no any missing snapshot
            assertThat(lakeCommitter.getMissingLakeSnapshot(1L)).isNull();
        }

        Map<Tuple2<String, Integer>, List<LogRecord>> recordsByBucket = new HashMap<>();
        List<String> partitions =
                isPartitioned ? Arrays.asList("p1", "p2", "p3") : Collections.singletonList(null);
        // first, write data
        for (int bucket = 0; bucket < bucketNum; bucket++) {
            for (String partition : partitions) {
                try (LakeWriter<LanceWriteResult> lakeWriter =
                        createLakeWriter(tablePath, bucket, partition, schema)) {
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
                createLakeCommitter(tablePath)) {
            // serialize/deserialize committable
            LanceCommittable lanceCommittable = lakeCommitter.toCommittable(lanceWriteResults);
            byte[] serialized = committableSerializer.serialize(lanceCommittable);
            lanceCommittable =
                    committableSerializer.deserialize(
                            committableSerializer.getVersion(), serialized);
            long snapshot = lakeCommitter.commit(lanceCommittable);
            assertThat(snapshot).isEqualTo(1);
        }

        ArrowReader reader = LanceDatasetAdapter.getArrowReader(config);
        VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
        //        while (reader.loadNextBatch()) {
        //            System.out.print(readerRoot.contentToTSVString());
        //        }

        // then, check data
        for (int bucket = 0; bucket < 3; bucket++) {
            for (String partition : partitions) {
                reader.loadNextBatch();
                Tuple2<String, Integer> partitionBucket = Tuple2.of(partition, bucket);
                List<LogRecord> expectRecords = recordsByBucket.get(partitionBucket);
                verifyLogTableRecords(readerRoot, expectRecords, bucket, isPartitioned, partition);
            }
        }
        assertThat(reader.loadNextBatch()).isFalse();

        // then, let's verify getMissingLakeSnapshot works
        try (LakeCommitter<LanceWriteResult, LanceCommittable> lakeCommitter =
                createLakeCommitter(tablePath)) {
            // use snapshot id 0 as the known snapshot id
            CommittedLakeSnapshot committedLakeSnapshot = lakeCommitter.getMissingLakeSnapshot(0L);
            assertThat(committedLakeSnapshot).isNotNull();
            Map<Tuple2<String, Integer>, Long> offsets = committedLakeSnapshot.getLogEndOffsets();
            for (int bucket = 0; bucket < 3; bucket++) {
                for (String partition : partitions) {
                    // we only write 10 records, so expected log offset should be 9
                    assertThat(offsets.get(Tuple2.of(partition, bucket))).isEqualTo(9);
                }
            }
            assertThat(committedLakeSnapshot.getLakeSnapshotId()).isOne();

            // use null as the known snapshot id
            CommittedLakeSnapshot committedLakeSnapshot2 =
                    lakeCommitter.getMissingLakeSnapshot(null);
            assertThat(committedLakeSnapshot2).isEqualTo(committedLakeSnapshot);

            // use snapshot id 1 as the known snapshot id
            committedLakeSnapshot = lakeCommitter.getMissingLakeSnapshot(1L);
            // no any missing committed offset since the latest snapshot is 1L
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
            // check system columns: __bucket, __offset, __timestamp
            assertThat((int) (root.getVector(3).getObject(i))).isEqualTo(expectBucket);
            assertThat((long) (root.getVector(4).getObject(i))).isEqualTo(expectRecord.logOffset());
        }
    }

    private LakeCommitter<LanceWriteResult, LanceCommittable> createLakeCommitter(
            TablePath tablePath) throws IOException {
        return lanceLakeTieringFactory.createLakeCommitter(() -> tablePath);
    }

    private LakeWriter<LanceWriteResult> createLakeWriter(
            TablePath tablePath, int bucket, @Nullable String partition, Schema schema)
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
                    public com.alibaba.fluss.metadata.Schema schema() {
                        return schema;
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

    private Schema createTable(
            TablePath tablePath,
            boolean isPartitioned,
            @Nullable Integer numBuckets,
            LanceConfig config)
            throws Exception {
        List<Schema.Column> columns = new ArrayList<>();
        columns.add(new Schema.Column("c1", DataTypes.INT()));
        columns.add(new Schema.Column("c2", DataTypes.STRING()));
        columns.add(new Schema.Column("c3", DataTypes.STRING()));

        doCreateLanceTable(tablePath, columns, config);
        return Schema.newBuilder().fromColumns(columns).build();
    }

    private void doCreateLanceTable(
            TablePath tablePath, List<Schema.Column> columns, LanceConfig config) throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder().fromColumns(columns);
        schemaBuilder.column(BUCKET_COLUMN_NAME, DataTypes.INT());
        schemaBuilder.column(OFFSET_COLUMN_NAME, DataTypes.BIGINT());
        schemaBuilder.column(TIMESTAMP_COLUMN_NAME, DataTypes.TIMESTAMP_LTZ());
        Schema schema = schemaBuilder.build();
        WriteParams params = LanceConfig.genWriteParamsFromConfig(config);
        LanceDatasetAdapter.createDataset(
                config.getDatasetUri(), LanceArrowUtils.toArrowSchema(schema.getRowType()), params);
    }
}
