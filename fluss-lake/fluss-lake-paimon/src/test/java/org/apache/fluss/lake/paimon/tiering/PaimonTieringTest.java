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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.utils.types.Tuple2;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CloseableIterator;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.fluss.flink.tiering.committer.TieringCommitOperator.toBucketOffsetsProperty;
import static org.apache.fluss.lake.committer.BucketOffset.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.fluss.record.ChangeType.DELETE;
import static org.apache.fluss.record.ChangeType.INSERT;
import static org.apache.fluss.record.ChangeType.UPDATE_AFTER;
import static org.apache.fluss.record.ChangeType.UPDATE_BEFORE;
import static org.apache.fluss.utils.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/** The UT for tiering to Paimon via {@link PaimonLakeTieringFactory}. */
class PaimonTieringTest {

    private @TempDir File tempWarehouseDir;
    private PaimonLakeTieringFactory paimonLakeTieringFactory;
    private Catalog paimonCatalog;

    @BeforeEach
    void beforeEach() {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", tempWarehouseDir.toString());
        paimonLakeTieringFactory = new PaimonLakeTieringFactory(configuration);
        paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(configuration.toMap())));
    }

    private static Stream<Arguments> tieringWriteArgs() {
        return Stream.of(
                Arguments.of(true, true),
                Arguments.of(true, false),
                Arguments.of(false, true),
                Arguments.of(false, false));
    }

    @ParameterizedTest
    @MethodSource("tieringWriteArgs")
    void testTieringWriteTable(boolean isPrimaryKeyTable, boolean isPartitioned) throws Exception {
        int bucketNum = 3;
        TablePath tablePath =
                TablePath.of(
                        "paimon",
                        String.format(
                                "test_tiering_table_%s_%s",
                                isPrimaryKeyTable ? "primary_key" : "log",
                                isPartitioned ? "partitioned" : "non_partitioned"));
        createTable(
                tablePath, isPrimaryKeyTable, isPartitioned, isPrimaryKeyTable ? bucketNum : null);

        List<PaimonWriteResult> paimonWriteResults = new ArrayList<>();
        SimpleVersionedSerializer<PaimonWriteResult> writeResultSerializer =
                paimonLakeTieringFactory.getWriteResultSerializer();
        SimpleVersionedSerializer<PaimonCommittable> committableSerializer =
                paimonLakeTieringFactory.getCommittableSerializer();

        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath)) {
            // should no any missing snapshot
            assertThat(lakeCommitter.getMissingLakeSnapshot(1L)).isNull();
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
        Map<TableBucket, Long> tableBucketOffsets = new HashMap<>();
        // first, write data
        for (int bucket = 0; bucket < bucketNum; bucket++) {
            for (Map.Entry<Long, String> entry : partitionIdAndName.entrySet()) {
                String partition = entry.getValue();
                try (LakeWriter<PaimonWriteResult> lakeWriter =
                        createLakeWriter(tablePath, bucket, partition, entry.getKey())) {
                    Tuple2<String, Integer> partitionBucket = Tuple2.of(partition, bucket);
                    Tuple2<List<LogRecord>, List<LogRecord>> writeAndExpectRecords =
                            isPrimaryKeyTable
                                    ? genPrimaryKeyTableRecords(partition, bucket)
                                    : genLogTableRecords(partition, bucket, 10);
                    List<LogRecord> writtenRecords = writeAndExpectRecords.f0;
                    List<LogRecord> expectRecords = writeAndExpectRecords.f1;
                    recordsByBucket.put(partitionBucket, expectRecords);
                    tableBucketOffsets.put(new TableBucket(0, entry.getKey(), bucket), 10L);
                    for (LogRecord logRecord : writtenRecords) {
                        lakeWriter.write(logRecord);
                    }
                    // serialize/deserialize writeResult
                    PaimonWriteResult paimonWriteResult = lakeWriter.complete();
                    byte[] serialized = writeResultSerializer.serialize(paimonWriteResult);
                    paimonWriteResults.add(
                            writeResultSerializer.deserialize(
                                    writeResultSerializer.getVersion(), serialized));
                }
            }
        }

        // second, commit data
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath)) {
            // serialize/deserialize committable
            PaimonCommittable paimonCommittable = lakeCommitter.toCommittable(paimonWriteResults);
            byte[] serialized = committableSerializer.serialize(paimonCommittable);
            paimonCommittable =
                    committableSerializer.deserialize(
                            committableSerializer.getVersion(), serialized);
            long snapshot =
                    lakeCommitter.commit(
                            paimonCommittable,
                            toBucketOffsetsProperty(
                                    tableBucketOffsets,
                                    partitionIdAndName,
                                    getPartitionKeys(tablePath)));
            assertThat(snapshot).isEqualTo(1);
        }

        // then, check data
        for (int bucket = 0; bucket < 3; bucket++) {
            for (String partition : partitionIdAndName.values()) {
                Tuple2<String, Integer> partitionBucket = Tuple2.of(partition, bucket);
                List<LogRecord> expectRecords = recordsByBucket.get(partitionBucket);
                CloseableIterator<InternalRow> actualRecords =
                        getPaimonRows(tablePath, partition, isPrimaryKeyTable, bucket);
                verifyTableRecords(actualRecords, expectRecords, bucket, partition);
            }
        }

        // then, let's verify getMissingLakeSnapshot works
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath)) {
            // use snapshot id 0 as the known snapshot id
            CommittedLakeSnapshot committedLakeSnapshot = lakeCommitter.getMissingLakeSnapshot(0L);
            assertThat(committedLakeSnapshot).isNotNull();
            Map<Tuple2<Long, Integer>, Long> offsets = committedLakeSnapshot.getLogEndOffsets();
            for (int bucket = 0; bucket < 3; bucket++) {
                for (Long partitionId : partitionIdAndName.keySet()) {
                    // we only write 10 records, so expected log offset should be 10
                    assertThat(offsets.get(Tuple2.of(partitionId, bucket))).isEqualTo(10);
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

    @Test
    void testMultiPartitionTiering() throws Exception {
        // Test multiple partitions: region + year
        TablePath tablePath = TablePath.of("paimon", "test_multi_partition");
        createMultiPartitionTable(tablePath);

        Map<String, List<LogRecord>> recordsByPartition = new HashMap<>();
        List<PaimonWriteResult> paimonWriteResults = new ArrayList<>();
        Map<TableBucket, Long> tableBucketOffsets = new HashMap<>();

        // Test data for different partitions using $ separator
        Map<Long, String> partitionIdAndName =
                new HashMap<Long, String>() {
                    {
                        put(1L, "us-east$2024");
                        put(2L, "us-west$2024");
                        put(3L, "eu-central$2023");
                    }
                };

        int bucket = 0;

        for (Map.Entry<Long, String> entry : partitionIdAndName.entrySet()) {
            String partition = entry.getValue();
            try (LakeWriter<PaimonWriteResult> lakeWriter =
                    createLakeWriter(tablePath, bucket, partition, entry.getKey())) {
                List<LogRecord> logRecords =
                        genLogTableRecordsForMultiPartition(partition, bucket, 3);
                recordsByPartition.put(partition, logRecords);

                for (LogRecord logRecord : logRecords) {
                    lakeWriter.write(logRecord);
                }
                tableBucketOffsets.put(new TableBucket(0, entry.getKey(), bucket), 3L);
                PaimonWriteResult result = lakeWriter.complete();
                paimonWriteResults.add(result);
            }
        }

        // Commit all data
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath)) {
            PaimonCommittable committable = lakeCommitter.toCommittable(paimonWriteResults);
            long snapshot =
                    lakeCommitter.commit(
                            committable,
                            toBucketOffsetsProperty(
                                    tableBucketOffsets,
                                    partitionIdAndName,
                                    getPartitionKeys(tablePath)));
            assertThat(snapshot).isEqualTo(1);
        }

        // Verify data for each partition
        for (String partition : partitionIdAndName.values()) {
            List<LogRecord> expectRecords = recordsByPartition.get(partition);
            CloseableIterator<InternalRow> actualRecords =
                    getPaimonRowsMultiPartition(tablePath, partition);
            verifyLogTableRecordsMultiPartition(actualRecords, expectRecords, bucket);
        }
    }

    @Test
    void testThreePartitionTiering() throws Exception {
        // Test three partitions: region + year + month
        TablePath tablePath = TablePath.of("paimon", "test_three_partition");
        createThreePartitionTable(tablePath);

        Map<String, List<LogRecord>> recordsByPartition = new HashMap<>();
        List<PaimonWriteResult> paimonWriteResults = new ArrayList<>();
        Map<TableBucket, Long> tableBucketOffsets = new HashMap<>();

        // Test data for different three-level partitions using $ separator
        Map<Long, String> partitionIdAndName =
                new LinkedHashMap<Long, String>() {
                    {
                        put(1L, "us-east$2024$01");
                        put(2L, "eu-central$2023$12");
                    }
                };
        int bucket = 0;

        for (Map.Entry<Long, String> entry : partitionIdAndName.entrySet()) {
            String partition = entry.getValue();
            try (LakeWriter<PaimonWriteResult> lakeWriter =
                    createLakeWriter(tablePath, bucket, partition, entry.getKey())) {
                List<LogRecord> logRecords =
                        genLogTableRecordsForMultiPartition(
                                partition, bucket, 2); // Use same method
                recordsByPartition.put(partition, logRecords);

                for (LogRecord logRecord : logRecords) {
                    lakeWriter.write(logRecord);
                }
                tableBucketOffsets.put(new TableBucket(0, entry.getKey(), bucket), 2L);

                PaimonWriteResult result = lakeWriter.complete();
                paimonWriteResults.add(result);
            }
        }

        // Commit all data
        long snapshot;
        try (LakeCommitter<PaimonWriteResult, PaimonCommittable> lakeCommitter =
                createLakeCommitter(tablePath)) {
            PaimonCommittable committable = lakeCommitter.toCommittable(paimonWriteResults);
            snapshot =
                    lakeCommitter.commit(
                            committable,
                            toBucketOffsetsProperty(
                                    tableBucketOffsets,
                                    partitionIdAndName,
                                    getPartitionKeys(tablePath)));
            assertThat(snapshot).isEqualTo(1);
        }

        // check fluss offsets in paimon snapshot property
        String offsetProperty = getSnapshotLogOffsetProperty(tablePath, snapshot);
        assertThat(offsetProperty)
                .isEqualTo(
                        "[{\"partition_id\":1,\"bucket_id\":0,\"partition_name\":\"region=us-east/year=2024/month=01\",\"log_offset\":2},"
                                + "{\"partition_id\":2,\"bucket_id\":0,\"partition_name\":\"region=eu-central/year=2023/month=12\",\"log_offset\":2}]");

        // Verify data for each partition
        for (String partition : partitionIdAndName.values()) {
            List<LogRecord> expectRecords = recordsByPartition.get(partition);
            CloseableIterator<InternalRow> actualRecords =
                    getPaimonRowsThreePartition(tablePath, partition);
            verifyLogTableRecordsThreePartition(actualRecords, expectRecords, bucket);
        }
    }

    private void verifyLogTableRecordsMultiPartition(
            CloseableIterator<InternalRow> actualRecords,
            List<LogRecord> expectRecords,
            int expectBucket)
            throws Exception {
        for (LogRecord expectRecord : expectRecords) {
            InternalRow actualRow = actualRecords.next();
            // check business columns:
            assertThat(actualRow.getInt(0)).isEqualTo(expectRecord.getRow().getInt(0));
            assertThat(actualRow.getString(1).toString())
                    .isEqualTo(expectRecord.getRow().getString(1).toString());

            // check partition columns (should match record data)
            assertThat(actualRow.getString(2).toString())
                    .isEqualTo(expectRecord.getRow().getString(2).toString()); // region
            assertThat(actualRow.getString(3).toString())
                    .isEqualTo(expectRecord.getRow().getString(3).toString()); // year

            // check system columns: __bucket, __offset, __timestamp
            assertThat(actualRow.getInt(4)).isEqualTo(expectBucket);
            assertThat(actualRow.getLong(5)).isEqualTo(expectRecord.logOffset());
            assertThat(actualRow.getTimestamp(6, 6).getMillisecond())
                    .isEqualTo(expectRecord.timestamp());
        }
        assertThat(actualRecords.hasNext()).isFalse();
        actualRecords.close();
    }

    private void verifyLogTableRecordsThreePartition(
            CloseableIterator<InternalRow> actualRecords,
            List<LogRecord> expectRecords,
            int expectBucket)
            throws Exception {
        for (LogRecord expectRecord : expectRecords) {
            InternalRow actualRow = actualRecords.next();
            // check business columns:
            assertThat(actualRow.getInt(0)).isEqualTo(expectRecord.getRow().getInt(0));
            assertThat(actualRow.getString(1).toString())
                    .isEqualTo(expectRecord.getRow().getString(1).toString());

            // check partition columns (should match record data)
            assertThat(actualRow.getString(2).toString())
                    .isEqualTo(expectRecord.getRow().getString(2).toString()); // region
            assertThat(actualRow.getString(3).toString())
                    .isEqualTo(expectRecord.getRow().getString(3).toString()); // year
            assertThat(actualRow.getString(4).toString())
                    .isEqualTo(expectRecord.getRow().getString(4).toString()); // month

            // check system columns: __bucket, __offset, __timestamp
            assertThat(actualRow.getInt(5)).isEqualTo(expectBucket);
            assertThat(actualRow.getLong(6)).isEqualTo(expectRecord.logOffset());
            assertThat(actualRow.getTimestamp(7, 6).getMillisecond())
                    .isEqualTo(expectRecord.timestamp());
        }
        assertThat(actualRecords.hasNext()).isFalse();
        actualRecords.close();
    }

    private void verifyTableRecords(
            CloseableIterator<InternalRow> actualRecords,
            List<LogRecord> expectRecords,
            int expectBucket,
            @Nullable String partition)
            throws Exception {
        for (LogRecord expectRecord : expectRecords) {
            InternalRow actualRow = actualRecords.next();
            // check business columns:
            assertThat(actualRow.getInt(0)).isEqualTo(expectRecord.getRow().getInt(0));
            assertThat(actualRow.getString(1).toString())
                    .isEqualTo(expectRecord.getRow().getString(1).toString());

            assertThat(actualRow.getString(2).toString())
                    .isEqualTo(expectRecord.getRow().getString(2).toString());
            if (partition != null) {
                assertThat(actualRow.getString(2).toString()).isEqualTo(partition);
            }
            // check system columns: __bucket, __offset, __timestamp
            assertThat(actualRow.getInt(3)).isEqualTo(expectBucket);
            assertThat(actualRow.getLong(4)).isEqualTo(expectRecord.logOffset());
            assertThat(actualRow.getTimestamp(5, 6).getMillisecond())
                    .isEqualTo(expectRecord.timestamp());
        }
        assertThat(actualRecords.hasNext()).isFalse();
        actualRecords.close();
    }

    private Tuple2<List<LogRecord>, List<LogRecord>> genLogTableRecords(
            @Nullable String partition, int bucket, int numRecords) {
        List<LogRecord> logRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            GenericRow genericRow;
            // Partitioned table: include partition field in data
            genericRow = new GenericRow(3); // c1, c2, c3(partition)
            genericRow.setField(0, i);
            genericRow.setField(1, BinaryString.fromString("bucket" + bucket + "_" + i));
            if (partition != null) {
                genericRow.setField(2, BinaryString.fromString(partition)); // partition field
            } else {
                genericRow.setField(2, BinaryString.fromString("bucket" + bucket));
            }
            LogRecord logRecord =
                    new GenericRecord(
                            i, System.currentTimeMillis(), ChangeType.APPEND_ONLY, genericRow);
            logRecords.add(logRecord);
        }
        return Tuple2.of(logRecords, logRecords);
    }

    private List<LogRecord> genLogTableRecordsForMultiPartition(
            String partition, int bucket, int numRecords) {
        String[] partitionValues = partition.split("\\$");
        List<LogRecord> logRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            GenericRow genericRow =
                    new GenericRow(2 + partitionValues.length); // c1, c2, region, year
            genericRow.setField(0, i);
            genericRow.setField(
                    1, BinaryString.fromString(partitionValues[0] + "_data_" + bucket + "_" + i));

            // Add partition fields to record data
            for (int partitionIndex = 0;
                    partitionIndex < partitionValues.length;
                    partitionIndex++) {
                genericRow.setField(
                        2 + partitionIndex,
                        BinaryString.fromString(partitionValues[partitionIndex]));
            }

            LogRecord logRecord =
                    new GenericRecord(
                            i, System.currentTimeMillis(), ChangeType.APPEND_ONLY, genericRow);
            logRecords.add(logRecord);
        }
        return logRecords;
    }

    private Tuple2<List<LogRecord>, List<LogRecord>> genPrimaryKeyTableRecords(
            @Nullable String partition, int bucket) {
        int offset = -1;
        // gen +I, -U, +U, -D
        List<GenericRow> rows = genKvRow(partition, bucket, 0, 0, 4);
        List<LogRecord> writtenLogRecords =
                new ArrayList<>(
                        Arrays.asList(
                                toRecord(++offset, rows.get(0), INSERT),
                                toRecord(++offset, rows.get(1), UPDATE_BEFORE),
                                toRecord(++offset, rows.get(2), UPDATE_AFTER),
                                toRecord(++offset, rows.get(3), DELETE)));
        List<LogRecord> expectLogRecords = new ArrayList<>();

        // gen +I, -U, +U
        rows = genKvRow(partition, bucket, 1, 4, 7);
        writtenLogRecords.addAll(
                Arrays.asList(
                        toRecord(++offset, rows.get(0), INSERT),
                        toRecord(++offset, rows.get(1), UPDATE_BEFORE),
                        toRecord(++offset, rows.get(2), UPDATE_AFTER)));
        expectLogRecords.add(writtenLogRecords.get(writtenLogRecords.size() - 1));

        // gen +I, +U
        rows = genKvRow(partition, bucket, 2, 7, 9);
        writtenLogRecords.addAll(
                Arrays.asList(
                        toRecord(++offset, rows.get(0), INSERT),
                        toRecord(++offset, rows.get(1), UPDATE_AFTER)));
        expectLogRecords.add(writtenLogRecords.get(writtenLogRecords.size() - 1));

        // gen +I
        rows = genKvRow(partition, bucket, 3, 9, 10);
        writtenLogRecords.add(toRecord(++offset, rows.get(0), INSERT));
        expectLogRecords.add(writtenLogRecords.get(writtenLogRecords.size() - 1));

        return Tuple2.of(writtenLogRecords, expectLogRecords);
    }

    private List<GenericRow> genKvRow(
            @Nullable String partition, int bucket, int key, int from, int to) {
        List<GenericRow> rows = new ArrayList<>();
        for (int i = from; i < to; i++) {
            GenericRow genericRow;
            if (partition != null) {
                // Partitioned table: include partition field in data
                genericRow = new GenericRow(3); // c1, c2, c3(partition)
                genericRow.setField(0, key);
                genericRow.setField(1, BinaryString.fromString("bucket" + bucket + "_" + i));
                genericRow.setField(2, BinaryString.fromString(partition)); // partition field
            } else {
                // Non-partitioned table
                genericRow = new GenericRow(3);
                genericRow.setField(0, key);
                genericRow.setField(1, BinaryString.fromString("bucket" + bucket + "_" + i));
                genericRow.setField(2, BinaryString.fromString("bucket" + bucket));
            }
            rows.add(genericRow);
        }
        return rows;
    }

    private GenericRecord toRecord(long offset, GenericRow row, ChangeType changeType) {
        return new GenericRecord(offset, System.currentTimeMillis(), changeType, row);
    }

    private CloseableIterator<InternalRow> getPaimonRows(
            TablePath tablePath, @Nullable String partition, boolean isPrimaryKeyTable, int bucket)
            throws Exception {
        Identifier identifier = toPaimon(tablePath);
        FileStoreTable fileStoreTable = (FileStoreTable) paimonCatalog.getTable(identifier);

        ReadBuilder readBuilder = fileStoreTable.newReadBuilder();

        if (partition != null) {
            readBuilder =
                    readBuilder.withPartitionFilter(Collections.singletonMap("c3", partition));
        }
        List<Split> splits = new ArrayList<>();
        if (isPrimaryKeyTable) {
            splits = readBuilder.withBucketFilter(b -> b == bucket).newScan().plan().splits();
        } else {
            // for log table, we can't filter by bucket directly, filter file by __bucket column
            for (Split split : readBuilder.newScan().plan().splits()) {
                DataSplit dataSplit = (DataSplit) split;
                List<DataFileMeta> dataFileMetas = dataSplit.dataFiles();
                checkState(dataFileMetas.size() == 1);
                DataFileMeta dataFileMeta = dataFileMetas.get(0);
                // filter by __bucket column
                if (dataFileMeta.valueStats().maxValues().getInt(3) == bucket
                        && dataFileMeta.valueStats().minValues().getInt(3) == bucket) {
                    splits.add(split);
                }
            }
        }
        return readBuilder.newRead().createReader(splits).toCloseableIterator();
    }

    private CloseableIterator<InternalRow> getPaimonRowsMultiPartition(
            TablePath tablePath, String partition) throws Exception {
        Identifier identifier = toPaimon(tablePath);
        FileStoreTable fileStoreTable = (FileStoreTable) paimonCatalog.getTable(identifier);

        ReadBuilder readBuilder = fileStoreTable.newReadBuilder();

        // Parse partition: "us-east$2024" -> ["us-east", "2024"]
        String[] partitionValues = partition.split("\\$");
        String region = partitionValues[0];
        String year = partitionValues[1];

        Map<String, String> partitionFilter = new HashMap<>();
        partitionFilter.put("region", region);
        partitionFilter.put("year", year);
        readBuilder = readBuilder.withPartitionFilter(partitionFilter);

        List<Split> splits = readBuilder.newScan().plan().splits();
        return readBuilder.newRead().createReader(splits).toCloseableIterator();
    }

    private CloseableIterator<InternalRow> getPaimonRowsThreePartition(
            TablePath tablePath, String partition) throws Exception {
        Identifier identifier = toPaimon(tablePath);
        FileStoreTable fileStoreTable = (FileStoreTable) paimonCatalog.getTable(identifier);

        ReadBuilder readBuilder = fileStoreTable.newReadBuilder();

        // Parse partition: "us-east$2024$01" -> ["us-east", "2024", "01"]
        String[] partitionValues = partition.split("\\$");
        String region = partitionValues[0];
        String year = partitionValues[1];
        String month = partitionValues[2];

        Map<String, String> partitionFilter = new HashMap<>();
        partitionFilter.put("region", region);
        partitionFilter.put("year", year);
        partitionFilter.put("month", month);
        readBuilder = readBuilder.withPartitionFilter(partitionFilter);

        List<Split> splits = readBuilder.newScan().plan().splits();
        return readBuilder.newRead().createReader(splits).toCloseableIterator();
    }

    private LakeWriter<PaimonWriteResult> createLakeWriter(
            TablePath tablePath, int bucket, @Nullable String partition, @Nullable Long partitionId)
            throws IOException {
        return paimonLakeTieringFactory.createLakeWriter(
                new WriterInitContext() {
                    @Override
                    public TablePath tablePath() {
                        return tablePath;
                    }

                    @Override
                    public TableBucket tableBucket() {
                        // don't care about tableId & partitionId
                        return new TableBucket(0, partitionId, bucket);
                    }

                    @Nullable
                    @Override
                    public String partition() {
                        return partition;
                    }

                    @Override
                    public Map<String, String> customProperties() {
                        // don't care about table custom properties for Paimon lake writer
                        return new HashMap<>();
                    }

                    @Override
                    public org.apache.fluss.metadata.Schema schema() {
                        throw new UnsupportedOperationException(
                                "The lake writer in Paimon currently uses paimonCatalog to determine the schema.");
                    }
                });
    }

    private LakeCommitter<PaimonWriteResult, PaimonCommittable> createLakeCommitter(
            TablePath tablePath) throws IOException {
        return paimonLakeTieringFactory.createLakeCommitter(() -> tablePath);
    }

    private void createTable(
            TablePath tablePath,
            boolean isPrimaryTable,
            boolean isPartitioned,
            @Nullable Integer numBuckets)
            throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .column("c3", DataTypes.STRING());
        if (isPartitioned) {
            builder.partitionKeys("c3");
        }
        if (isPrimaryTable) {
            if (isPartitioned) {
                builder.primaryKey("c1", "c3");
            } else {
                builder.primaryKey("c1");
            }
            builder.option(
                    CoreOptions.CHANGELOG_PRODUCER.key(),
                    CoreOptions.ChangelogProducer.INPUT.toString());
        }
        if (numBuckets != null) {
            builder.option(CoreOptions.BUCKET.key(), String.valueOf(numBuckets));
        }
        doCreatePaimonTable(tablePath, builder);
    }

    private void createMultiPartitionTable(TablePath tablePath) throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .column("region", DataTypes.STRING())
                        .column("year", DataTypes.STRING())
                        .partitionKeys("region", "year");
        doCreatePaimonTable(tablePath, builder);
    }

    private void createThreePartitionTable(TablePath tablePath) throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .column("region", DataTypes.STRING())
                        .column("year", DataTypes.STRING())
                        .column("month", DataTypes.STRING())
                        .partitionKeys("region", "year", "month");
        doCreatePaimonTable(tablePath, builder);
    }

    private void doCreatePaimonTable(TablePath tablePath, Schema.Builder paimonSchemaBuilder)
            throws Exception {
        paimonSchemaBuilder.column(BUCKET_COLUMN_NAME, DataTypes.INT());
        paimonSchemaBuilder.column(OFFSET_COLUMN_NAME, DataTypes.BIGINT());
        paimonSchemaBuilder.column(
                TIMESTAMP_COLUMN_NAME, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
        paimonSchemaBuilder.option(
                CoreOptions.COMMIT_CALLBACKS.key(),
                PaimonLakeCommitter.PaimonCommitCallback.class.getName());
        paimonCatalog.createDatabase(tablePath.getDatabaseName(), true);
        paimonCatalog.createTable(toPaimon(tablePath), paimonSchemaBuilder.build(), true);
    }

    private String getSnapshotLogOffsetProperty(TablePath tablePath, long snapshotId)
            throws Exception {
        Identifier identifier = toPaimon(tablePath);
        FileStoreTable fileStoreTable = (FileStoreTable) paimonCatalog.getTable(identifier);
        return fileStoreTable
                .snapshotManager()
                .snapshot(snapshotId)
                .properties()
                .get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
    }

    private List<String> getPartitionKeys(TablePath tablePath) throws Exception {
        Identifier identifier = toPaimon(tablePath);
        FileStoreTable fileStoreTable = (FileStoreTable) paimonCatalog.getTable(identifier);
        return fileStoreTable.partitionKeys();
    }
}
