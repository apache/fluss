/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.tiering;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lakehouse.committer.LakeCommitter;
import com.alibaba.fluss.lakehouse.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.lakehouse.writer.LakeWriter;
import com.alibaba.fluss.lakehouse.writer.WriterInitContext;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.GenericRecord;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.utils.types.Tuple2;

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

import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static com.alibaba.fluss.record.ChangeType.DELETE;
import static com.alibaba.fluss.record.ChangeType.INSERT;
import static com.alibaba.fluss.record.ChangeType.UPDATE_AFTER;
import static com.alibaba.fluss.record.ChangeType.UPDATE_BEFORE;
import static com.alibaba.fluss.utils.Preconditions.checkState;
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
                paimonLakeTieringFactory.getCommitableSerializer();

        Map<Tuple2<String, Integer>, List<LogRecord>> recordsByBucket = new HashMap<>();
        List<String> partitions =
                isPartitioned ? Arrays.asList("p1", "p2", "p3") : Collections.singletonList(null);
        // first, write data
        for (int bucket = 0; bucket < bucketNum; bucket++) {
            for (String partition : partitions) {
                try (LakeWriter<PaimonWriteResult> lakeWriter =
                        createLakeWriter(tablePath, bucket, partition)) {
                    Tuple2<String, Integer> partitionBucket = Tuple2.of(partition, bucket);
                    Tuple2<List<LogRecord>, List<LogRecord>> writeAndExpectRecords =
                            isPrimaryKeyTable
                                    ? genPrimaryKeyTableRecords(partition, bucket)
                                    : genLogTableRecords(partition, bucket, 10);
                    List<LogRecord> writtenRecords = writeAndExpectRecords.f0;
                    List<LogRecord> expectRecords = writeAndExpectRecords.f1;
                    recordsByBucket.put(partitionBucket, expectRecords);
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
            PaimonCommittable paimonCommittable = lakeCommitter.toCommitable(paimonWriteResults);
            byte[] serialized = committableSerializer.serialize(paimonCommittable);
            paimonCommittable =
                    committableSerializer.deserialize(
                            committableSerializer.getVersion(), serialized);
            long snapshot = lakeCommitter.commit(paimonCommittable);
            assertThat(snapshot).isEqualTo(1);
        }

        // then, check data
        for (int bucket = 0; bucket < 3; bucket++) {
            for (String partition : partitions) {
                Tuple2<String, Integer> partitionBucket = Tuple2.of(partition, bucket);
                List<LogRecord> expectRecords = recordsByBucket.get(partitionBucket);
                CloseableIterator<InternalRow> actualRecords =
                        getPaimonRows(tablePath, partition, isPrimaryKeyTable, bucket);
                if (isPrimaryKeyTable) {
                    verifyPrimaryKeyTableRecord(actualRecords, expectRecords, bucket);
                } else {
                    verifyLogTableRecords(actualRecords, expectRecords, bucket);
                }
            }
        }
    }

    private void verifyLogTableRecords(
            CloseableIterator<InternalRow> actualRecords,
            List<LogRecord> expectRecords,
            int expectBucket)
            throws Exception {
        for (LogRecord expectRecord : expectRecords) {
            InternalRow actualRow = actualRecords.next();
            // check normal columns:
            assertThat(actualRow.getInt(0)).isEqualTo(expectRecord.getRow().getInt(0));
            assertThat(actualRow.getString(1).toString())
                    .isEqualTo(expectRecord.getRow().getString(1).toString());
            assertThat(actualRow.getString(2).toString())
                    .isEqualTo(expectRecord.getRow().getString(2).toString());

            // check system columns: __bucket, __offset, __timestamp
            assertThat(actualRow.getInt(3)).isEqualTo(expectBucket);
            assertThat(actualRow.getLong(4)).isEqualTo(expectRecord.logOffset());
            assertThat(actualRow.getTimestamp(5, 6).getMillisecond())
                    .isEqualTo(expectRecord.timestamp());
        }
        assertThat(actualRecords.hasNext()).isFalse();
        actualRecords.close();
    }

    private void verifyPrimaryKeyTableRecord(
            CloseableIterator<InternalRow> actualRecords,
            List<LogRecord> expectRecords,
            int expectBucket)
            throws Exception {
        for (LogRecord expectRecord : expectRecords) {
            InternalRow actualRow = actualRecords.next();
            // check normal columns:
            assertThat(actualRow.getInt(0)).isEqualTo(expectRecord.getRow().getInt(0));
            assertThat(actualRow.getString(1).toString())
                    .isEqualTo(expectRecord.getRow().getString(1).toString());
            assertThat(actualRow.getString(2).toString())
                    .isEqualTo(expectRecord.getRow().getString(2).toString());

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
            GenericRow genericRow = new GenericRow(3);
            genericRow.setField(0, i);
            genericRow.setField(1, BinaryString.fromString("bucket" + bucket + "_" + i));
            genericRow.setField(
                    2,
                    partition == null
                            ? BinaryString.fromString("bucket" + bucket)
                            : BinaryString.fromString(partition));
            LogRecord logRecord =
                    new GenericRecord(
                            i, System.currentTimeMillis(), ChangeType.APPEND_ONLY, genericRow);
            logRecords.add(logRecord);
        }
        return Tuple2.of(logRecords, logRecords);
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
        expectLogRecords.add(toRecord(offset, rows.get(2), UPDATE_AFTER));

        // gen +I, +U
        rows = genKvRow(partition, bucket, 2, 7, 9);
        writtenLogRecords.addAll(
                Arrays.asList(
                        toRecord(++offset, rows.get(0), INSERT),
                        toRecord(++offset, rows.get(1), UPDATE_AFTER)));
        expectLogRecords.add(toRecord(offset, rows.get(1), UPDATE_AFTER));

        // gen +I
        rows = genKvRow(partition, bucket, 3, 9, 10);
        writtenLogRecords.add(toRecord(++offset, rows.get(0), INSERT));
        expectLogRecords.add(toRecord(offset, rows.get(0), INSERT));

        return Tuple2.of(writtenLogRecords, expectLogRecords);
    }

    private List<GenericRow> genKvRow(
            @Nullable String partition, int bucket, int key, int from, int to) {
        List<GenericRow> rows = new ArrayList<>();
        for (int i = from; i < to; i++) {
            GenericRow genericRow = new GenericRow(3);
            genericRow.setField(0, key);
            genericRow.setField(1, BinaryString.fromString("bucket" + bucket + "_" + i));
            genericRow.setField(
                    2,
                    partition == null
                            ? BinaryString.fromString("bucket" + bucket)
                            : BinaryString.fromString(partition));
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

    private LakeWriter<PaimonWriteResult> createLakeWriter(
            TablePath tablePath, int bucket, @Nullable String partition) throws IOException {
        return paimonLakeTieringFactory.createLakeWriter(
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
        }

        builder.column(BUCKET_COLUMN_NAME, DataTypes.INT());
        builder.column(OFFSET_COLUMN_NAME, DataTypes.BIGINT());
        builder.column(TIMESTAMP_COLUMN_NAME, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());

        if (numBuckets != null) {
            builder.option(CoreOptions.BUCKET.key(), String.valueOf(numBuckets));
        }

        paimonCatalog.createDatabase(tablePath.getDatabaseName(), true);
        paimonCatalog.createTable(toPaimon(tablePath), builder.build(), true);
    }
}
