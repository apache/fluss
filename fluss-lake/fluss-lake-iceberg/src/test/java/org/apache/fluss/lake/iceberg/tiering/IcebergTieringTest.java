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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.fluss.record.ChangeType.DELETE;
import static org.apache.fluss.record.ChangeType.INSERT;
import static org.apache.fluss.record.ChangeType.UPDATE_AFTER;
import static org.apache.fluss.record.ChangeType.UPDATE_BEFORE;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for tiering to Iceberg via {@link IcebergLakeTieringFactory}. */
class IcebergTieringTest {

    private static final int BUCKET_NUM = 3;

    private @TempDir File tempWarehouseDir;
    private IcebergLakeTieringFactory icebergLakeTieringFactory;
    private Catalog icebergCatalog;

    @BeforeEach
    void beforeEach() {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", "file://" + tempWarehouseDir);
        configuration.setString("type", "hadoop");
        configuration.setString("name", "test");
        IcebergCatalogProvider provider = new IcebergCatalogProvider(configuration);
        icebergCatalog = provider.get();

        icebergLakeTieringFactory = new IcebergLakeTieringFactory(configuration);
    }

    private static Stream<Arguments> tieringWriteArgs() {
        return Stream.of(
                // isPrimaryKeyTable, isPartitionedTable
                Arguments.of(true, true),
                Arguments.of(true, false),
                Arguments.of(false, true),
                Arguments.of(false, false));
    }

    @ParameterizedTest
    @MethodSource("tieringWriteArgs")
    void testTieringWriteTable(boolean isPrimaryKeyTable, boolean isPartitionedTable)
            throws Exception {
        TablePath tablePath =
                TablePath.of(
                        "iceberg",
                        String.format(
                                "test_tiering_table_%s_%s",
                                isPrimaryKeyTable ? "pk" : "log",
                                isPartitionedTable ? "partitioned" : "unpartitioned"));
        createTable(tablePath, isPrimaryKeyTable, isPartitionedTable);

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                org.apache.fluss.metadata.Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .column("c3", DataTypes.STRING())
                                        .build())
                        .distributedBy(BUCKET_NUM)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .build();
        TableInfo tableInfo = TableInfo.of(tablePath, 0, 1, descriptor, 1L, 1L);

        Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));

        Map<Tuple2<String, Integer>, List<LogRecord>> recordsByBucket = new HashMap<>();
        Map<Long, String> partitionIdAndName =
                isPartitionedTable
                        ? new HashMap<Long, String>() {
                            {
                                put(1L, "p1");
                                put(2L, "p2");
                                put(3L, "p3");
                            }
                        }
                        : Collections.singletonMap(null, null);

        List<IcebergWriteResult> icebergWriteResults = new ArrayList<>();
        SimpleVersionedSerializer<IcebergWriteResult> writeResultSerializer =
                icebergLakeTieringFactory.getWriteResultSerializer();
        SimpleVersionedSerializer<IcebergCommittable> committableSerializer =
                icebergLakeTieringFactory.getCommittableSerializer();

        // first, write data
        for (int bucket = 0; bucket < BUCKET_NUM; bucket++) {
            for (Map.Entry<Long, String> entry : partitionIdAndName.entrySet()) {
                String partition = entry.getValue();
                try (LakeWriter<IcebergWriteResult> writer =
                        createLakeWriter(tablePath, bucket, partition, entry.getKey(), tableInfo)) {
                    Tuple2<String, Integer> partitionBucket = Tuple2.of(partition, bucket);
                    Tuple2<List<LogRecord>, List<LogRecord>> writeAndExpectRecords =
                            isPrimaryKeyTable
                                    ? genPrimaryKeyTableRecords(partition, bucket)
                                    : genLogTableRecords(partition, bucket, 10);

                    List<LogRecord> writtenRecords = writeAndExpectRecords.f0;
                    List<LogRecord> expectRecords = writeAndExpectRecords.f1;
                    recordsByBucket.put(partitionBucket, expectRecords);

                    for (LogRecord record : writtenRecords) {
                        writer.write(record);
                    }
                    IcebergWriteResult result = writer.complete();
                    byte[] serialized = writeResultSerializer.serialize(result);
                    icebergWriteResults.add(
                            writeResultSerializer.deserialize(
                                    writeResultSerializer.getVersion(), serialized));
                }
            }
        }

        // second, commit data
        try (LakeCommitter<IcebergWriteResult, IcebergCommittable> lakeCommitter =
                createLakeCommitter(tablePath, tableInfo)) {
            // serialize/deserialize committable
            IcebergCommittable icebergCommittable =
                    lakeCommitter.toCommittable(icebergWriteResults);
            byte[] serialized = committableSerializer.serialize(icebergCommittable);
            icebergCommittable =
                    committableSerializer.deserialize(
                            committableSerializer.getVersion(), serialized);
            long snapshot =
                    lakeCommitter.commit(icebergCommittable, Collections.singletonMap("k1", "v1"));
            icebergTable.refresh();
            Snapshot icebergSnapshot = icebergTable.currentSnapshot();
            assertThat(snapshot).isEqualTo(icebergSnapshot.snapshotId());
            assertThat(icebergSnapshot.summary()).containsEntry("k1", "v1");
        }

        // then, check data
        for (int bucket = 0; bucket < 3; bucket++) {
            for (String partition : partitionIdAndName.values()) {
                Tuple2<String, Integer> partitionBucket = Tuple2.of(partition, bucket);
                List<LogRecord> expectRecords = recordsByBucket.get(partitionBucket);
                CloseableIterator<Record> actualRecords =
                        getIcebergRows(icebergTable, partition, bucket);
                verifyTableRecords(actualRecords, expectRecords, bucket, partition);
            }
        }
    }

    private LakeWriter<IcebergWriteResult> createLakeWriter(
            TablePath tablePath,
            int bucket,
            @Nullable String partition,
            @Nullable Long partitionId,
            TableInfo tableInfo)
            throws IOException {
        return icebergLakeTieringFactory.createLakeWriter(
                new WriterInitContext() {
                    @Override
                    public TablePath tablePath() {
                        return tablePath;
                    }

                    @Override
                    public TableBucket tableBucket() {
                        return new TableBucket(0, partitionId, bucket);
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

    private LakeCommitter<IcebergWriteResult, IcebergCommittable> createLakeCommitter(
            TablePath tablePath, TableInfo tableInfo) throws IOException {
        return icebergLakeTieringFactory.createLakeCommitter(
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

    private Tuple2<List<LogRecord>, List<LogRecord>> genLogTableRecords(
            @Nullable String partition, int bucket, int numRecords) {
        List<LogRecord> logRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            GenericRow genericRow = new GenericRow(3);
            genericRow.setField(0, i);
            genericRow.setField(1, BinaryString.fromString("bucket" + bucket + "_" + i));
            if (partition != null) {
                genericRow.setField(2, BinaryString.fromString(partition));
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
            genericRow = new GenericRow(3);
            genericRow.setField(0, key);
            genericRow.setField(1, BinaryString.fromString("bucket" + bucket + "_" + i));

            if (partition != null) {
                genericRow.setField(2, BinaryString.fromString(partition));
            } else {
                genericRow.setField(2, BinaryString.fromString("bucket" + bucket));
            }
            rows.add(genericRow);
        }
        return rows;
    }

    private GenericRecord toRecord(long offset, GenericRow row, ChangeType changeType) {
        return new GenericRecord(offset, System.currentTimeMillis(), changeType, row);
    }

    private void createTable(
            TablePath tablePath, boolean isPrimaryTable, boolean isPartitionedTable) {
        Namespace namespace = Namespace.of(tablePath.getDatabaseName());
        if (icebergCatalog instanceof SupportsNamespaces) {
            SupportsNamespaces ns = (SupportsNamespaces) icebergCatalog;
            if (!ns.namespaceExists(namespace)) {
                ns.createNamespace(namespace);
            }
        }

        Set<Integer> identifierFieldIds = new HashSet<>();
        if (isPrimaryTable) {
            identifierFieldIds.add(1);
            if (isPartitionedTable) {
                // we use c3 as the partition column, so c3 should also be included the pk
                identifierFieldIds.add(3);
            }
        }

        org.apache.iceberg.Schema schema =
                new org.apache.iceberg.Schema(
                        Arrays.asList(
                                Types.NestedField.required(1, "c1", Types.IntegerType.get()),
                                Types.NestedField.optional(2, "c2", Types.StringType.get()),
                                Types.NestedField.required(3, "c3", Types.StringType.get()),
                                Types.NestedField.required(
                                        4, BUCKET_COLUMN_NAME, Types.IntegerType.get()),
                                Types.NestedField.required(
                                        5, OFFSET_COLUMN_NAME, Types.LongType.get()),
                                Types.NestedField.required(
                                        6, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone())),
                        identifierFieldIds);

        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        if (isPartitionedTable) {
            builder.identity("c3");
        }

        PartitionSpec partitionSpec;
        if (isPrimaryTable) {
            partitionSpec = builder.bucket("c1", BUCKET_NUM).build();
        } else {
            partitionSpec = builder.identity(BUCKET_COLUMN_NAME).build();
        }

        TableIdentifier tableId =
                TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName());
        icebergCatalog.createTable(tableId, schema, partitionSpec);
    }

    private CloseableIterator<Record> getIcebergRows(
            Table table, @Nullable String partition, int bucket) {
        IcebergGenerics.ScanBuilder scanBuilder =
                IcebergGenerics.read(table).where(equal(BUCKET_COLUMN_NAME, bucket));
        if (partition != null) {
            String partitionCol = table.spec().fields().get(0).name();
            scanBuilder = scanBuilder.where(equal(partitionCol, partition));
        }

        return scanBuilder.build().iterator();
    }

    private void verifyTableRecords(
            CloseableIterator<Record> actualRecords,
            List<LogRecord> expectRecords,
            int expectBucket,
            @Nullable String partition) {
        for (LogRecord expectRecord : expectRecords) {
            Record actualRecord = actualRecords.next();
            // check business columns:
            assertThat(actualRecord.get(0)).isEqualTo(expectRecord.getRow().getInt(0));
            assertThat(actualRecord.get(1, String.class))
                    .isEqualTo(expectRecord.getRow().getString(1).toString());
            assertThat(actualRecord.get(2, String.class))
                    .isEqualTo(expectRecord.getRow().getString(2).toString());
            if (partition != null) {
                assertThat(actualRecord.get(2, String.class)).isEqualTo(partition);
            }

            // check system columns: __bucket, __offset, __timestamp
            assertThat(actualRecord.get(3)).isEqualTo(expectBucket);
            assertThat(actualRecord.get(4)).isEqualTo(expectRecord.logOffset());
            assertThat(
                            actualRecord
                                    .get(5, OffsetDateTime.class)
                                    .atZoneSameInstant(ZoneOffset.UTC)
                                    .toInstant()
                                    .toEpochMilli())
                    .isEqualTo(expectRecord.timestamp());
        }
    }
}
