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

package com.alibaba.fluss.lake.iceberg;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.committer.LakeCommitter;
import com.alibaba.fluss.lake.iceberg.tiering.IcebergCatalogProvider;
import com.alibaba.fluss.lake.iceberg.tiering.IcebergCommittable;
import com.alibaba.fluss.lake.iceberg.tiering.IcebergLakeTieringFactory;
import com.alibaba.fluss.lake.iceberg.tiering.IcebergWriteResult;
import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.lake.writer.LakeWriter;
import com.alibaba.fluss.lake.writer.WriterInitContext;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.GenericRecord;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.types.DataTypes;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for tiering to Iceberg via {@link IcebergLakeTieringFactory}. */
class IcebergTieringTest {

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

    @Test
    void testTieringWriteTable() throws Exception {
        TablePath tablePath = TablePath.of("iceberg", "test_table");
        createTable(tablePath);

        Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));

        int bucketNum = 3;

        Map<Integer, List<LogRecord>> recordsByBucket = new HashMap<>();

        List<IcebergWriteResult> icebergWriteResults = new ArrayList<>();
        SimpleVersionedSerializer<IcebergWriteResult> writeResultSerializer =
                icebergLakeTieringFactory.getWriteResultSerializer();
        SimpleVersionedSerializer<IcebergCommittable> committableSerializer =
                icebergLakeTieringFactory.getCommittableSerializer();

        // first, write data
        for (int bucket = 0; bucket < bucketNum; bucket++) {
            try (LakeWriter<IcebergWriteResult> writer = createLakeWriter(tablePath, bucket)) {
                List<LogRecord> records = genLogTableRecords(bucket, 5);
                for (LogRecord record : records) {
                    writer.write(record);
                }
                recordsByBucket.put(bucket, records);
                IcebergWriteResult result = writer.complete();
                byte[] serialized = writeResultSerializer.serialize(result);
                icebergWriteResults.add(
                        writeResultSerializer.deserialize(
                                writeResultSerializer.getVersion(), serialized));
            }
        }

        // second, commit data
        try (LakeCommitter<IcebergWriteResult, IcebergCommittable> lakeCommitter =
                createLakeCommitter(tablePath)) {
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
            List<LogRecord> expectRecords = recordsByBucket.get(bucket);
            CloseableIterator<Record> actualRecords = getIcebergRows(icebergTable, bucket);
            verifyLogTableRecords(actualRecords, bucket, expectRecords);
        }
    }

    private LakeWriter<IcebergWriteResult> createLakeWriter(TablePath tablePath, int bucket)
            throws IOException {
        return icebergLakeTieringFactory.createLakeWriter(
                new WriterInitContext() {
                    @Override
                    public TablePath tablePath() {
                        return tablePath;
                    }

                    @Override
                    public TableBucket tableBucket() {
                        return new TableBucket(0, null, bucket);
                    }

                    @Nullable
                    @Override
                    public String partition() {
                        return null;
                    }

                    @Override
                    public Map<String, String> customProperties() {
                        return Collections.emptyMap();
                    }

                    @Override
                    public com.alibaba.fluss.metadata.Schema schema() {
                        return com.alibaba.fluss.metadata.Schema.newBuilder()
                                .column("c1", DataTypes.INT())
                                .column("c2", DataTypes.STRING())
                                .column("c3", DataTypes.STRING())
                                .build();
                    }
                });
    }

    private LakeCommitter<IcebergWriteResult, IcebergCommittable> createLakeCommitter(
            TablePath tablePath) throws IOException {
        return icebergLakeTieringFactory.createLakeCommitter(() -> tablePath);
    }

    private List<LogRecord> genLogTableRecords(int bucket, int numRecords) {
        List<LogRecord> logRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            GenericRow genericRow = new GenericRow(3);
            genericRow.setField(0, i);
            genericRow.setField(1, BinaryString.fromString("bucket" + bucket + "_" + i));
            genericRow.setField(2, BinaryString.fromString("bucket" + bucket));

            LogRecord logRecord =
                    new GenericRecord(
                            i, System.currentTimeMillis(), ChangeType.APPEND_ONLY, genericRow);
            logRecords.add(logRecord);
        }
        return logRecords;
    }

    private void createTable(TablePath tablePath) throws Exception {
        Namespace namespace = Namespace.of(tablePath.getDatabaseName());
        if (icebergCatalog instanceof SupportsNamespaces) {
            SupportsNamespaces ns = (SupportsNamespaces) icebergCatalog;
            if (!ns.namespaceExists(namespace)) {
                ns.createNamespace(namespace);
            }
        }

        org.apache.iceberg.Schema schema =
                new org.apache.iceberg.Schema(
                        Types.NestedField.optional(1, "c1", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "c2", Types.StringType.get()),
                        Types.NestedField.optional(3, "c3", Types.StringType.get()),
                        Types.NestedField.required(4, BUCKET_COLUMN_NAME, Types.IntegerType.get()),
                        Types.NestedField.required(5, OFFSET_COLUMN_NAME, Types.LongType.get()),
                        Types.NestedField.required(
                                6, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone()));

        TableIdentifier tableId =
                TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName());
        icebergCatalog.createTable(tableId, schema);
    }

    private CloseableIterator<Record> getIcebergRows(Table table, int bucket) {
        return IcebergGenerics.read(table)
                .where(Expressions.equal(BUCKET_COLUMN_NAME, bucket))
                .build()
                .iterator();
    }

    private void verifyLogTableRecords(
            CloseableIterator<Record> actualRecords,
            int expectBucket,
            List<LogRecord> expectRecords) {
        for (LogRecord expectRecord : expectRecords) {
            Record actualRecord = actualRecords.next();
            // check business columns:
            assertThat(actualRecord.get(0)).isEqualTo(expectRecord.getRow().getInt(0));
            assertThat(actualRecord.get(1, String.class))
                    .isEqualTo(expectRecord.getRow().getString(1).toString());
            assertThat(actualRecord.get(2, String.class))
                    .isEqualTo(expectRecord.getRow().getString(2).toString());
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
