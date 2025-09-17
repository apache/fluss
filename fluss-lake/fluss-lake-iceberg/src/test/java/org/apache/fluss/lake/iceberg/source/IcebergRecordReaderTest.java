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

import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.flink.types.Row;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenericReader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link IcebergRecordReader}. */
class IcebergRecordReaderTest extends IcebergSourceTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testReadLogTable(boolean isPartitioned) throws Exception {
        // prepare iceberg table
        TablePath tablePath =
                TablePath.of(
                        DEFAULT_DB, isPartitioned ? DEFAULT_TABLE + "_partitioned" : DEFAULT_TABLE);
        createFullTypeLogTable(tablePath, isPartitioned, DEFAULT_BUCKET_NUM);

        InternalRow.FieldGetter[] fieldGetters =
                InternalRow.createFieldGetters(
                        RowType.of(
                                new BigIntType(),
                                new StringType(),
                                new IntType(),
                                new DoubleType(),
                                new BooleanType(),
                                new TinyIntType(),
                                new SmallIntType(),
                                new FloatType(),
                                new DecimalType(10, 2),
                                new TimestampType(),
                                new LocalZonedTimestampType(),
                                new BinaryType(),
                                new CharType(),
                                new StringType()));

        // write data
        Table table = getTable(tablePath);
        List<InternalRow> writtenRows = new ArrayList<>();
        writtenRows.addAll(writeFullTypeRows(table, 9, isPartitioned ? "p1" : null));
        writtenRows.addAll(writeFullTypeRows(table, 20, isPartitioned ? "p2" : null));

        // refresh table
        table.refresh();
        Snapshot snapshot = table.currentSnapshot();

        List<Row> actual = new ArrayList<>();

        LakeSource<IcebergSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        for (IcebergSplit icebergSplit : lakeSource.createPlanner(snapshot::snapshotId).plan()) {
            RecordReader recordReader = lakeSource.createRecordReader(() -> icebergSplit);
            CloseableIterator<LogRecord> iterator = recordReader.read();
            actual.addAll(
                    convertToFlinkRow(
                            fieldGetters,
                            TransformingCloseableIterator.transform(iterator, LogRecord::getRow)));
            iterator.close();
        }
        List<Row> expect =
                convertToFlinkRow(fieldGetters, CloseableIterator.wrap(writtenRows.iterator()));
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expect);

        // test project
        InternalRow.FieldGetter[] projectFieldGetters =
                InternalRow.createFieldGetters(
                        RowType.of(new TinyIntType(), new StringType(), new DoubleType()));
        lakeSource.withProject(new int[][] {new int[] {5}, new int[] {1}, new int[] {3}});

        List<Row> projectActual = new ArrayList<>();
        for (IcebergSplit icebergSplit : lakeSource.createPlanner(snapshot::snapshotId).plan()) {
            RecordReader recordReader = lakeSource.createRecordReader(() -> icebergSplit);
            CloseableIterator<LogRecord> iterator = recordReader.read();
            projectActual.addAll(
                    convertToFlinkRow(
                            projectFieldGetters,
                            TransformingCloseableIterator.transform(iterator, LogRecord::getRow)));
            iterator.close();
        }

        TableScan tableScan =
                table.newScan()
                        .project(
                                new Schema(
                                        optional(6, "tiny_int", Types.IntegerType.get()),
                                        optional(2, "name", Types.StringType.get()),
                                        optional(4, "salary", Types.DoubleType.get())));
        IcebergGenericReader reader = new IcebergGenericReader(tableScan, true);
        List<Row> projectExpect = new ArrayList<>();
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            for (FileScanTask task : fileScanTasks) {
                org.apache.iceberg.io.CloseableIterator<Record> iterator =
                        reader.open(task).iterator();
                IcebergRecordAsFlussRow recordAsFlussRow = new IcebergRecordAsFlussRow();
                projectExpect.addAll(
                        convertToFlinkRow(
                                projectFieldGetters,
                                TransformingCloseableIterator.transform(
                                        CloseableIterator.wrap(iterator),
                                        recordAsFlussRow::replaceIcebergRecord)));
                iterator.close();
            }
        }
        assertThat(projectActual).containsExactlyInAnyOrderElementsOf(projectExpect);
    }

    private void createFullTypeLogTable(TablePath tablePath, boolean isPartitioned, int bucketNum)
            throws Exception {
        // Create a schema with various data types
        Schema schema =
                new Schema(
                        required(1, "id", Types.LongType.get()),
                        optional(2, "name", Types.StringType.get()),
                        optional(3, "age", Types.IntegerType.get()),
                        optional(4, "salary", Types.DoubleType.get()),
                        optional(5, "is_active", Types.BooleanType.get()),
                        optional(6, "tiny_int", Types.IntegerType.get()),
                        optional(7, "small_int", Types.IntegerType.get()),
                        optional(8, "float_val", Types.FloatType.get()),
                        optional(9, "decimal_val", Types.DecimalType.of(10, 2)),
                        optional(10, "timestamp_ntz", Types.TimestampType.withoutZone()),
                        optional(11, "timestamp_ltz", Types.TimestampType.withZone()),
                        optional(12, "binary_data", Types.BinaryType.get()),
                        optional(13, "char_data", Types.StringType.get()),
                        optional(14, "dt", Types.StringType.get()),

                        // System columns
                        required(15, "__bucket", Types.IntegerType.get()),
                        required(16, "__offset", Types.LongType.get()),
                        required(17, "__timestamp", Types.TimestampType.withZone()));
        PartitionSpec partitionSpec =
                isPartitioned
                        ? PartitionSpec.builderFor(schema)
                                .identity("dt")
                                .bucket("id", bucketNum)
                                .build()
                        : PartitionSpec.builderFor(schema).bucket("id", bucketNum).build();
        createTable(tablePath, schema, partitionSpec);
    }

    private List<InternalRow> writeFullTypeRows(
            Table table, int rowCount, @Nullable String partition) throws Exception {
        List<Record> records = new ArrayList<>();
        List<InternalRow> flussRows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            GenericRecord record = GenericRecord.create(table.schema());

            record.setField("id", (long) i);
            record.setField("name", "name_" + i);
            record.setField("age", 20 + (i % 30));
            record.setField("salary", 50000.0 + (i * 1000));
            record.setField("is_active", i % 2 == 0);
            record.setField("tiny_int", i % 128);
            record.setField("small_int", i % 32768);
            record.setField("float_val", 100.5f + i);
            record.setField("decimal_val", new BigDecimal(i + 100.25));
            record.setField("timestamp_ntz", LocalDateTime.now());
            record.setField("timestamp_ltz", OffsetDateTime.now(ZoneOffset.UTC));
            record.setField("binary_data", ByteBuffer.wrap("Hello World!".getBytes()));
            record.setField("char_data", "char_" + i);
            record.setField("dt", partition);

            record.setField("__bucket", 0);
            record.setField("__offset", (long) i);
            record.setField("__timestamp", OffsetDateTime.now(ZoneOffset.UTC));

            records.add(record);

            IcebergRecordAsFlussRow row = new IcebergRecordAsFlussRow();
            row.replaceIcebergRecord(record);
            flussRows.add(row);
        }
        writeRecord(table, records, partition, 0);
        return flussRows;
    }
}
