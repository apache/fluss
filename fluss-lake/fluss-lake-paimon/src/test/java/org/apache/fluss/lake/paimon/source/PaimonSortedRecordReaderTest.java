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
 *
 *
 */

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.SortedRecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test case for {@link PaimonSortedRecordReader}. */
class PaimonSortedRecordReaderTest extends PaimonSourceTestBase {
    @BeforeAll
    protected static void beforeAll() {
        PaimonSourceTestBase.beforeAll();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testReadPkTable(boolean isPartitioned) throws Exception {
        // first of all, create table and prepare data
        String tableName = "pkTable_" + (isPartitioned ? "partitioned" : "non_partitioned");

        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);

        List<InternalRow> writtenRows = new ArrayList<>();
        preparePkTable(tablePath, isPartitioned, DEFAULT_BUCKET_NUM, writtenRows);

        LakeSource<PaimonSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        Table table = getTable(tablePath);

        int[] pkIndex = table.rowType().getFieldIndices(table.primaryKeys());
        ProjectedRow projectedPkRow = ProjectedRow.from(pkIndex);
        Snapshot snapshot = table.latestSnapshot().get();
        List<PaimonSplit> paimonSplits = lakeSource.createPlanner(snapshot::id).plan();

        for (PaimonSplit paimonSplit : paimonSplits) {
            RecordReader recordReader = lakeSource.createRecordReader(() -> paimonSplit);
            assertThat(recordReader).isInstanceOf(PaimonSortedRecordReader.class);
            CloseableIterator<LogRecord> iterator = recordReader.read();

            assertThat(
                            isSorted(
                                    TransformingCloseableIterator.transform(
                                            iterator,
                                            record -> {
                                                projectedPkRow.replaceRow(record.getRow());
                                                return projectedPkRow;
                                            }),
                                    ((SortedRecordReader) recordReader).order()))
                    .isTrue();
            iterator.close();
        }
    }

    @Test
    void testComparatorUsesPrimaryKeyRowType() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "pkTable_composite_timestamp_key");

        createTable(
                tablePath,
                Schema.newBuilder()
                        .column("member_id", DataTypes.BIGINT())
                        .column("product_id", DataTypes.STRING())
                        .column("channel_key", DataTypes.STRING())
                        .column("product_name", DataTypes.STRING())
                        .column("seq_time", DataTypes.TIMESTAMP(0))
                        .column("order_id", DataTypes.STRING())
                        .primaryKey("member_id", "channel_key", "seq_time", "order_id")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "member_id,channel_key,seq_time")
                        .build());

        LakeSource<PaimonSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        RecordReader recordReader = lakeSource.createRecordReader(() -> null);
        assertThat(recordReader).isInstanceOf(PaimonSortedRecordReader.class);

        Comparator<InternalRow> comparator = ((SortedRecordReader) recordReader).order();
        Table table = getTable(tablePath);
        int[] pkIndex = table.rowType().getFieldIndices(table.primaryKeys());

        GenericRow row1 = new GenericRow(6);
        row1.setField(0, 1L);
        row1.setField(1, org.apache.fluss.row.BinaryString.fromString("product-1"));
        row1.setField(2, org.apache.fluss.row.BinaryString.fromString("channel-a"));
        row1.setField(3, org.apache.fluss.row.BinaryString.fromString("name-1"));
        row1.setField(4, TimestampNtz.fromMillis(1_700_000_000_000L));
        row1.setField(5, org.apache.fluss.row.BinaryString.fromString("order-1"));

        GenericRow row2 = new GenericRow(6);
        row2.setField(0, 1L);
        row2.setField(1, org.apache.fluss.row.BinaryString.fromString("product-2"));
        row2.setField(2, org.apache.fluss.row.BinaryString.fromString("channel-a"));
        row2.setField(3, org.apache.fluss.row.BinaryString.fromString("name-2"));
        row2.setField(4, TimestampNtz.fromMillis(1_700_000_000_001L));
        row2.setField(5, org.apache.fluss.row.BinaryString.fromString("order-2"));

        InternalRow pkRow1 = ProjectedRow.from(pkIndex).replaceRow(row1);
        InternalRow pkRow2 = ProjectedRow.from(pkIndex).replaceRow(row2);

        assertThat(comparator.compare(pkRow1, pkRow2)).isLessThan(0);
    }

    private static <T> boolean isSorted(Iterator<T> iterator, Comparator<? super T> comparator) {
        if (!iterator.hasNext()) {
            return true;
        }

        T previous = iterator.next();
        while (iterator.hasNext()) {
            T current = iterator.next();
            if (comparator.compare(previous, current) > 0) {
                return false;
            }
            previous = current;
        }
        return true;
    }

    private void preparePkTable(
            TablePath tablePath, boolean isPartitioned, int bucketNum, List<InternalRow> rows)
            throws Exception {
        createFullTypePkTable(tablePath, isPartitioned, bucketNum);
        rows.addAll(writeFullTypeRows(tablePath, 10, isPartitioned ? "test" : null));
    }

    private void createFullTypePkTable(TablePath tablePath, boolean isPartitioned, int bucketNum)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("f_int", DataTypes.INT())
                        .column("f_boolean", DataTypes.BOOLEAN())
                        .column("f_byte", DataTypes.TINYINT())
                        .column("f_short", DataTypes.SMALLINT())
                        .column("f_long", DataTypes.BIGINT())
                        .column("f_float", DataTypes.FLOAT())
                        .column("f_double", DataTypes.DOUBLE())
                        .column("f_string", DataTypes.STRING())
                        .column("f_decimal1", DataTypes.DECIMAL(5, 2))
                        .column("f_decimal2", DataTypes.DECIMAL(20, 0))
                        .column("f_timestamp_ltz1", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
                        .column("f_timestamp_ltz2", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6))
                        .column("f_timestamp_ntz1", DataTypes.TIMESTAMP(3))
                        .column("f_timestamp_ntz2", DataTypes.TIMESTAMP(6))
                        .column("f_binary", DataTypes.BINARY(4));

        if (isPartitioned) {
            schemaBuilder.column("p", DataTypes.STRING());
            schemaBuilder.partitionKeys("p");
            schemaBuilder.primaryKey("f_int", "p");
            schemaBuilder.option(CoreOptions.BUCKET.key(), String.valueOf(bucketNum));
            schemaBuilder.option(CoreOptions.BUCKET_KEY.key(), "f_int");
        } else {
            schemaBuilder.primaryKey("f_int");
            schemaBuilder.option(CoreOptions.BUCKET.key(), String.valueOf(bucketNum));
            schemaBuilder.option(CoreOptions.BUCKET_KEY.key(), "f_int");
        }
        schemaBuilder
                .column("__bucket", DataTypes.INT())
                .column("__offset", DataTypes.BIGINT())
                .column("__timestamp", DataTypes.TIMESTAMP(6));
        createTable(tablePath, schemaBuilder.build());
    }

    private List<InternalRow> writeFullTypeRows(
            TablePath tablePath, int rowCount, @Nullable String partition) throws Exception {
        List<org.apache.paimon.data.InternalRow> rows = new ArrayList<>();
        List<InternalRow> flussRows = new ArrayList<>();
        Table table = getTable(tablePath);

        for (int i = 0; i < rowCount; i++) {
            if (partition == null) {
                org.apache.fluss.row.GenericRow row =
                        row(
                                i + 30,
                                true,
                                (byte) 100,
                                (short) 200,
                                400L,
                                500.1f,
                                600.0d,
                                org.apache.fluss.row.BinaryString.fromString("another_string_" + i),
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8},
                                0,
                                (long) i,
                                TimestampNtz.fromMillis(System.currentTimeMillis()));
                rows.add(new FlussRowAsPaimonRow(row, table.rowType()));
                flussRows.add(row);
            } else {
                org.apache.fluss.row.GenericRow row =
                        row(
                                i + 30,
                                true,
                                (byte) 100,
                                (short) 200,
                                400L,
                                500.1f,
                                600.0d,
                                org.apache.fluss.row.BinaryString.fromString("another_string_" + i),
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8},
                                org.apache.fluss.row.BinaryString.fromString(partition),
                                0,
                                (long) i,
                                TimestampNtz.fromMillis(System.currentTimeMillis()));
                rows.add(new FlussRowAsPaimonRow(row, table.rowType()));
                flussRows.add(row);
            }
        }
        writeRecord(tablePath, rows);
        return flussRows;
    }
}
