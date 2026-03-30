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

package org.apache.fluss.flink.utils;

import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link BinlogRowConverter}. */
class BinlogRowConverterTest {

    private RowType testRowType;
    private BinlogRowConverter converter;

    @BeforeEach
    void setUp() {
        // Create a simple test table schema: (id INT, name STRING, amount BIGINT)
        testRowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .field("amount", DataTypes.BIGINT())
                        .build();

        converter = new BinlogRowConverter(testRowType);
    }

    @Test
    void testConvertInsertRecord() throws Exception {
        LogRecord record = createLogRecord(ChangeType.INSERT, 100L, 1000L, 1, "Alice", 5000L);

        RowData result = converter.convert(record);

        // Verify row kind is always INSERT for virtual tables
        assertThat(result).isNotNull();
        assertThat(result.getRowKind()).isEqualTo(RowKind.INSERT);

        // Verify metadata columns
        assertThat(result.getString(0)).isEqualTo(StringData.fromString("insert"));
        assertThat(result.getLong(1)).isEqualTo(100L); // log offset
        assertThat(result.getTimestamp(2, 3)).isEqualTo(TimestampData.fromEpochMillis(1000L));

        // Verify before is null for INSERT
        assertThat(result.isNullAt(3)).isTrue();

        // Verify after contains the row data
        RowData afterRow = result.getRow(4, 3);
        assertThat(afterRow).isNotNull();
        assertThat(afterRow.getInt(0)).isEqualTo(1); // id
        assertThat(afterRow.getString(1).toString()).isEqualTo("Alice"); // name
        assertThat(afterRow.getLong(2)).isEqualTo(5000L); // amount
    }

    @Test
    void testConvertUpdateMerge() throws Exception {
        // Send -U (UPDATE_BEFORE) - should return null (buffered)
        LogRecord beforeRecord =
                createLogRecord(ChangeType.UPDATE_BEFORE, 200L, 2000L, 2, "Bob", 3000L);
        RowData beforeResult = converter.convert(beforeRecord);
        assertThat(beforeResult).isNull();

        // Send +U (UPDATE_AFTER) - should return merged row
        LogRecord afterRecord =
                createLogRecord(ChangeType.UPDATE_AFTER, 201L, 2000L, 2, "Bob-Updated", 4000L);
        RowData result = converter.convert(afterRecord);

        assertThat(result).isNotNull();
        assertThat(result.getRowKind()).isEqualTo(RowKind.INSERT);

        // Verify metadata columns
        assertThat(result.getString(0)).isEqualTo(StringData.fromString("update"));
        // Offset and timestamp should be from the -U record (first entry of update pair)
        assertThat(result.getLong(1)).isEqualTo(200L);
        assertThat(result.getTimestamp(2, 3)).isEqualTo(TimestampData.fromEpochMillis(2000L));

        // Verify before contains old data
        RowData beforeRow = result.getRow(3, 3);
        assertThat(beforeRow).isNotNull();
        assertThat(beforeRow.getInt(0)).isEqualTo(2);
        assertThat(beforeRow.getString(1).toString()).isEqualTo("Bob");
        assertThat(beforeRow.getLong(2)).isEqualTo(3000L);

        // Verify after contains new data
        RowData afterRow = result.getRow(4, 3);
        assertThat(afterRow).isNotNull();
        assertThat(afterRow.getInt(0)).isEqualTo(2);
        assertThat(afterRow.getString(1).toString()).isEqualTo("Bob-Updated");
        assertThat(afterRow.getLong(2)).isEqualTo(4000L);
    }

    @Test
    void testConvertDeleteRecord() throws Exception {
        LogRecord record = createLogRecord(ChangeType.DELETE, 300L, 3000L, 3, "Charlie", 1000L);

        RowData result = converter.convert(record);

        assertThat(result).isNotNull();
        assertThat(result.getRowKind()).isEqualTo(RowKind.INSERT);

        // Verify metadata columns
        assertThat(result.getString(0)).isEqualTo(StringData.fromString("delete"));
        assertThat(result.getLong(1)).isEqualTo(300L);
        assertThat(result.getTimestamp(2, 3)).isEqualTo(TimestampData.fromEpochMillis(3000L));

        // Verify before contains the deleted row data
        RowData beforeRow = result.getRow(3, 3);
        assertThat(beforeRow).isNotNull();
        assertThat(beforeRow.getInt(0)).isEqualTo(3);
        assertThat(beforeRow.getString(1).toString()).isEqualTo("Charlie");
        assertThat(beforeRow.getLong(2)).isEqualTo(1000L);

        // Verify after is null for DELETE
        assertThat(result.isNullAt(4)).isTrue();
    }

    @Test
    void testUpdateBeforeReturnsNull() throws Exception {
        LogRecord record =
                createLogRecord(ChangeType.UPDATE_BEFORE, 400L, 4000L, 4, "Diana", 2000L);

        // UPDATE_BEFORE should return null (buffered for merging)
        RowData result = converter.convert(record);
        assertThat(result).isNull();
    }

    @Test
    void testUpdateAfterWithoutBeforeThrows() throws Exception {
        // Sending +U without a preceding -U should throw
        LogRecord record = createLogRecord(ChangeType.UPDATE_AFTER, 500L, 5000L, 5, "Eve", 6000L);

        assertThatThrownBy(() -> converter.convert(record))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("UPDATE_AFTER (+U) without a preceding UPDATE_BEFORE (-U)");
    }

    @Test
    void testAppendOnlyUnsupported() throws Exception {
        LogRecord record = createLogRecord(ChangeType.APPEND_ONLY, 600L, 6000L, 6, "Frank", 7000L);

        assertThatThrownBy(() -> converter.convert(record))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("$binlog virtual table does not support change type");
    }

    @Test
    void testProducedTypeHasNestedRowColumns() {
        org.apache.flink.table.types.logical.RowType producedType = converter.getProducedType();

        // Should have 5 columns: _change_type, _log_offset, _commit_timestamp, before, after
        assertThat(producedType.getFieldCount()).isEqualTo(5);

        // Check column names
        assertThat(producedType.getFieldNames())
                .containsExactly(
                        "_change_type", "_log_offset", "_commit_timestamp", "before", "after");

        // Check metadata column types
        assertThat(producedType.getTypeAt(0))
                .isInstanceOf(org.apache.flink.table.types.logical.VarCharType.class);
        assertThat(producedType.getTypeAt(1))
                .isInstanceOf(org.apache.flink.table.types.logical.BigIntType.class);
        assertThat(producedType.getTypeAt(2))
                .isInstanceOf(org.apache.flink.table.types.logical.LocalZonedTimestampType.class);

        // Check before and after are ROW types
        assertThat(producedType.getTypeAt(3))
                .isInstanceOf(org.apache.flink.table.types.logical.RowType.class);
        assertThat(producedType.getTypeAt(4))
                .isInstanceOf(org.apache.flink.table.types.logical.RowType.class);

        // Check nested ROW has the original columns
        org.apache.flink.table.types.logical.RowType beforeType =
                (org.apache.flink.table.types.logical.RowType) producedType.getTypeAt(3);
        assertThat(beforeType.getFieldNames()).containsExactly("id", "name", "amount");

        // before/after ROW types should be nullable
        assertThat(producedType.getTypeAt(3).isNullable()).isTrue();
        assertThat(producedType.getTypeAt(4).isNullable()).isTrue();
    }

    @Test
    void testMultipleUpdatesInSequence() throws Exception {
        // First update pair
        converter.convert(createLogRecord(ChangeType.UPDATE_BEFORE, 10L, 1000L, 1, "A", 100L));
        RowData result1 =
                converter.convert(
                        createLogRecord(ChangeType.UPDATE_AFTER, 11L, 1000L, 1, "B", 200L));
        assertThat(result1).isNotNull();
        assertThat(result1.getString(0)).isEqualTo(StringData.fromString("update"));

        // Second update pair (converter state should be clean after first merge)
        converter.convert(createLogRecord(ChangeType.UPDATE_BEFORE, 20L, 2000L, 1, "B", 200L));
        RowData result2 =
                converter.convert(
                        createLogRecord(ChangeType.UPDATE_AFTER, 21L, 2000L, 1, "C", 300L));
        assertThat(result2).isNotNull();
        assertThat(result2.getString(0)).isEqualTo(StringData.fromString("update"));
        assertThat(result2.getLong(1)).isEqualTo(20L); // offset from second -U
    }

    private LogRecord createLogRecord(
            ChangeType changeType, long offset, long timestamp, int id, String name, long amount)
            throws Exception {
        // Create an IndexedRow with test data
        IndexedRow row = new IndexedRow(testRowType.getChildren().toArray(new DataType[0]));
        try (IndexedRowWriter writer =
                new IndexedRowWriter(testRowType.getChildren().toArray(new DataType[0]))) {
            writer.writeInt(id);
            writer.writeString(BinaryString.fromString(name));
            writer.writeLong(amount);
            writer.complete();

            row.pointTo(writer.segment(), 0, writer.position());

            return new GenericRecord(offset, timestamp, changeType, row);
        }
    }
}
