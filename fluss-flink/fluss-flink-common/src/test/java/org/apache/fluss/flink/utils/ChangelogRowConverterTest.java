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
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link ChangelogRowConverter}. */
class ChangelogRowConverterTest {

    private RowType testRowType;
    private ChangelogRowConverter converter;

    @BeforeEach
    void setUp() {
        // Create a simple test table schema: (id INT, name STRING, amount BIGINT)
        testRowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .field("amount", DataTypes.BIGINT())
                        .build();

        converter = new ChangelogRowConverter(testRowType);
    }

    @Test
    void testConvertInsertRecord() throws Exception {
        LogRecord record = createLogRecord(ChangeType.INSERT, 100L, 1, "Alice", 5000L);

        RowData result = converter.convert(record);

        // Verify row kind
        assertThat(result.getRowKind()).isEqualTo(RowKind.INSERT);

        // Verify metadata columns
        assertThat(result.getString(0)).isEqualTo(StringData.fromString("+I"));
        assertThat(result.getLong(1)).isEqualTo(100L); // log offset
        assertThat(result.getTimestamp(2, 3)).isNotNull(); // commit timestamp

        // Verify physical columns
        assertThat(result.getInt(3)).isEqualTo(1); // id
        assertThat(result.getString(4).toString()).isEqualTo("Alice"); // name
        assertThat(result.getLong(5)).isEqualTo(5000L); // amount

        // Verify it's a JoinedRowData
        assertThat(result).isInstanceOf(JoinedRowData.class);
    }

    @Test
    void testConvertUpdateBeforeRecord() throws Exception {
        LogRecord record = createLogRecord(ChangeType.UPDATE_BEFORE, 200L, 2, "Bob", 3000L);

        RowData result = converter.convert(record);

        // Verify row kind (always INSERT for virtual table)
        assertThat(result.getRowKind()).isEqualTo(RowKind.INSERT);

        // Verify change type metadata
        assertThat(result.getString(0)).isEqualTo(StringData.fromString("-U"));
        assertThat(result.getLong(1)).isEqualTo(200L);

        // Verify physical columns
        assertThat(result.getInt(3)).isEqualTo(2);
        assertThat(result.getString(4).toString()).isEqualTo("Bob");
        assertThat(result.getLong(5)).isEqualTo(3000L);
    }

    @Test
    void testConvertUpdateAfterRecord() throws Exception {
        LogRecord record = createLogRecord(ChangeType.UPDATE_AFTER, 201L, 2, "Bob", 4000L);

        RowData result = converter.convert(record);

        assertThat(result.getString(0)).isEqualTo(StringData.fromString("+U"));
        assertThat(result.getLong(1)).isEqualTo(201L);
        assertThat(result.getInt(3)).isEqualTo(2);
        assertThat(result.getString(4).toString()).isEqualTo("Bob");
        assertThat(result.getLong(5)).isEqualTo(4000L);
    }

    @Test
    void testConvertDeleteRecord() throws Exception {
        LogRecord record = createLogRecord(ChangeType.DELETE, 300L, 3, "Charlie", 1000L);

        RowData result = converter.convert(record);

        assertThat(result.getString(0)).isEqualTo(StringData.fromString("-D"));
        assertThat(result.getLong(1)).isEqualTo(300L);
        assertThat(result.getInt(3)).isEqualTo(3);
        assertThat(result.getString(4).toString()).isEqualTo("Charlie");
        assertThat(result.getLong(5)).isEqualTo(1000L);
    }

    @Test
    void testProducedTypeHasMetadataColumns() {
        org.apache.flink.table.types.logical.RowType producedType = converter.getProducedType();

        // Should have 3 metadata columns + 3 physical columns
        assertThat(producedType.getFieldCount()).isEqualTo(6);

        // Check metadata column names and types
        assertThat(producedType.getFieldNames())
                .containsExactly(
                        "_change_type", "_log_offset", "_commit_timestamp", "id", "name", "amount");

        // Check metadata column types
        assertThat(producedType.getTypeAt(0))
                .isInstanceOf(org.apache.flink.table.types.logical.VarCharType.class);
        assertThat(producedType.getTypeAt(1))
                .isInstanceOf(org.apache.flink.table.types.logical.BigIntType.class);
        assertThat(producedType.getTypeAt(2))
                .isInstanceOf(org.apache.flink.table.types.logical.LocalZonedTimestampType.class);
    }

    @Test
    void testAllChangeTypes() throws Exception {
        // Test all change type conversions
        assertThat(
                        converter
                                .convert(createLogRecord(ChangeType.INSERT, 1L, 1, "Test", 100L))
                                .getString(0))
                .isEqualTo(StringData.fromString("+I"));

        assertThat(
                        converter
                                .convert(
                                        createLogRecord(
                                                ChangeType.UPDATE_BEFORE, 2L, 1, "Test", 100L))
                                .getString(0))
                .isEqualTo(StringData.fromString("-U"));

        assertThat(
                        converter
                                .convert(
                                        createLogRecord(
                                                ChangeType.UPDATE_AFTER, 3L, 1, "Test", 100L))
                                .getString(0))
                .isEqualTo(StringData.fromString("+U"));

        assertThat(
                        converter
                                .convert(createLogRecord(ChangeType.DELETE, 4L, 1, "Test", 100L))
                                .getString(0))
                .isEqualTo(StringData.fromString("-D"));

        // For log tables (append-only)
        assertThat(
                        converter
                                .convert(
                                        createLogRecord(
                                                ChangeType.APPEND_ONLY, 5L, 1, "Test", 100L))
                                .getString(0))
                .isEqualTo(StringData.fromString("+A"));
    }

    private LogRecord createLogRecord(
            ChangeType changeType, long offset, int id, String name, long amount) throws Exception {
        // Create an IndexedRow with test data
        IndexedRow row = new IndexedRow(testRowType.getChildren().toArray(new DataType[0]));
        try (IndexedRowWriter writer =
                new IndexedRowWriter(testRowType.getChildren().toArray(new DataType[0]))) {
            writer.writeInt(id);
            writer.writeString(BinaryString.fromString(name));
            writer.writeLong(amount);
            writer.complete();

            row.pointTo(writer.segment(), 0, writer.position());

            return new GenericRecord(
                    offset, // log offset
                    System.currentTimeMillis(), // timestamp
                    changeType, // change type
                    row // row data
                    );
        }
    }
}
