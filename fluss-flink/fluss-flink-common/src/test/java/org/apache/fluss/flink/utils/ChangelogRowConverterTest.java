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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

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

    @ParameterizedTest
    @MethodSource("changeTypeCases")
    void testConvertAllChangeTypes(ChangeType changeType, String expectedChangeType)
            throws Exception {
        LogRecord record = createLogRecord(changeType, 100L, 1, "Alice", 5000L);

        RowData result = converter.convert(record);

        // Virtual table always emits INSERT row kind, regardless of the underlying change type.
        assertThat(result.getRowKind()).isEqualTo(RowKind.INSERT);

        // Verify metadata columns
        assertThat(result.getString(0)).isEqualTo(StringData.fromString(expectedChangeType));
        assertThat(result.getLong(1)).isEqualTo(100L); // log offset
        assertThat(result.getTimestamp(2, 3)).isNotNull(); // commit timestamp

        // Verify physical columns
        assertThat(result.getInt(3)).isEqualTo(1); // id
        assertThat(result.getString(4).toString()).isEqualTo("Alice"); // name
        assertThat(result.getLong(5)).isEqualTo(5000L); // amount

        // Verify it's a JoinedRowData
        assertThat(result).isInstanceOf(JoinedRowData.class);
    }

    private static Stream<Arguments> changeTypeCases() {
        return Stream.of(
                Arguments.of(ChangeType.INSERT, "insert"),
                Arguments.of(ChangeType.DELETE, "delete"),
                Arguments.of(ChangeType.UPDATE_BEFORE, "update_before"),
                Arguments.of(ChangeType.UPDATE_AFTER, "update_after"),
                Arguments.of(ChangeType.APPEND_ONLY, "append_only"));
    }

    @Test
    void testProjectionEmitsSelectedColumnsInOrder() throws Exception {
        // Project the base changelog row
        // [_change_type(0), _log_offset(1), _commit_timestamp(2), id(3), name(4), amount(5)]
        // down to [_log_offset, amount, id] to verify subsetting + reordering.
        ChangelogRowConverter projected =
                new ChangelogRowConverter(testRowType, new int[] {1, 5, 3});

        org.apache.flink.table.types.logical.RowType producedType = projected.getProducedType();
        assertThat(producedType.getFieldNames()).containsExactly("_log_offset", "amount", "id");

        RowData result =
                projected.convert(createLogRecord(ChangeType.INSERT, 100L, 1, "Alice", 5000L));

        assertThat(result.getArity()).isEqualTo(3);
        assertThat(result.getRowKind()).isEqualTo(RowKind.INSERT);
        assertThat(result.getLong(0)).isEqualTo(100L); // _log_offset
        assertThat(result.getLong(1)).isEqualTo(5000L); // amount
        assertThat(result.getInt(2)).isEqualTo(1); // id
    }

    @Test
    void testProjectionMetadataOnly() throws Exception {
        // Project only the _change_type metadata column.
        ChangelogRowConverter projected = new ChangelogRowConverter(testRowType, new int[] {0});

        assertThat(projected.getProducedType().getFieldNames()).containsExactly("_change_type");

        RowData result =
                projected.convert(createLogRecord(ChangeType.DELETE, 300L, 3, "Charlie", 1000L));
        assertThat(result.getArity()).isEqualTo(1);
        assertThat(result.getString(0)).isEqualTo(StringData.fromString("delete"));
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
