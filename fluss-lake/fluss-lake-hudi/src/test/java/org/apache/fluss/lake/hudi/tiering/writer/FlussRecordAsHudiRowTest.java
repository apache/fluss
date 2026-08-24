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

package org.apache.fluss.lake.hudi.tiering.writer;

import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.fluss.record.ChangeType.APPEND_ONLY;
import static org.apache.fluss.record.ChangeType.DELETE;
import static org.apache.fluss.record.ChangeType.UPDATE_AFTER;
import static org.apache.fluss.record.ChangeType.UPDATE_BEFORE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlussRecordAsHudiRow}. */
class FlussRecordAsHudiRowTest {

    private static final LogicalType[] BUSINESS_TYPES =
            new LogicalType[] {
                new BooleanType(),
                new IntType(),
                new BigIntType(),
                new VarCharType(),
                new DecimalType(10, 2),
                new LocalZonedTimestampType(6),
                new TimestampType(6),
                new BinaryType()
            };

    private static GenericRow businessRow() {
        GenericRow row = new GenericRow(8);
        row.setField(0, true);
        row.setField(1, 1);
        row.setField(2, 2L);
        row.setField(3, BinaryString.fromString("fluss"));
        row.setField(4, Decimal.fromBigDecimal(new BigDecimal("12.34"), 10, 2));
        row.setField(5, TimestampLtz.fromEpochMillis(1698235273182L, 5678));
        row.setField(6, TimestampNtz.fromMillis(1698235273182L, 5678));
        row.setField(7, new byte[] {1, 2, 3});
        return row;
    }

    private static void assertBusinessFields(FlussRecordAsHudiRow hudiRow) {
        assertThat(hudiRow.getBoolean(0)).isTrue();
        assertThat(hudiRow.getInt(1)).isEqualTo(1);
        assertThat(hudiRow.getLong(2)).isEqualTo(2L);
        assertThat(hudiRow.getString(3).toString()).isEqualTo("fluss");
        assertThat(hudiRow.getDecimal(4, 10, 2).toBigDecimal()).isEqualTo(new BigDecimal("12.34"));
        assertThat(hudiRow.getTimestamp(5, 6).getMillisecond()).isEqualTo(1698235273182L);
        assertThat(hudiRow.getTimestamp(5, 6).getNanoOfMillisecond()).isEqualTo(5678);
        assertThat(hudiRow.getTimestamp(6, 6).getMillisecond()).isEqualTo(1698235273182L);
        assertThat(hudiRow.getBinary(7)).containsExactly(1, 2, 3);
    }

    @Test
    void testLogRecordFieldsAndSystemColumnsForLegacyTable() {
        // A legacy table carries the three trailing system columns, named __bucket/__offset/
        // __timestamp so isLegacyTable(rowType) detects the legacy layout.
        RowType rowType =
                RowType.of(
                        new LogicalType[] {
                            BUSINESS_TYPES[0],
                            BUSINESS_TYPES[1],
                            BUSINESS_TYPES[2],
                            BUSINESS_TYPES[3],
                            BUSINESS_TYPES[4],
                            BUSINESS_TYPES[5],
                            BUSINESS_TYPES[6],
                            BUSINESS_TYPES[7],
                            new IntType(),
                            new BigIntType(),
                            new TimestampType(6)
                        },
                        new String[] {
                            "f0",
                            "f1",
                            "f2",
                            "f3",
                            "f4",
                            "f5",
                            "f6",
                            "f7",
                            BUCKET_COLUMN_NAME,
                            OFFSET_COLUMN_NAME,
                            TIMESTAMP_COLUMN_NAME
                        });

        int bucket = 3;
        long offset = 11L;
        long timestamp = 1698235273999L;
        LogRecord logRecord = new GenericRecord(offset, timestamp, APPEND_ONLY, businessRow());
        FlussRecordAsHudiRow hudiRow = new FlussRecordAsHudiRow(bucket, rowType);
        hudiRow.setFlussRecord(logRecord);

        assertBusinessFields(hudiRow);
        // legacy tables also expose the trailing system columns
        assertThat(hudiRow.getInt(8)).isEqualTo(bucket);
        assertThat(hudiRow.getLong(9)).isEqualTo(offset);
        assertThat(hudiRow.getLong(10)).isEqualTo(timestamp);
        assertThat(hudiRow.getTimestamp(10, 6).getMillisecond()).isEqualTo(timestamp);
        assertThat(hudiRow.getArity()).isEqualTo(11);
        assertThat(hudiRow.getRowKind()).isEqualTo(RowKind.INSERT);
    }

    @Test
    void testLogRecordFieldsForCleanTable() {
        // FIP-27: a clean table has no system columns; the row type is the business columns only.
        RowType rowType = RowType.of(BUSINESS_TYPES);

        LogRecord logRecord = new GenericRecord(11L, 1698235273999L, APPEND_ONLY, businessRow());
        FlussRecordAsHudiRow hudiRow = new FlussRecordAsHudiRow(3, rowType);
        hudiRow.setFlussRecord(logRecord);

        assertBusinessFields(hudiRow);
        // clean tables expose only the business columns; no system columns are appended
        assertThat(hudiRow.getArity()).isEqualTo(8);
        assertThat(hudiRow.getRowKind()).isEqualTo(RowKind.INSERT);
    }

    @Test
    void testChangeTypeToRowKind() {
        // A clean single-column table: the row type matches the business row exactly.
        RowType rowType = RowType.of(new BooleanType());
        GenericRow row = new GenericRow(1);
        row.setField(0, true);
        FlussRecordAsHudiRow hudiRow = new FlussRecordAsHudiRow(0, rowType);

        hudiRow.setFlussRecord(new GenericRecord(0, 1, UPDATE_BEFORE, row));
        assertThat(hudiRow.getRowKind()).isEqualTo(RowKind.UPDATE_BEFORE);

        hudiRow.setFlussRecord(new GenericRecord(0, 1, UPDATE_AFTER, row));
        assertThat(hudiRow.getRowKind()).isEqualTo(RowKind.UPDATE_AFTER);

        hudiRow.setFlussRecord(new GenericRecord(0, 1, DELETE, row));
        assertThat(hudiRow.getRowKind()).isEqualTo(RowKind.DELETE);
    }
}
