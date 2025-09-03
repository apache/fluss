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

import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.apache.fluss.record.ChangeType.APPEND_ONLY;
import static org.apache.fluss.record.ChangeType.DELETE;
import static org.apache.fluss.record.ChangeType.INSERT;
import static org.apache.fluss.record.ChangeType.UPDATE_AFTER;
import static org.apache.fluss.record.ChangeType.UPDATE_BEFORE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test case for {@link FlussRowAsPaimonRow}. */
class FlussRowAsPaimonRowTest {
    @Test
    void testLogTableRecordAllTypes() {
        // Construct a FlussRowAsPaimonRow instance
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.BooleanType(),
                        new org.apache.paimon.types.TinyIntType(),
                        new org.apache.paimon.types.SmallIntType(),
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.FloatType(),
                        new org.apache.paimon.types.DoubleType(),
                        new org.apache.paimon.types.VarCharType(),
                        new org.apache.paimon.types.DecimalType(5, 2),
                        new org.apache.paimon.types.DecimalType(20, 0),
                        new org.apache.paimon.types.LocalZonedTimestampType(6),
                        new org.apache.paimon.types.TimestampType(6),
                        new org.apache.paimon.types.BinaryType(),
                        new org.apache.paimon.types.VarCharType());

        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(14);
        genericRow.setField(0, true);
        genericRow.setField(1, (byte) 1);
        genericRow.setField(2, (short) 2);
        genericRow.setField(3, 3);
        genericRow.setField(4, 4L);
        genericRow.setField(5, 5.1f);
        genericRow.setField(6, 6.0d);
        genericRow.setField(7, BinaryString.fromString("string"));
        genericRow.setField(8, Decimal.fromUnscaledLong(9, 5, 2));
        genericRow.setField(9, Decimal.fromBigDecimal(new BigDecimal(10), 20, 0));
        genericRow.setField(10, TimestampLtz.fromEpochMillis(1698235273182L, 5678));
        genericRow.setField(11, TimestampNtz.fromMillis(1698235273182L, 5678));
        genericRow.setField(12, new byte[] {1, 2, 3, 4});
        genericRow.setField(13, null);
        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        FlussRowAsPaimonRow flussRowAsPaimonRow =
                new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType);

        // verify FlussRecordAsPaimonRow normal columns
        assertThat(flussRowAsPaimonRow.getBoolean(0)).isTrue();
        assertThat(flussRowAsPaimonRow.getByte(1)).isEqualTo((byte) 1);
        assertThat(flussRowAsPaimonRow.getShort(2)).isEqualTo((short) 2);
        assertThat(flussRowAsPaimonRow.getInt(3)).isEqualTo(3);
        assertThat(flussRowAsPaimonRow.getLong(4)).isEqualTo(4L);
        assertThat(flussRowAsPaimonRow.getFloat(5)).isEqualTo(5.1f);
        assertThat(flussRowAsPaimonRow.getDouble(6)).isEqualTo(6.0d);
        assertThat(flussRowAsPaimonRow.getString(7).toString()).isEqualTo("string");
        assertThat(flussRowAsPaimonRow.getDecimal(8, 5, 2).toBigDecimal())
                .isEqualTo(new BigDecimal("0.09"));
        assertThat(flussRowAsPaimonRow.getDecimal(9, 20, 0).toBigDecimal())
                .isEqualTo(new BigDecimal(10));
        assertThat(flussRowAsPaimonRow.getTimestamp(10, 6).getMillisecond())
                .isEqualTo(1698235273182L);
        assertThat(flussRowAsPaimonRow.getTimestamp(10, 6).getNanoOfMillisecond()).isEqualTo(5678);
        assertThat(flussRowAsPaimonRow.getTimestamp(11, 6).getMillisecond())
                .isEqualTo(1698235273182L);
        assertThat(flussRowAsPaimonRow.getTimestamp(11, 6).getNanoOfMillisecond()).isEqualTo(5678);
        assertThat(flussRowAsPaimonRow.getBinary(12)).isEqualTo(new byte[] {1, 2, 3, 4});
        assertThat(flussRowAsPaimonRow.isNullAt(13)).isTrue();
    }

    @Test
    void testPrimaryKeyTableRecord() {
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BooleanType());

        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(2);
        genericRow.setField(0, 10);
        genericRow.setField(1, true);

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, INSERT, genericRow);
        FlussRowAsPaimonRow flussRowAsPaimonRow =
                new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType);

        assertThat(flussRowAsPaimonRow.getInt(0)).isEqualTo(10);
        // verify rowkind
        assertThat(flussRowAsPaimonRow.getRowKind()).isEqualTo(RowKind.INSERT);

        logRecord = new GenericRecord(logOffset, timeStamp, UPDATE_BEFORE, genericRow);
        assertThat(new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType).getRowKind())
                .isEqualTo(RowKind.INSERT);

        logRecord = new GenericRecord(logOffset, timeStamp, UPDATE_AFTER, genericRow);
        assertThat(new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType).getRowKind())
                .isEqualTo(RowKind.INSERT);

        logRecord = new GenericRecord(logOffset, timeStamp, DELETE, genericRow);
        assertThat(new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType).getRowKind())
                .isEqualTo(RowKind.INSERT);
    }
}
