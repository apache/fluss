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

import com.alibaba.fluss.record.GenericRecord;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.TimestampLtz;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static com.alibaba.fluss.record.ChangeType.APPEND_ONLY;
import static com.alibaba.fluss.record.ChangeType.DELETE;
import static com.alibaba.fluss.record.ChangeType.INSERT;
import static com.alibaba.fluss.record.ChangeType.UPDATE_AFTER;
import static com.alibaba.fluss.record.ChangeType.UPDATE_BEFORE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlussRecordAsPaimonRow}. */
class FlussRecordAsPaimonRowTest {

    @Test
    void testLogTableRecordAllTypes() {
        // Construct a FlussRecordAsPaimonRow instance
        int bucket = 0;
        FlussRecordAsPaimonRow flussRecordAsPaimonRow = new FlussRecordAsPaimonRow(bucket);
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
        genericRow.setField(10, TimestampLtz.fromEpochMillis(1698235273182L));
        genericRow.setField(11, TimestampLtz.fromEpochMillis(1698235273182L, 45678));
        genericRow.setField(12, new byte[] {1, 2, 3, 4});
        genericRow.setField(13, null);
        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        flussRecordAsPaimonRow.setFlussRecord(logRecord);

        // verify FlussRecordAsPaimonRow normal columns
        assertThat(flussRecordAsPaimonRow.getBoolean(0)).isTrue();
        assertThat(flussRecordAsPaimonRow.getByte(1)).isEqualTo((byte) 1);
        assertThat(flussRecordAsPaimonRow.getShort(2)).isEqualTo((short) 2);
        assertThat(flussRecordAsPaimonRow.getInt(3)).isEqualTo(3);
        assertThat(flussRecordAsPaimonRow.getLong(4)).isEqualTo(4L);
        assertThat(flussRecordAsPaimonRow.getFloat(5)).isEqualTo(5.1f);
        assertThat(flussRecordAsPaimonRow.getDouble(6)).isEqualTo(6.0d);
        assertThat(flussRecordAsPaimonRow.getString(7).toString()).isEqualTo("string");
        assertThat(flussRecordAsPaimonRow.getDecimal(8, 5, 2).toBigDecimal())
                .isEqualTo(new BigDecimal("0.09"));
        assertThat(flussRecordAsPaimonRow.getDecimal(9, 20, 0).toBigDecimal())
                .isEqualTo(new BigDecimal(10));
        assertThat(flussRecordAsPaimonRow.getTimestamp(10, 3).getMillisecond())
                .isEqualTo(1698235273182L);
        assertThat(flussRecordAsPaimonRow.getTimestamp(11, 6).getMillisecond())
                .isEqualTo(1698235273182L);
        assertThat(flussRecordAsPaimonRow.getBinary(12)).isEqualTo(new byte[] {1, 2, 3, 4});
        assertThat(flussRecordAsPaimonRow.isNullAt(13)).isTrue();

        // verify FlussRecordAsPaimonRow system columns (no partition fields, so indices stay same)
        assertThat(flussRecordAsPaimonRow.getInt(14)).isEqualTo(bucket);
        assertThat(flussRecordAsPaimonRow.getLong(15)).isEqualTo(logOffset);
        assertThat(flussRecordAsPaimonRow.getLong(16)).isEqualTo(timeStamp);
        assertThat(flussRecordAsPaimonRow.getTimestamp(16, 4))
                .isEqualTo(Timestamp.fromEpochMillis(timeStamp));
        assertThat(flussRecordAsPaimonRow.getRowKind()).isEqualTo(RowKind.INSERT);

        assertThat(flussRecordAsPaimonRow.getFieldCount())
                .isEqualTo(14 + 3); // business  + system = 14 + 0 + 3 = 17
    }

    @Test
    void testPrimaryKeyTableRecord() {
        int bucket = 0;
        FlussRecordAsPaimonRow flussRecordAsPaimonRow = new FlussRecordAsPaimonRow(bucket);
        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(0, true);

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, INSERT, genericRow);
        flussRecordAsPaimonRow.setFlussRecord(logRecord);

        assertThat(flussRecordAsPaimonRow.getBoolean(0)).isTrue();
        // normal columns + system columns
        assertThat(flussRecordAsPaimonRow.getFieldCount()).isEqualTo(4);
        // verify rowkind
        assertThat(flussRecordAsPaimonRow.getRowKind()).isEqualTo(RowKind.INSERT);

        logRecord = new GenericRecord(logOffset, timeStamp, UPDATE_BEFORE, genericRow);
        flussRecordAsPaimonRow.setFlussRecord(logRecord);
        assertThat(flussRecordAsPaimonRow.getRowKind()).isEqualTo(RowKind.UPDATE_BEFORE);

        logRecord = new GenericRecord(logOffset, timeStamp, UPDATE_AFTER, genericRow);
        flussRecordAsPaimonRow.setFlussRecord(logRecord);
        assertThat(flussRecordAsPaimonRow.getRowKind()).isEqualTo(RowKind.UPDATE_AFTER);

        logRecord = new GenericRecord(logOffset, timeStamp, DELETE, genericRow);
        flussRecordAsPaimonRow.setFlussRecord(logRecord);
        assertThat(flussRecordAsPaimonRow.getRowKind()).isEqualTo(RowKind.DELETE);
    }
}
