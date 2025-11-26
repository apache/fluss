/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.row.encode;

import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AlignedRowEncoder}. */
class AlignedRowEncoderTest {

    @Test
    void testEncodeSimpleTypes() throws Exception {
        DataType[] fieldTypes =
                new DataType[] {
                    DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.BOOLEAN()
                };
        try (AlignedRowEncoder encoder = new AlignedRowEncoder(fieldTypes)) {
            encoder.startNewRow();
            encoder.encodeField(0, 100);
            encoder.encodeField(1, 200L);
            encoder.encodeField(2, BinaryString.fromString("test"));
            encoder.encodeField(3, true);
            BinaryRow row = encoder.finishRow();

            assertThat(row.getInt(0)).isEqualTo(100);
            assertThat(row.getLong(1)).isEqualTo(200L);
            assertThat(row.getString(2)).isEqualTo(BinaryString.fromString("test"));
            assertThat(row.getBoolean(3)).isTrue();
        }
    }

    @Test
    void testEncodeMultipleRows() throws Exception {
        DataType[] fieldTypes = new DataType[] {DataTypes.INT(), DataTypes.STRING()};
        try (AlignedRowEncoder encoder = new AlignedRowEncoder(fieldTypes)) {
            encoder.startNewRow();
            encoder.encodeField(0, 1);
            encoder.encodeField(1, BinaryString.fromString("first"));
            BinaryRow row1 = encoder.finishRow();
            BinaryRow row1Copy = row1.copy();

            encoder.startNewRow();
            encoder.encodeField(0, 2);
            encoder.encodeField(1, BinaryString.fromString("second"));
            BinaryRow row2 = encoder.finishRow();

            assertThat(row1Copy.getInt(0)).isEqualTo(1);
            assertThat(row1Copy.getString(1)).isEqualTo(BinaryString.fromString("first"));

            assertThat(row2.getInt(0)).isEqualTo(2);
            assertThat(row2.getString(1)).isEqualTo(BinaryString.fromString("second"));
        }
    }

    @Test
    void testEncodeAllPrimitiveDataTypes() throws Exception {
        DataType[] fieldTypes =
                new DataType[] {
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.BOOLEAN(),
                    DataTypes.STRING(),
                    DataTypes.BYTES(),
                    DataTypes.DECIMAL(10, 2)
                };
        try (AlignedRowEncoder encoder = new AlignedRowEncoder(fieldTypes)) {
            encoder.startNewRow();
            encoder.encodeField(0, (byte) 1);
            encoder.encodeField(1, (short) 100);
            encoder.encodeField(2, 1000);
            encoder.encodeField(3, 10000L);
            encoder.encodeField(4, 1.5f);
            encoder.encodeField(5, 2.5);
            encoder.encodeField(6, false);
            encoder.encodeField(7, BinaryString.fromString("hello"));
            encoder.encodeField(8, new byte[] {1, 2, 3});
            encoder.encodeField(9, Decimal.fromBigDecimal(new BigDecimal("123.45"), 10, 2));
            BinaryRow row = encoder.finishRow();

            assertThat(row.getByte(0)).isEqualTo((byte) 1);
            assertThat(row.getShort(1)).isEqualTo((short) 100);
            assertThat(row.getInt(2)).isEqualTo(1000);
            assertThat(row.getLong(3)).isEqualTo(10000L);
            assertThat(row.getFloat(4)).isEqualTo(1.5f);
            assertThat(row.getDouble(5)).isEqualTo(2.5);
            assertThat(row.getBoolean(6)).isFalse();
            assertThat(row.getString(7)).isEqualTo(BinaryString.fromString("hello"));
            assertThat(row.getBytes(8)).isEqualTo(new byte[] {1, 2, 3});
            assertThat(row.getDecimal(9, 10, 2))
                    .isEqualTo(Decimal.fromBigDecimal(new BigDecimal("123.45"), 10, 2));
        }
    }

    @Test
    void testEncodeWithNullValues() throws Exception {
        DataType[] fieldTypes =
                new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()};
        try (AlignedRowEncoder encoder = new AlignedRowEncoder(fieldTypes)) {
            encoder.startNewRow();
            encoder.encodeField(0, 42);
            encoder.encodeField(1, null);
            encoder.encodeField(2, 100L);
            BinaryRow row = encoder.finishRow();

            assertThat(row.getInt(0)).isEqualTo(42);
            assertThat(row.isNullAt(1)).isTrue();
            assertThat(row.getLong(2)).isEqualTo(100L);
        }
    }

    @Test
    void testEncodeTimestampTypes() throws Exception {
        DataType[] fieldTypes = new DataType[] {DataTypes.TIMESTAMP_LTZ(3), DataTypes.INT()};
        try (AlignedRowEncoder encoder = new AlignedRowEncoder(fieldTypes)) {
            TimestampLtz timestampLtz = TimestampLtz.fromEpochMillis(1000000L);

            encoder.startNewRow();
            encoder.encodeField(0, timestampLtz);
            encoder.encodeField(1, 999);
            BinaryRow row = encoder.finishRow();

            assertThat(row.getTimestampLtz(0, 3)).isNotNull();
            assertThat(row.getInt(1)).isEqualTo(999);
        }
    }

    @Test
    void testReuseEncoder() throws Exception {
        DataType[] fieldTypes = new DataType[] {DataTypes.INT(), DataTypes.STRING()};
        try (AlignedRowEncoder encoder = new AlignedRowEncoder(fieldTypes)) {
            encoder.startNewRow();
            encoder.encodeField(0, 5);
            encoder.encodeField(1, BinaryString.fromString("row5"));
            BinaryRow row = encoder.finishRow();

            assertThat(row.getInt(0)).isEqualTo(5);
            assertThat(row.getString(1)).isEqualTo(BinaryString.fromString("row5"));

            encoder.startNewRow();
            encoder.encodeField(0, 10);
            encoder.encodeField(1, BinaryString.fromString("row10"));
            row = encoder.finishRow();

            assertThat(row.getInt(0)).isEqualTo(10);
            assertThat(row.getString(1)).isEqualTo(BinaryString.fromString("row10"));
        }
    }

    @Test
    void testEncodeEmptyRow() throws Exception {
        DataType[] fieldTypes = new DataType[] {};
        try (AlignedRowEncoder encoder = new AlignedRowEncoder(fieldTypes)) {
            encoder.startNewRow();
            BinaryRow row = encoder.finishRow();

            assertThat(row.getFieldCount()).isEqualTo(0);
        }
    }

    @Test
    void testEncodeSingleField() throws Exception {
        DataType[] fieldTypes = new DataType[] {DataTypes.STRING()};
        try (AlignedRowEncoder encoder = new AlignedRowEncoder(fieldTypes)) {
            encoder.startNewRow();
            encoder.encodeField(0, BinaryString.fromString("single"));
            BinaryRow row = encoder.finishRow();

            assertThat(row.getString(0)).isEqualTo(BinaryString.fromString("single"));
        }
    }

    @Test
    void testClose() throws Exception {
        DataType[] fieldTypes = new DataType[] {DataTypes.INT()};
        AlignedRowEncoder encoder = new AlignedRowEncoder(fieldTypes);
        encoder.close();
    }

    @Test
    void testEncodeWithLargeStrings() throws Exception {
        DataType[] fieldTypes = new DataType[] {DataTypes.STRING(), DataTypes.INT()};
        try (AlignedRowEncoder encoder = new AlignedRowEncoder(fieldTypes)) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                sb.append("test");
            }
            String largeString = sb.toString();

            encoder.startNewRow();
            encoder.encodeField(0, BinaryString.fromString(largeString));
            encoder.encodeField(1, 123);
            BinaryRow row = encoder.finishRow();

            assertThat(row.getString(0)).isEqualTo(BinaryString.fromString(largeString));
            assertThat(row.getInt(1)).isEqualTo(123);
        }
    }

    @Test
    void testEncodeWithBinaryData() throws Exception {
        DataType[] fieldTypes = new DataType[] {DataTypes.BYTES(), DataTypes.BYTES()};
        try (AlignedRowEncoder encoder = new AlignedRowEncoder(fieldTypes)) {
            byte[] data1 = new byte[100];
            byte[] data2 = new byte[200];
            for (int i = 0; i < data1.length; i++) {
                data1[i] = (byte) i;
            }
            for (int i = 0; i < data2.length; i++) {
                data2[i] = (byte) (i % 256);
            }

            encoder.startNewRow();
            encoder.encodeField(0, data1);
            encoder.encodeField(1, data2);
            BinaryRow row = encoder.finishRow();

            assertThat(row.getBytes(0)).isEqualTo(data1);
            assertThat(row.getBytes(1)).isEqualTo(data2);
        }
    }
}
