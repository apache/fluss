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

package com.alibaba.fluss.row.encode.paimon;

import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.row.indexed.IndexedRowWriter;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.TypeUtils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import static com.alibaba.fluss.row.TestInternalRowGenerator.createAllRowType;
import static org.assertj.core.api.Assertions.assertThat;

/** UT for {@link PaimonKeyEncoder} to verify the encoding result is same to Paimon's. */
class PaimonKeyEncoderTest {

    @Test
    void testEncodeKey() {
        // create a row with all types
        RowType allRowType = createAllRowType();
        DataType[] allDataTypes = allRowType.getChildren().toArray(new DataType[0]);

        IndexedRow indexedRow = genFlussRowForAllTypes(allDataTypes);
        List<String> encodedKeys = allRowType.getFieldNames();
        PaimonKeyEncoder paimonKeyEncoder = new PaimonKeyEncoder(allRowType, encodedKeys);

        // encode with Fluss own implementation for Paimon
        byte[] encodedKey = paimonKeyEncoder.encodeKey(indexedRow);

        // encode with Paimon implementation
        byte[] paimonEncodedKey = genPaimonRowForAllTypes(allRowType.getFieldCount()).toBytes();

        // verify both the result should be same
        assertThat(encodedKey).isEqualTo(paimonEncodedKey);
    }

    private IndexedRow genFlussRowForAllTypes(DataType[] dataTypes) {
        IndexedRow indexedRow = new IndexedRow(dataTypes);
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeBoolean(true);
        writer.writeByte((byte) 2);
        writer.writeShort(Short.parseShort("10"));
        writer.writeInt(100);
        writer.writeLong(new BigInteger("12345678901234567890").longValue());
        writer.writeFloat(Float.parseFloat("13.2"));
        writer.writeDouble(Double.parseDouble("15.21"));
        writer.writeInt((int) TypeUtils.castFromString("2023-10-25", DataTypes.DATE()));
        writer.writeInt((int) TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()));
        writer.writeBinary("1234567890".getBytes(), 20);
        writer.writeBytes("20".getBytes());
        writer.writeChar(com.alibaba.fluss.row.BinaryString.fromString("1"), 2);
        writer.writeString(com.alibaba.fluss.row.BinaryString.fromString("hello"));
        writer.writeDecimal(com.alibaba.fluss.row.Decimal.fromUnscaledLong(9, 5, 2), 5);
        writer.writeDecimal(
                com.alibaba.fluss.row.Decimal.fromBigDecimal(new BigDecimal(10), 20, 0), 20);
        writer.writeTimestampNtz(TimestampNtz.fromMillis(1698235273182L), 1);
        writer.writeTimestampNtz(TimestampNtz.fromMillis(1698235273182L), 5);
        writer.writeTimestampLtz(TimestampLtz.fromEpochMillis(1698235273182L, 45678), 1);
        writer.setNullAt(18);
        indexedRow.pointTo(writer.segment(), 0, writer.position());
        return indexedRow;
    }

    private BinaryRow genPaimonRowForAllTypes(int arity) {
        BinaryRow binaryRow = new BinaryRow(arity);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRow);
        binaryRowWriter.writeBoolean(0, true);
        binaryRowWriter.writeByte(1, (byte) 2);
        binaryRowWriter.writeShort(2, Short.parseShort("10"));
        binaryRowWriter.writeInt(3, 100);
        binaryRowWriter.writeLong(4, new BigInteger("12345678901234567890").longValue());
        binaryRowWriter.writeFloat(5, Float.parseFloat("13.2"));
        binaryRowWriter.writeDouble(6, Double.parseDouble("15.21"));
        binaryRowWriter.writeInt(
                7,
                (int)
                        TypeUtils.castFromString(
                                "2023-10-25", com.alibaba.fluss.types.DataTypes.DATE()));
        binaryRowWriter.writeInt(
                8,
                (int)
                        TypeUtils.castFromString(
                                "09:30:00.0", com.alibaba.fluss.types.DataTypes.TIME()));
        binaryRowWriter.writeBinary(9, "1234567890".getBytes());
        binaryRowWriter.writeBinary(10, "20".getBytes());
        binaryRowWriter.writeString(11, BinaryString.fromString("1"));
        binaryRowWriter.writeString(12, BinaryString.fromString("hello"));
        binaryRowWriter.writeDecimal(13, Decimal.fromUnscaledLong(9, 5, 2), 5);
        binaryRowWriter.writeDecimal(14, Decimal.fromBigDecimal(new BigDecimal(10), 20, 0), 20);
        binaryRowWriter.writeTimestamp(15, Timestamp.fromEpochMillis(1698235273182L), 1);
        binaryRowWriter.writeTimestamp(16, Timestamp.fromEpochMillis(1698235273182L), 5);
        binaryRowWriter.writeTimestamp(17, Timestamp.fromEpochMillis(1698235273182L, 45678), 1);
        binaryRowWriter.setNullAt(18);
        binaryRowWriter.complete();
        return binaryRow;
    }
}
