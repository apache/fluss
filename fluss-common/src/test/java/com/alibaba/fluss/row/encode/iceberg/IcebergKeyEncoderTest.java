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

package com.alibaba.fluss.row.encode.iceberg;

import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.row.indexed.IndexedRowWriter;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.TypeUtils;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import static com.alibaba.fluss.row.TestInternalRowGenerator.createAllRowType;
import static org.assertj.core.api.Assertions.assertThat;

/** UT for {@link IcebergKeyEncoder} to verify the encoding result matches Iceberg's format. */
class IcebergKeyEncoderTest {

    @Test
    void testEncodeKey() {
        // create a row with all types
        RowType allRowType = createAllRowType();
        DataType[] allDataTypes = allRowType.getChildren().toArray(new DataType[0]);

        IndexedRow indexedRow = genFlussRowForAllTypes(allDataTypes);
        List<String> encodedKeys = allRowType.getFieldNames();
        IcebergKeyEncoder icebergKeyEncoder = new IcebergKeyEncoder(allRowType, encodedKeys);

        // encode with Fluss own implementation for Iceberg
        byte[] encodedKey = icebergKeyEncoder.encodeKey(indexedRow);

        // encode with Iceberg implementation using native Iceberg classes
        byte[] icebergEncodedKey = genIcebergRowForAllTypes(allRowType.getFieldCount());

        // verify both results should be same
        assertThat(encodedKey).isEqualTo(icebergEncodedKey);
    }

    @Test
    void testEncodeKeyConsistency() {
        // Test that encoding the same data multiple times produces identical results
        RowType simpleRowType =
                RowType.of(DataTypes.INT(), DataTypes.STRING(), DataTypes.BOOLEAN());

        IndexedRow row = genSimpleFlussRow();
        List<String> keys = simpleRowType.getFieldNames();
        IcebergKeyEncoder encoder = new IcebergKeyEncoder(simpleRowType, keys);

        byte[] encoded1 = encoder.encodeKey(row);
        byte[] encoded2 = encoder.encodeKey(row);

        assertThat(encoded1).isEqualTo(encoded2);
        assertThat(encoded1.length).isGreaterThan(0);
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

    private IndexedRow genSimpleFlussRow() {
        DataType[] dataTypes = {DataTypes.INT(), DataTypes.STRING(), DataTypes.BOOLEAN()};
        IndexedRow indexedRow = new IndexedRow(dataTypes);
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeInt(42);
        writer.writeString(com.alibaba.fluss.row.BinaryString.fromString("test"));
        writer.writeBoolean(true);
        indexedRow.pointTo(writer.segment(), 0, writer.position());
        return indexedRow;
    }

    private byte[] genIcebergRowForAllTypes(int arity) {
        IcebergBinaryRowWriter icebergWriter = new IcebergBinaryRowWriter(arity);
        icebergWriter.reset();

        // Write same data as Fluss row but using Iceberg encoding
        icebergWriter.writeBoolean(0, true);
        icebergWriter.writeByte(1, (byte) 2);
        icebergWriter.writeShort(2, Short.parseShort("10"));
        icebergWriter.writeInt(3, 100);
        icebergWriter.writeLong(4, new BigInteger("12345678901234567890").longValue());
        icebergWriter.writeFloat(5, Float.parseFloat("13.2"));
        icebergWriter.writeDouble(6, Double.parseDouble("15.21"));
        icebergWriter.writeInt(7, (int) TypeUtils.castFromString("2023-10-25", DataTypes.DATE()));
        icebergWriter.writeInt(8, (int) TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()));
        icebergWriter.writeBytes(9, "1234567890".getBytes());
        icebergWriter.writeBytes(10, "20".getBytes());
        icebergWriter.writeString(11, com.alibaba.fluss.row.BinaryString.fromString("1"));
        icebergWriter.writeString(12, com.alibaba.fluss.row.BinaryString.fromString("hello"));
        icebergWriter.writeDecimal(13, com.alibaba.fluss.row.Decimal.fromUnscaledLong(9, 5, 2), 5);
        icebergWriter.writeDecimal(
                14, com.alibaba.fluss.row.Decimal.fromBigDecimal(new BigDecimal(10), 20, 0), 20);
        icebergWriter.writeTimestampNtz(15, TimestampNtz.fromMillis(1698235273182L), 1);
        icebergWriter.writeTimestampNtz(16, TimestampNtz.fromMillis(1698235273182L), 5);
        icebergWriter.writeTimestampLtz(17, TimestampLtz.fromEpochMillis(1698235273182L, 0), 1);
        icebergWriter.setNullAt(18);

        return icebergWriter.toBytes();
    }
}
