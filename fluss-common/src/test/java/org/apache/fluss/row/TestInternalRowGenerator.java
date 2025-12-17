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

package org.apache.fluss.row;

import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.CompactedRowEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.TypeUtils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Random;

import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.INDEXED;
import static org.apache.fluss.row.BinaryString.fromString;

/** Test all types and generate test internal row. */
public class TestInternalRowGenerator {
    public static DataType[] createAllTypes() {
        return createAllRowType().getChildren().toArray(new DataType[0]);
    }

    public static RowType createAllRowType() {
        return DataTypes.ROW(
                new DataField("f0", DataTypes.BOOLEAN()),
                new DataField("f1", DataTypes.TINYINT()),
                new DataField("f2", DataTypes.SMALLINT()),
                new DataField("f3", DataTypes.INT()),
                new DataField("f4", DataTypes.BIGINT()),
                new DataField("f5", DataTypes.FLOAT()),
                new DataField("f6", DataTypes.DOUBLE()),
                new DataField("f7", DataTypes.DATE()),
                new DataField("f8", DataTypes.TIME()),
                new DataField("f9", DataTypes.BINARY(20)),
                new DataField("f10", DataTypes.BYTES()),
                new DataField("f11", DataTypes.CHAR(2)),
                new DataField("f12", DataTypes.STRING()),
                new DataField("f13", DataTypes.DECIMAL(5, 2)),
                new DataField("f14", DataTypes.DECIMAL(20, 0)),
                new DataField("f15", DataTypes.TIMESTAMP(1)),
                new DataField("f16", DataTypes.TIMESTAMP(5)),
                new DataField("f17", DataTypes.TIMESTAMP_LTZ(1)),
                new DataField("f18", DataTypes.TIMESTAMP_LTZ(5)),
                new DataField("f19", DataTypes.ARRAY(DataTypes.INT())),
                new DataField(
                        "f20",
                        DataTypes.ARRAY(DataTypes.FLOAT().copy(false))), // vector embedding type
                new DataField(
                        "f21",
                        DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING()))), // nested array
                new DataField("f22", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())),
                new DataField(
                        "f23",
                        DataTypes.ROW(
                                new DataField("u1", DataTypes.INT()),
                                new DataField(
                                        "u2", DataTypes.ROW(new DataField("v1", DataTypes.INT()))),
                                new DataField("u3", DataTypes.STRING()))));
    }

    public static IndexedRow genIndexedRowForAllType() {
        DataType[] dataTypes = createAllTypes();
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);

        BinaryWriter.ValueWriter[] writers = new BinaryWriter.ValueWriter[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            writers[i] = BinaryWriter.createValueWriter(dataTypes[i], INDEXED);
        }

        Random rnd = new Random();
        setRandomNull(writers[0], writer, 0, rnd, rnd.nextBoolean());
        setRandomNull(writers[1], writer, 1, rnd, (byte) rnd.nextInt());
        setRandomNull(writers[2], writer, 2, rnd, (short) rnd.nextInt());
        setRandomNull(writers[3], writer, 3, rnd, rnd.nextInt());
        setRandomNull(writers[4], writer, 4, rnd, rnd.nextLong());
        setRandomNull(writers[5], writer, 5, rnd, rnd.nextFloat());
        setRandomNull(writers[6], writer, 6, rnd, rnd.nextDouble());
        setRandomNull(writers[7], writer, 7, rnd, generateRandomDate(rnd));
        setRandomNull(writers[8], writer, 8, rnd, generateRandomTime(rnd));
        setRandomNull(writers[9], writer, 9, rnd, generateRandomBinary(rnd, 20));
        setRandomNull(writers[10], writer, 10, rnd, generateRandomBytes(rnd));
        setRandomNull(writers[11], writer, 11, rnd, fromString("12"));
        setRandomNull(writers[12], writer, 12, rnd, fromString(rnd.nextInt() + ""));
        setRandomNull(writers[13], writer, 13, rnd, Decimal.fromUnscaledLong(rnd.nextLong(), 5, 2));
        setRandomNull(
                writers[14],
                writer,
                14,
                rnd,
                Decimal.fromBigDecimal(BigDecimal.valueOf(rnd.nextDouble()), 20, 0));
        setRandomNull(
                writers[15], writer, 15, rnd, TimestampNtz.fromMillis(System.currentTimeMillis()));
        setRandomNull(
                writers[16], writer, 16, rnd, TimestampNtz.fromMillis(System.currentTimeMillis()));
        setRandomNull(
                writers[17],
                writer,
                17,
                rnd,
                TimestampLtz.fromEpochMillis(System.currentTimeMillis()));
        setRandomNull(
                writers[18],
                writer,
                18,
                rnd,
                TimestampLtz.fromEpochMillis(System.currentTimeMillis()));

        GenericArray array1 = GenericArray.of(1, 2, 3, 4, 5, -11, null, 444, 102234);
        setRandomNull(writers[19], writer, 19, rnd, array1);

        GenericArray array2 =
                GenericArray.of(0.1f, 1.1f, -0.5f, 6.6f, Float.MAX_VALUE, Float.MIN_VALUE);
        setRandomNull(writers[20], writer, 20, rnd, array2);

        GenericArray array3 =
                GenericArray.of(
                        GenericArray.of(fromString("a"), null, fromString("c")),
                        null,
                        GenericArray.of(fromString("hello"), fromString("world")));
        setRandomNull(writers[21], writer, 21, rnd, array3);

        GenericMap map = GenericMap.of(0, null, 1, fromString("1"), 2, fromString("2"));
        setRandomNull(writers[22], writer, 22, rnd, map);

        GenericRow innerRow = GenericRow.of(22);
        GenericRow genericRow = GenericRow.of(123, innerRow, BinaryString.fromString("Test"));
        setRandomNull(writers[23], writer, 23, rnd, genericRow);

        IndexedRow row = new IndexedRow(dataTypes);
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }

    public static CompactedRow genCompactedRowForAllType() {
        IndexedRow indexedRow = genIndexedRowForAllType();

        // convert indexed row to compacted row
        DataType[] dataTypes = createAllTypes();
        InternalRow.FieldGetter[] fieldGetters = new InternalRow.FieldGetter[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            fieldGetters[i] = InternalRow.createFieldGetter(dataTypes[i], i);
        }
        CompactedRowEncoder rowEncoder = new CompactedRowEncoder(dataTypes);
        rowEncoder.startNewRow();
        for (int i = 0; i < dataTypes.length; i++) {
            rowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(indexedRow));
        }
        return rowEncoder.finishRow();
    }

    private static void setRandomNull(
            BinaryWriter.ValueWriter fieldWriter,
            IndexedRowWriter writer,
            int pos,
            Random rnd,
            Object value) {
        fieldWriter.writeValue(writer, pos, rnd.nextBoolean() ? null : value);
    }

    private static int generateRandomDate(Random rnd) {
        int year = rnd.nextInt(3000);
        int month = rnd.nextInt(12) + 1;
        int day = rnd.nextInt(28) + 1;

        LocalDate randomDate = LocalDate.of(year, month, day);
        String formattedDate = randomDate.toString(); // xxxx-xx-xx
        return (int) TypeUtils.castFromString(formattedDate, DataTypes.DATE());
    }

    private static int generateRandomTime(Random rnd) {
        int hour = rnd.nextInt(24);
        int minute = rnd.nextInt(60);
        int second = rnd.nextInt(60);

        LocalTime randomTime = LocalTime.of(hour, minute, second);
        String formattedTime = randomTime.toString(); // xx:xx:xx
        return (int) TypeUtils.castFromString(formattedTime, DataTypes.TIME());
    }

    private static byte[] generateRandomBinary(Random rnd, int len) {
        byte[] bytes = new byte[len];
        rnd.nextBytes(bytes);
        return bytes;
    }

    public static byte[] generateRandomBytes(Random rnd) {
        int len = rnd.nextInt(100);
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            for (int next = rnd.nextInt(), n = Math.min(len - i, Integer.SIZE / Byte.SIZE);
                    n-- > 0;
                    next >>= Byte.SIZE) {
                bytes[i++] = (byte) next;
            }
        }
        return bytes;
    }
}
