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

import static org.apache.fluss.row.BinaryString.fromString;

/** Test all types and generate test internal row. */
public class TestInternalRowGenerator {
    public static DataType[] createAllTypes() {
        return createAllRowType().getChildren().toArray(new DataType[0]);
    }

    public static RowType createAllRowType() {
        return DataTypes.ROW(
                new DataField("a", DataTypes.BOOLEAN()),
                new DataField("b", DataTypes.TINYINT()),
                new DataField("c", DataTypes.SMALLINT()),
                new DataField("d", DataTypes.INT()),
                new DataField("e", DataTypes.BIGINT()),
                new DataField("f", DataTypes.FLOAT()),
                new DataField("g", DataTypes.DOUBLE()),
                new DataField("h", DataTypes.DATE()),
                new DataField("i", DataTypes.TIME()),
                new DataField("j", DataTypes.BINARY(20)),
                new DataField("k", DataTypes.BYTES()),
                new DataField("l", DataTypes.CHAR(2)),
                new DataField("m", DataTypes.STRING()),
                new DataField("n", DataTypes.DECIMAL(5, 2)),
                new DataField("o", DataTypes.DECIMAL(20, 0)),
                new DataField("p", DataTypes.TIMESTAMP(1)),
                new DataField("q", DataTypes.TIMESTAMP(5)),
                new DataField("r", DataTypes.TIMESTAMP_LTZ(1)),
                new DataField("s", DataTypes.TIMESTAMP_LTZ(5)),
                new DataField("t", DataTypes.ARRAY(DataTypes.INT()))
                // TODO: Add Map and Row fields in Issue #1973 and #1974
                // new DataField("u", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())),
                // new DataField(
                //         "v",
                //         DataTypes.ROW(
                //                 new DataField("u1", DataTypes.INT()),
                //                 new DataField("u2", DataTypes.ROW(DataTypes.INT())),
                //                 new DataField("u3", DataTypes.STRING())))
                );
    }

    public static IndexedRow genIndexedRowForAllType() {
        DataType[] dataTypes = createAllTypes();
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);

        IndexedRowWriter.FieldWriter[] writers = new IndexedRowWriter.FieldWriter[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            writers[i] = IndexedRowWriter.createFieldWriter(dataTypes[i]);
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

        GenericArray array = new GenericArray(new int[] {1, 2, 3});
        setRandomNull(writers[19], writer, 19, rnd, array);

        // TODO: Map type support will be added in Issue #1973
        // Map<Object, Object> javaMap = new HashMap<>();
        // javaMap.put(0, null);
        // javaMap.put(1, fromString("1"));
        // javaMap.put(2, fromString("2"));
        // GenericMap map = new GenericMap(javaMap);
        // setRandomNull(writers[20], writer, 20, rnd, map);

        // TODO: Row type support will be added in Issue #1974
        // GenericRow innerRow = GenericRow.of(123);
        // GenericRow genericRow = GenericRow.of(20, innerRow, BinaryString.fromString("Test"));
        // setRandomNull(writers[21], writer, 21, rnd, genericRow);

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
            IndexedRowWriter.FieldWriter fieldWriter,
            IndexedRowWriter writer,
            int pos,
            Random rnd,
            Object value) {
        fieldWriter.writeField(writer, pos, rnd.nextBoolean() ? null : value);
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
