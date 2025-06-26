/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row;

import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.row.serializer.InternalMapSerializer;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.row.serializer.InternalMapSerializer.convertToJavaMap;
import static org.assertj.core.api.Assertions.assertThat;

/** MapTest. */
public class MapTest {
    @Test
    public void testBinaryMapWithIndexedRow() throws IOException {
        BinaryArray array1 = new BinaryArray();
        BinaryArrayWriter writer1 =
                new BinaryArrayWriter(
                        array1, 4, BinaryArray.calculateFixLengthPartSize(DataTypes.INT()));
        writer1.writeInt(0, 6);
        writer1.writeInt(1, 5);
        writer1.writeInt(2, 666);
        writer1.writeInt(3, 0);
        writer1.complete();

        BinaryArray array2 = new BinaryArray();
        BinaryArrayWriter writer2 =
                new BinaryArrayWriter(
                        array2, 4, BinaryArray.calculateFixLengthPartSize(DataTypes.STRING()));
        writer2.writeString(0, fromString("6"));
        writer2.writeString(1, fromString("5"));
        writer2.writeString(2, fromString("666"));
        writer2.setNullAt(3);
        writer2.complete();

        BinaryMap binaryMap = BinaryMap.valueOf(array1, array2);

        BinaryRow row =
                new IndexedRow(new DataType[] {DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())});
        try (IndexedRowWriter rowWriter =
                new IndexedRowWriter(
                        new DataType[] {DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())})) {
            InternalMapSerializer serializer =
                    new InternalMapSerializer(DataTypes.STRING(), DataTypes.INT());
            rowWriter.writeMap(binaryMap, serializer);
            row.pointTo(rowWriter.segment(), 0, rowWriter.position());
        }

        BinaryMap map = (BinaryMap) row.getMap(0);
        BinaryArray key = map.keyArray();
        BinaryArray value = map.valueArray();

        assertThat(map).isEqualTo(binaryMap);
        assertThat(key).isEqualTo(array1);
        assertThat(value).isEqualTo(array2);

        assertThat(key.getInt(1)).isEqualTo(5);
        assertThat(value.getString(1)).isEqualTo(fromString("5"));
        assertThat(key.getInt(3)).isEqualTo(0);
        assertThat(value.isNullAt(3)).isTrue();
    }

    @Test
    public void testBinaryMapWithCompactedRow() throws IOException {
        BinaryArray array1 = new BinaryArray();
        BinaryArrayWriter writer1 =
                new BinaryArrayWriter(
                        array1, 4, BinaryArray.calculateFixLengthPartSize(DataTypes.INT()));
        writer1.writeInt(0, 6);
        writer1.writeInt(1, 5);
        writer1.writeInt(2, 666);
        writer1.writeInt(3, 0);
        writer1.complete();

        BinaryArray array2 = new BinaryArray();
        BinaryArrayWriter writer2 =
                new BinaryArrayWriter(
                        array2, 4, BinaryArray.calculateFixLengthPartSize(DataTypes.STRING()));
        writer2.writeString(0, fromString("6"));
        writer2.writeString(1, fromString("5"));
        writer2.writeString(2, fromString("666"));
        writer2.setNullAt(3);
        writer2.complete();

        BinaryMap binaryMap = BinaryMap.valueOf(array1, array2);
        CompactedRow row =
                new CompactedRow(
                        new DataType[] {DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())});
        try (CompactedRowWriter rowWriter = new CompactedRowWriter(1)) {
            InternalMapSerializer serializer =
                    new InternalMapSerializer(DataTypes.STRING(), DataTypes.INT());
            rowWriter.writeMap(binaryMap, serializer);
            row.pointTo(rowWriter.segment(), 0, rowWriter.position());
        }

        BinaryMap map = (BinaryMap) row.getMap(0);
        BinaryArray key = map.keyArray();
        BinaryArray value = map.valueArray();

        assertThat(map).isEqualTo(binaryMap);
        assertThat(key).isEqualTo(array1);
        assertThat(value).isEqualTo(array2);

        assertThat(key.getInt(1)).isEqualTo(5);
        assertThat(value.getString(1)).isEqualTo(fromString("5"));
        assertThat(key.getInt(3)).isEqualTo(0);
        assertThat(value.isNullAt(3)).isTrue();
    }

    @Test
    public void testGenericMapWithIndexedRow() throws IOException {
        Map<Object, Object> javaMap = new HashMap<>();
        javaMap.put(6, fromString("6"));
        javaMap.put(5, fromString("5"));
        javaMap.put(666, fromString("666"));
        javaMap.put(0, null);

        GenericMap genericMap = new GenericMap(javaMap);

        BinaryRow row =
                new IndexedRow(new DataType[] {DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())});
        try (IndexedRowWriter rowWriter =
                new IndexedRowWriter(
                        new DataType[] {DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())})) {
            InternalMapSerializer serializer =
                    new InternalMapSerializer(DataTypes.INT(), DataTypes.STRING());
            rowWriter.writeMap(genericMap, serializer);
            row.pointTo(rowWriter.segment(), 0, rowWriter.position());
        }

        Map<Object, Object> map =
                convertToJavaMap(row.getMap(0), DataTypes.INT(), DataTypes.STRING());
        assertThat(map.get(6)).isEqualTo(fromString("6"));
        assertThat(map.get(5)).isEqualTo(fromString("5"));
        assertThat(map.get(666)).isEqualTo(fromString("666"));
        assertThat(map).containsKey(0);
        assertThat(map.get(0)).isNull();
    }

    @Test
    public void testGenericMapWithCompactedRow() throws IOException {
        Map<Object, Object> javaMap = new HashMap<>();
        javaMap.put(6, fromString("6"));
        javaMap.put(5, fromString("5"));
        javaMap.put(666, fromString("666"));
        javaMap.put(0, null);

        GenericMap genericMap = new GenericMap(javaMap);

        CompactedRow row =
                new CompactedRow(
                        new DataType[] {DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())});
        try (CompactedRowWriter rowWriter = new CompactedRowWriter(1)) {
            InternalMapSerializer serializer =
                    new InternalMapSerializer(DataTypes.INT(), DataTypes.STRING());
            rowWriter.writeMap(genericMap, serializer);
            row.pointTo(rowWriter.segment(), 0, rowWriter.position());
        }

        Map<Object, Object> map =
                convertToJavaMap(row.getMap(0), DataTypes.INT(), DataTypes.STRING());
        assertThat(map.get(6)).isEqualTo(fromString("6"));
        assertThat(map.get(5)).isEqualTo(fromString("5"));
        assertThat(map.get(666)).isEqualTo(fromString("666"));
        assertThat(map).containsKey(0);
        assertThat(map.get(0)).isNull();
    }
}
