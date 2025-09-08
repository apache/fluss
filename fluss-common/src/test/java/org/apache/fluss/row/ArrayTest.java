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
import org.apache.fluss.row.compacted.CompactedRowDeserializer;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.row.serializer.InternalArraySerializer;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** ArrayTest. */
public class ArrayTest {

    @Test
    public void testBinaryArrayWithIndexedRow() throws IOException {
        // 1. array test
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter arrayWriter =
                new BinaryArrayWriter(
                        array, 3, BinaryArray.calculateFixLengthPartSize(DataTypes.STRING()));

        arrayWriter.writeString(0, fromString("Hello"));
        arrayWriter.setNullAt(1);
        arrayWriter.writeString(2, fromString("World"));
        arrayWriter.complete();

        assertThat("Hello").isEqualTo(array.getString(0).toString());
        assertThat(array.isNullAt(1)).isTrue();
        assertThat("World").isEqualTo(array.getString(2).toString());

        // 2. test write array to binary row
        BinaryRow row = new IndexedRow(new DataType[] {DataTypes.ARRAY(DataTypes.STRING())});
        try (IndexedRowWriter rowWriter =
                new IndexedRowWriter(new DataType[] {DataTypes.ARRAY(DataTypes.STRING())})) {

            // write array to binary row
            InternalArraySerializer serializer =
                    new InternalArraySerializer(DataTypes.ARRAY(DataTypes.STRING()));
            rowWriter.writeArray(array, serializer);

            row.pointTo(rowWriter.segment(), 0, rowWriter.position());
        }

        BinaryArray array2 = (BinaryArray) row.getArray(0);
        assertThat(array2).isEqualTo(array);
        assertThat(array2.getString(0).toString()).isEqualTo("Hello");
        assertThat(array2.isNullAt(1)).isTrue();
        assertThat(array2.getString(2).toString()).isEqualTo("World");
    }

    @Test
    public void testGenericArrayWithIndexedRow() throws IOException {
        // 1. array test
        Integer[] javaArray = {6, null, 666};
        GenericArray array = new GenericArray(javaArray);

        assertThat(6).isEqualTo(array.getInt(0));
        assertThat(array.isNullAt(1)).isTrue();
        assertThat(666).isEqualTo(array.getInt(2));

        // 2. test write array to binary row
        BinaryRow row = new IndexedRow(new DataType[] {DataTypes.ARRAY(DataTypes.INT())});
        try (IndexedRowWriter rowWriter =
                new IndexedRowWriter(new DataType[] {DataTypes.ARRAY(DataTypes.INT())})) {
            InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.INT());
            rowWriter.writeArray(array, serializer);
            row.pointTo(rowWriter.segment(), 0, rowWriter.position());
        }

        InternalArray array2 = row.getArray(0);
        assertThat(array2.getInt(0)).isEqualTo(6);
        assertThat(array2.isNullAt(1)).isTrue();
        assertThat(array2.getInt(2)).isEqualTo(666);
    }

    @Test
    public void testBinaryArrayWithCompactedRow() throws IOException {
        // 1. array test
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter arrayWriter =
                new BinaryArrayWriter(
                        array, 3, BinaryArray.calculateFixLengthPartSize(DataTypes.STRING()));

        arrayWriter.writeString(0, fromString("Hello"));
        arrayWriter.setNullAt(1);
        arrayWriter.writeString(2, fromString("World"));
        arrayWriter.complete();

        assertThat("Hello").isEqualTo(array.getString(0).toString());
        assertThat(array.isNullAt(1)).isTrue();
        assertThat("World").isEqualTo(array.getString(2).toString());

        CompactedRow row =
                new CompactedRow(
                        2,
                        new CompactedRowDeserializer(
                                new DataType[] {
                                    DataTypes.INT(), DataTypes.ARRAY(DataTypes.STRING())
                                }));

        try (CompactedRowWriter rowWriter = new CompactedRowWriter(2)) {
            rowWriter.writeInt(6);

            InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.STRING());
            rowWriter.writeArray(array, serializer);

            row.pointTo(rowWriter.segment(), 0, rowWriter.position());
        }

        assertThat(row.getInt(0)).isEqualTo(6);

        InternalArray array2 = row.getArray(1);
        assertThat(array2.getString(0).toString()).isEqualTo("Hello");
        assertThat(array2.isNullAt(1)).isEqualTo(true);
        assertThat(array2.getString(2).toString()).isEqualTo("World");
    }

    @Test
    public void testGenericArrayWithCompactedRow() throws IOException {
        // 1. array test
        Integer[] javaArray = {6, null, 666};
        GenericArray array = new GenericArray(javaArray);

        assertThat(6).isEqualTo(array.getInt(0));
        assertThat(array.isNullAt(1)).isTrue();
        assertThat(666).isEqualTo(array.getInt(2));

        CompactedRow row = new CompactedRow(new DataType[] {DataTypes.ARRAY(DataTypes.INT())});

        try (CompactedRowWriter rowWriter = new CompactedRowWriter(1)) {
            InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.INT());
            rowWriter.writeArray(array, serializer);

            row.pointTo(rowWriter.segment(), 0, rowWriter.position());
        }

        InternalArray array2 = row.getArray(0);
        assertThat(array2.getInt(0)).isEqualTo(6);
        assertThat(array2.isNullAt(1)).isTrue();
        assertThat(array2.getInt(2)).isEqualTo(666);
    }
}
