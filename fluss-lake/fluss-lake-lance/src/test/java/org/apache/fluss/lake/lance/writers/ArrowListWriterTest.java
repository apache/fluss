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

package org.apache.fluss.lake.lance.writers;

import org.apache.fluss.lake.lance.writers.array.ArrowArrayIntWriter;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link ArrowListWriter}. */
class ArrowListWriterTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    @Test
    void testWriteSimpleArray() {
        Field elementField =
                new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field listField =
                new Field(
                        "list",
                        FieldType.nullable(ArrowType.List.INSTANCE),
                        Collections.singletonList(elementField));

        ListVector listVector = (ListVector) listField.createVector(allocator);
        listVector.allocateNew();

        IntVector elementVector = (IntVector) listVector.getDataVector();
        ArrowFieldWriter<InternalArray> elementWriter = ArrowArrayIntWriter.forField(elementVector);
        ArrowListWriter writer = ArrowListWriter.forField(listVector, elementWriter);

        // Create test data: row with array [1, 2, 3]
        GenericRow row = new GenericRow(1);
        row.setField(0, new GenericArray(new int[] {1, 2, 3}));

        // Write the row
        writer.write(row, 0, true);
        writer.finish();

        // Verify
        assertThat(listVector.getValueCount()).isEqualTo(1);
        assertThat(listVector.isNull(0)).isFalse();
        // Additional verification would require reading back the values

        listVector.close();
    }

    @Test
    void testWriteNullArray() {
        Field elementField =
                new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field listField =
                new Field(
                        "list",
                        FieldType.nullable(ArrowType.List.INSTANCE),
                        Collections.singletonList(elementField));

        ListVector listVector = (ListVector) listField.createVector(allocator);
        listVector.allocateNew();

        IntVector elementVector = (IntVector) listVector.getDataVector();
        ArrowFieldWriter<InternalArray> elementWriter = ArrowArrayIntWriter.forField(elementVector);
        ArrowListWriter writer = ArrowListWriter.forField(listVector, elementWriter);

        // Create test data: row with null array
        GenericRow row = new GenericRow(1);
        row.setField(0, null);

        // Write the row
        writer.write(row, 0, true);
        writer.finish();

        // Verify
        assertThat(listVector.getValueCount()).isEqualTo(1);
        assertThat(listVector.isNull(0)).isTrue();

        listVector.close();
    }

    @Test
    void testWriteEmptyArray() {
        Field elementField =
                new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field listField =
                new Field(
                        "list",
                        FieldType.nullable(ArrowType.List.INSTANCE),
                        Collections.singletonList(elementField));

        ListVector listVector = (ListVector) listField.createVector(allocator);
        listVector.allocateNew();

        IntVector elementVector = (IntVector) listVector.getDataVector();
        ArrowFieldWriter<InternalArray> elementWriter = ArrowArrayIntWriter.forField(elementVector);
        ArrowListWriter writer = ArrowListWriter.forField(listVector, elementWriter);

        // Create test data: row with empty array []
        GenericRow row = new GenericRow(1);
        row.setField(0, new GenericArray(new int[] {}));

        // Write the row
        writer.write(row, 0, true);
        writer.finish();

        // Verify
        assertThat(listVector.getValueCount()).isEqualTo(1);
        assertThat(listVector.isNull(0)).isFalse();

        listVector.close();
    }
}
