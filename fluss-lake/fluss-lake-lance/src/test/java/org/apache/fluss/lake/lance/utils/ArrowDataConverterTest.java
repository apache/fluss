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

package org.apache.fluss.lake.lance.utils;

import org.apache.fluss.lake.lance.tiering.ShadedArrowBatchWriter;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ArrowDataConverter#convertToNonShaded}. */
class ArrowDataConverterTest {

    private org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator shadedAllocator;
    private BufferAllocator nonShadedAllocator;

    @BeforeEach
    void setUp() {
        shadedAllocator =
                new org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator(
                        Long.MAX_VALUE);
        nonShadedAllocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        shadedAllocator.close();
        nonShadedAllocator.close();
    }

    @Test
    void testConvertListToFixedSizeList() {
        int listSize = 3;
        RowType rowType =
                DataTypes.ROW(DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())));

        Map<String, String> properties = new HashMap<>();
        properties.put("embedding.arrow.fixed-size-list.size", String.valueOf(listSize));
        Schema nonShadedSchema = LanceArrowUtils.toArrowSchema(rowType, properties);

        // Populate shaded root with 3 rows, each having a list of exactly 3 floats
        float[][] data = {
            {1.0f, 2.0f, 3.0f},
            {4.0f, 5.0f, 6.0f},
            {7.0f, 8.0f, 9.0f}
        };

        try (ShadedArrowBatchWriter writer = new ShadedArrowBatchWriter(shadedAllocator, rowType)) {
            for (float[] floats : data) {
                GenericRow row = new GenericRow(1);
                row.setField(0, new GenericArray(floats));
                writer.writeRow(row);
            }
            writer.finish();

            try (VectorSchemaRoot nonShadedRoot =
                    ArrowDataConverter.convertToNonShaded(
                            writer.getShadedRoot(), nonShadedAllocator, nonShadedSchema)) {
                assertThat(nonShadedRoot.getRowCount()).isEqualTo(3);
                assertThat(nonShadedRoot.getVector("embedding"))
                        .isInstanceOf(FixedSizeListVector.class);

                FixedSizeListVector fixedSizeListVector =
                        (FixedSizeListVector) nonShadedRoot.getVector("embedding");
                assertThat(fixedSizeListVector.getListSize()).isEqualTo(listSize);

                for (int i = 0; i < data.length; i++) {
                    List<?> values = fixedSizeListVector.getObject(i);
                    assertThat(values).hasSize(listSize);
                    for (int j = 0; j < listSize; j++) {
                        assertThat((Float) values.get(j)).isEqualTo(data[i][j]);
                    }
                }
            }
        }
    }

    @Test
    void testConvertListToFixedSizeListMismatchedCount() {
        int listSize = 3;
        RowType rowType =
                DataTypes.ROW(DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())));

        Map<String, String> properties = new HashMap<>();
        properties.put("embedding.arrow.fixed-size-list.size", String.valueOf(listSize));
        Schema nonShadedSchema = LanceArrowUtils.toArrowSchema(rowType, properties);

        // Write 2 rows with 2 elements each: total child elements = 4, expected = 2*3 = 6
        try (ShadedArrowBatchWriter writer = new ShadedArrowBatchWriter(shadedAllocator, rowType)) {
            GenericRow row1 = new GenericRow(1);
            row1.setField(0, new GenericArray(new float[] {1.0f, 2.0f}));
            writer.writeRow(row1);

            GenericRow row2 = new GenericRow(1);
            row2.setField(0, new GenericArray(new float[] {3.0f, 4.0f}));
            writer.writeRow(row2);
            writer.finish();

            assertThatThrownBy(
                            () ->
                                    ArrowDataConverter.convertToNonShaded(
                                            writer.getShadedRoot(),
                                            nonShadedAllocator,
                                            nonShadedSchema))
                    .isInstanceOf(IllegalArgumentException.class);

            // Verify no memory leaked from the failed conversion
            assertThat(nonShadedAllocator.getAllocatedMemory()).isZero();
        }
    }

    @Test
    void testConvertListToFixedSizeListWithNulls() {
        int listSize = 3;
        RowType rowType =
                DataTypes.ROW(DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())));

        Map<String, String> properties = new HashMap<>();
        properties.put("embedding.arrow.fixed-size-list.size", String.valueOf(listSize));
        Schema nonShadedSchema = LanceArrowUtils.toArrowSchema(rowType, properties);

        // 3 rows: non-null, null, non-null
        try (ShadedArrowBatchWriter writer = new ShadedArrowBatchWriter(shadedAllocator, rowType)) {
            GenericRow row1 = new GenericRow(1);
            row1.setField(0, new GenericArray(new float[] {1.0f, 2.0f, 3.0f}));
            writer.writeRow(row1);

            GenericRow row2 = new GenericRow(1);
            row2.setField(0, null); // null embedding
            writer.writeRow(row2);

            GenericRow row3 = new GenericRow(1);
            row3.setField(0, new GenericArray(new float[] {7.0f, 8.0f, 9.0f}));
            writer.writeRow(row3);
            writer.finish();

            try (VectorSchemaRoot nonShadedRoot =
                    ArrowDataConverter.convertToNonShaded(
                            writer.getShadedRoot(), nonShadedAllocator, nonShadedSchema)) {
                assertThat(nonShadedRoot.getRowCount()).isEqualTo(3);

                FixedSizeListVector fixedSizeListVector =
                        (FixedSizeListVector) nonShadedRoot.getVector("embedding");

                // Row 0: non-null
                assertThat(fixedSizeListVector.isNull(0)).isFalse();
                List<?> values0 = fixedSizeListVector.getObject(0);
                assertThat(values0).hasSize(3);
                assertThat((Float) values0.get(0)).isEqualTo(1.0f);
                assertThat((Float) values0.get(1)).isEqualTo(2.0f);
                assertThat((Float) values0.get(2)).isEqualTo(3.0f);

                // Row 1: null
                assertThat(fixedSizeListVector.isNull(1)).isTrue();

                // Row 2: non-null
                assertThat(fixedSizeListVector.isNull(2)).isFalse();
                List<?> values2 = fixedSizeListVector.getObject(2);
                assertThat(values2).hasSize(3);
                assertThat((Float) values2.get(0)).isEqualTo(7.0f);
                assertThat((Float) values2.get(1)).isEqualTo(8.0f);
                assertThat((Float) values2.get(2)).isEqualTo(9.0f);
            }
        }
    }
}
