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

package org.apache.fluss.row.arrow.vectors;

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.arrow.writers.ArrowShreddedVariantWriter;
import org.apache.fluss.row.arrow.writers.ArrowVariantWriter;
import org.apache.fluss.row.columnar.BytesColumnVector;
import org.apache.fluss.row.columnar.ColumnVector;
import org.apache.fluss.row.columnar.LongColumnVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.StructVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.variant.ShreddedField;
import org.apache.fluss.types.variant.ShreddingSchema;
import org.apache.fluss.types.variant.Variant;
import org.apache.fluss.utils.ArrowUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ShreddedFieldColumnVector}. */
class ShreddedFieldColumnVectorTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    @Test
    void testRawVariantFallbackReadsTopLevelField() {
        RowType rowType = RowType.of(DataTypes.VARIANT());
        try (VectorSchemaRoot root =
                VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator)) {
            allocate(root.getVector(0));

            StructVector variantVector = (StructVector) root.getVector(0);
            ArrowVariantWriter writer = new ArrowVariantWriter(variantVector);
            writer.write(
                    0, GenericRow.of(Variant.fromJson("{\"age\":30,\"name\":\"Ada\"}")), 0, true);

            ArrowVariantColumnVector rawParent = new ArrowVariantColumnVector(variantVector);
            ColumnVector age =
                    ShreddedFieldColumnVector.create(rawParent, "age", DataTypes.BIGINT());
            ColumnVector name =
                    ShreddedFieldColumnVector.create(rawParent, "name", DataTypes.STRING());

            assertThat(age.isNullAt(0)).isFalse();
            assertThat(((LongColumnVector) age).getLong(0)).isEqualTo(30L);
            assertThat(name.isNullAt(0)).isFalse();
            assertThat(
                            new String(
                                    ((BytesColumnVector) name).getBytes(0).getBytes(),
                                    StandardCharsets.UTF_8))
                    .isEqualTo("Ada");
        }
    }

    @Test
    void testRawVariantFallbackReturnsNullForMissingOrMismatchedField() {
        RowType rowType = RowType.of(DataTypes.VARIANT());
        try (VectorSchemaRoot root =
                VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator)) {
            allocate(root.getVector(0));

            StructVector variantVector = (StructVector) root.getVector(0);
            ArrowVariantWriter writer = new ArrowVariantWriter(variantVector);
            writer.write(0, GenericRow.of(Variant.fromJson("{\"age\":30}")), 0, true);

            ArrowVariantColumnVector rawParent = new ArrowVariantColumnVector(variantVector);
            ColumnVector missing =
                    ShreddedFieldColumnVector.create(rawParent, "missing", DataTypes.BIGINT());
            ColumnVector mismatched =
                    ShreddedFieldColumnVector.create(rawParent, "age", DataTypes.STRING());

            assertThat(missing.isNullAt(0)).isTrue();
            assertThat(mismatched.isNullAt(0)).isTrue();
        }
    }

    @Test
    void testShreddedMissFallbackReadsResidualTopLevelField() {
        ShreddingSchema shreddingSchema =
                new ShreddingSchema(
                        "data",
                        Collections.singletonList(new ShreddedField("name", DataTypes.STRING())));
        Schema arrowSchema =
                new Schema(
                        Collections.singletonList(
                                ArrowUtils.toVariantFieldWithShredding("data", shreddingSchema)));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            allocate(root.getVector(0));

            StructVector variantVector = (StructVector) root.getVector(0);
            ArrowShreddedVariantWriter writer =
                    new ArrowShreddedVariantWriter(variantVector, shreddingSchema);
            writer.write(
                    0, GenericRow.of(Variant.fromJson("{\"age\":31,\"name\":\"Bob\"}")), 0, true);

            ShreddedVariantColumnVector shreddedParent =
                    new ShreddedVariantColumnVector(variantVector, shreddingSchema);
            ColumnVector age =
                    ShreddedFieldColumnVector.create(shreddedParent, "age", DataTypes.BIGINT());

            assertThat(age.isNullAt(0)).isFalse();
            assertThat(((LongColumnVector) age).getLong(0)).isEqualTo(31L);
        }
    }

    private static void allocate(FieldVector vector) {
        vector.allocateNew();
        if (vector instanceof StructVector) {
            for (FieldVector child : ((StructVector) vector).getChildrenFromFields()) {
                allocate(child);
            }
        }
    }
}
