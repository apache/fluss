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

package org.apache.fluss.types;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link VectorType}. */
public class VectorTypeTest {

    @Test
    void testConstructorValidation() {
        // Valid construction
        VectorType t = new VectorType(1536);
        assertThat(t.getDimension()).isEqualTo(1536);
        assertThat(t.getElementType()).isEqualTo(VectorElementType.FLOAT32);
        assertThat(t.isNullable()).isTrue();

        // Zero dimension should throw
        assertThatThrownBy(() -> new VectorType(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Dimension must be positive");

        // Negative dimension should throw
        assertThatThrownBy(() -> new VectorType(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Dimension must be positive");
    }

    @Test
    void testUnsupportedElementTypes() {
        assertThatThrownBy(() -> new VectorType(true, 4, VectorElementType.FLOAT16))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("reserved for future use");

        assertThatThrownBy(() -> new VectorType(true, 4, VectorElementType.INT8))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("reserved for future use");
    }

    @Test
    void testNullability() {
        VectorType nullable = new VectorType(4);
        assertThat(nullable.isNullable()).isTrue();

        VectorType notNull = (VectorType) nullable.copy(false);
        assertThat(notNull.isNullable()).isFalse();
        assertThat(notNull.getDimension()).isEqualTo(4);
        assertThat(notNull.getElementType()).isEqualTo(VectorElementType.FLOAT32);
    }

    @Test
    void testCopy() {
        VectorType original = new VectorType(768);
        VectorType copied = (VectorType) original.copy();
        assertThat(copied).isEqualTo(original);
        assertThat(copied).isNotSameAs(original);

        VectorType notNullCopy = (VectorType) original.copy(false);
        assertThat(notNullCopy.isNullable()).isFalse();
        assertThat(notNullCopy.getDimension()).isEqualTo(768);
    }

    @Test
    void testAsSerializableString() {
        VectorType nullable = new VectorType(1536);
        assertThat(nullable.asSerializableString()).isEqualTo("VECTOR(1536)");

        VectorType notNull = new VectorType(false, 1536, VectorElementType.FLOAT32);
        assertThat(notNull.asSerializableString()).isEqualTo("VECTOR(1536) NOT NULL");
    }

    @Test
    void testAsSummaryString() {
        VectorType t = new VectorType(256);
        assertThat(t.asSummaryString()).isEqualTo("VECTOR(256)");
    }

    @Test
    void testGetChildren() {
        VectorType t = new VectorType(4);
        assertThat(t.getChildren()).isEqualTo(Collections.emptyList());
    }

    @Test
    void testAcceptVisitor() {
        VectorType vectorType = new VectorType(4);
        boolean[] visited = {false};
        DataTypeVisitor<Void> visitor =
                new DataTypeDefaultVisitor<Void>() {
                    @Override
                    public Void visit(VectorType vt) {
                        visited[0] = true;
                        return null;
                    }

                    @Override
                    protected Void defaultMethod(DataType dataType) {
                        return null;
                    }
                };
        vectorType.accept(visitor);
        assertThat(visited[0]).isTrue();
    }

    @Test
    void testEqualsAndHashCode() {
        VectorType a = new VectorType(4);
        VectorType b = new VectorType(4);
        VectorType c = new VectorType(8);
        VectorType d = new VectorType(false, 4, VectorElementType.FLOAT32);

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());

        assertThat(a).isNotEqualTo(c);
        assertThat(a).isNotEqualTo(d); // different nullability
        assertThat(a).isNotEqualTo(new FloatType());
    }

    @Test
    void testDataTypeRootFamily() {
        VectorType t = new VectorType(4);
        assertThat(t.getTypeRoot()).isEqualTo(DataTypeRoot.VECTOR);
        assertThat(t.is(DataTypeRoot.VECTOR)).isTrue();
        assertThat(t.is(DataTypeFamily.VECTOR)).isTrue();
        assertThat(t.is(DataTypeFamily.CONSTRUCTED)).isTrue();
        assertThat(t.is(DataTypeFamily.COLLECTION)).isFalse();
        assertThat(t.is(DataTypeFamily.PREDEFINED)).isFalse();
    }

    @Test
    void testGetDimensionViaDataTypeChecks() {
        VectorType t = new VectorType(1024);
        assertThat(DataTypeChecks.getDimension(t)).isEqualTo(1024);
    }

    @Test
    void testGetDimensionOnNonVectorTypeThrows() {
        assertThatThrownBy(() -> DataTypeChecks.getDimension(new IntType()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
