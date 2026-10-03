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

import org.apache.fluss.metadata.ValidationException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for parsing {@link VectorType} via {@link DataTypeParser}. */
public class DataTypeParserVectorTest {

    @Test
    void testParseVectorBasic() {
        DataType result = DataTypeParser.parse("VECTOR(1536)");
        assertThat(result).isInstanceOf(VectorType.class);
        VectorType vectorType = (VectorType) result;
        assertThat(vectorType.getDimension()).isEqualTo(1536);
        assertThat(vectorType.getElementType()).isEqualTo(VectorElementType.FLOAT32);
        assertThat(vectorType.isNullable()).isTrue();
    }

    @Test
    void testParseVectorSmallDimension() {
        DataType result = DataTypeParser.parse("VECTOR(1)");
        assertThat(result).isInstanceOf(VectorType.class);
        assertThat(((VectorType) result).getDimension()).isEqualTo(1);
    }

    @Test
    void testParseVectorNotNull() {
        DataType result = DataTypeParser.parse("VECTOR(1536) NOT NULL");
        assertThat(result).isInstanceOf(VectorType.class);
        VectorType vectorType = (VectorType) result;
        assertThat(vectorType.getDimension()).isEqualTo(1536);
        assertThat(vectorType.isNullable()).isFalse();
    }

    @Test
    void testParseVectorNull() {
        DataType result = DataTypeParser.parse("VECTOR(768) NULL");
        assertThat(result).isInstanceOf(VectorType.class);
        assertThat(result.isNullable()).isTrue();
    }

    @Test
    void testParseVectorLowercaseKeyword() {
        DataType result = DataTypeParser.parse("vector(4)");
        assertThat(result).isInstanceOf(VectorType.class);
        assertThat(((VectorType) result).getDimension()).isEqualTo(4);
    }

    @Test
    void testParseVectorMixedCase() {
        DataType result = DataTypeParser.parse("Vector(128)");
        assertThat(result).isInstanceOf(VectorType.class);
        assertThat(((VectorType) result).getDimension()).isEqualTo(128);
    }

    @Test
    void testParseVectorMissingDimension() {
        // VECTOR without parentheses should fail
        assertThatThrownBy(() -> DataTypeParser.parse("VECTOR"))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testParseVectorZeroDimension() {
        // VECTOR(0) should parse but fail at VectorType construction
        assertThatThrownBy(() -> DataTypeParser.parse("VECTOR(0)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Dimension must be positive");
    }

    @Test
    void testRoundTrip() {
        VectorType original = new VectorType(1536);
        String serialized = original.asSerializableString();
        DataType parsed = DataTypeParser.parse(serialized);
        assertThat(parsed).isEqualTo(original);
    }

    @Test
    void testRoundTripNotNull() {
        VectorType original = new VectorType(false, 768, VectorElementType.FLOAT32);
        String serialized = original.asSerializableString();
        DataType parsed = DataTypeParser.parse(serialized);
        assertThat(parsed).isEqualTo(original);
    }

    @Test
    void testVectorInsideRowType() {
        DataType result = DataTypeParser.parse("ROW(id BIGINT, embedding VECTOR(4))");
        assertThat(result).isInstanceOf(RowType.class);
        RowType rowType = (RowType) result;
        assertThat(rowType.getFieldCount()).isEqualTo(2);
        assertThat(rowType.getTypeAt(1)).isInstanceOf(VectorType.class);
        assertThat(((VectorType) rowType.getTypeAt(1)).getDimension()).isEqualTo(4);
    }
}
