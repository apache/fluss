/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.types;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link MultisetType}. */
public class MultisetTypeTest {

    @Test
    void testMultisetTypeConstructor() {
        DataType elementType = DataTypes.INT();

        MultisetType multisetType = new MultisetType(true, elementType);
        assertThat(multisetType.isNullable()).isTrue();
        assertThat(multisetType.getElementType()).isEqualTo(elementType);
        assertThat(multisetType.getTypeRoot()).isEqualTo(DataTypeRoot.MAP);

        MultisetType notNullMultisetType = new MultisetType(false, elementType);
        assertThat(notNullMultisetType.isNullable()).isFalse();
        assertThat(notNullMultisetType.getElementType()).isEqualTo(elementType);

        MultisetType defaultMultisetType = new MultisetType(elementType);
        assertThat(defaultMultisetType.isNullable()).isTrue();
        assertThat(defaultMultisetType.getElementType()).isEqualTo(elementType);
    }

    @Test
    void testMultisetTypeConstructorWithNullElementType() {
        assertThatThrownBy(() -> new MultisetType(true, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Element type must not be null");

        assertThatThrownBy(() -> new MultisetType(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Element type must not be null");
    }

    @Test
    void testGetElementType() {
        DataType intType = DataTypes.INT();
        DataType stringType = DataTypes.STRING();

        MultisetType intMultiset = new MultisetType(intType);
        assertThat(intMultiset.getElementType()).isEqualTo(intType);

        MultisetType stringMultiset = new MultisetType(stringType);
        assertThat(stringMultiset.getElementType()).isEqualTo(stringType);
    }

    @Test
    void testCopy() {
        DataType elementType = DataTypes.DOUBLE();
        MultisetType original = new MultisetType(true, elementType);

        MultisetType copy = (MultisetType) original.copy(true);
        assertThat(copy.isNullable()).isTrue();
        assertThat(copy.getElementType()).isEqualTo(elementType);
        assertThat(copy).isNotSameAs(original);

        MultisetType notNullCopy = (MultisetType) original.copy(false);
        assertThat(notNullCopy.isNullable()).isFalse();
        assertThat(notNullCopy.getElementType()).isEqualTo(elementType);
        assertThat(notNullCopy).isNotSameAs(original);
    }

    @Test
    void testAsSerializableString() {
        DataType elementType = DataTypes.STRING();

        MultisetType nullableMultiset = new MultisetType(true, elementType);
        assertThat(nullableMultiset.asSerializableString()).isEqualTo("MULTISET<STRING>");

        MultisetType notNullMultiset = new MultisetType(false, elementType);
        assertThat(notNullMultiset.asSerializableString()).isEqualTo("MULTISET<STRING> NOT NULL");

        DataType complexElementType = DataTypes.ARRAY(DataTypes.INT());
        MultisetType complexMultiset = new MultisetType(complexElementType);
        assertThat(complexMultiset.asSerializableString()).isEqualTo("MULTISET<ARRAY<INT>>");
    }

    @Test
    void testGetChildren() {
        DataType elementType = DataTypes.BOOLEAN();
        MultisetType multisetType = new MultisetType(elementType);

        assertThat(multisetType.getChildren()).isEmpty();
    }

    @Test
    void testAccept() {
        DataType elementType = DataTypes.FLOAT();
        MultisetType multisetType = new MultisetType(elementType);

        DataTypeVisitor<String> visitor = new DataTypeVisitorTest.ToStringVisitor();

        String result = multisetType.accept(visitor);
        assertThat(result).isEqualTo("MULTISET<FLOAT>");
    }

    @Test
    void testEquals() {
        DataType elementType1 = DataTypes.INT();
        DataType elementType2 = DataTypes.STRING();

        MultisetType multiset1 = new MultisetType(true, elementType1);
        MultisetType multiset2 = new MultisetType(true, elementType1);
        MultisetType multiset3 = new MultisetType(false, elementType1);
        MultisetType multiset4 = new MultisetType(true, elementType2);

        assertThat(multiset1).isEqualTo(multiset2);
        assertThat(multiset1).isNotEqualTo(multiset3);
        assertThat(multiset1).isNotEqualTo(multiset4);
        assertThat(multiset1).isEqualTo(multiset1);
        assertThat(multiset1).isNotEqualTo(null);
        assertThat(multiset1).isNotEqualTo("not a multiset");
    }

    @Test
    void testHashCode() {
        DataType elementType = DataTypes.DOUBLE();

        MultisetType multiset1 = new MultisetType(true, elementType);
        MultisetType multiset2 = new MultisetType(true, elementType);

        assertThat(multiset1.hashCode()).isEqualTo(multiset2.hashCode());
    }
}
