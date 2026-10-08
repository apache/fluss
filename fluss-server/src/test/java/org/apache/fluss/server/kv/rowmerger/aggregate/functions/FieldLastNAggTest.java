/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.rowmerger.aggregate.functions;

import org.apache.fluss.metadata.AggFunctionType;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.server.kv.rowmerger.aggregate.factory.FieldAggregatorFactory;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FieldLastNAgg}. */
class FieldLastNAggTest {

    private static final ArrayType BIGINTS = DataTypes.ARRAY(DataTypes.BIGINT());

    @Test
    void testKeepsTheLastN() {
        FieldLastNAgg agg = new FieldLastNAgg(BIGINTS, 3);
        Object acc = null;
        for (long i = 0; i < 10; i++) {
            acc = agg.agg(acc, longs(i));
        }
        assertThat(toList(acc)).containsExactly(7L, 8L, 9L);
    }

    @Test
    void testBelowSizeKeepsEverythingInOrder() {
        FieldLastNAgg agg = new FieldLastNAgg(BIGINTS, 5);
        Object acc = agg.agg(null, longs(1));
        acc = agg.agg(acc, longs(2, 3));
        assertThat(toList(acc)).containsExactly(1L, 2L, 3L);
    }

    @Test
    void testInputLongerThanSize() {
        FieldLastNAgg agg = new FieldLastNAgg(BIGINTS, 2);
        assertThat(toList(agg.agg(longs(1), longs(2, 3, 4)))).containsExactly(3L, 4L);
    }

    @Test
    void testNullAndEmptyInputs() {
        FieldLastNAgg agg = new FieldLastNAgg(BIGINTS, 2);
        GenericArray acc = longs(1, 2);
        assertThat(agg.agg(acc, null)).isSameAs(acc);
        assertThat(toList(agg.agg(acc, longs()))).containsExactly(1L, 2L);
    }

    @Test
    void testReversedOrderTreatsInputAsOlder() {
        FieldLastNAgg agg = new FieldLastNAgg(BIGINTS, 3);
        assertThat(toList(agg.aggReversed(longs(3, 4), longs(1, 2)))).containsExactly(2L, 3L, 4L);
    }

    @Test
    void testNonPrimitiveElements() {
        FieldLastNAgg agg = new FieldLastNAgg(DataTypes.ARRAY(DataTypes.STRING()), 2);
        Object acc = null;
        for (String s : new String[] {"a", "b", "c"}) {
            acc = agg.agg(acc, new GenericArray(new Object[] {BinaryString.fromString(s)}));
        }
        InternalArray result = (InternalArray) acc;
        assertThat(result.size()).isEqualTo(2);
        assertThat(result.getString(0).toString()).isEqualTo("b");
        assertThat(result.getString(1).toString()).isEqualTo("c");
    }

    @Test
    void testFactoryAndValidation() {
        FieldAggregatorFactory factory = FieldAggregatorFactory.getFactory(AggFunctionType.LAST_N);
        assertThat(factory.create(BIGINTS, AggFunctions.LAST_N(4)))
                .isInstanceOf(FieldLastNAgg.class);
        assertThatThrownBy(() -> factory.create(DataTypes.BIGINT(), AggFunctions.LAST_N(4)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AggFunctionType.LAST_N.validateParameter("size", "0"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AggFunctionType.LAST_N.validateParameter("size", "x"))
                .isInstanceOf(IllegalArgumentException.class);
        AggFunctionType.LAST_N.validateParameter("size", "1000");
        AggFunctionType.LAST_N.validateDataType(BIGINTS);
    }

    private static GenericArray longs(long... values) {
        return new GenericArray(values);
    }

    private static List<Long> toList(Object array) {
        InternalArray a = (InternalArray) array;
        List<Long> out = new ArrayList<>();
        for (int i = 0; i < a.size(); i++) {
            out.add(a.getLong(i));
        }
        return out;
    }
}
