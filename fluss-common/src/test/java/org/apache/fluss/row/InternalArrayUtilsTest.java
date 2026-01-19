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

package org.apache.fluss.row;

import org.apache.fluss.row.BinaryRow.BinaryRowFormat;
import org.apache.fluss.row.array.CompactedArray;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link InternalArrayUtils}. */
public class InternalArrayUtilsTest {

    @Test
    public void testToObjectArrayConvertsBinaryString() {
        GenericArray input =
                new GenericArray(
                        new Object[] {BinaryString.fromString("a"), BinaryString.fromString("b")});

        Object[] result = InternalArrayUtils.toObjectArray(input, DataTypes.STRING());

        assertThat(result).containsExactly("a", "b");
    }

    @Test
    public void testConcatToBinaryArrayCompacted() {
        InternalArray left = GenericArray.of(1, 2);
        InternalArray right = GenericArray.of(3);

        BinaryArray result =
                InternalArrayUtils.concatToBinaryArray(
                        left, right, DataTypes.INT(), BinaryRowFormat.COMPACTED);

        assertThat(result).isInstanceOf(CompactedArray.class);
        assertThat((Integer[]) result.toObjectArray(DataTypes.INT())).containsExactly(1, 2, 3);
    }

    @Test
    public void testConcatToBinaryArrayEmptyLeft() {
        InternalArray left = new GenericArray(new Object[0]);
        InternalArray right = GenericArray.of(7);

        BinaryArray result =
                InternalArrayUtils.concatToBinaryArray(
                        left, right, DataTypes.INT(), BinaryRowFormat.COMPACTED);

        assertThat((Integer[]) result.toObjectArray(DataTypes.INT())).containsExactly(7);
    }
}
