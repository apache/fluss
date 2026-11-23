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

package org.apache.fluss.flink.source.reader;

import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.function.BiPredicate;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ProjectedRowEquality}. */
class ProjectedRowEqualityTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testBinaryMapKeys(boolean fixedLength) {
        DataType keyType = fixedLength ? DataTypes.BINARY(2) : DataTypes.BYTES();
        BiPredicate<InternalRow, InternalRow> equal =
                ProjectedRowEquality.create(RowType.of(DataTypes.MAP(keyType, DataTypes.BYTES())));
        // The two keys have the same content hash.
        GenericRow before =
                GenericRow.of(
                        GenericMap.of(new byte[] {0, 31}, null, new byte[] {1, 0}, new byte[] {2}));
        GenericRow reordered =
                GenericRow.of(
                        GenericMap.of(new byte[] {1, 0}, new byte[] {2}, new byte[] {0, 31}, null));
        assertThat(equal.test(before, reordered)).isTrue();
        GenericRow changedValue =
                GenericRow.of(
                        GenericMap.of(new byte[] {1, 0}, new byte[] {3}, new byte[] {0, 31}, null));
        assertThat(equal.test(before, changedValue)).isFalse();
        GenericRow changedKey =
                GenericRow.of(
                        GenericMap.of(new byte[] {1, 0}, new byte[] {2}, new byte[] {0, 30}, null));
        assertThat(equal.test(before, changedKey)).isFalse();
    }

    @Test
    void testNestedMapKeys() {
        DataType innerMap = DataTypes.MAP(DataTypes.BYTES(), DataTypes.INT());
        RowType nestedKey = RowType.of(DataTypes.ARRAY(innerMap), DataTypes.BYTES());
        BiPredicate<InternalRow, InternalRow> equal =
                ProjectedRowEquality.create(RowType.of(DataTypes.MAP(nestedKey, innerMap)));
        GenericMap first = GenericMap.of(new byte[] {1}, 10, new byte[] {2}, 20);
        GenericMap reordered = GenericMap.of(new byte[] {2}, 20, new byte[] {1}, 10);
        GenericRow firstKey = GenericRow.of(GenericArray.of(null, first), new byte[] {3});
        GenericRow secondKey = GenericRow.of(GenericArray.of(null, reordered), new byte[] {3});
        GenericRow before = GenericRow.of(GenericMap.of(firstKey, first));
        assertThat(equal.test(before, GenericRow.of(GenericMap.of(secondKey, reordered)))).isTrue();
        GenericMap changed = GenericMap.of(new byte[] {2}, 21, new byte[] {1}, 10);
        assertThat(equal.test(before, GenericRow.of(GenericMap.of(secondKey, changed)))).isFalse();
    }
}
