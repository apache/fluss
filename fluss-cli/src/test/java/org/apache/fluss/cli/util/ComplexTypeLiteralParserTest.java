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

package org.apache.fluss.cli.util;

import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ComplexTypeLiteralParserTest {

    @Test
    void testParseEmptyArray() {
        ArrayType arrayType = DataTypes.ARRAY(DataTypes.STRING());
        GenericArray result =
                (GenericArray) ComplexTypeLiteralParser.parseArray("ARRAY[]", arrayType);

        assertThat(result.toObjectArray()).isEmpty();
    }

    @Test
    void testParseStringArrayWithBrackets() {
        ArrayType arrayType = DataTypes.ARRAY(DataTypes.STRING());
        GenericArray result =
                (GenericArray)
                        ComplexTypeLiteralParser.parseArray("ARRAY['a', 'b', 'c']", arrayType);

        assertThat(result.toObjectArray()).hasSize(3);
        assertThat(result.toObjectArray()[0].toString()).isEqualTo("a");
        assertThat(result.toObjectArray()[1].toString()).isEqualTo("b");
        assertThat(result.toObjectArray()[2].toString()).isEqualTo("c");
    }

    @Test
    void testParseStringArrayWithParentheses() {
        ArrayType arrayType = DataTypes.ARRAY(DataTypes.STRING());
        GenericArray result =
                (GenericArray)
                        ComplexTypeLiteralParser.parseArray("ARRAY('a', 'b', 'c')", arrayType);

        assertThat(result.toObjectArray()).hasSize(3);
    }

    @Test
    void testParseIntegerArray() {
        ArrayType arrayType = DataTypes.ARRAY(DataTypes.INT());
        GenericArray result =
                (GenericArray) ComplexTypeLiteralParser.parseArray("ARRAY[1, 2, 3]", arrayType);

        assertThat(result.toObjectArray()).containsExactly(1, 2, 3);
    }

    @Test
    void testParseArrayCaseInsensitive() {
        ArrayType arrayType = DataTypes.ARRAY(DataTypes.INT());
        GenericArray result =
                (GenericArray) ComplexTypeLiteralParser.parseArray("array[1, 2, 3]", arrayType);

        assertThat(result.toObjectArray()).containsExactly(1, 2, 3);
    }

    @Test
    void testParseNestedArray() {
        ArrayType arrayType = DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()));
        GenericArray result =
                (GenericArray)
                        ComplexTypeLiteralParser.parseArray(
                                "ARRAY[ARRAY[1,2], ARRAY[3,4]]", arrayType);

        assertThat(result.toObjectArray()).hasSize(2);
        assertThat(((GenericArray) result.toObjectArray()[0]).toObjectArray())
                .containsExactly(1, 2);
        assertThat(((GenericArray) result.toObjectArray()[1]).toObjectArray())
                .containsExactly(3, 4);
    }

    @Test
    void testParseInvalidArrayLiteral() {
        ArrayType arrayType = DataTypes.ARRAY(DataTypes.INT());

        assertThatThrownBy(
                        () -> ComplexTypeLiteralParser.parseArray("NOT_AN_ARRAY[1, 2]", arrayType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid ARRAY literal");
    }

    @Test
    void testParseEmptyMap() {
        MapType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
        GenericMap result = (GenericMap) ComplexTypeLiteralParser.parseMap("MAP[]", mapType);

        assertThat(result.size()).isEqualTo(0);
    }

    @Test
    void testParseMapWithBrackets() {
        MapType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
        GenericMap result =
                (GenericMap)
                        ComplexTypeLiteralParser.parseMap("MAP['key1', 1, 'key2', 2]", mapType);

        assertThat(result.size()).isEqualTo(2);
        assertThat(result.get(org.apache.fluss.row.BinaryString.fromString("key1"))).isEqualTo(1);
        assertThat(result.get(org.apache.fluss.row.BinaryString.fromString("key2"))).isEqualTo(2);
    }

    @Test
    void testParseMapWithParentheses() {
        MapType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
        GenericMap result =
                (GenericMap) ComplexTypeLiteralParser.parseMap("MAP('k1', 10, 'k2', 20)", mapType);

        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    void testParseMapCaseInsensitive() {
        MapType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
        GenericMap result =
                (GenericMap) ComplexTypeLiteralParser.parseMap("map['key', 100]", mapType);

        assertThat(result.size()).isEqualTo(1);
    }

    @Test
    void testParseMapOddNumberOfElements() {
        MapType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());

        assertThatThrownBy(
                        () -> ComplexTypeLiteralParser.parseMap("MAP['key1', 1, 'key2']", mapType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("even number of elements");
    }

    @Test
    void testParseInvalidMapLiteral() {
        MapType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());

        assertThatThrownBy(() -> ComplexTypeLiteralParser.parseMap("NOT_A_MAP['k', 1]", mapType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid MAP literal");
    }

    @Test
    void testParseRowWithKeyword() {
        RowType rowType = DataTypes.ROW(DataTypes.STRING(), DataTypes.INT());
        GenericRow result =
                (GenericRow) ComplexTypeLiteralParser.parseRow("ROW('Alice', 30)", rowType);

        assertThat(result.getFieldCount()).isEqualTo(2);
        assertThat(result.getString(0).toString()).isEqualTo("Alice");
        assertThat(result.getInt(1)).isEqualTo(30);
    }

    @Test
    void testParseRowWithoutKeyword() {
        RowType rowType = DataTypes.ROW(DataTypes.STRING(), DataTypes.INT());
        GenericRow result = (GenericRow) ComplexTypeLiteralParser.parseRow("('Bob', 25)", rowType);

        assertThat(result.getFieldCount()).isEqualTo(2);
        assertThat(result.getString(0).toString()).isEqualTo("Bob");
        assertThat(result.getInt(1)).isEqualTo(25);
    }

    @Test
    void testParseRowCaseInsensitive() {
        RowType rowType = DataTypes.ROW(DataTypes.STRING(), DataTypes.INT());
        GenericRow result = (GenericRow) ComplexTypeLiteralParser.parseRow("row('X', 1)", rowType);

        assertThat(result.getFieldCount()).isEqualTo(2);
    }

    @Test
    void testParseRowFieldCountMismatch() {
        RowType rowType = DataTypes.ROW(DataTypes.STRING(), DataTypes.INT());

        assertThatThrownBy(
                        () ->
                                ComplexTypeLiteralParser.parseRow(
                                        "ROW('Alice', 30, 'extra')", rowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("has 3 values but type expects 2 fields");
    }

    @Test
    void testParseInvalidRowLiteral() {
        RowType rowType = DataTypes.ROW(DataTypes.STRING(), DataTypes.INT());

        assertThatThrownBy(() -> ComplexTypeLiteralParser.parseRow("NOT_A_ROW", rowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid ROW literal");
    }

    @Test
    void testParseNestedRow() {
        RowType innerType = DataTypes.ROW(DataTypes.STRING(), DataTypes.INT());
        RowType outerType = DataTypes.ROW(DataTypes.STRING(), innerType);

        GenericRow result =
                (GenericRow)
                        ComplexTypeLiteralParser.parseRow(
                                "ROW('outer', ROW('inner', 42))", outerType);

        assertThat(result.getFieldCount()).isEqualTo(2);
        assertThat(result.getString(0).toString()).isEqualTo("outer");

        GenericRow inner = (GenericRow) result.getRow(1, 2);
        assertThat(inner.getString(0).toString()).isEqualTo("inner");
        assertThat(inner.getInt(1)).isEqualTo(42);
    }

    @Test
    void testParseArrayWithNullElements() {
        ArrayType arrayType = DataTypes.ARRAY(DataTypes.INT());
        GenericArray result =
                (GenericArray) ComplexTypeLiteralParser.parseArray("ARRAY[1, NULL, 3]", arrayType);

        Object[] array = result.toObjectArray();
        assertThat(array).hasSize(3);
        assertThat(array[0]).isEqualTo(1);
        assertThat(array[1]).isNull();
        assertThat(array[2]).isEqualTo(3);
    }

    @Test
    void testParseRowWithNullField() {
        RowType rowType = DataTypes.ROW(DataTypes.STRING(), DataTypes.INT());
        GenericRow result =
                (GenericRow) ComplexTypeLiteralParser.parseRow("ROW('Alice', NULL)", rowType);

        assertThat(result.isNullAt(1)).isTrue();
    }

    @Test
    void testParseComplexNested() {
        ArrayType arrayType = DataTypes.ARRAY(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        GenericArray result =
                (GenericArray)
                        ComplexTypeLiteralParser.parseArray(
                                "ARRAY[MAP['k1', 1], MAP['k2', 2]]", arrayType);

        assertThat(result.toObjectArray()).hasSize(2);
        assertThat(((GenericMap) result.toObjectArray()[0]).size()).isEqualTo(1);
        assertThat(((GenericMap) result.toObjectArray()[1]).size()).isEqualTo(1);
    }
}
