/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.testutils;

import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.TimestampType;

import org.assertj.core.api.AbstractAssert;

import java.util.Objects;

import static org.apache.fluss.testutils.InternalMapAssert.assertThatMap;
import static org.apache.fluss.types.DataTypeChecks.getLength;
import static org.assertj.core.api.Assertions.assertThat;

/** AssertJ assert for {@link InternalArray}. */
public class InternalArrayAssert extends AbstractAssert<InternalArrayAssert, InternalArray> {

    private DataType elementType;

    InternalArrayAssert(InternalArray actual) {
        super(actual, InternalArrayAssert.class);
    }

    public static InternalArrayAssert assertThatArray(InternalArray actual) {
        return new InternalArrayAssert(actual);
    }

    public InternalArrayAssert withElementType(DataType elementType) {
        this.elementType = elementType;
        return this;
    }

    public InternalArrayAssert isEqualTo(InternalArray expected) {
        assert elementType != null;
        assertThat(actual.size()).isEqualTo(expected.size());
        switch (elementType.getTypeRoot()) {
            case CHAR:
                int charLength = getLength(elementType);
                for (int i = 0; i < actual.size(); i++) {
                    if (expected.isNullAt(i)) {
                        assertThat(actual.isNullAt(i)).isTrue();
                    } else {
                        assertThat(actual.getChar(i, charLength))
                                .isEqualTo(expected.getChar(i, charLength));
                    }
                }
                break;

            case STRING:
                for (int i = 0; i < actual.size(); i++) {
                    if (expected.isNullAt(i)) {
                        assertThat(actual.isNullAt(i)).isTrue();
                    } else {
                        assertThat(actual.getString(i)).isEqualTo(expected.getString(i));
                    }
                }
                break;
            case BOOLEAN:
                for (int i = 0; i < actual.size(); i++) {
                    if (expected.isNullAt(i)) {
                        assertThat(actual.isNullAt(i)).isTrue();
                    } else {
                        assertThat(actual.getBoolean(i)).isEqualTo(expected.getBoolean(i));
                    }
                }
                break;
            case BINARY:
                int binaryLength = getLength(elementType);
                for (int i = 0; i < actual.size(); i++) {
                    if (expected.isNullAt(i)) {
                        assertThat(actual.isNullAt(i)).isTrue();
                    } else {
                        assertThat(actual.getBinary(i, binaryLength))
                                .isEqualTo(expected.getBinary(i, binaryLength));
                    }
                }
                break;
            case BYTES:
                for (int i = 0; i < actual.size(); i++) {
                    if (expected.isNullAt(i)) {
                        assertThat(actual.isNullAt(i)).isTrue();
                    } else {
                        assertThat(actual.getBytes(i)).isEqualTo(expected.getBytes(i));
                    }
                }
                break;
            case TINYINT:
                for (int i = 0; i < actual.size(); i++) {
                    if (expected.isNullAt(i)) {
                        assertThat(actual.isNullAt(i)).isTrue();
                    } else {
                        assertThat(actual.getByte(i)).isEqualTo(expected.getByte(i));
                    }
                }
                break;
            case DECIMAL:
                for (int i = 0; i < actual.size(); i++) {
                    if (expected.isNullAt(i)) {
                        assertThat(actual.isNullAt(i)).isTrue();
                    } else {
                        assertThat(actual.getDecimal(i, 0, 0))
                                .isEqualTo(expected.getDecimal(i, 0, 0));
                    }
                }
                break;
            case SMALLINT:
                for (int i = 0; i < actual.size(); i++) {
                    if (expected.isNullAt(i)) {
                        assertThat(actual.isNullAt(i)).isTrue();
                    } else {
                        assertThat(actual.getShort(i)).isEqualTo(expected.getShort(i));
                    }
                }
                break;
            case INTEGER:
            case TIME_WITHOUT_TIME_ZONE:
            case DATE:
                for (int i = 0; i < actual.size(); i++) {
                    if (expected.isNullAt(i)) {
                        assertThat(actual.isNullAt(i)).isTrue();
                    } else {
                        assertThat(actual.getInt(i)).isEqualTo(expected.getInt(i));
                    }
                }
                break;
            case BIGINT:
                for (int i = 0; i < actual.size(); i++) {
                    if (expected.isNullAt(i)) {
                        assertThat(actual.isNullAt(i)).isTrue();
                    } else {
                        assertThat(actual.getLong(i)).isEqualTo(expected.getLong(i));
                    }
                }
                break;
            case FLOAT:
                for (int i = 0; i < actual.size(); i++) {
                    if (expected.isNullAt(i)) {
                        assertThat(actual.isNullAt(i)).isTrue();
                    } else {
                        assertThat(actual.getFloat(i)).isEqualTo(expected.getFloat(i));
                    }
                }
                break;
            case DOUBLE:
                for (int i = 0; i < actual.size(); i++) {
                    if (expected.isNullAt(i)) {
                        assertThat(actual.isNullAt(i)).isTrue();
                    } else {
                        assertThat(actual.getDouble(i)).isEqualTo(expected.getDouble(i));
                    }
                }
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) elementType;
                for (int i = 0; i < actual.size(); i++) {
                    if (expected.isNullAt(i)) {
                        assertThat(actual.isNullAt(i)).isTrue();
                    } else {
                        assertThat(actual.getTimestampNtz(i, timestampType.getPrecision()))
                                .isEqualTo(
                                        expected.getTimestampNtz(i, timestampType.getPrecision()));
                    }
                }
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedTimestampType =
                        (LocalZonedTimestampType) elementType;
                for (int i = 0; i < actual.size(); i++) {
                    if (expected.isNullAt(i)) {
                        assertThat(actual.isNullAt(i)).isTrue();
                    } else {
                        assertThat(
                                        actual.getTimestampLtz(
                                                i, localZonedTimestampType.getPrecision()))
                                .isEqualTo(
                                        expected.getTimestampLtz(
                                                i, localZonedTimestampType.getPrecision()));
                    }
                }
                break;
            case ARRAY:
                for (int i = 0; i < actual.size(); i++) {
                    if (expected.isNullAt(i)) {
                        assertThat(actual.isNullAt(i)).isTrue();
                    } else {
                        assertThatArray(actual.getArray(i))
                                .withElementType(((ArrayType) elementType).getElementType())
                                .isEqualTo(expected.getArray(i));
                    }
                }
                break;
            case MAP:
                for (int i = 0; i < actual.size(); i++) {
                    if (expected.isNullAt(i)) {
                        assertThat(actual.isNullAt(i)).isTrue();
                    } else {
                        assertThatMap(actual.getMap(i))
                                .withMapType((MapType) elementType)
                                .isEqualTo(expected.getMap(i));
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported element type: " + elementType);
        }
        return this;
    }

    static Object getValueAt(InternalArray array, int pos, DataType elementType) {
        if (array.isNullAt(pos)) {
            return null;
        }
        switch (elementType.getTypeRoot()) {
            case CHAR:
                return array.getChar(pos, getLength(elementType));
            case STRING:
                return array.getString(pos);
            case BOOLEAN:
                return array.getBoolean(pos);
            case BINARY:
                return array.getBinary(pos, getLength(elementType));
            case BYTES:
                return array.getBytes(pos);
            case TINYINT:
                return array.getByte(pos);
            case DECIMAL:
                return array.getDecimal(pos, 0, 0);
            case SMALLINT:
                return array.getShort(pos);
            case INTEGER:
            case TIME_WITHOUT_TIME_ZONE:
            case DATE:
                return array.getInt(pos);
            case BIGINT:
                return array.getLong(pos);
            case FLOAT:
                return array.getFloat(pos);
            case DOUBLE:
                return array.getDouble(pos);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return array.getTimestampNtz(pos, ((TimestampType) elementType).getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return array.getTimestampLtz(
                        pos, ((LocalZonedTimestampType) elementType).getPrecision());
            case ARRAY:
                return array.getArray(pos);
            case MAP:
                return array.getMap(pos);
            case ROW:
                return array.getRow(pos, ((RowType) elementType).getFieldCount());
            default:
                throw new UnsupportedOperationException("Unsupported element type: " + elementType);
        }
    }

    static boolean valuesEqual(Object value1, Object value2, DataType elementType) {
        if (value1 == null && value2 == null) {
            return true;
        }
        if (value1 == null || value2 == null) {
            return false;
        }
        switch (elementType.getTypeRoot()) {
            case ARRAY:
                InternalArray array1 = (InternalArray) value1;
                InternalArray array2 = (InternalArray) value2;
                if (array1.size() != array2.size()) {
                    return false;
                }
                DataType elemType = ((ArrayType) elementType).getElementType();
                for (int i = 0; i < array1.size(); i++) {
                    if (!valuesEqual(
                            getValueAt(array1, i, elemType),
                            getValueAt(array2, i, elemType),
                            elemType)) {
                        return false;
                    }
                }
                return true;
            case MAP:
                InternalMap map1 = (InternalMap) value1;
                InternalMap map2 = (InternalMap) value2;
                if (map1.size() != map2.size()) {
                    return false;
                }
                MapType mapType = (MapType) elementType;
                for (int i = 0; i < map1.size(); i++) {
                    Object key1 = getValueAt(map1.keyArray(), i, mapType.getKeyType());
                    Object val1 = getValueAt(map1.valueArray(), i, mapType.getValueType());
                    boolean found = false;
                    for (int j = 0; j < map2.size(); j++) {
                        Object key2 = getValueAt(map2.keyArray(), j, mapType.getKeyType());
                        if (valuesEqual(key1, key2, mapType.getKeyType())) {
                            Object val2 = getValueAt(map2.valueArray(), j, mapType.getValueType());
                            if (valuesEqual(val1, val2, mapType.getValueType())) {
                                found = true;
                                break;
                            }
                        }
                    }
                    if (!found) {
                        return false;
                    }
                }
                return true;
            case ROW:
                InternalRow row1 = (InternalRow) value1;
                InternalRow row2 = (InternalRow) value2;
                RowType rowType = (RowType) elementType;
                if (row1.getFieldCount() != row2.getFieldCount()) {
                    return false;
                }
                for (int i = 0; i < row1.getFieldCount(); i++) {
                    DataType fieldType = rowType.getTypeAt(i);
                    InternalRow.FieldGetter getter = InternalRow.createFieldGetter(fieldType, i);
                    if (!valuesEqual(
                            getter.getFieldOrNull(row1), getter.getFieldOrNull(row2), fieldType)) {
                        return false;
                    }
                }
                return true;
            default:
                return Objects.equals(value1, value2);
        }
    }
}
