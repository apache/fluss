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

package org.apache.fluss.flink.utils;

import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.MapType;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;

import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.flink.utils.FlussRowToFlinkRowConverter.createInternalConverter;
import static org.apache.fluss.types.DataTypeChecks.getFieldCount;
import static org.apache.fluss.types.DataTypeChecks.getLength;
import static org.apache.fluss.types.DataTypeChecks.getPrecision;
import static org.apache.fluss.types.DataTypeChecks.getScale;

/** MapData Converter. */
public class FlinkMapConverter implements MapData {

    private final MapData mapData;

    FlinkMapConverter(DataType eleType, Object flussField) {
        this.mapData =
                copyMap(
                        (InternalMap) flussField,
                        ((MapType) eleType).getKeyType(),
                        ((MapType) eleType).getValueType());
    }

    private MapData copyMap(InternalMap map, DataType keyType, DataType valueType) {
        FlussDeserializationConverter keyConverter = createInternalConverter(keyType);
        FlussDeserializationConverter valueConverter = createInternalConverter(valueType);
        Map<Object, Object> javaMap = new HashMap<>();
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        for (int i = 0; i < keys.size(); i++) {
            Object key = keyConverter.deserialize(getFieldValue(keys, i, keyType));
            Object value =
                    isNullAt(values, i)
                            ? null
                            : valueConverter.deserialize(getFieldValue(values, i, valueType));
            javaMap.put(key, value);
        }
        return new GenericMapData(javaMap);
    }

    @Override
    public int size() {
        return mapData.size();
    }

    @Override
    public ArrayData keyArray() {
        return mapData.keyArray();
    }

    @Override
    public ArrayData valueArray() {
        return mapData.valueArray();
    }

    public MapData getMapData() {
        return mapData;
    }

    public static MapData deserialize(DataType flussDataType, Object flussField) {
        return new FlinkMapConverter(flussDataType, flussField).getMapData();
    }

    // Helper methods to replace InternalRowUtils functionality
    public static Object getFieldValue(InternalArray array, int pos, DataType dataType) {
        if (array.isNullAt(pos)) {
            return null;
        }

        // Switch based on data type to get the correct value
        switch (dataType.getTypeRoot()) {
            case CHAR:
                final int charLength = getLength(dataType);
                return array.getChar(pos, charLength);
            case STRING:
                return array.getString(pos);
            case BOOLEAN:
                return array.getBoolean(pos);
            case BINARY:
                final int binaryLength = getLength(dataType);
                return array.getBinary(pos, binaryLength);
            case BYTES:
                return array.getBytes(pos);
            case DECIMAL:
                final int precision = getPrecision(dataType);
                final int scale = getScale(dataType);
                return array.getDecimal(pos, precision, scale);
            case TINYINT:
                return array.getByte(pos);
            case SMALLINT:
                return array.getShort(pos);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return array.getInt(pos);
            case BIGINT:
                return array.getLong(pos);
            case FLOAT:
                return array.getFloat(pos);
            case DOUBLE:
                return array.getDouble(pos);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int ntzPrecision = getPrecision(dataType);
                return array.getTimestampNtz(pos, ntzPrecision);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int ltzPrecision = getPrecision(dataType);
                return array.getTimestampLtz(pos, ltzPrecision);
            case ARRAY:
                return array.getArray(pos);
            case MAP:
                return array.getMap(pos);
            case ROW:
                final int fieldCount = getFieldCount(dataType);
                return array.getRow(pos, fieldCount);
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    private static boolean isNullAt(InternalArray array, int pos) {
        return array.isNullAt(pos);
    }
}
