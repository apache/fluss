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

import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeDefaultVisitor;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ByteArrayWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.CollectionUtils.newHashMapWithExpectedSize;

/** Builds equality checks for projected log rows materialized by the Fluss scanner. */
final class ProjectedRowEquality extends DataTypeDefaultVisitor<UnaryOperator<Object>> {
    private static final ProjectedRowEquality INSTANCE = new ProjectedRowEquality();
    private static final UnaryOperator<Object> IDENTITY = UnaryOperator.identity();
    private static final UnaryOperator<Object> BINARY =
            value -> new ByteArrayWrapper((byte[]) value);

    private ProjectedRowEquality() {}

    static BiPredicate<InternalRow, InternalRow> create(RowType type) {
        List<UnaryOperator<Object>> fields = INSTANCE.fieldKeys(type);
        if (fields.stream().allMatch(ProjectedRowEquality::supportsDeepEquals)) {
            return Objects::equals;
        }
        InternalRow.FieldGetter[] getters = InternalRow.createFieldGetters(type);
        for (int i = 0; i < getters.length; i++) {
            UnaryOperator<Object> key = fields.get(i);
            if (!supportsDeepEquals(key)) {
                InternalRow.FieldGetter getter = getters[i];
                getters[i] = row -> comparisonKey(key, getter.getFieldOrNull(row));
            }
        }
        return (left, right) -> {
            for (InternalRow.FieldGetter getter : getters) {
                if (!Objects.deepEquals(
                        getter.getFieldOrNull(left), getter.getFieldOrNull(right))) {
                    return false;
                }
            }
            return true;
        };
    }

    // GenericRow, GenericArray and GenericMap values already compare byte[] by content.
    // Java map keys also need a content-based hash code.
    private static boolean supportsDeepEquals(UnaryOperator<Object> key) {
        return key == IDENTITY || key == BINARY;
    }

    @Override
    protected UnaryOperator<Object> defaultMethod(DataType type) {
        return IDENTITY;
    }

    @Override
    public UnaryOperator<Object> visit(BinaryType type) {
        return BINARY;
    }

    @Override
    public UnaryOperator<Object> visit(BytesType type) {
        return BINARY;
    }

    @Override
    public UnaryOperator<Object> visit(MapType type) {
        UnaryOperator<Object> key = type.getKeyType().accept(this);
        UnaryOperator<Object> value = type.getValueType().accept(this);
        if (key == IDENTITY && supportsDeepEquals(value)) {
            return IDENTITY;
        }
        InternalArray.ElementGetter keyGetter =
                InternalArray.createElementGetter(type.getKeyType());
        InternalArray.ElementGetter valueGetter =
                InternalArray.createElementGetter(type.getValueType());
        return object -> {
            InternalMap map = (InternalMap) object;
            Map<Object, Object> entries = newHashMapWithExpectedSize(map.size());
            InternalArray keys = map.keyArray();
            InternalArray values = map.valueArray();
            for (int i = 0; i < map.size(); i++) {
                entries.put(
                        key.apply(keyGetter.getElementOrNull(keys, i)),
                        comparisonKey(value, valueGetter.getElementOrNull(values, i)));
            }
            return entries;
        };
    }

    @Override
    public UnaryOperator<Object> visit(ArrayType type) {
        UnaryOperator<Object> element = type.getElementType().accept(this);
        if (supportsDeepEquals(element)) {
            return IDENTITY;
        }
        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(type.getElementType());
        return object -> {
            InternalArray array = (InternalArray) object;
            List<Object> elements = new ArrayList<>(array.size());
            for (int i = 0; i < array.size(); i++) {
                elements.add(comparisonKey(element, getter.getElementOrNull(array, i)));
            }
            return elements;
        };
    }

    @Override
    public UnaryOperator<Object> visit(RowType type) {
        List<UnaryOperator<Object>> fields = fieldKeys(type);
        if (fields.stream().allMatch(ProjectedRowEquality::supportsDeepEquals)) {
            return IDENTITY;
        }
        InternalRow.FieldGetter[] getters = InternalRow.createFieldGetters(type);
        return object -> {
            InternalRow row = (InternalRow) object;
            List<Object> values = new ArrayList<>(fields.size());
            for (int i = 0; i < getters.length; i++) {
                values.add(comparisonKey(fields.get(i), getters[i].getFieldOrNull(row)));
            }
            return values;
        };
    }

    private List<UnaryOperator<Object>> fieldKeys(RowType type) {
        return type.getFieldTypes().stream()
                .map(field -> field.accept(this))
                .collect(Collectors.toList());
    }

    private static Object comparisonKey(UnaryOperator<Object> key, Object value) {
        return value == null ? null : key.apply(value);
    }
}
