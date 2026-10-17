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

package org.apache.fluss.server.kv;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Compares KV rows by logical values, including nested values and fields from older schemas.
 *
 * <p>Fields are aligned to the latest schema by stable field ID. A field absent from an older
 * schema compares as null; fields dropped from the latest schema are ignored. This class is used
 * under the tablet write lock and is not thread-safe.
 */
final class LogicalRowEqualizer {

    private final SchemaGetter schemaGetter;
    private final RowType latestRowType;
    private final Map<Integer, RowEqualizer> equalizers = new HashMap<>();

    LogicalRowEqualizer(SchemaGetter schemaGetter, Schema latestSchema) {
        this.schemaGetter = schemaGetter;
        this.latestRowType = latestSchema.getRowType();
    }

    boolean equals(BinaryValue left, BinaryValue right) {
        int schemaPair = ((left.schemaId & 0xffff) << 16) | (right.schemaId & 0xffff);
        RowEqualizer equalizer = equalizers.get(schemaPair);
        if (equalizer == null) {
            equalizer =
                    createRowEqualizer(
                            schemaGetter.getSchema(left.schemaId).getRowType(),
                            schemaGetter.getSchema(right.schemaId).getRowType(),
                            latestRowType);
            equalizers.put(schemaPair, equalizer);
        }
        return equalizer.equals(left.row, right.row);
    }

    private static RowEqualizer createRowEqualizer(
            RowType leftType, RowType rightType, RowType latestType) {
        Map<Integer, Integer> leftIndexes = fieldIndexes(leftType);
        Map<Integer, Integer> rightIndexes = fieldIndexes(rightType);
        List<DataField> latestFields = latestType.getFields();
        FieldEqualizer[] fields = new FieldEqualizer[latestFields.size()];
        for (int i = 0; i < fields.length; i++) {
            DataField latestField = latestFields.get(i);
            Integer leftIndex = leftIndexes.get(latestField.getFieldId());
            Integer rightIndex = rightIndexes.get(latestField.getFieldId());
            DataType leftFieldType = leftIndex == null ? null : leftType.getTypeAt(leftIndex);
            DataType rightFieldType = rightIndex == null ? null : rightType.getTypeAt(rightIndex);
            fields[i] =
                    new FieldEqualizer(
                            leftIndex == null
                                    ? null
                                    : InternalRow.createFieldGetter(leftFieldType, leftIndex),
                            rightIndex == null
                                    ? null
                                    : InternalRow.createFieldGetter(rightFieldType, rightIndex),
                            createValueEqualizer(
                                    leftFieldType, rightFieldType, latestField.getType()));
        }
        return (left, right) -> {
            for (FieldEqualizer field : fields) {
                if (!field.equals(left, right)) {
                    return false;
                }
            }
            return true;
        };
    }

    private static Map<Integer, Integer> fieldIndexes(RowType type) {
        Map<Integer, Integer> indexes = new HashMap<>();
        List<DataField> fields = type.getFields();
        for (int i = 0; i < fields.size(); i++) {
            indexes.put(fields.get(i).getFieldId(), i);
        }
        return indexes;
    }

    private static ValueEqualizer createValueEqualizer(
            @Nullable DataType leftType, @Nullable DataType rightType, DataType latestType) {
        if (leftType == null
                || rightType == null
                || !leftType.equals(latestType)
                || !rightType.equals(latestType)) {
            return (left, right) -> false;
        }

        switch (latestType.getTypeRoot()) {
            case FLOAT:
                return (left, right) -> {
                    float leftFloat = (Float) left;
                    float rightFloat = (Float) right;
                    return leftFloat == rightFloat
                            || (Float.isNaN(leftFloat) && Float.isNaN(rightFloat));
                };
            case DOUBLE:
                return (left, right) -> {
                    double leftDouble = (Double) left;
                    double rightDouble = (Double) right;
                    return leftDouble == rightDouble
                            || (Double.isNaN(leftDouble) && Double.isNaN(rightDouble));
                };
            case BINARY:
            case BYTES:
                return (left, right) -> Arrays.equals((byte[]) left, (byte[]) right);
            case ARRAY:
                return createArrayEqualizer(
                        ((ArrayType) leftType).getElementType(),
                        ((ArrayType) rightType).getElementType(),
                        ((ArrayType) latestType).getElementType());
            case MAP:
                return createMapEqualizer(
                        (MapType) leftType, (MapType) rightType, (MapType) latestType);
            case ROW:
                RowEqualizer rowEqualizer =
                        createRowEqualizer(
                                (RowType) leftType, (RowType) rightType, (RowType) latestType);
                return (left, right) ->
                        rowEqualizer.equals((InternalRow) left, (InternalRow) right);
            default:
                return Object::equals;
        }
    }

    private static ValueEqualizer createArrayEqualizer(
            DataType leftType, DataType rightType, DataType latestType) {
        InternalArray.ElementGetter leftGetter = InternalArray.createElementGetter(leftType);
        InternalArray.ElementGetter rightGetter = InternalArray.createElementGetter(rightType);
        ValueEqualizer elementEqualizer = createValueEqualizer(leftType, rightType, latestType);
        return (left, right) -> {
            InternalArray leftArray = (InternalArray) left;
            InternalArray rightArray = (InternalArray) right;
            if (leftArray.size() != rightArray.size()) {
                return false;
            }
            for (int i = 0; i < leftArray.size(); i++) {
                if (!equalsNullable(
                        leftGetter.getElementOrNull(leftArray, i),
                        rightGetter.getElementOrNull(rightArray, i),
                        elementEqualizer)) {
                    return false;
                }
            }
            return true;
        };
    }

    private static ValueEqualizer createMapEqualizer(
            MapType leftType, MapType rightType, MapType latestType) {
        InternalArray.ElementGetter leftKeyGetter =
                InternalArray.createElementGetter(leftType.getKeyType());
        InternalArray.ElementGetter rightKeyGetter =
                InternalArray.createElementGetter(rightType.getKeyType());
        InternalArray.ElementGetter leftValueGetter =
                InternalArray.createElementGetter(leftType.getValueType());
        InternalArray.ElementGetter rightValueGetter =
                InternalArray.createElementGetter(rightType.getValueType());
        ValueEqualizer keyEqualizer =
                createValueEqualizer(
                        leftType.getKeyType(), rightType.getKeyType(), latestType.getKeyType());
        ValueEqualizer valueEqualizer =
                createValueEqualizer(
                        leftType.getValueType(),
                        rightType.getValueType(),
                        latestType.getValueType());
        return (left, right) -> {
            InternalMap leftMap = (InternalMap) left;
            InternalMap rightMap = (InternalMap) right;
            if (leftMap.size() != rightMap.size()) {
                return false;
            }
            InternalArray leftKeys = leftMap.keyArray();
            InternalArray rightKeys = rightMap.keyArray();
            InternalArray leftValues = leftMap.valueArray();
            InternalArray rightValues = rightMap.valueArray();
            boolean[] matched = new boolean[rightMap.size()];
            for (int i = 0; i < leftMap.size(); i++) {
                Object key = leftKeyGetter.getElementOrNull(leftKeys, i);
                Object value = leftValueGetter.getElementOrNull(leftValues, i);
                boolean found = false;
                for (int j = 0; j < rightMap.size(); j++) {
                    if (!matched[j]
                            && equalsNullable(
                                    key,
                                    rightKeyGetter.getElementOrNull(rightKeys, j),
                                    keyEqualizer)
                            && equalsNullable(
                                    value,
                                    rightValueGetter.getElementOrNull(rightValues, j),
                                    valueEqualizer)) {
                        matched[j] = true;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return false;
                }
            }
            return true;
        };
    }

    private static boolean equalsNullable(
            @Nullable Object left, @Nullable Object right, ValueEqualizer equalizer) {
        if (left == null || right == null) {
            return left == right;
        }
        return equalizer.equals(left, right);
    }

    private interface RowEqualizer {
        boolean equals(InternalRow left, InternalRow right);
    }

    private interface ValueEqualizer {
        boolean equals(Object left, Object right);
    }

    private static final class FieldEqualizer {
        private final @Nullable InternalRow.FieldGetter leftGetter;
        private final @Nullable InternalRow.FieldGetter rightGetter;
        private final ValueEqualizer equalizer;

        private FieldEqualizer(
                @Nullable InternalRow.FieldGetter leftGetter,
                @Nullable InternalRow.FieldGetter rightGetter,
                ValueEqualizer equalizer) {
            this.leftGetter = leftGetter;
            this.rightGetter = rightGetter;
            this.equalizer = equalizer;
        }

        private boolean equals(InternalRow left, InternalRow right) {
            return equalsNullable(
                    leftGetter == null ? null : leftGetter.getFieldOrNull(left),
                    rightGetter == null ? null : rightGetter.getFieldOrNull(right),
                    equalizer);
        }
    }
}
