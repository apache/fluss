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

package com.alibaba.fluss.predicate;

import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.TimestampType;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Leaf node of a {@link Predicate} tree. Compares a field in the row with literals. */
public class LeafPredicate implements Predicate {

    private static final long serialVersionUID = 1L;

    private final LeafFunction function;
    private final DataType type;
    private final int fieldIndex;
    private final String fieldName;

    private final List<Object> literals;

    public LeafPredicate(
            LeafFunction function,
            DataType type,
            int fieldIndex,
            String fieldName,
            List<Object> literals) {
        this.function = function;
        this.type = type;
        this.fieldIndex = fieldIndex;
        this.fieldName = fieldName;
        this.literals = literals;
    }

    public LeafFunction function() {
        return function;
    }

    public DataType type() {
        return type;
    }

    public int index() {
        return fieldIndex;
    }

    public String fieldName() {
        return fieldName;
    }

    public FieldRef fieldRef() {
        return new FieldRef(fieldIndex, fieldName, type);
    }

    public List<Object> literals() {
        return literals;
    }

    public LeafPredicate copyWithNewIndex(int fieldIndex) {
        return new LeafPredicate(function, type, fieldIndex, fieldName, literals);
    }

    @Override
    public boolean test(InternalRow row) {
        return function.test(type, get(row, fieldIndex, type), literals);
    }

    @Override
    public boolean test(
            long rowCount, InternalRow minValues, InternalRow maxValues, Long[] nullCounts) {
        Object min = get(minValues, fieldIndex, type);
        Object max = get(maxValues, fieldIndex, type);
        Long nullCount = nullCounts != null ? nullCounts[fieldIndex] : null;
        if (nullCount == null || rowCount != nullCount) {
            // not all null
            // min or max is null
            // unknown stats
            if (min == null || max == null) {
                return true;
            }
        }
        return function.test(type, rowCount, min, max, nullCount, literals);
    }

    @Override
    public Optional<Predicate> negate() {
        return function.negate()
                .map(negate -> new LeafPredicate(negate, type, fieldIndex, fieldName, literals));
    }

    @Override
    public <T> T visit(PredicateVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LeafPredicate that = (LeafPredicate) o;
        return fieldIndex == that.fieldIndex
                && Objects.equals(fieldName, that.fieldName)
                && Objects.equals(function, that.function)
                && Objects.equals(type, that.type)
                && Objects.equals(literals, that.literals);
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, type, fieldIndex, fieldName, literals);
    }

    @Override
    public String toString() {
        String literalsStr;
        if (literals == null || literals.isEmpty()) {
            literalsStr = "";
        } else if (literals.size() == 1) {
            literalsStr = Objects.toString(literals.get(0));
        } else {
            literalsStr = literals.toString();
        }
        return literalsStr.isEmpty()
                ? function + "(" + fieldName + ")"
                : function + "(" + fieldName + ", " + literalsStr + ")";
    }

    public static Object get(InternalRow internalRow, int pos, DataType fieldType) {
        if (internalRow.isNullAt(pos)) {
            return null;
        }
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return internalRow.getBoolean(pos);
            case TINYINT:
                return internalRow.getByte(pos);
            case SMALLINT:
                return internalRow.getShort(pos);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return internalRow.getInt(pos);
            case BIGINT:
                return internalRow.getLong(pos);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) fieldType;
                return internalRow.getTimestampNtz(pos, timestampType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType lzTs = (LocalZonedTimestampType) fieldType;
                return internalRow.getTimestampNtz(pos, lzTs.getPrecision());
            case FLOAT:
                return internalRow.getFloat(pos);
            case DOUBLE:
                return internalRow.getDouble(pos);
            case CHAR:
            case STRING:
                return internalRow.getString(pos);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                return internalRow.getDecimal(
                        pos, decimalType.getPrecision(), decimalType.getScale());
            case BINARY:
                return internalRow.getBytes(pos);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + fieldType);
        }
    }
}
