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

package org.apache.fluss.rpc.util;

import org.apache.fluss.predicate.And;
import org.apache.fluss.predicate.CompoundPredicate;
import org.apache.fluss.predicate.Contains;
import org.apache.fluss.predicate.EndsWith;
import org.apache.fluss.predicate.Equal;
import org.apache.fluss.predicate.GreaterOrEqual;
import org.apache.fluss.predicate.GreaterThan;
import org.apache.fluss.predicate.In;
import org.apache.fluss.predicate.IsNotNull;
import org.apache.fluss.predicate.IsNull;
import org.apache.fluss.predicate.LeafFunction;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.LessOrEqual;
import org.apache.fluss.predicate.LessThan;
import org.apache.fluss.predicate.NotEqual;
import org.apache.fluss.predicate.NotIn;
import org.apache.fluss.predicate.Or;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateVisitor;
import org.apache.fluss.predicate.StartsWith;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.rpc.messages.PbCompoundPredicate;
import org.apache.fluss.rpc.messages.PbLeafPredicate;
import org.apache.fluss.rpc.messages.PbLiteralValue;
import org.apache.fluss.rpc.messages.PbPredicate;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.RowType;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utils for converting Predicate to PbPredicate and vice versa. */
public class PredicateMessageUtils {

    // -------------------------------------------------------------------------
    //  Deserialization: PbPredicate -> Predicate
    // -------------------------------------------------------------------------

    public static Predicate toPredicate(PbPredicate pbPredicate, RowType rowType) {
        PredicateType type = PredicateType.fromValue(pbPredicate.getType());
        switch (type) {
            case LEAF:
                return toLeafPredicate(pbPredicate.getLeaf(), rowType);
            case COMPOUND:
                return toCompoundPredicate(pbPredicate.getCompound(), rowType);
            default:
                throw new IllegalArgumentException("Unknown predicate type: " + type);
        }
    }

    public static CompoundPredicate toCompoundPredicate(
            PbCompoundPredicate pbCompound, RowType rowType) {
        List<Predicate> children =
                pbCompound.getChildrensList().stream()
                        .map(child -> toPredicate(child, rowType))
                        .collect(Collectors.toList());
        return new CompoundPredicate(
                CompoundFunctionCode.fromValue(pbCompound.getFunction()).getFunction(), children);
    }

    private static LeafPredicate toLeafPredicate(PbLeafPredicate pbLeaf, RowType rowType) {
        ResolvedField resolvedField = resolveField(rowType, pbLeaf.getFieldId());
        DataType fieldType = resolvedField.field.getType();
        String fieldName = resolvedField.field.getName();
        List<Object> literals =
                pbLeaf.getLiteralsList().stream()
                        .map(lit -> toLiteralValue(lit, fieldType))
                        .collect(Collectors.toList());

        return new LeafPredicate(
                LeafFunctionCode.fromValue(pbLeaf.getFunction()).getFunction(),
                fieldType,
                resolvedField.index,
                fieldName,
                literals);
    }

    private static Object toLiteralValue(PbLiteralValue pbLiteral, DataType fieldType) {
        if (pbLiteral.isIsNull()) {
            return null;
        }
        DataTypeRoot root =
                DataTypeRootCode.fromValue(pbLiteral.getLiteralType()).getDataTypeRoot();
        validateLiteralType(fieldType, root);
        switch (root) {
            case BOOLEAN:
                return pbLiteral.isBooleanValue();
            case TINYINT:
                return (byte) pbLiteral.getIntValue();
            case SMALLINT:
                return (short) pbLiteral.getIntValue();
            case INTEGER:
                return pbLiteral.getIntValue();
            case BIGINT:
                return pbLiteral.getBigintValue();
            case FLOAT:
                return pbLiteral.getFloatValue();
            case DOUBLE:
                return pbLiteral.getDoubleValue();
            case CHAR:
            case STRING:
                String stringValue = pbLiteral.getStringValue();
                return stringValue == null ? null : BinaryString.fromString(stringValue);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                if (pbLiteral.hasDecimalBytes()) {
                    return Decimal.fromUnscaledBytes(
                            pbLiteral.getDecimalBytes(),
                            decimalType.getPrecision(),
                            decimalType.getScale());
                } else {
                    return Decimal.fromUnscaledLong(
                            pbLiteral.getDecimalValue(),
                            decimalType.getPrecision(),
                            decimalType.getScale());
                }
            case DATE:
                return LocalDate.ofEpochDay(pbLiteral.getBigintValue());
            case TIME_WITHOUT_TIME_ZONE:
                return LocalTime.ofNanoOfDay(pbLiteral.getIntValue() * 1_000_000L);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampNtz.fromMillis(
                        pbLiteral.getTimestampMillisValue(),
                        pbLiteral.getTimestampNanoOfMillisValue());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampLtz.fromEpochMillis(
                        pbLiteral.getTimestampMillisValue(),
                        pbLiteral.getTimestampNanoOfMillisValue());
            case BINARY:
            case BYTES:
                return pbLiteral.getBinaryValue();
            default:
                throw new IllegalArgumentException("Unknown literal value type: " + root);
        }
    }

    private static void validateLiteralType(DataType fieldType, DataTypeRoot literalRoot) {
        DataTypeRoot fieldRoot = fieldType.getTypeRoot();
        if (fieldRoot == literalRoot) {
            return;
        }
        if (isCharacterString(fieldRoot) && isCharacterString(literalRoot)) {
            return;
        }
        if (isBinaryString(fieldRoot) && isBinaryString(literalRoot)) {
            return;
        }
        throw new IllegalArgumentException(
                "Literal type "
                        + literalRoot
                        + " does not match target schema field type "
                        + fieldRoot
                        + ".");
    }

    private static boolean isCharacterString(DataTypeRoot root) {
        return root == DataTypeRoot.CHAR || root == DataTypeRoot.STRING;
    }

    private static boolean isBinaryString(DataTypeRoot root) {
        return root == DataTypeRoot.BINARY || root == DataTypeRoot.BYTES;
    }

    // -------------------------------------------------------------------------
    //  Serialization: Predicate -> PbPredicate
    // -------------------------------------------------------------------------

    public static PbPredicate toPbPredicate(Predicate predicate, RowType rowType) {
        return predicate.visit(
                new PredicateVisitor<PbPredicate>() {
                    @Override
                    public PbPredicate visit(LeafPredicate predicate) {
                        DataField field = resolveSourceField(predicate, rowType);
                        if (field.getFieldId() < 0) {
                            throw new IllegalArgumentException(
                                    "Field "
                                            + field.getName()
                                            + " at index "
                                            + predicate.index()
                                            + " does not have a valid field id.");
                        }
                        PbLeafPredicate pbLeaf = new PbLeafPredicate();
                        pbLeaf.setFunction(
                                LeafFunctionCode.fromFunction(predicate.function()).getValue());
                        pbLeaf.setFieldId(field.getFieldId());

                        List<PbLiteralValue> literals = new ArrayList<>();
                        for (Object literal : predicate.literals()) {
                            literals.add(toPbLiteralValue(field.getType(), literal));
                        }
                        pbLeaf.addAllLiterals(literals);

                        PbPredicate pbPredicate = new PbPredicate();
                        pbPredicate.setType(PredicateType.LEAF.getValue());
                        pbPredicate.setLeaf(pbLeaf);
                        return pbPredicate;
                    }

                    @Override
                    public PbPredicate visit(CompoundPredicate predicate) {
                        PbCompoundPredicate pbCompound = new PbCompoundPredicate();
                        pbCompound.setFunction(
                                CompoundFunctionCode.fromFunction(predicate.function()).getValue());
                        pbCompound.addAllChildrens(
                                predicate.children().stream()
                                        .map(child -> toPbPredicate(child, rowType))
                                        .collect(Collectors.toList()));

                        PbPredicate pbPredicate = new PbPredicate();
                        pbPredicate.setType(PredicateType.COMPOUND.getValue());
                        pbPredicate.setCompound(pbCompound);
                        return pbPredicate;
                    }
                });
    }

    private static PbLiteralValue toPbLiteralValue(DataType type, Object literal) {
        PbLiteralValue pbLiteral = new PbLiteralValue();
        pbLiteral.setLiteralType(DataTypeRootCode.fromDataTypeRoot(type.getTypeRoot()).getValue());
        if (literal == null) {
            pbLiteral.setIsNull(true);
            return pbLiteral;
        }
        pbLiteral.setIsNull(false);
        switch (type.getTypeRoot()) {
            case CHAR:
            case STRING:
                pbLiteral.setStringValue(literal.toString());
                break;
            case BOOLEAN:
                pbLiteral.setBooleanValue((Boolean) literal);
                break;
            case BINARY:
            case BYTES:
                pbLiteral.setBinaryValue((byte[]) literal);
                break;
            case DECIMAL:
                Decimal decimal = (Decimal) literal;
                if (decimal.isCompact()) {
                    pbLiteral.setDecimalValue(decimal.toUnscaledLong());
                } else {
                    pbLiteral.setDecimalBytes(decimal.toUnscaledBytes());
                }
                break;
            case TINYINT:
                pbLiteral.setIntValue((Byte) literal);
                break;
            case SMALLINT:
                pbLiteral.setIntValue((Short) literal);
                break;
            case INTEGER:
                pbLiteral.setIntValue((Integer) literal);
                break;
            case DATE:
                pbLiteral.setBigintValue(((LocalDate) literal).toEpochDay());
                break;
            case TIME_WITHOUT_TIME_ZONE:
                pbLiteral.setIntValue((int) (((LocalTime) literal).toNanoOfDay() / 1_000_000L));
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                pbLiteral.setTimestampMillisValue(((TimestampNtz) literal).getMillisecond());
                pbLiteral.setTimestampNanoOfMillisValue(
                        ((TimestampNtz) literal).getNanoOfMillisecond());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                pbLiteral.setTimestampMillisValue(((TimestampLtz) literal).getEpochMillisecond());
                pbLiteral.setTimestampNanoOfMillisValue(
                        ((TimestampLtz) literal).getNanoOfMillisecond());
                break;
            case BIGINT:
                pbLiteral.setBigintValue((Long) literal);
                break;
            case FLOAT:
                pbLiteral.setFloatValue((Float) literal);
                break;
            case DOUBLE:
                pbLiteral.setDoubleValue((Double) literal);
                break;
            default:
                throw new IllegalArgumentException("Unknown data type: " + type.getTypeRoot());
        }
        return pbLiteral;
    }

    private static DataField resolveSourceField(LeafPredicate predicate, RowType rowType) {
        if (predicate.index() < 0 || predicate.index() >= rowType.getFieldCount()) {
            throw new IllegalArgumentException(
                    "Predicate field index "
                            + predicate.index()
                            + " is out of bounds for row type with "
                            + rowType.getFieldCount()
                            + " fields.");
        }
        DataField field = rowType.getFields().get(predicate.index());
        if (!field.getName().equals(predicate.fieldName())) {
            throw new IllegalArgumentException(
                    "Predicate field name "
                            + predicate.fieldName()
                            + " does not match schema field "
                            + field.getName()
                            + " at index "
                            + predicate.index()
                            + ".");
        }
        if (!field.getType().equals(predicate.type())) {
            throw new IllegalArgumentException(
                    "Predicate field type "
                            + predicate.type()
                            + " does not match schema field type "
                            + field.getType()
                            + " for field "
                            + field.getName()
                            + ".");
        }
        return field;
    }

    private static ResolvedField resolveField(RowType rowType, int fieldId) {
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            if (field.getFieldId() == fieldId) {
                return new ResolvedField(i, field);
            }
        }
        throw new IllegalArgumentException(
                "Cannot resolve field id " + fieldId + " from row type.");
    }

    // -------------------------------------------------------------------------
    //  Proto int32 <-> domain object mapping enums
    // -------------------------------------------------------------------------

    /** Maps PbPredicate.type int32 values to predicate kinds. */
    private enum PredicateType {
        LEAF(0),
        COMPOUND(1);

        private final int value;
        private static final PredicateType[] VALUES = new PredicateType[2];

        static {
            for (PredicateType t : values()) {
                VALUES[t.value] = t;
            }
        }

        PredicateType(int value) {
            this.value = value;
        }

        int getValue() {
            return value;
        }

        static PredicateType fromValue(int value) {
            if (value < 0 || value >= VALUES.length) {
                throw new IllegalArgumentException("Unknown predicate type: " + value);
            }
            return VALUES[value];
        }
    }

    /** Maps PbLeafPredicate.function int32 values to {@link LeafFunction} instances. */
    private enum LeafFunctionCode {
        EQUAL(0, Equal.INSTANCE),
        NOT_EQUAL(1, NotEqual.INSTANCE),
        LESS_THAN(2, LessThan.INSTANCE),
        LESS_OR_EQUAL(3, LessOrEqual.INSTANCE),
        GREATER_THAN(4, GreaterThan.INSTANCE),
        GREATER_OR_EQUAL(5, GreaterOrEqual.INSTANCE),
        IS_NULL(6, IsNull.INSTANCE),
        IS_NOT_NULL(7, IsNotNull.INSTANCE),
        STARTS_WITH(8, StartsWith.INSTANCE),
        CONTAINS(9, Contains.INSTANCE),
        END_WITH(10, EndsWith.INSTANCE),
        IN(11, In.INSTANCE),
        NOT_IN(12, NotIn.INSTANCE);

        private final int value;
        private final LeafFunction function;
        private static final LeafFunctionCode[] VALUES = new LeafFunctionCode[13];
        private static final Map<Class<? extends LeafFunction>, LeafFunctionCode> FUNCTION_MAP =
                new HashMap<>();

        static {
            for (LeafFunctionCode c : values()) {
                VALUES[c.value] = c;
                FUNCTION_MAP.put(c.function.getClass(), c);
            }
        }

        LeafFunctionCode(int value, LeafFunction function) {
            this.value = value;
            this.function = function;
        }

        int getValue() {
            return value;
        }

        LeafFunction getFunction() {
            return function;
        }

        static LeafFunctionCode fromValue(int value) {
            if (value < 0 || value >= VALUES.length) {
                throw new IllegalArgumentException("Unknown leaf function: " + value);
            }
            return VALUES[value];
        }

        static LeafFunctionCode fromFunction(LeafFunction function) {
            LeafFunctionCode c = FUNCTION_MAP.get(function.getClass());
            if (c == null) {
                throw new IllegalArgumentException("Unknown leaf function: " + function);
            }
            return c;
        }
    }

    /** Maps PbCompoundPredicate.function int32 values to {@link CompoundPredicate.Function}. */
    private enum CompoundFunctionCode {
        AND(0, And.INSTANCE),
        OR(1, Or.INSTANCE);

        private final int value;
        private final CompoundPredicate.Function function;
        private static final CompoundFunctionCode[] VALUES = new CompoundFunctionCode[2];
        private static final Map<Class<? extends CompoundPredicate.Function>, CompoundFunctionCode>
                FUNCTION_MAP = new HashMap<>();

        static {
            for (CompoundFunctionCode c : values()) {
                VALUES[c.value] = c;
                FUNCTION_MAP.put(c.function.getClass(), c);
            }
        }

        CompoundFunctionCode(int value, CompoundPredicate.Function function) {
            this.value = value;
            this.function = function;
        }

        int getValue() {
            return value;
        }

        CompoundPredicate.Function getFunction() {
            return function;
        }

        static CompoundFunctionCode fromValue(int value) {
            if (value < 0 || value >= VALUES.length) {
                throw new IllegalArgumentException("Unknown compound function: " + value);
            }
            return VALUES[value];
        }

        static CompoundFunctionCode fromFunction(CompoundPredicate.Function function) {
            CompoundFunctionCode c = FUNCTION_MAP.get(function.getClass());
            if (c == null) {
                throw new IllegalArgumentException("Unknown compound function: " + function);
            }
            return c;
        }
    }

    /**
     * Maps PbLiteralValue.literal_type int32 values to {@link DataTypeRoot}.
     *
     * <p>Note: proto uses INT/VARCHAR while the domain model uses INTEGER/STRING.
     */
    private enum DataTypeRootCode {
        BOOLEAN(0, DataTypeRoot.BOOLEAN),
        TINYINT(1, DataTypeRoot.TINYINT),
        SMALLINT(2, DataTypeRoot.SMALLINT),
        INT(3, DataTypeRoot.INTEGER),
        BIGINT(4, DataTypeRoot.BIGINT),
        FLOAT(5, DataTypeRoot.FLOAT),
        DOUBLE(6, DataTypeRoot.DOUBLE),
        CHAR(7, DataTypeRoot.CHAR),
        VARCHAR(8, DataTypeRoot.STRING),
        DECIMAL(9, DataTypeRoot.DECIMAL),
        DATE(10, DataTypeRoot.DATE),
        TIME_WITHOUT_TIME_ZONE(11, DataTypeRoot.TIME_WITHOUT_TIME_ZONE),
        TIMESTAMP_WITHOUT_TIME_ZONE(12, DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE),
        TIMESTAMP_WITH_LOCAL_TIME_ZONE(13, DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
        BINARY(14, DataTypeRoot.BINARY),
        BYTES(15, DataTypeRoot.BYTES);

        private final int value;
        private final DataTypeRoot dataTypeRoot;
        private static final DataTypeRootCode[] VALUES = new DataTypeRootCode[16];
        private static final Map<DataTypeRoot, DataTypeRootCode> ROOT_MAP = new HashMap<>();

        static {
            for (DataTypeRootCode c : values()) {
                VALUES[c.value] = c;
                ROOT_MAP.put(c.dataTypeRoot, c);
            }
        }

        DataTypeRootCode(int value, DataTypeRoot dataTypeRoot) {
            this.value = value;
            this.dataTypeRoot = dataTypeRoot;
        }

        int getValue() {
            return value;
        }

        DataTypeRoot getDataTypeRoot() {
            return dataTypeRoot;
        }

        static DataTypeRootCode fromValue(int value) {
            if (value < 0 || value >= VALUES.length) {
                throw new IllegalArgumentException("Unknown data type root: " + value);
            }
            return VALUES[value];
        }

        static DataTypeRootCode fromDataTypeRoot(DataTypeRoot root) {
            DataTypeRootCode c = ROOT_MAP.get(root);
            if (c == null) {
                throw new IllegalArgumentException("Unknown data type root: " + root);
            }
            return c;
        }
    }

    private static final class ResolvedField {
        private final int index;
        private final DataField field;

        private ResolvedField(int index, DataField field) {
            this.index = index;
            this.field = field;
        }
    }
}
