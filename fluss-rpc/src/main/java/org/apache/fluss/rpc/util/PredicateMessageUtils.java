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
import org.apache.fluss.predicate.StartsWith;
import org.apache.fluss.record.Filter;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.rpc.messages.PbCompoundFunction;
import org.apache.fluss.rpc.messages.PbCompoundPredicate;
import org.apache.fluss.rpc.messages.PbFieldRef;
import org.apache.fluss.rpc.messages.PbFilter;
import org.apache.fluss.rpc.messages.PbLeafFunction;
import org.apache.fluss.rpc.messages.PbLeafPredicate;
import org.apache.fluss.rpc.messages.PbLiteralValue;
import org.apache.fluss.rpc.messages.PbPredicate;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Utility class for serializing and deserializing predicates between the internal {@link Predicate}
 * model and the RPC {@link PbPredicate} Protobuf messages.
 *
 * <p>This class provides the entry points for:
 *
 * <ul>
 *   <li>Converting Protobuf messages back to internal Predicate objects for server-side evaluation.
 *   <li>Converting internal Predicate objects to Protobuf for client-server communication.
 *   <li>Wrapping predicates into schema-aware {@link Filter} objects.
 * </ul>
 */
public class PredicateMessageUtils {

    /**
     * Converts a Protobuf {@link PbPredicate} to an internal {@link Predicate}.
     *
     * @param pbPredicate the protobuf predicate, must not be null.
     * @return the corresponding internal predicate.
     * @throws IllegalArgumentException if the predicate type is unknown.
     */
    public static Predicate toPredicate(PbPredicate pbPredicate) {
        Objects.requireNonNull(pbPredicate, "pbPredicate must not be null");
        switch (pbPredicate.getType()) {
            case LEAF:
                return toLeafPredicate(pbPredicate.getLeaf());
            case COMPOUND:
                return toCompoundPredicate(pbPredicate.getCompound());
            default:
                throw new IllegalArgumentException(
                        "Unknown predicate type in PbPredicate: " + pbPredicate.getType());
        }
    }

    public static CompoundPredicate toCompoundPredicate(PbCompoundPredicate pbCompound) {
        List<Predicate> children =
                pbCompound.getChildrensList().stream()
                        .map(PredicateMessageUtils::toPredicate)
                        .collect(Collectors.toList());
        return new CompoundPredicate(toCompoundFunction(pbCompound.getFunction()), children);
    }

    private static CompoundPredicate.Function toCompoundFunction(PbCompoundFunction function) {
        switch (function) {
            case AND:
                return And.INSTANCE;
            case OR:
                return Or.INSTANCE;
            default:
                throw new IllegalArgumentException("Unknown compound function: " + function);
        }
    }

    private static LeafPredicate toLeafPredicate(PbLeafPredicate pbLeaf) {
        PbFieldRef fieldRef = pbLeaf.getFieldRef();
        List<Object> literals =
                pbLeaf.getLiteralsList().stream()
                        .map(PredicateMessageUtils::toLiteralValue)
                        .collect(Collectors.toList());

        return new LeafPredicate(
                toLeafFunction(pbLeaf.getFunction()),
                CommonRpcMessageUtils.toDataType(fieldRef.getDataType()),
                fieldRef.getFieldIndex(),
                fieldRef.getFieldName(),
                literals);
    }

    private static Object toLiteralValue(PbLiteralValue pbLiteral) {
        if (pbLiteral.isIsNull()) {
            return null;
        }
        switch (pbLiteral.getType().getRoot()) {
            case BOOLEAN:
                return pbLiteral.isBooleanValue();
            case INT:
                return pbLiteral.getIntValue();
            case TINYINT:
                return (byte) pbLiteral.getIntValue();
            case SMALLINT:
                return (short) pbLiteral.getIntValue();
            case BIGINT:
                return pbLiteral.getBigintValue();
            case FLOAT:
                return pbLiteral.getFloatValue();
            case DOUBLE:
                return pbLiteral.getDoubleValue();
            case CHAR:
            case VARCHAR:
                {
                    String stringValue = pbLiteral.getStringValue();
                    return null == stringValue ? null : BinaryString.fromString(stringValue);
                }
            case DECIMAL:
                if (pbLiteral.hasDecimalBytes()) {
                    // Non-compact decimal
                    return Decimal.fromUnscaledBytes(
                            pbLiteral.getDecimalBytes(),
                            pbLiteral.getType().getPrecision(),
                            pbLiteral.getType().getScale());
                } else {
                    // Compact decimal
                    return Decimal.fromUnscaledLong(
                            pbLiteral.getDecimalValue(),
                            pbLiteral.getType().getPrecision(),
                            pbLiteral.getType().getScale());
                }
            case DATE:
                return LocalDate.ofEpochDay(pbLiteral.getBigintValue());
            case TIME_WITHOUT_TIME_ZONE:
                return LocalTime.ofNanoOfDay(
                        TimeUnit.MILLISECONDS.toNanos(pbLiteral.getIntValue()));
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
                throw new IllegalArgumentException(
                        "Unknown literal value type in PbLiteralValue: "
                                + pbLiteral.getType().getRoot());
        }
    }

    private static LeafFunction toLeafFunction(PbLeafFunction function) {
        switch (function) {
            case EQUAL:
                return Equal.INSTANCE;
            case NOT_EQUAL:
                return NotEqual.INSTANCE;
            case LESS_THAN:
                return LessThan.INSTANCE;
            case LESS_OR_EQUAL:
                return LessOrEqual.INSTANCE;
            case GREATER_THAN:
                return GreaterThan.INSTANCE;
            case GREATER_OR_EQUAL:
                return GreaterOrEqual.INSTANCE;
            case IS_NULL:
                return IsNull.INSTANCE;
            case IS_NOT_NULL:
                return IsNotNull.INSTANCE;
            case STARTS_WITH:
                return StartsWith.INSTANCE;
            case CONTAINS:
                return Contains.INSTANCE;
            case END_WITH:
                return EndsWith.INSTANCE;
            case IN:
                return In.INSTANCE;
            case NOT_IN:
                return NotIn.INSTANCE;
            default:
                throw new IllegalArgumentException("Unknown leaf function: " + function);
        }
    }

    /**
     * Converts an internal {@link Predicate} into a {@link PbPredicate}.
     *
     * @param predicate the internal predicate to convert.
     * @return the corresponding protobuf predicate.
     */
    public static PbPredicate toPbPredicate(Predicate predicate) {
        return predicate.visit(PbPredicateConverter.INSTANCE);
    }

    /**
     * Converts an internal {@link Predicate} and schema ID into a {@link PbFilter}.
     *
     * @param predicate the predicate to wrap in the filter.
     * @param schemaId the ID of the schema this filter is associated with.
     * @return the protobuf filter message.
     */
    public static PbFilter toPbFilter(Predicate predicate, int schemaId) {
        PbFilter pbFilter = new PbFilter();
        pbFilter.setPredicate(toPbPredicate(predicate));
        pbFilter.setSchemaId(schemaId);
        return pbFilter;
    }

    /**
     * Converts a Protobuf {@link PbFilter} to an internal {@link Filter}.
     *
     * @param pbFilter the protobuf filter, must not be null.
     * @return the corresponding internal filter.
     */
    public static Filter toFilter(PbFilter pbFilter) {
        Objects.requireNonNull(pbFilter, "pbFilter must not be null");
        Predicate predicate = toPredicate(pbFilter.getPredicate());
        int schemaId = pbFilter.getSchemaId();
        return new Filter(predicate, schemaId);
    }
}
