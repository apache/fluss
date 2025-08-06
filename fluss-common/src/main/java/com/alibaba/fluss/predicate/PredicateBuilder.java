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

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.TypeUtils;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static java.util.Collections.singletonList;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A utility class to create {@link Predicate} object for common filter conditions.
 *
 * @since 0.8
 */
@PublicEvolving
public class PredicateBuilder {

    private static final LocalDate EPOCH_DAY =
            Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC).toLocalDate();

    private final RowType rowType;
    private final List<String> fieldNames;

    public PredicateBuilder(RowType rowType) {
        this.rowType = rowType;
        this.fieldNames = rowType.getFieldNames();
    }

    public int indexOf(String field) {
        return fieldNames.indexOf(field);
    }

    public Predicate equal(int idx, Object literal) {
        return leaf(Equal.INSTANCE, idx, literal);
    }

    public Predicate notEqual(int idx, Object literal) {
        return leaf(NotEqual.INSTANCE, idx, literal);
    }

    public Predicate lessThan(int idx, Object literal) {
        return leaf(LessThan.INSTANCE, idx, literal);
    }

    public Predicate lessOrEqual(int idx, Object literal) {
        return leaf(LessOrEqual.INSTANCE, idx, literal);
    }

    public Predicate greaterThan(int idx, Object literal) {
        return leaf(GreaterThan.INSTANCE, idx, literal);
    }

    public Predicate greaterOrEqual(int idx, Object literal) {
        return leaf(GreaterOrEqual.INSTANCE, idx, literal);
    }

    public Predicate isNull(int idx) {
        return leaf(IsNull.INSTANCE, idx);
    }

    public Predicate isNotNull(int idx) {
        return leaf(IsNotNull.INSTANCE, idx);
    }

    public Predicate startsWith(int idx, Object patternLiteral) {
        return leaf(StartsWith.INSTANCE, idx, patternLiteral);
    }

    public Predicate endsWith(int idx, Object patternLiteral) {
        return leaf(EndsWith.INSTANCE, idx, patternLiteral);
    }

    public Predicate contains(int idx, Object patternLiteral) {
        return leaf(Contains.INSTANCE, idx, patternLiteral);
    }

    public Predicate leaf(NullFalseLeafBinaryFunction function, int idx, Object literal) {
        validateIndex(idx);
        DataField field = rowType.getFields().get(idx);
        return new LeafPredicate(
                function, field.getType(), idx, field.getName(), singletonList(literal));
    }

    public Predicate leaf(LeafUnaryFunction function, int idx) {
        validateIndex(idx);
        DataField field = rowType.getFields().get(idx);
        return new LeafPredicate(
                function, field.getType(), idx, field.getName(), Collections.emptyList());
    }

    public Predicate in(int idx, List<Object> literals) {
        validateIndex(idx);
        // In the IN predicate, 20 literals are critical for performance.
        // If there are more than 20 literals, the performance will decrease.
        if (literals.size() > 20) {
            DataField field = rowType.getFields().get(idx);
            return new LeafPredicate(In.INSTANCE, field.getType(), idx, field.getName(), literals);
        }

        List<Predicate> equals = new ArrayList<>(literals.size());
        for (Object literal : literals) {
            equals.add(equal(idx, literal));
        }
        return or(equals);
    }

    public Predicate notIn(int idx, List<Object> literals) {
        return in(idx, literals).negate().get();
    }

    public Predicate between(int idx, Object includedLowerBound, Object includedUpperBound) {
        return new CompoundPredicate(
                And.INSTANCE,
                Arrays.asList(
                        greaterOrEqual(idx, includedLowerBound),
                        lessOrEqual(idx, includedUpperBound)));
    }

    public static Predicate and(Predicate... predicates) {
        return and(Arrays.asList(predicates));
    }

    public static Predicate and(List<Predicate> predicates) {
        checkArgument(
                !predicates.isEmpty(),
                "There must be at least 1 inner predicate to construct an AND predicate");
        if (predicates.size() == 1) {
            return predicates.get(0);
        }
        return predicates.stream()
                .reduce((a, b) -> new CompoundPredicate(And.INSTANCE, Arrays.asList(a, b)))
                .get();
    }

    @Nullable
    public static Predicate andNullable(Predicate... predicates) {
        return andNullable(Arrays.asList(predicates));
    }

    @Nullable
    public static Predicate andNullable(List<Predicate> predicates) {
        predicates = predicates.stream().filter(Objects::nonNull).collect(Collectors.toList());
        if (predicates.isEmpty()) {
            return null;
        }

        return and(predicates);
    }

    public static Predicate or(Predicate... predicates) {
        return or(Arrays.asList(predicates));
    }

    public static Predicate or(List<Predicate> predicates) {
        checkArgument(
                !predicates.isEmpty(),
                "There must be at least 1 inner predicate to construct an OR predicate");
        if (predicates.size() == 1) {
            return predicates.get(0);
        }
        return predicates.stream()
                .reduce((a, b) -> new CompoundPredicate(Or.INSTANCE, Arrays.asList(a, b)))
                .get();
    }

    public static List<Predicate> splitAnd(@Nullable Predicate predicate) {
        if (predicate == null) {
            return Collections.emptyList();
        }
        List<Predicate> result = new ArrayList<>();
        splitCompound(And.INSTANCE, predicate, result);
        return result;
    }

    public static List<Predicate> splitOr(@Nullable Predicate predicate) {
        if (predicate == null) {
            return Collections.emptyList();
        }
        List<Predicate> result = new ArrayList<>();
        splitCompound(Or.INSTANCE, predicate, result);
        return result;
    }

    private static void splitCompound(
            CompoundPredicate.Function function, Predicate predicate, List<Predicate> result) {
        if (predicate instanceof CompoundPredicate
                && ((CompoundPredicate) predicate).function().equals(function)) {
            for (Predicate child : ((CompoundPredicate) predicate).children()) {
                splitCompound(function, child, result);
            }
        } else {
            result.add(predicate);
        }
    }

    private void validateIndex(int idx) {
        if (idx < 0 || idx >= rowType.getFieldCount()) {
            throw new UnsupportedExpression("idx is not valid");
        }
    }

    public static Object convertJavaObject(DataType literalType, Object o) {
        if (o == null) {
            return null;
        }
        switch (literalType.getTypeRoot()) {
            case BOOLEAN:
                return o;
            case BIGINT:
                return ((Number) o).longValue();
            case DOUBLE:
                return ((Number) o).doubleValue();
            case TINYINT:
                return ((Number) o).byteValue();
            case SMALLINT:
                return ((Number) o).shortValue();
            case INTEGER:
                return ((Number) o).intValue();
            case FLOAT:
                return ((Number) o).floatValue();
            case STRING:
                return BinaryString.fromString(o.toString());
            case DATE:
                // Hive uses `java.sql.Date.valueOf(lit.toString());` to convert a literal to Date
                // Which uses `java.util.Date()` internally to create the object and that uses the
                // TimeZone.getDefaultRef()
                // To get back the expected date we have to use the LocalDate which gets rid of the
                // TimeZone misery as it uses the year/month/day to generate the object
                LocalDate localDate;
                if (o instanceof java.sql.Timestamp) {
                    localDate = ((java.sql.Timestamp) o).toLocalDateTime().toLocalDate();
                } else if (o instanceof Date) {
                    localDate = ((Date) o).toLocalDate();
                } else if (o instanceof LocalDate) {
                    localDate = (LocalDate) o;
                } else {
                    throw new UnsupportedOperationException(
                            "Unexpected date literal of class " + o.getClass().getName());
                }
                return (int) ChronoUnit.DAYS.between(EPOCH_DAY, localDate);
            case TIME_WITHOUT_TIME_ZONE:
                LocalTime localTime;
                if (o instanceof java.sql.Time) {
                    localTime = ((java.sql.Time) o).toLocalTime();
                } else if (o instanceof LocalTime) {
                    localTime = (LocalTime) o;
                } else {
                    throw new UnsupportedOperationException(
                            "Unexpected time literal of class " + o.getClass().getName());
                }
                // return millis of a day
                return (int) (localTime.toNanoOfDay() / 1_000_000);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) literalType;
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                return Decimal.fromBigDecimal((BigDecimal) o, precision, scale);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (o instanceof java.sql.Timestamp) {
                    LocalDateTime localDateTime =
                            ((Timestamp) o)
                                    .toInstant()
                                    .atZone(ZoneId.systemDefault())
                                    .toLocalDateTime();
                    return TimestampNtz.fromLocalDateTime(localDateTime);
                } else if (o instanceof Instant) {
                    Instant o1 = (Instant) o;
                    LocalDateTime dateTime = o1.atZone(ZoneId.systemDefault()).toLocalDateTime();
                    return TimestampNtz.fromLocalDateTime(dateTime);
                } else if (o instanceof LocalDateTime) {
                    return TimestampNtz.fromLocalDateTime((LocalDateTime) o);
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Unsupported class %s for timestamp without timezone ",
                                    o.getClass()));
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (o instanceof java.sql.Timestamp) {
                    java.sql.Timestamp timestamp = (java.sql.Timestamp) o;
                    return TimestampLtz.fromInstant(timestamp.toInstant());
                } else if (o instanceof Instant) {
                    return TimestampLtz.fromInstant((Instant) o);
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Unsupported class %s for timestamp with local time zone ",
                                    o.getClass()));
                }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported predicate leaf type " + literalType.getTypeRoot().name());
        }
    }

    public static List<Predicate> pickTransformFieldMapping(
            List<Predicate> predicates, List<String> inputFields, List<String> pickedFields) {
        return pickTransformFieldMapping(
                predicates, inputFields.stream().mapToInt(pickedFields::indexOf).toArray());
    }

    public static List<Predicate> pickTransformFieldMapping(
            List<Predicate> predicates, int[] fieldIdxMapping) {
        List<Predicate> pick = new ArrayList<>();
        for (Predicate p : predicates) {
            Optional<Predicate> mapped = transformFieldMapping(p, fieldIdxMapping);
            mapped.ifPresent(pick::add);
        }
        return pick;
    }

    public static Optional<Predicate> transformFieldMapping(
            Predicate predicate, int[] fieldIdxMapping) {
        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
            List<Predicate> children = new ArrayList<>();
            for (Predicate child : compoundPredicate.children()) {
                Optional<Predicate> mapped = transformFieldMapping(child, fieldIdxMapping);
                if (mapped.isPresent()) {
                    children.add(mapped.get());
                } else {
                    return Optional.empty();
                }
            }
            return Optional.of(new CompoundPredicate(compoundPredicate.function(), children));
        } else {
            LeafPredicate leafPredicate = (LeafPredicate) predicate;
            int mapped = fieldIdxMapping[leafPredicate.index()];
            if (mapped >= 0) {
                return Optional.of(
                        new LeafPredicate(
                                leafPredicate.function(),
                                leafPredicate.type(),
                                mapped,
                                leafPredicate.fieldName(),
                                leafPredicate.literals()));
            } else {
                return Optional.empty();
            }
        }
    }

    public static boolean containsFields(Predicate predicate, Set<String> fields) {
        if (predicate instanceof CompoundPredicate) {
            for (Predicate child : ((CompoundPredicate) predicate).children()) {
                if (containsFields(child, fields)) {
                    return true;
                }
            }
            return false;
        } else {
            LeafPredicate leafPredicate = (LeafPredicate) predicate;
            return fields.contains(leafPredicate.fieldName());
        }
    }

    public static List<Predicate> excludePredicateWithFields(
            @Nullable List<Predicate> predicates, Set<String> fields) {
        if (predicates == null || predicates.isEmpty() || fields.isEmpty()) {
            return predicates;
        }
        return predicates.stream()
                .filter(f -> !containsFields(f, fields))
                .collect(Collectors.toList());
    }

    /**
     * Creates a {@link Predicate} that represents a condition where partition fields are equal to
     * the specified partition values.
     *
     * @param partitionSpec A map containing partition field names as keys and their corresponding
     *     values as strings.
     * @param rowType The {@link RowType} describing the schema of the row, including field names
     *     and types.
     * @return A {@link Predicate} representing the equality conditions for the partition fields, or
     *     {@code null} if no conditions are specified.
     */
    @Nullable
    public static Predicate partition(Map<String, String> partitionSpec, RowType rowType) {
        Map<String, Object> internalValues = convertSpecToInternal(partitionSpec, rowType);
        List<String> fieldNames = rowType.getFieldNames();
        Predicate predicate = null;
        PredicateBuilder builder = new PredicateBuilder(rowType);
        for (Map.Entry<String, Object> entry : internalValues.entrySet()) {
            int idx = fieldNames.indexOf(entry.getKey());
            Object literal = internalValues.get(entry.getKey());
            Predicate predicateTemp =
                    literal == null ? builder.isNull(idx) : builder.equal(idx, literal);
            if (predicate == null) {
                predicate = predicateTemp;
            } else {
                predicate = PredicateBuilder.and(predicate, predicateTemp);
            }
        }
        return predicate;
    }

    public static Predicate partitions(List<Map<String, String>> partitions, RowType rowType) {
        return PredicateBuilder.or(
                partitions.stream()
                        .map(p -> PredicateBuilder.partition(p, rowType))
                        .toArray(Predicate[]::new));
    }

    public static Map<String, Object> convertSpecToInternal(
            Map<String, String> spec, RowType partType) {
        Map<String, Object> partValues = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : spec.entrySet()) {
            partValues.put(
                    entry.getKey(),
                    TypeUtils.castFromString(
                            entry.getValue(), partType.getField(entry.getKey()).getType()));
        }
        return partValues;
    }
}
