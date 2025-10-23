/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.utils;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.predicate.UnsupportedExpression;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.TypeUtils;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsImplicitCast;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Convert Flink {@link Expression} to Fluss {@link Predicate}.
 *
 * <p>For {@link FieldReferenceExpression}, please use name instead of index, if the project
 * pushdown is before and the filter pushdown is after, the index of the filter will be projected.
 */
public class PredicateConverter implements ExpressionVisitor<Predicate> {

    private final PredicateBuilder builder;

    public PredicateConverter(PredicateBuilder builder) {
        this.builder = builder;
    }

    @VisibleForTesting
    PredicateConverter(RowType type) {
        this(new PredicateBuilder(FlinkConversions.toFlussRowType(type)));
    }

    /** Accepts simple LIKE patterns like "abc%". */
    private static final Pattern BEGIN_PATTERN = Pattern.compile("^[^%_]+%$");

    private static final Pattern END_PATTERN = Pattern.compile("^%[^%_]+$");
    private static final Pattern CONTAINS_PATTERN = Pattern.compile("^%[^%_]+%$");

    @Override
    public Predicate visit(CallExpression call) {
        FunctionDefinition func = call.getFunctionDefinition();
        List<Expression> children = call.getChildren();

        if (func == BuiltInFunctionDefinitions.AND) {
            return PredicateBuilder.and(children.get(0).accept(this), children.get(1).accept(this));
        } else if (func == BuiltInFunctionDefinitions.OR) {
            return PredicateBuilder.or(children.get(0).accept(this), children.get(1).accept(this));
        } else if (func == BuiltInFunctionDefinitions.EQUALS) {
            return visitBiFunction(children, builder::equal, builder::equal);
        } else if (func == BuiltInFunctionDefinitions.NOT_EQUALS) {
            return visitBiFunction(children, builder::notEqual, builder::notEqual);
        } else if (func == BuiltInFunctionDefinitions.GREATER_THAN) {
            return visitBiFunction(children, builder::greaterThan, builder::lessThan);
        } else if (func == BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL) {
            return visitBiFunction(children, builder::greaterOrEqual, builder::lessOrEqual);
        } else if (func == BuiltInFunctionDefinitions.LESS_THAN) {
            return visitBiFunction(children, builder::lessThan, builder::greaterThan);
        } else if (func == BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL) {
            return visitBiFunction(children, builder::lessOrEqual, builder::greaterOrEqual);
        } else if (func == BuiltInFunctionDefinitions.IN) {
            FieldReferenceExpression fieldRefExpr =
                    extractFieldReference(children.get(0)).orElseThrow(UnsupportedExpression::new);
            List<Object> literals = new ArrayList<>();
            for (int i = 1; i < children.size(); i++) {
                literals.add(extractLiteral(fieldRefExpr.getOutputDataType(), children.get(i)));
            }
            return builder.in(builder.indexOf(fieldRefExpr.getName()), literals);
        } else if (func == BuiltInFunctionDefinitions.IS_NULL) {
            return extractFieldReference(children.get(0))
                    .map(FieldReferenceExpression::getName)
                    .map(builder::indexOf)
                    .map(builder::isNull)
                    .orElseThrow(UnsupportedExpression::new);
        } else if (func == BuiltInFunctionDefinitions.IS_NOT_NULL) {
            return extractFieldReference(children.get(0))
                    .map(FieldReferenceExpression::getName)
                    .map(builder::indexOf)
                    .map(builder::isNotNull)
                    .orElseThrow(UnsupportedExpression::new);
        } else if (func == BuiltInFunctionDefinitions.NOT) {
            return extractFieldReference(children.get(0))
                    .map(FieldReferenceExpression::getName)
                    .map(builder::indexOf)
                    .map(idx -> builder.equal(idx, Boolean.FALSE))
                    .orElseThrow(UnsupportedExpression::new);
        } else if (func == BuiltInFunctionDefinitions.BETWEEN) {
            FieldReferenceExpression fieldRefExpr =
                    extractFieldReference(children.get(0)).orElseThrow(UnsupportedExpression::new);
            return builder.between(
                    builder.indexOf(fieldRefExpr.getName()), children.get(1), children.get(2));
        } else if (func == BuiltInFunctionDefinitions.LIKE) {
            FieldReferenceExpression fieldRefExpr =
                    extractFieldReference(children.get(0)).orElseThrow(UnsupportedExpression::new);
            if (fieldRefExpr
                            .getOutputDataType()
                            .getLogicalType()
                            .getTypeRoot()
                            .getFamilies()
                            .contains(LogicalTypeFamily.CHARACTER_STRING)
                    && builder.indexOf(fieldRefExpr.getName()) != -1) {
                String sqlPattern =
                        Objects.requireNonNull(
                                        extractLiteral(
                                                fieldRefExpr.getOutputDataType(), children.get(1)))
                                .toString();
                String escape =
                        children.size() <= 2
                                ? null
                                : Objects.requireNonNull(
                                                extractLiteral(
                                                        fieldRefExpr.getOutputDataType(),
                                                        children.get(2)))
                                        .toString();

                if (escape == null) {
                    if (BEGIN_PATTERN.matcher(sqlPattern).matches()) {
                        String prefix = sqlPattern.substring(0, sqlPattern.length() - 1);
                        return builder.startsWith(
                                builder.indexOf(fieldRefExpr.getName()),
                                BinaryString.fromString(prefix));
                    }
                    if (END_PATTERN.matcher(sqlPattern).matches()) {
                        String suffix = sqlPattern.substring(1);
                        return builder.endsWith(
                                builder.indexOf(fieldRefExpr.getName()),
                                BinaryString.fromString(suffix));
                    }
                    if (CONTAINS_PATTERN.matcher(sqlPattern).matches()
                            && sqlPattern.indexOf('%', 1) == sqlPattern.length() - 1) {
                        String mid = sqlPattern.substring(1, sqlPattern.length() - 1);
                        return builder.contains(
                                builder.indexOf(fieldRefExpr.getName()),
                                BinaryString.fromString(mid));
                    }
                }
            }
        }

        // TODO is_true, is_false, between_xxx, similar, not_in?

        throw new UnsupportedExpression();
    }

    private Predicate visitBiFunction(
            List<Expression> children,
            BiFunction<Integer, Object, Predicate> visit1,
            BiFunction<Integer, Object, Predicate> visit2) {
        Optional<FieldReferenceExpression> fieldRefExpr = extractFieldReference(children.get(0));
        if (fieldRefExpr.isPresent() && builder.indexOf(fieldRefExpr.get().getName()) != -1) {
            Object literal =
                    extractLiteral(fieldRefExpr.get().getOutputDataType(), children.get(1));
            return visit1.apply(builder.indexOf(fieldRefExpr.get().getName()), literal);
        } else {
            fieldRefExpr = extractFieldReference(children.get(1));
            if (fieldRefExpr.isPresent()) {
                Object literal =
                        extractLiteral(fieldRefExpr.get().getOutputDataType(), children.get(0));
                return visit2.apply(builder.indexOf(fieldRefExpr.get().getName()), literal);
            }
        }

        throw new UnsupportedExpression();
    }

    private Optional<FieldReferenceExpression> extractFieldReference(Expression expression) {
        if (expression instanceof FieldReferenceExpression) {
            return Optional.of((FieldReferenceExpression) expression);
        }
        return Optional.empty();
    }

    private Object extractLiteral(DataType expectedType, Expression expression) {
        LogicalType expectedLogicalType = expectedType.getLogicalType();
        if (!supportsPredicate(expectedLogicalType)) {
            throw new UnsupportedExpression();
        }

        if (expression instanceof ValueLiteralExpression) {
            ValueLiteralExpression valueExpression = (ValueLiteralExpression) expression;
            if (valueExpression.isNull()) {
                return null;
            }

            DataType actualType = valueExpression.getOutputDataType();
            LogicalType actualLogicalType = actualType.getLogicalType();
            Optional<?> valueOpt = valueExpression.getValueAs(actualType.getConversionClass());
            if (valueOpt.isPresent()) {
                Object value = valueOpt.get();
                if (actualLogicalType.getTypeRoot().equals(expectedLogicalType.getTypeRoot())) {
                    return fromFlinkObject(
                            DataStructureConverters.getConverter(expectedType)
                                    .toInternalOrNull(value),
                            expectedType);
                } else if (supportsImplicitCast(actualLogicalType, expectedLogicalType)) {
                    try {
                        return TypeUtils.castFromString(
                                value.toString(), FlinkConversions.toFlussType(expectedType));
                    } catch (Exception ignored) {
                    }
                }
            }
        }

        throw new UnsupportedExpression();
    }

    private static Object fromFlinkObject(Object o, DataType type) {
        if (o == null) {
            return null;
        }
        return InternalRow.createFieldGetter(FlinkConversions.toFlussType(type), 0)
                .getFieldOrNull((new FlinkAsFlussRow()).replace(GenericRowData.of(o)));
    }

    private boolean supportsPredicate(LogicalType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return true;
            default:
                return false;
        }
    }

    @Override
    public Predicate visit(ValueLiteralExpression valueLiteralExpression) {
        throw new UnsupportedExpression();
    }

    @Override
    public Predicate visit(FieldReferenceExpression fieldReferenceExpression) {
        throw new UnsupportedExpression();
    }

    @Override
    public Predicate visit(TypeLiteralExpression typeLiteralExpression) {
        throw new UnsupportedExpression();
    }

    @Override
    public Predicate visit(Expression expression) {
        throw new UnsupportedExpression();
    }

    /**
     * Try best to convert a {@link ResolvedExpression} to {@link Predicate}.
     *
     * @param filter a resolved expression
     * @return {@link Predicate} if no {@link UnsupportedExpression} thrown.
     */
    public static Optional<Predicate> convertToFlussPredicate(
            org.apache.fluss.types.RowType rowType, ResolvedExpression filter) {
        try {
            return Optional.ofNullable(
                    filter.accept(new PredicateConverter(new PredicateBuilder(rowType))));
        } catch (UnsupportedExpression e) {
            return Optional.empty();
        }
    }

    public static Optional<Predicate> convertToFlussPredicate(
            RowType rowType, ResolvedExpression filter) {
        return convertToFlussPredicate(FlinkConversions.toFlussRowType(rowType), filter);
    }
}
