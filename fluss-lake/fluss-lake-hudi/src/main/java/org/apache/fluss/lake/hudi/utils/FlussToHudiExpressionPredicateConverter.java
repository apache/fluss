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

package org.apache.fluss.lake.hudi.utils;

import org.apache.fluss.predicate.And;
import org.apache.fluss.predicate.CompoundPredicate;
import org.apache.fluss.predicate.FieldRef;
import org.apache.fluss.predicate.FunctionVisitor;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.Or;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateVisitor;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.hudi.org.apache.avro.Schema;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.util.AvroSchemaConverter;

import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Converts Fluss predicates to Hudi expression predicates for Hudi reader pushdown. */
public class FlussToHudiExpressionPredicateConverter
        implements PredicateVisitor<ExpressionPredicates.Predicate> {

    private static final String HUDI_METADATA_COLUMN_PREFIX = "_hoodie_";

    private final Schema hudiSchema;
    private final int metadataFieldCount;
    private final LeafFunctionConverter converter = new LeafFunctionConverter();

    private FlussToHudiExpressionPredicateConverter(Schema hudiSchema) {
        this.hudiSchema = hudiSchema;
        this.metadataFieldCount = metadataFieldCount(hudiSchema);
    }

    public static Optional<ExpressionPredicates.Predicate> convert(
            Schema hudiSchema, Predicate flussPredicate) {
        try {
            return Optional.of(
                    flussPredicate.visit(new FlussToHudiExpressionPredicateConverter(hudiSchema)));
        } catch (UnsupportedOperationException e) {
            return Optional.empty();
        }
    }

    @Override
    public ExpressionPredicates.Predicate visit(LeafPredicate predicate) {
        return predicate.visit(converter);
    }

    @Override
    public ExpressionPredicates.Predicate visit(CompoundPredicate predicate) {
        List<ExpressionPredicates.Predicate> children =
                predicate.children().stream().map(p -> p.visit(this)).collect(Collectors.toList());

        CompoundPredicate.Function function = predicate.function();
        if (function instanceof And) {
            return reducePredicates(children, true);
        } else if (function instanceof Or) {
            return reducePredicates(children, false);
        }

        throw new UnsupportedOperationException(
                "Unsupported Fluss compound predicate function: " + function);
    }

    private ExpressionPredicates.Predicate reducePredicates(
            List<ExpressionPredicates.Predicate> children, boolean and) {
        if (children.isEmpty()) {
            throw new UnsupportedOperationException("Empty compound predicate.");
        }

        ExpressionPredicates.Predicate result = children.get(0);
        for (int i = 1; i < children.size(); i++) {
            result =
                    and
                            ? ExpressionPredicates.And.getInstance()
                                    .bindPredicates(result, children.get(i))
                            : ExpressionPredicates.Or.getInstance()
                                    .bindPredicates(result, children.get(i));
        }
        return result;
    }

    private class LeafFunctionConverter implements FunctionVisitor<ExpressionPredicates.Predicate> {

        @Override
        public ExpressionPredicates.Predicate visitIsNotNull(FieldRef fieldRef) {
            throw new UnsupportedOperationException("Hudi does not support isNotNull pushdown.");
        }

        @Override
        public ExpressionPredicates.Predicate visitIsNull(FieldRef fieldRef) {
            throw new UnsupportedOperationException("Hudi does not support isNull pushdown.");
        }

        @Override
        public ExpressionPredicates.Predicate visitStartsWith(FieldRef fieldRef, Object literal) {
            throw new UnsupportedOperationException("Hudi does not support startsWith pushdown.");
        }

        @Override
        public ExpressionPredicates.Predicate visitEndsWith(FieldRef fieldRef, Object literal) {
            throw new UnsupportedOperationException("Hudi does not support endsWith pushdown.");
        }

        @Override
        public ExpressionPredicates.Predicate visitContains(FieldRef fieldRef, Object literal) {
            throw new UnsupportedOperationException("Hudi does not support contains pushdown.");
        }

        @Override
        public ExpressionPredicates.Predicate visitLessThan(FieldRef fieldRef, Object literal) {
            return bind(ExpressionPredicates.LessThan.getInstance(), fieldRef, literal);
        }

        @Override
        public ExpressionPredicates.Predicate visitGreaterOrEqual(
                FieldRef fieldRef, Object literal) {
            return bind(ExpressionPredicates.GreaterThanOrEqual.getInstance(), fieldRef, literal);
        }

        @Override
        public ExpressionPredicates.Predicate visitNotEqual(FieldRef fieldRef, Object literal) {
            return bind(ExpressionPredicates.NotEquals.getInstance(), fieldRef, literal);
        }

        @Override
        public ExpressionPredicates.Predicate visitLessOrEqual(FieldRef fieldRef, Object literal) {
            return bind(ExpressionPredicates.LessThanOrEqual.getInstance(), fieldRef, literal);
        }

        @Override
        public ExpressionPredicates.Predicate visitEqual(FieldRef fieldRef, Object literal) {
            return bind(ExpressionPredicates.Equals.getInstance(), fieldRef, literal);
        }

        @Override
        public ExpressionPredicates.Predicate visitGreaterThan(FieldRef fieldRef, Object literal) {
            return bind(ExpressionPredicates.GreaterThan.getInstance(), fieldRef, literal);
        }

        @Override
        public ExpressionPredicates.Predicate visitIn(FieldRef fieldRef, List<Object> literals) {
            Schema.Field field = getFieldByFlussIndex(fieldRef.index());
            DataType dataType = supportedHudiDataType(field);
            List<ValueLiteralExpression> literalExpressions =
                    literals.stream()
                            .map(literal -> createValueLiteral(literal, dataType))
                            .collect(Collectors.toList());
            return ExpressionPredicates.In.getInstance()
                    .bindValueLiterals(literalExpressions)
                    .bindFieldReference(createFieldReference(field, dataType));
        }

        @Override
        public ExpressionPredicates.Predicate visitNotIn(FieldRef fieldRef, List<Object> literals) {
            ExpressionPredicates.Predicate inPredicate = visitIn(fieldRef, literals);
            return ExpressionPredicates.Not.getInstance().bindPredicate(inPredicate);
        }

        @Override
        public ExpressionPredicates.Predicate visitAnd(
                List<ExpressionPredicates.Predicate> children) {
            throw new UnsupportedOperationException("Unsupported visitAnd method.");
        }

        @Override
        public ExpressionPredicates.Predicate visitOr(
                List<ExpressionPredicates.Predicate> children) {
            throw new UnsupportedOperationException("Unsupported visitOr method.");
        }

        private ExpressionPredicates.Predicate bind(
                ExpressionPredicates.ColumnPredicate predicate, FieldRef fieldRef, Object literal) {
            Schema.Field field = getFieldByFlussIndex(fieldRef.index());
            DataType dataType = supportedHudiDataType(field);
            return predicate
                    .bindValueLiteral(createValueLiteral(literal, dataType))
                    .bindFieldReference(createFieldReference(field, dataType));
        }

        private FieldReferenceExpression createFieldReference(
                Schema.Field field, DataType dataType) {
            return new FieldReferenceExpression(field.name(), dataType, 0, field.pos());
        }

        private ValueLiteralExpression createValueLiteral(Object literal, DataType dataType) {
            if (literal == null) {
                throw new UnsupportedOperationException(
                        "Hudi does not support null literal pushdown.");
            }
            return new ValueLiteralExpression(convertValue(literal), dataType.notNull());
        }

        private Schema.Field getFieldByFlussIndex(int index) {
            int hudiFieldIndex = metadataFieldCount + index;
            if (index < 0 || hudiFieldIndex >= hudiSchema.getFields().size()) {
                throw new IllegalArgumentException("Field index out of bounds: " + index);
            }
            return hudiSchema.getFields().get(hudiFieldIndex);
        }

        private DataType supportedHudiDataType(Schema.Field field) {
            DataType dataType = AvroSchemaConverter.convertToDataType(field.schema());
            LogicalTypeRoot typeRoot = dataType.getLogicalType().getTypeRoot();
            switch (typeRoot) {
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                case BIGINT:
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case FLOAT:
                case DOUBLE:
                case BINARY:
                case VARBINARY:
                case CHAR:
                case VARCHAR:
                    return dataType;
                default:
                    throw new UnsupportedOperationException(
                            "Hudi does not support predicate pushdown for field "
                                    + field.name()
                                    + " with type "
                                    + typeRoot
                                    + ".");
            }
        }
    }

    private static int metadataFieldCount(Schema schema) {
        int metadataFieldCount = 0;
        for (Schema.Field field : schema.getFields()) {
            if (!field.name().startsWith(HUDI_METADATA_COLUMN_PREFIX)) {
                break;
            }
            metadataFieldCount++;
        }
        return metadataFieldCount;
    }

    private static Object convertValue(Object value) {
        if (value instanceof BinaryString) {
            return value.toString();
        }
        if (value instanceof TimestampNtz) {
            return ((TimestampNtz) value).toLocalDateTime();
        }
        if (value instanceof TimestampLtz) {
            return ((TimestampLtz) value).toInstant().atZone(ZoneOffset.UTC).toLocalDateTime();
        }
        return value;
    }
}
