/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.utils;

import org.apache.fluss.predicate.And;
import org.apache.fluss.predicate.CompoundPredicate;
import org.apache.fluss.predicate.FieldRef;
import org.apache.fluss.predicate.FunctionVisitor;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.Or;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateVisitor;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIcebergLiteral;

/**
 * Converts a Fluss {@link org.apache.fluss.predicate.Predicate} into an Iceberg {@link Expression}.
 *
 * <p>This class implements the {@link PredicateVisitor} pattern to traverse a tree of Fluss
 * predicates. It handles both leaf-level conditions (like equals, greater than) and compound
 * conditions (AND, OR).
 */
public class FlussToIcebergPredicateConverter implements PredicateVisitor<Expression> {

    private final Schema icebergSchema;
    private final LeafFunctionConverter converter = new LeafFunctionConverter();

    public FlussToIcebergPredicateConverter(Schema schema) {
        this.icebergSchema = schema;
    }

    public static Optional<Expression> convert(Schema schema, Predicate flussPredicate) {
        try {
            return Optional.of(flussPredicate.visit(new FlussToIcebergPredicateConverter(schema)));
        } catch (UnsupportedOperationException e) {
            return Optional.empty();
        }
    }

    @Override
    public Expression visit(LeafPredicate predicate) {
        // Delegate the conversion of the specific function to a dedicated visitor.
        // This avoids a long chain of 'if-instanceof' checks.
        return predicate.visit(converter);
    }

    @Override
    public Expression visit(CompoundPredicate predicate) {
        List<Expression> children =
                predicate.children().stream().map(p -> p.visit(this)).collect(Collectors.toList());

        CompoundPredicate.Function function = predicate.function();
        if (function instanceof And) {
            return children.stream().reduce(Expressions::and).orElse(Expressions.alwaysTrue());
        } else if (function instanceof Or) {
            return children.stream().reduce(Expressions::or).orElse(Expressions.alwaysTrue());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported fluss compound predicate function: " + predicate.function());
        }
    }

    /**
     * A visitor that implements the logic to convert each type of {@link
     * org.apache.fluss.predicate.LeafFunction} to an Iceberg {@link Expression}.
     */
    private class LeafFunctionConverter implements FunctionVisitor<Expression> {

        @Override
        public Expression visitIsNotNull(FieldRef fieldRef) {
            String fieldName = getField(fieldRef.index()).name();
            return Expressions.notNull(fieldName);
        }

        @Override
        public Expression visitIsNull(FieldRef fieldRef) {
            String fieldName = getField(fieldRef.index()).name();
            return Expressions.isNull(fieldName);
        }

        @Override
        public Expression visitStartsWith(FieldRef fieldRef, Object literal) {
            String fieldName = getField(fieldRef.index()).name();
            return Expressions.startsWith(
                    fieldName, convertToIcebergLiteral(fieldRef.index(), literal).toString());
        }

        @Override
        public Expression visitEndsWith(FieldRef fieldRef, Object literal) {
            // iceberg not support endswith filter
            throw new UnsupportedOperationException("Iceberg not supported endswith filter.");
        }

        @Override
        public Expression visitContains(FieldRef fieldRef, Object literal) {
            // iceberg not support contains filter
            throw new UnsupportedOperationException("Iceberg not supported contains filter.");
        }

        @Override
        public Expression visitLessThan(FieldRef fieldRef, Object literal) {
            String fieldName = getField(fieldRef.index()).name();
            return Expressions.lessThan(
                    fieldName, convertToIcebergLiteral(fieldRef.index(), literal));
        }

        @Override
        public Expression visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
            String fieldName = getField(fieldRef.index()).name();
            return Expressions.greaterThanOrEqual(
                    fieldName, convertToIcebergLiteral(fieldRef.index(), literal));
        }

        @Override
        public Expression visitNotEqual(FieldRef fieldRef, Object literal) {
            String fieldName = getField(fieldRef.index()).name();
            return Expressions.notEqual(
                    fieldName, convertToIcebergLiteral(fieldRef.index(), literal));
        }

        @Override
        public Expression visitLessOrEqual(FieldRef fieldRef, Object literal) {
            String fieldName = getField(fieldRef.index()).name();
            return Expressions.lessThanOrEqual(
                    fieldName, convertToIcebergLiteral(fieldRef.index(), literal));
        }

        @Override
        public Expression visitEqual(FieldRef fieldRef, Object literal) {
            String fieldName = getField(fieldRef.index()).name();
            return Expressions.equal(fieldName, convertToIcebergLiteral(fieldRef.index(), literal));
        }

        @Override
        public Expression visitGreaterThan(FieldRef fieldRef, Object literal) {
            String fieldName = getField(fieldRef.index()).name();
            Object icebergLiteral = convertToIcebergLiteral(fieldRef.index(), literal);
            return Expressions.greaterThan(fieldName, icebergLiteral);
        }

        @Override
        public Expression visitIn(FieldRef fieldRef, List<Object> literals) {
            String fieldName = getField(fieldRef.index()).name();
            List<Object> icebergLiterals =
                    literals.stream()
                            .map(literal -> convertToIcebergLiteral(fieldRef.index(), literal))
                            .collect(Collectors.toList());
            return Expressions.in(fieldName, icebergLiterals);
        }

        @Override
        public Expression visitNotIn(FieldRef fieldRef, List<Object> literals) {
            String fieldName = getField(fieldRef.index()).name();
            List<Object> icebergLiterals =
                    literals.stream()
                            .map(literal -> convertToIcebergLiteral(fieldRef.index(), literal))
                            .collect(Collectors.toList());
            return Expressions.notIn(fieldName, icebergLiterals);
        }

        @Override
        public Expression visitAnd(List<Expression> children) {
            // shouldn't come to here
            throw new UnsupportedOperationException("Unsupported visitAnd method.");
        }

        @Override
        public Expression visitOr(List<Expression> children) {
            // shouldn't come to here
            throw new UnsupportedOperationException("Unsupported visitOr method.");
        }

        private Types.NestedField getField(int fieldIndex) {
            return icebergSchema.columns().get(fieldIndex);
        }

        private Object convertToIcebergLiteral(int fieldIndex, Object flussLiteral) {
            return toIcebergLiteral(getField(fieldIndex), flussLiteral);
        }
    }
}
