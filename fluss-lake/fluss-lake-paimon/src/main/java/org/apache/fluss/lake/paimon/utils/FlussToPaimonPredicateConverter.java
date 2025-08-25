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

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.predicate.And;
import org.apache.fluss.predicate.CompoundPredicate;
import org.apache.fluss.predicate.FieldRef;
import org.apache.fluss.predicate.FunctionVisitor;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.Or;
import org.apache.fluss.predicate.PredicateVisitor;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimonLiteral;

/**
 * Converts a Fluss {@link org.apache.fluss.predicate.Predicate} into a Paimon {@link Predicate}.
 *
 * <p>This class implements the {@link PredicateVisitor} pattern to traverse a tree of Fluss
 * predicates. It handles both leaf-level conditions (like equals, greater than) and compound
 * conditions (AND, OR).
 */
public class FlussToPaimonPredicateConverter implements PredicateVisitor<Predicate> {

    private final PredicateBuilder builder;
    private final LeafFunctionConverter converter = new LeafFunctionConverter();
    private final RowType paimonRowType;

    public FlussToPaimonPredicateConverter(RowType rowType) {
        this.builder = new PredicateBuilder(rowType);
        this.paimonRowType = rowType;
    }

    public static Optional<Predicate> convert(
            RowType rowType, org.apache.fluss.predicate.Predicate flussPredicate) {
        try {
            return Optional.of(flussPredicate.visit(new FlussToPaimonPredicateConverter(rowType)));
        } catch (UnsupportedOperationException e) {
            return Optional.empty();
        }
    }

    @Override
    public Predicate visit(LeafPredicate predicate) {
        // Delegate the conversion of the specific function to a dedicated visitor.
        // This avoids a long chain of 'if-instanceof' checks.
        return predicate.visit(converter);
    }

    @Override
    public Predicate visit(CompoundPredicate predicate) {
        List<Predicate> children =
                predicate.children().stream().map(p -> p.visit(this)).collect(Collectors.toList());
        CompoundPredicate.Function function = predicate.function();
        if (function instanceof And) {
            return PredicateBuilder.and(children);
        } else if (function instanceof Or) {
            return PredicateBuilder.or(children);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported fluss compound predicate function: " + predicate.function());
        }
    }

    /**
     * A visitor that implements the logic to convert each type of {@link
     * org.apache.fluss.predicate.LeafFunction} to a Paimon {@link Predicate}.
     */
    private class LeafFunctionConverter implements FunctionVisitor<Predicate> {

        @Override
        public Predicate visitIsNotNull(FieldRef fieldRef) {
            return builder.isNotNull(fieldRef.index());
        }

        @Override
        public Predicate visitIsNull(FieldRef fieldRef) {
            return builder.isNull(fieldRef.index());
        }

        @Override
        public Predicate visitStartsWith(FieldRef fieldRef, Object literal) {
            return builder.startsWith(
                    fieldRef.index(), convertToPaimonLiteral(fieldRef.index(), literal));
        }

        @Override
        public Predicate visitEndsWith(FieldRef fieldRef, Object literal) {
            return builder.endsWith(
                    fieldRef.index(), convertToPaimonLiteral(fieldRef.index(), literal));
        }

        @Override
        public Predicate visitContains(FieldRef fieldRef, Object literal) {
            return builder.contains(
                    fieldRef.index(), convertToPaimonLiteral(fieldRef.index(), literal));
        }

        @Override
        public Predicate visitLessThan(FieldRef fieldRef, Object literal) {
            return builder.lessThan(
                    fieldRef.index(), convertToPaimonLiteral(fieldRef.index(), literal));
        }

        @Override
        public Predicate visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
            return builder.greaterOrEqual(
                    fieldRef.index(), convertToPaimonLiteral(fieldRef.index(), literal));
        }

        @Override
        public Predicate visitNotEqual(FieldRef fieldRef, Object literal) {
            return builder.notEqual(
                    fieldRef.index(), convertToPaimonLiteral(fieldRef.index(), literal));
        }

        @Override
        public Predicate visitLessOrEqual(FieldRef fieldRef, Object literal) {
            return builder.lessOrEqual(
                    fieldRef.index(), convertToPaimonLiteral(fieldRef.index(), literal));
        }

        @Override
        public Predicate visitEqual(FieldRef fieldRef, Object literal) {
            return builder.equal(
                    fieldRef.index(), convertToPaimonLiteral(fieldRef.index(), literal));
        }

        @Override
        public Predicate visitGreaterThan(FieldRef fieldRef, Object literal) {
            return builder.greaterThan(
                    fieldRef.index(), convertToPaimonLiteral(fieldRef.index(), literal));
        }

        @Override
        public Predicate visitIn(FieldRef fieldRef, List<Object> literals) {
            return builder.in(
                    fieldRef.index(),
                    literals.stream()
                            .map(literal -> convertToPaimonLiteral(fieldRef.index(), literal))
                            .collect(Collectors.toList()));
        }

        @Override
        public Predicate visitNotIn(FieldRef fieldRef, List<Object> literals) {
            return builder.notIn(
                    fieldRef.index(),
                    literals.stream()
                            .map(literal -> convertToPaimonLiteral(fieldRef.index(), literal))
                            .collect(Collectors.toList()));
        }

        @Override
        public Predicate visitAnd(List<Predicate> children) {
            // shouldn't come to here
            throw new UnsupportedOperationException("Unsupported visitAnd method.");
        }

        @Override
        public Predicate visitOr(List<Predicate> children) {
            // shouldn't come to here
            throw new UnsupportedOperationException("Unsupported visitOr method.");
        }

        private Object convertToPaimonLiteral(int fieldIndex, Object flussLiteral) {
            return toPaimonLiteral(paimonRowType.getTypeAt(fieldIndex), flussLiteral);
        }
    }
}
