/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.trino;

import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.predicate.And;
import org.apache.fluss.predicate.CompoundPredicate;
import org.apache.fluss.predicate.Or;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.trino.FlussPredicateValues.toFlussValue;

/** Converts Trino domains into conservative Arrow batch statistics predicates. */
final class FlussLogPredicateConverter {
    private FlussLogPredicateConverter() {}

    static Optional<Predicate> convert(
            TableInfo table, TupleDomain<FlussColumnHandle> constraint, int threshold) {
        if (constraint.isAll() || constraint.isNone()) {
            return Optional.empty();
        }
        RowType rowType = table.getRowType();
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Map<FlussColumnHandle, Domain> domains = constraint.getDomains().get();
        List<Predicate> conjuncts = new ArrayList<>();
        for (int index : table.getStatsIndexMapping()) {
            Domain domain =
                    domains.get(
                            new FlussColumnHandle(rowType.getFields().get(index).getName(), index));
            if (domain == null || domain.isNullAllowed()) {
                continue;
            }
            DataTypeRoot root = rowType.getTypeAt(index).getTypeRoot();
            // The core BYTES comparator uses signed bytes, unlike Trino VARBINARY.
            if (root != DataTypeRoot.BYTES) {
                convertColumn(builder, index, root, domain.simplify(threshold))
                        .ifPresent(conjuncts::add);
            }
        }
        return conjuncts.isEmpty()
                ? Optional.empty()
                : Optional.of(combine(And.INSTANCE, conjuncts));
    }

    private static Optional<Predicate> convertColumn(
            PredicateBuilder builder, int index, DataTypeRoot root, Domain domain) {
        ValueSet values = domain.getValues();
        if (values.isAll() || values.isNone()) {
            return Optional.empty();
        }
        if (values.isDiscreteSet()) {
            List<Object> literals = new ArrayList<>();
            for (Object value : values.getDiscreteSet()) {
                Optional<Object> literal = toFlussValue(root, domain.getType(), value);
                if (!literal.isPresent()) {
                    return Optional.empty();
                }
                literals.add(literal.get());
            }
            return Optional.of(builder.in(index, literals));
        }
        if (!domain.getType().isOrderable()) {
            return Optional.empty();
        }

        List<Predicate> disjuncts = new ArrayList<>();
        for (Range range : values.getRanges().getOrderedRanges()) {
            List<Predicate> bounds = new ArrayList<>(2);
            if (!range.isLowUnbounded()) {
                Optional<Object> low =
                        toFlussValue(root, domain.getType(), range.getLowBoundedValue());
                if (!low.isPresent()) {
                    return Optional.empty();
                }
                bounds.add(
                        range.isLowInclusive()
                                ? builder.greaterOrEqual(index, low.get())
                                : builder.greaterThan(index, low.get()));
            }
            if (!range.isHighUnbounded()) {
                Optional<Object> high =
                        toFlussValue(root, domain.getType(), range.getHighBoundedValue());
                if (!high.isPresent()) {
                    return Optional.empty();
                }
                bounds.add(
                        range.isHighInclusive()
                                ? builder.lessOrEqual(index, high.get())
                                : builder.lessThan(index, high.get()));
            }
            disjuncts.add(PredicateBuilder.and(bounds));
        }
        return Optional.of(combine(Or.INSTANCE, disjuncts));
    }

    private static Predicate combine(
            CompoundPredicate.Function function, List<Predicate> children) {
        // Keep RPC serialization depth independent of the configured domain threshold.
        return children.size() == 1 ? children.get(0) : new CompoundPredicate(function, children);
    }
}
