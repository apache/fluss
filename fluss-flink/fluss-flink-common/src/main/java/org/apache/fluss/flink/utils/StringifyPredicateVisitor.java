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

import org.apache.fluss.predicate.CompoundPredicate;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateVisitor;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.PartitionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link PredicateVisitor} that converts all literals in {@link LeafPredicate} to string type in
 * the string format of {@link PartitionUtils#convertValueOfType(Object, DataTypeRoot)}. This is
 * necessary because partition metadata is stored as string.
 */
public class StringifyPredicateVisitor implements PredicateVisitor<Predicate> {

    public static Predicate stringifyPartitionPredicate(Predicate predicate) {
        StringifyPredicateVisitor visitor = new StringifyPredicateVisitor();
        return predicate.visit(visitor);
    }

    @Override
    public Predicate visit(LeafPredicate predicate) {
        List<Object> convertedLiterals = new ArrayList<>();
        for (Object literal : predicate.literals()) {
            if (literal != null) {
                String stringValue =
                        PartitionUtils.convertValueOfType(literal, predicate.type().getTypeRoot());
                convertedLiterals.add(BinaryString.fromString(stringValue));
            } else {
                convertedLiterals.add(null);
            }
        }
        return new LeafPredicate(
                predicate.function(),
                DataTypes.STRING(),
                predicate.index(),
                predicate.fieldName(),
                convertedLiterals);
    }

    @Override
    public Predicate visit(CompoundPredicate predicate) {
        List<Predicate> newChildren = new ArrayList<>();
        for (Predicate child : predicate.children()) {
            newChildren.add(child.visit(this));
        }
        return new CompoundPredicate(predicate.function(), newChildren);
    }
}
