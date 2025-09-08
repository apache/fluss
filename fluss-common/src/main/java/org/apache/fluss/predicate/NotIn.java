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

package org.apache.fluss.predicate;

import org.apache.fluss.types.DataType;

import java.util.List;
import java.util.Optional;

import static org.apache.fluss.predicate.CompareUtils.compareLiteral;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** A {@link LeafFunction} to eval not in. */
public class NotIn extends LeafFunction {

    private static final long serialVersionUID = 1L;
    public static final NotIn INSTANCE = new NotIn();

    private NotIn() {}

    @Override
    public boolean test(DataType type, Object field, List<Object> literals) {
        if (field == null) {
            return false;
        }
        for (Object literal : literals) {
            if (literal == null || compareLiteral(type, literal, field) == 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals) {
        if (nullCount != null && rowCount == nullCount) {
            return false;
        }
        for (Object literal : literals) {
            if (literal == null
                    // only if max == min == literal, the row set are all IN the literal, return
                    // false; other cases, the row set MAY contain elements NOT IN the literal,
                    // return true
                    || (compareLiteral(type, literal, min) == 0
                            && compareLiteral(type, literal, max) == 0)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Optional<LeafFunction> negate() {
        return Optional.of(In.INSTANCE);
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        return visitor.visitNotIn(fieldRef, literals);
    }
}
