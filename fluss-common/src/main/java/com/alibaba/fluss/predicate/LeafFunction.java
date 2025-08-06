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

import com.alibaba.fluss.types.DataType;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Function to test a field with literals. */
public abstract class LeafFunction implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Tests whether a field satisfies the condition based on the provided literals.
     *
     * @param type the data type of the field being tested
     * @param field the value of the field to test
     * @param literals the list of literals to test against the field
     * @return true if the field satisfies the condition, false otherwise
     */
    public abstract boolean test(DataType type, Object field, List<Object> literals);

    /**
     * Tests whether a set of rows satisfies the condition based on the provided statistics.
     *
     * @param type the data type of the field being tested
     * @param rowCount the total number of rows
     * @param min the minimum value of the field in the rows
     * @param max the maximum value of the field in the rows
     * @param nullCount the number of null values in the field, or null if unknown
     * @param literals the literals to test against the field
     * @return true if there is any row satisfies the condition, false otherwise
     */
    public abstract boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals);

    public abstract Optional<LeafFunction> negate();

    public abstract <T> T visit(
            FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals);

    @Override
    public int hashCode() {
        return this.getClass().getName().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o != null && getClass() == o.getClass();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
