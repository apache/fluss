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

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** A {@link NullFalseLeafBinaryFunction} to eval is null. */
public class IsNull extends LeafUnaryFunction {

    private static final long serialVersionUID = 1L;

    public static final IsNull INSTANCE = new IsNull();

    private IsNull() {}

    @Override
    public boolean test(DataType type, Object field) {
        return field == null;
    }

    @Override
    public boolean test(DataType type, long rowCount, Object min, Object max, Long nullCount) {
        return nullCount == null || nullCount > 0;
    }

    @Override
    public Optional<LeafFunction> negate() {
        return Optional.of(IsNotNull.INSTANCE);
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        return visitor.visitIsNull(fieldRef);
    }
}
