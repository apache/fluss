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

package org.apache.fluss.types;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Collections;
import java.util.List;

/**
 * Data type of a semi-structured value represented by {@link
 * org.apache.fluss.types.variant.Variant}, including primitives, strings, arrays, and objects.
 *
 * @since 1.1
 */
@PublicEvolving
public class VariantType extends DataType {
    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "VARIANT";

    /** Creates a Variant type with the specified SQL nullability. */
    public VariantType(boolean isNullable) {
        super(isNullable, DataTypeRoot.VARIANT);
    }

    /** Creates a nullable Variant type. */
    public VariantType() {
        this(true);
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new VariantType(isNullable);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT);
    }

    @Override
    public List<DataType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
