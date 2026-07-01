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

package org.apache.fluss.types.variant;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.types.DataType;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Represents a single field that has been extracted (shredded) from a Variant column into a
 * typed_value sub-vector within the Variant's StructVector for efficient columnar access.
 *
 * <p>For example, if a Variant column "data" frequently contains an integer field "age", it will be
 * shredded into a typed_value child: {@code StructVector{value: VarBinary, typed_value: BigInt}}.
 *
 * @since 0.7
 */
@PublicEvolving
public final class ShreddedField implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The field path within the Variant object. The first implementation only writes top-level
     * fields such as "age"; nested paths such as "address.city" are reserved for a follow-up
     * design.
     */
    private final String fieldPath;

    /** The data type of the shredded column. */
    private final DataType shreddedType;

    public ShreddedField(String fieldPath, DataType shreddedType) {
        this.fieldPath = checkNotNull(fieldPath, "fieldPath must not be null");
        this.shreddedType = checkNotNull(shreddedType, "shreddedType must not be null");
    }

    /**
     * Returns whether the field path can be handled by the current top-level-only shredding path.
     */
    public static boolean isTopLevelFieldPath(String fieldPath) {
        return fieldPath != null
                && !fieldPath.isEmpty()
                && fieldPath.indexOf('.') < 0
                && fieldPath.indexOf('[') < 0
                && fieldPath.indexOf(']') < 0;
    }

    /** Returns the field path within the Variant object. */
    public String getFieldPath() {
        return fieldPath;
    }

    /** Returns the data type of the shredded column. */
    public DataType getShreddedType() {
        return shreddedType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ShreddedField)) {
            return false;
        }
        ShreddedField that = (ShreddedField) o;
        return Objects.equals(fieldPath, that.fieldPath)
                && Objects.equals(shreddedType, that.shreddedType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldPath, shreddedType);
    }

    @Override
    public String toString() {
        return String.format(
                "ShreddedField{fieldPath='%s', type=%s}",
                fieldPath, shreddedType.asSummaryString());
    }
}
