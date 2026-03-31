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

package org.apache.fluss.row.arrow.vectors;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.types.DataType;

import javax.annotation.Nullable;

/**
 * Describes one column of a flattened projection row produced by the Arrow reader.
 *
 * <ul>
 *   <li>If {@link #getSubField()} is {@code null}, the column refers to a top-level Arrow column at
 *       index {@link #getPhysicalColumnIndex()} in the underlying {@code VectorSchemaRoot}.
 *   <li>If {@link #getSubField()} is non-null and {@link #getCastType()} is null, the column refers
 *       to an untyped Variant sub-field exposed as {@link
 *       org.apache.fluss.types.DataTypes#VARIANT()}.
 *   <li>If both {@link #getSubField()} and {@link #getCastType()} are non-null, the column refers
 *       to a typed shredded sub-field of the Variant column at {@link #getPhysicalColumnIndex()},
 *       exposed as a typed scalar via {@link ShreddedFieldColumnVector}. The {@link #getCastType()
 *       castType} must be a scalar type.
 * </ul>
 */
@Internal
public final class FlatColumnSpec {

    private final int physicalColumnIndex;
    @Nullable private final String subField;
    @Nullable private final DataType castType;

    public FlatColumnSpec(
            int physicalColumnIndex, @Nullable String subField, @Nullable DataType castType) {
        this.physicalColumnIndex = physicalColumnIndex;
        this.subField = subField;
        this.castType = castType;
    }

    /** Convenience factory for a regular top-level physical column. */
    public static FlatColumnSpec ofPhysical(int physicalColumnIndex) {
        return new FlatColumnSpec(physicalColumnIndex, null, null);
    }

    /** Convenience factory for a typed Variant sub-field exposed as a top-level scalar. */
    public static FlatColumnSpec ofShreddedSubField(
            int physicalColumnIndex, String subField, DataType castType) {
        return new FlatColumnSpec(physicalColumnIndex, subField, castType);
    }

    /** Convenience factory for an untyped Variant sub-field exposed as a Variant column. */
    public static FlatColumnSpec ofVariantSubField(int physicalColumnIndex, String subField) {
        return new FlatColumnSpec(physicalColumnIndex, subField, null);
    }

    public int getPhysicalColumnIndex() {
        return physicalColumnIndex;
    }

    @Nullable
    public String getSubField() {
        return subField;
    }

    @Nullable
    public DataType getCastType() {
        return castType;
    }

    /** Whether this spec references a typed shredded sub-field rather than a regular column. */
    public boolean isShreddedSubField() {
        return subField != null && castType != null;
    }

    /** Whether this spec references an untyped Variant sub-field. */
    public boolean isVariantSubField() {
        return subField != null && castType == null;
    }
}
