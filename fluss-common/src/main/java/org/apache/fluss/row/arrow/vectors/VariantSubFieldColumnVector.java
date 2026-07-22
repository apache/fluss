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
import org.apache.fluss.row.columnar.VariantColumnVector;
import org.apache.fluss.types.variant.Variant;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A Variant column vector that exposes a top-level field of a parent Variant column. */
@Internal
public final class VariantSubFieldColumnVector implements VariantColumnVector {

    private final VariantColumnVector parent;
    private final String fieldName;

    private VariantSubFieldColumnVector(VariantColumnVector parent, String fieldName) {
        this.parent = checkNotNull(parent);
        this.fieldName = checkNotNull(fieldName);
    }

    /** Creates a vector for an untyped top-level Variant sub-field. */
    public static VariantColumnVector create(
            @Nullable VariantColumnVector parent, String fieldName) {
        if (parent == null) {
            return new AlwaysNullVariantColumnVector();
        }
        return new VariantSubFieldColumnVector(parent, fieldName);
    }

    @Override
    public Variant getVariant(int i) {
        Variant variant = parent.getVariant(i);
        if (variant == null || variant.isNull() || !variant.isObject()) {
            return null;
        }
        // TODO: support nested field paths once the writer, wire protocol, and pruning path are
        // path-aware. V1 intentionally extracts direct children of the Variant root only.
        return variant.getFieldByName(fieldName);
    }

    @Override
    public boolean isNullAt(int i) {
        Variant field = getVariant(i);
        return field == null || field.isNull();
    }

    private static final class AlwaysNullVariantColumnVector implements VariantColumnVector {

        @Override
        public Variant getVariant(int i) {
            return null;
        }

        @Override
        public boolean isNullAt(int i) {
            return true;
        }
    }
}
