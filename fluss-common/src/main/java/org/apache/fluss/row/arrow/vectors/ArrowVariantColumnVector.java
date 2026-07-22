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
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarBinaryVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.StructVector;
import org.apache.fluss.types.variant.Variant;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Arrow column vector for Variant type. Reads {@link Variant} values from a {@link StructVector}
 * with two children: metadata (VarBinaryVector) and value (VarBinaryVector).
 */
@Internal
public class ArrowVariantColumnVector implements VariantColumnVector {

    /** The StructVector that contains metadata and value sub-vectors. */
    private final StructVector structVector;

    private final VarBinaryVector metadataVector;
    private final VarBinaryVector valueVector;

    public ArrowVariantColumnVector(StructVector structVector) {
        this.structVector = checkNotNull(structVector);
        this.metadataVector = (VarBinaryVector) structVector.getChild("metadata");
        this.valueVector = (VarBinaryVector) structVector.getChild("value");
    }

    @Override
    public Variant getVariant(int i) {
        byte[] metadata = metadataVector.get(i);
        byte[] value = valueVector.get(i);
        return new Variant(metadata, value);
    }

    @Override
    public boolean isNullAt(int i) {
        return structVector.isNull(i);
    }
}
