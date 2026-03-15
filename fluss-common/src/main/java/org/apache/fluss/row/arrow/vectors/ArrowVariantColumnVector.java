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
import org.apache.fluss.row.columnar.BytesColumnVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarBinaryVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.StructVector;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Arrow column vector for Variant type.
 *
 * <p>Reads variant data from an Arrow STRUCT with two VARBINARY children ("value" and "metadata")
 * and combines them into a single {@code byte[]} in the format: [4-byte value length
 * (big-endian)][value bytes][metadata bytes].
 */
@Internal
public class ArrowVariantColumnVector implements BytesColumnVector {

    private final StructVector structVector;
    private final VarBinaryVector valueVector;
    private final VarBinaryVector metadataVector;

    public ArrowVariantColumnVector(StructVector structVector) {
        this.structVector = checkNotNull(structVector);
        this.valueVector = (VarBinaryVector) structVector.getChild("value");
        this.metadataVector = (VarBinaryVector) structVector.getChild("metadata");
    }

    @Override
    public Bytes getBytes(int i) {
        byte[] value = valueVector.get(i);
        byte[] metadata = metadataVector.get(i);

        // Combine into: [4-byte value length (big-endian)][value][metadata]
        int totalLength = 4 + value.length + metadata.length;
        byte[] combined = new byte[totalLength];

        // Write value length as big-endian 4-byte integer
        combined[0] = (byte) ((value.length >> 24) & 0xFF);
        combined[1] = (byte) ((value.length >> 16) & 0xFF);
        combined[2] = (byte) ((value.length >> 8) & 0xFF);
        combined[3] = (byte) (value.length & 0xFF);

        // Copy value bytes
        System.arraycopy(value, 0, combined, 4, value.length);
        // Copy metadata bytes
        System.arraycopy(metadata, 0, combined, 4 + value.length, metadata.length);

        return new BytesColumnVector.Bytes(combined, 0, combined.length);
    }

    @Override
    public boolean isNullAt(int i) {
        return structVector.isNull(i);
    }
}
