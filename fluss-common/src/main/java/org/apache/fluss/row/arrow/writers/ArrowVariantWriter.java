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

package org.apache.fluss.row.arrow.writers;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.DataGetters;
import org.apache.fluss.row.Variant;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarBinaryVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.StructVector;

/**
 * {@link ArrowFieldWriter} for Variant type.
 *
 * <p>A VARIANT is stored in Arrow as a STRUCT with two VARBINARY children: "value" (the
 * binary-encoded variant value) and "metadata" (the variant string dictionary / metadata).
 *
 * <p>Internally in Fluss, the VARIANT is represented as a {@link Variant} object containing two
 * separate {@code byte[]} arrays: value and metadata. This writer extracts them and writes to the
 * corresponding Arrow vectors.
 */
@Internal
public class ArrowVariantWriter extends ArrowFieldWriter {

    private final VarBinaryVector valueVector;
    private final VarBinaryVector metadataVector;

    public ArrowVariantWriter(StructVector structVector) {
        super(structVector);
        this.valueVector = (VarBinaryVector) structVector.getChild("value");
        this.metadataVector = (VarBinaryVector) structVector.getChild("metadata");
    }

    @Override
    public void doWrite(int rowIndex, DataGetters row, int ordinal, boolean handleSafe) {
        StructVector structVector = (StructVector) fieldVector;
        Variant variant = row.getVariant(ordinal);

        structVector.setIndexDefined(rowIndex);
        valueVector.setSafe(rowIndex, variant.value());
        metadataVector.setSafe(rowIndex, variant.metadata());
    }
}
