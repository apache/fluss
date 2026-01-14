/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row.serializer;

import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.BinaryArrayWriter;
import org.apache.fluss.row.BinaryRow.BinaryRowFormat;
import org.apache.fluss.row.BinaryWriter;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.array.AlignedArray;
import org.apache.fluss.row.array.CompactedArray;
import org.apache.fluss.row.array.IndexedArray;
import org.apache.fluss.row.array.PrimitiveBinaryArray;
import org.apache.fluss.types.DataType;

import java.io.Serializable;

import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.ALIGNED;
import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.COMPACTED;
import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.INDEXED;

/** Serializer for {@link InternalArray} to {@link BinaryArray} and {@code CompactedArray}. */
public class ArraySerializer implements Serializable {
    private static final long serialVersionUID = 1L;

    private final DataType eleType;
    private final BinaryRowFormat rowFormat;

    private transient BinaryArraySerializer alignedSerializer;

    public ArraySerializer(DataType eleType, BinaryRowFormat rowFormat) {
        this.eleType = eleType;
        this.rowFormat = rowFormat;
    }

    public BinaryArray toBinaryArray(InternalArray from) {
        if (alignedSerializer == null) {
            alignedSerializer = new BinaryArraySerializer();
        }
        return alignedSerializer.toAlignedArray(from);
    }

    private BinaryArray createBinaryArrayInstance() {
        switch (rowFormat) {
            case COMPACTED:
                return new CompactedArray(eleType);
            case INDEXED:
                return new IndexedArray(eleType);
            case ALIGNED:
                return new AlignedArray();
            default:
                throw new IllegalArgumentException("Unsupported row format: " + rowFormat);
        }
    }

    // ------------------------------------------------------------------------------------------

    /** Serializer function for AlignedArray. */
    private class BinaryArraySerializer {

        private transient BinaryArray reuseArray;
        private transient BinaryArrayWriter reuseWriter;
        private transient InternalArray.ElementGetter elementGetter;
        private transient BinaryWriter.ValueWriter valueWriter;

        public BinaryArray toAlignedArray(InternalArray from) {
            if (from instanceof BinaryArray) {
                if (from instanceof PrimitiveBinaryArray
                        || rowFormat == INDEXED && from instanceof IndexedArray
                        || rowFormat == COMPACTED && from instanceof CompactedArray
                        || rowFormat == ALIGNED && from instanceof AlignedArray) {
                    // directly return the original array iff the array is in the expected format
                    return (BinaryArray) from;
                }
            }

            if (from instanceof GenericArray) {
                GenericArray ga = (GenericArray) from;
                if (ga.isPrimitiveArray()) {
                    switch (eleType.getTypeRoot()) {
                        case BOOLEAN:
                            return BinaryArray.fromPrimitiveArray(ga.toBooleanArray());
                        case TINYINT:
                            return BinaryArray.fromPrimitiveArray(ga.toByteArray());
                        case SMALLINT:
                            return BinaryArray.fromPrimitiveArray(ga.toShortArray());
                        case INTEGER:
                            return BinaryArray.fromPrimitiveArray(ga.toIntArray());
                        case BIGINT:
                            return BinaryArray.fromPrimitiveArray(ga.toLongArray());
                        case FLOAT:
                            return BinaryArray.fromPrimitiveArray(ga.toFloatArray());
                        case DOUBLE:
                            return BinaryArray.fromPrimitiveArray(ga.toDoubleArray());
                        default:
                            // fall through
                    }
                }
            }

            int numElements = from.size();
            if (reuseArray == null) {
                reuseArray = createBinaryArrayInstance();
            }
            if (reuseWriter == null || reuseWriter.getNumElements() != numElements) {
                reuseWriter =
                        new BinaryArrayWriter(
                                reuseArray,
                                numElements,
                                BinaryArray.calculateFixLengthPartSize(eleType));
            } else {
                reuseWriter.reset();
            }
            if (elementGetter == null) {
                elementGetter = InternalArray.createElementGetter(eleType);
            }
            if (valueWriter == null) {
                valueWriter = BinaryWriter.createValueWriter(eleType, rowFormat);
            }

            for (int i = 0; i < numElements; i++) {
                if (from.isNullAt(i)) {
                    reuseWriter.setNullAt(i, eleType);
                } else {
                    valueWriter.writeValue(reuseWriter, i, elementGetter.getElementOrNull(from, i));
                }
            }
            reuseWriter.complete();

            return reuseArray;
        }
    }
}
