/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row.serializer;

import org.apache.fluss.memory.InputView;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.OutputView;
import org.apache.fluss.row.ArrayWriter;
import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.BinaryArrayWriter;
import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

/** Serializer for {@link InternalArray}. */
public class InternalArraySerializer implements Serializer<InternalArray> {
    private static final long serialVersionUID = 1L;

    private final DataType eleType;
    private final Serializer<Object> eleSer;

    private transient BinaryArray reuseArray;
    private transient BinaryArrayWriter reuseWriter;

    public InternalArraySerializer(DataType eleType) {
        this(eleType, InternalSerializers.create(eleType));
    }

    private InternalArraySerializer(DataType eleType, Serializer<Object> eleSer) {
        this.eleType = eleType;
        this.eleSer = eleSer;
    }

    @Override
    public InternalArraySerializer duplicate() {
        return new InternalArraySerializer(eleType, eleSer.duplicate());
    }

    @Override
    public InternalArray copy(InternalArray from) {
        if (from instanceof GenericArray) {
            return copyGenericArray((GenericArray) from);
        } else if (from instanceof BinaryArray) {
            return ((BinaryArray) from).copy();
        } else {
            return toBinaryArray(from).copy();
        }
    }

    private GenericArray copyGenericArray(GenericArray array) {
        if (array.isPrimitiveArray()) {
            switch (eleType.getTypeRoot()) {
                case BOOLEAN:
                    return new GenericArray(Arrays.copyOf(array.toBooleanArray(), array.size()));
                case TINYINT:
                    return new GenericArray(Arrays.copyOf(array.toByteArray(), array.size()));
                case SMALLINT:
                    return new GenericArray(Arrays.copyOf(array.toShortArray(), array.size()));
                case INTEGER:
                    return new GenericArray(Arrays.copyOf(array.toIntArray(), array.size()));
                case BIGINT:
                    return new GenericArray(Arrays.copyOf(array.toLongArray(), array.size()));
                case FLOAT:
                    return new GenericArray(Arrays.copyOf(array.toFloatArray(), array.size()));
                case DOUBLE:
                    return new GenericArray(Arrays.copyOf(array.toDoubleArray(), array.size()));
                default:
                    throw new RuntimeException("Unknown type: " + eleType);
            }
        } else {
            Object[] objectArray = array.toObjectArray();
            Object[] newArray =
                    (Object[]) Array.newInstance(InternalRow.getDataClass(eleType), array.size());
            for (int i = 0; i < array.size(); i++) {
                if (objectArray[i] != null) {
                    newArray[i] = eleSer.copy(objectArray[i]);
                }
            }
            return new GenericArray(newArray);
        }
    }

    @Override
    public void serialize(InternalArray record, OutputView target) throws IOException {
        BinaryArray binaryArray = toBinaryArray(record);
        target.writeInt(binaryArray.getSizeInBytes());
        BinarySegmentUtils.copyToView(
                binaryArray.getSegments(),
                binaryArray.getOffset(),
                binaryArray.getSizeInBytes(),
                target);
    }

    public BinaryArray toBinaryArray(InternalArray from) {
        if (from instanceof BinaryArray) {
            return (BinaryArray) from;
        }

        int numElements = from.size();
        if (reuseArray == null) {
            reuseArray = new BinaryArray();
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

        for (int i = 0; i < numElements; i++) {
            if (from.isNullAt(i)) {
                reuseWriter.setNullAt(i, eleType);
            } else {
                InternalArray.ElementGetter elementGetter =
                        InternalArray.createElementGetter(eleType, i);
                ArrayWriter.write(
                        reuseWriter, i, elementGetter.getElementOrNull(from), eleType, eleSer);
            }
        }
        reuseWriter.complete();

        return reuseArray;
    }

    @Override
    public InternalArray deserialize(InputView source) throws IOException {
        return deserializeReuse(new BinaryArray(), source);
    }

    private BinaryArray deserializeReuse(BinaryArray reuse, InputView source) throws IOException {
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        reuse.pointTo(MemorySegment.wrap(bytes), 0, bytes.length);
        return reuse;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InternalArraySerializer that = (InternalArraySerializer) o;

        return eleType.equals(that.eleType);
    }

    @Override
    public int hashCode() {
        return eleType.hashCode();
    }

    public static Object[] convertToJavaArray(InternalArray array, DataType dataType) {
        Object[] javaArray = new Object[array.size()];
        for (int i = 0; i < array.size(); i++) {
            InternalArray.ElementGetter keyGetter = InternalArray.createElementGetter(dataType, i);
            Object value = keyGetter.getElementOrNull(array);
            javaArray[i] = value;
        }
        return javaArray;
    }
}
