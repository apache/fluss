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
import org.apache.fluss.row.BinaryMap;
import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.types.DataType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Serializer for {@link InternalMap}. */
public class InternalMapSerializer implements Serializer<InternalMap> {

    private final DataType keyType;
    private final DataType valueType;

    private final Serializer keySerializer;
    private final Serializer valueSerializer;

    private transient BinaryArray reuseKeyArray;
    private transient BinaryArray reuseValueArray;
    private transient BinaryArrayWriter reuseKeyWriter;
    private transient BinaryArrayWriter reuseValueWriter;

    public InternalMapSerializer(DataType keyType, DataType valueType) {
        this(
                keyType,
                valueType,
                InternalSerializers.create(keyType),
                InternalSerializers.create(valueType));
    }

    private InternalMapSerializer(
            DataType keyType,
            DataType valueType,
            Serializer keySerializer,
            Serializer valueSerializer) {
        this.keyType = keyType;
        this.valueType = valueType;

        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public Serializer<InternalMap> duplicate() {
        return new InternalMapSerializer(
                keyType, valueType, keySerializer.duplicate(), valueSerializer.duplicate());
    }

    /**
     * NOTE: Map should be a HashMap, when we insert the key/value pairs of the TreeMap into a
     * HashMap, problems maybe occur.
     */
    @Override
    public InternalMap copy(InternalMap from) {
        if (from instanceof BinaryMap) {
            return ((BinaryMap) from).copy();
        } else {
            return toBinaryMap(from);
        }
    }

    @Override
    public void serialize(InternalMap record, OutputView target) throws IOException {
        BinaryMap binaryMap = toBinaryMap(record);
        target.writeInt(binaryMap.getSizeInBytes());
        BinarySegmentUtils.copyToView(
                binaryMap.getSegments(), binaryMap.getOffset(), binaryMap.getSizeInBytes(), target);
    }

    public BinaryMap toBinaryMap(InternalMap from) {
        if (from instanceof BinaryMap) {
            return (BinaryMap) from;
        }

        int numElements = from.size();
        if (reuseKeyArray == null) {
            reuseKeyArray = new BinaryArray();
        }
        if (reuseValueArray == null) {
            reuseValueArray = new BinaryArray();
        }
        if (reuseKeyWriter == null || reuseKeyWriter.getNumElements() != numElements) {
            reuseKeyWriter =
                    new BinaryArrayWriter(
                            reuseKeyArray,
                            numElements,
                            BinaryArray.calculateFixLengthPartSize(keyType));
        } else {
            reuseKeyWriter.reset();
        }
        if (reuseValueWriter == null || reuseValueWriter.getNumElements() != numElements) {
            reuseValueWriter =
                    new BinaryArrayWriter(
                            reuseValueArray,
                            numElements,
                            BinaryArray.calculateFixLengthPartSize(valueType));
        } else {
            reuseValueWriter.reset();
        }

        InternalArray keyArray = from.keyArray();
        InternalArray valueArray = from.valueArray();
        for (int i = 0; i < from.size(); i++) {
            InternalArray.ElementGetter keyGetter = InternalArray.createElementGetter(keyType, i);
            InternalArray.ElementGetter valueGetter =
                    InternalArray.createElementGetter(valueType, i);
            Object key = keyGetter.getElementOrNull(keyArray);
            Object value = valueGetter.getElementOrNull(valueArray);
            if (key == null) {
                reuseKeyWriter.setNullAt(i, keyType);
            } else {
                ArrayWriter.write(reuseKeyWriter, i, key, keyType, keySerializer);
            }
            if (value == null) {
                reuseValueWriter.setNullAt(i, valueType);
            } else {
                ArrayWriter.write(reuseValueWriter, i, value, valueType, valueSerializer);
            }
        }

        reuseKeyWriter.complete();
        reuseValueWriter.complete();

        return BinaryMap.valueOf(reuseKeyArray, reuseValueArray);
    }

    @Override
    public InternalMap deserialize(InputView source) throws IOException {
        return deserializeReuse(new BinaryMap(), source);
    }

    private BinaryMap deserializeReuse(BinaryMap reuse, InputView source) throws IOException {
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

        InternalMapSerializer that = (InternalMapSerializer) o;

        return keyType.equals(that.keyType) && valueType.equals(that.valueType);
    }

    @Override
    public int hashCode() {
        int result = keyType.hashCode();
        result = 31 * result + valueType.hashCode();
        return result;
    }

    /**
     * Converts a {@link InternalMap} into Java {@link Map}, the keys and values of the Java map
     * still holds objects of internal data structures.
     */
    public static Map<Object, Object> convertToJavaMap(
            InternalMap map, DataType keyType, DataType valueType) {
        InternalArray keyArray = map.keyArray();
        InternalArray valueArray = map.valueArray();
        Map<Object, Object> javaMap = new HashMap<>();
        for (int i = 0; i < map.size(); i++) {
            InternalArray.ElementGetter keyGetter = InternalArray.createElementGetter(keyType, i);
            InternalArray.ElementGetter valueGetter =
                    InternalArray.createElementGetter(valueType, i);
            Object key = keyGetter.getElementOrNull(keyArray);
            Object value = valueGetter.getElementOrNull(valueArray);
            javaMap.put(key, value);
        }
        return javaMap;
    }
}
