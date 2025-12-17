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

package org.apache.fluss.row;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.array.PrimitiveBinaryArray;

import java.io.Serializable;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Binary implementation of {@link InternalMap} backed by {@link MemorySegment}s.
 *
 * <p>The binary layout of {@link BinaryMap}:
 *
 * <pre>
 * [4 byte(keyArray size in bytes)] + [Key BinaryArray] + [Value BinaryArray].
 * </pre>
 *
 * <p>Influenced by Apache Spark UnsafeMapData.
 *
 * @since 0.9
 */
@PublicEvolving
public class BinaryMap extends BinarySection implements InternalMap {

    private static final long serialVersionUID = 1L;

    private transient BinaryArray keys;
    private transient BinaryArray values;

    private final transient Supplier<BinaryArray> keyArraySupplier;
    private final transient Supplier<BinaryArray> valueArraySupplier;
    private final transient Supplier<BinaryMap> nestedMapSupplier;

    public BinaryMap() {
        this(
                (Supplier<BinaryArray> & Serializable) PrimitiveBinaryArray::new,
                (Supplier<BinaryArray> & Serializable) PrimitiveBinaryArray::new,
                (Supplier<BinaryMap> & Serializable) BinaryMap::new);
    }

    public BinaryMap(
            Supplier<BinaryArray> keyArraySupplier,
            Supplier<BinaryArray> valueArraySupplier,
            Supplier<BinaryMap> nestedMapSupplier) {
        this.keyArraySupplier = keyArraySupplier;
        this.valueArraySupplier = valueArraySupplier;
        this.nestedMapSupplier = nestedMapSupplier;
    }

    @Override
    public int size() {
        return keys.size();
    }

    @Override
    public void pointTo(MemorySegment[] segments, int offset, int sizeInBytes) {
        // Read the numBytes of key array from the first 4 bytes.
        final int keyArrayBytes = BinarySegmentUtils.getInt(segments, offset);
        assert keyArrayBytes >= 0 : "keyArraySize (" + keyArrayBytes + ") should >= 0";
        final int valueArrayBytes = sizeInBytes - keyArrayBytes - 4;
        assert valueArrayBytes >= 0 : "valueArraySize (" + valueArrayBytes + ") should >= 0";

        // see BinarySection.readObject, on this call stack, keys and values are not initialized
        if (keys == null) {
            keys = createKeyArrayInstance();
        }
        keys.pointTo(segments, offset + 4, keyArrayBytes);
        if (values == null) {
            values = createValueArrayInstance();
        }
        values.pointTo(segments, offset + 4 + keyArrayBytes, valueArrayBytes);

        assert keys.size() == values.size();

        this.segments = segments;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
    }

    /** Creates a {@link BinaryArray} instance for keys with the nested data type information. */
    protected BinaryArray createKeyArrayInstance() {
        return keyArraySupplier.get();
    }

    /** Creates a {@link BinaryArray} instance for values with the nested data type information. */
    protected BinaryArray createValueArrayInstance() {
        return valueArraySupplier.get();
    }

    /** Creates a nested {@link BinaryMap} with the nested data type information. */
    protected BinaryMap createNestedMapInstance() {
        return nestedMapSupplier.get();
    }

    @Override
    public BinaryArray keyArray() {
        return keys;
    }

    @Override
    public BinaryArray valueArray() {
        return values;
    }

    public BinaryMap copy() {
        return copy(createNestedMapInstance());
    }

    public BinaryMap copy(BinaryMap reuse) {
        byte[] bytes = BinarySegmentUtils.copyToBytes(segments, offset, sizeInBytes);
        reuse.pointTo(MemorySegment.wrap(bytes), 0, sizeInBytes);
        return reuse;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BinaryMap)) {
            return false;
        }
        final BinarySection that = (BinarySection) o;
        return sizeInBytes == that.sizeInBytes
                && BinarySegmentUtils.equals(
                        segments, offset, that.segments, that.offset, sizeInBytes);
    }

    @Override
    public int hashCode() {
        return BinarySegmentUtils.hash(segments, offset, sizeInBytes);
    }

    public static BinaryMap valueOf(BinaryArray key, BinaryArray value, BinaryMap reuse) {
        checkArgument(key.segments.length == 1 && value.getSegments().length == 1);
        byte[] bytes = new byte[4 + key.sizeInBytes + value.sizeInBytes];
        MemorySegment segment = MemorySegment.wrap(bytes);
        segment.putInt(0, key.sizeInBytes);
        key.getSegments()[0].copyTo(key.getOffset(), segment, 4, key.sizeInBytes);
        value.getSegments()[0].copyTo(
                value.getOffset(), segment, 4 + key.sizeInBytes, value.sizeInBytes);
        reuse.pointTo(segment, 0, bytes.length);
        return reuse;
    }
}
