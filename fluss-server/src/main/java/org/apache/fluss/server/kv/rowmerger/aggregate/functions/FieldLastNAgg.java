/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.rowmerger.aggregate.functions;

import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.array.BinaryArraySplicer;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;

import javax.annotation.Nullable;

import java.util.Arrays;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Appends input arrays and keeps the last {@code size} elements, optionally deduplicating by last
 * occurrence.
 */
public class FieldLastNAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private static final int MAX_RETAINED_POSITIONS = 1024;
    private static final ThreadLocal<DistinctBuffers> DISTINCT_BUFFER_CACHE =
            ThreadLocal.withInitial(DistinctBuffers::new);

    private final int size;
    private final DataType elementType;
    private final boolean spliceable;
    private final InternalArray.ElementGetter elementGetter;
    private final boolean distinct;
    private final ArrayElementEquality elementEquality;
    private final boolean primitiveDistinct;

    /**
     * Creates a LAST_N aggregator.
     *
     * @param dataType the array type of the aggregated column
     * @param size the number of elements to keep, must be positive
     */
    public FieldLastNAgg(ArrayType dataType, int size) {
        this(dataType, size, false);
    }

    /**
     * Creates a LAST_N aggregator with optional deduplication by last occurrence.
     *
     * @param dataType the array type
     * @param size the positive maximum number of elements
     * @param distinct whether to keep only distinct elements
     */
    public FieldLastNAgg(ArrayType dataType, int size, boolean distinct) {
        super(dataType);
        checkArgument(size > 0, "size must be positive, but was %s", size);
        this.size = size;
        this.elementType = dataType.getElementType();
        this.spliceable = BinaryArraySplicer.supports(elementType);
        this.elementGetter = InternalArray.createElementGetter(elementType);
        this.distinct = distinct;
        this.elementEquality = distinct ? new ArrayElementEquality(elementType) : null;
        this.primitiveDistinct = distinct && elementEquality.supportsLongKey();
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (inputField == null) {
            return accumulator;
        }
        InternalArray older = (InternalArray) accumulator;
        InternalArray newer = (InternalArray) inputField;
        if (!distinct && older != null && newer.size() == 0) {
            return accumulator;
        }
        if (distinct) {
            return distinct(older, newer);
        }
        if (older == null && newer.size() <= size) {
            return inputField;
        }
        int olderSize = older == null ? 0 : older.size();
        int total = olderSize + newer.size();
        int skip = Math.max(0, total - size);
        if (spliceable && BinaryArraySplicer.canSplice(older, newer)) {
            return splice((BinaryArray) older, olderSize, (BinaryArray) newer, skip);
        }
        Object[] result = new Object[total - skip];
        for (int i = skip; i < total; i++) {
            result[i - skip] =
                    i < olderSize
                            ? elementGetter.getElementOrNull(older, i)
                            : elementGetter.getElementOrNull(newer, i - olderSize);
        }
        return new GenericArray(result);
    }

    private Object distinct(@Nullable InternalArray older, InternalArray newer) {
        DistinctSelection selection = new DistinctSelection(older, newer);
        boolean success = false;
        try {
            selection.scan(newer, false);
            selection.scan(older, true);
            Object result = selection.build();
            success = true;
            return result;
        } finally {
            selection.release(success);
        }
    }

    /** Primitive scratch arrays reused across distinct aggregations on the same write thread. */
    private static final class DistinctBuffers {
        // Negative positions encode ~ordinal in older; non-negative positions index newer.
        private int[] selectedPositions = new int[8];
        private int[] elementHashes = new int[8];
        // Zero marks an empty slot; other values are indices in selectedPositions plus one.
        private int[] hashTable = new int[16];
        private long[] primitiveKeys = new long[8];
        // A reentrant call on the same thread must use separate buffers.
        private boolean inUse;
    }

    /**
     * Selects source positions from newest to oldest, then builds the result in occurrence order.
     * The first encounter of each value is its last occurrence across the two inputs.
     */
    private final class DistinctSelection {

        private final InternalArray older;
        private final InternalArray newer;
        private final int limit;
        private final DistinctBuffers buffers;
        private int count;

        private DistinctSelection(@Nullable InternalArray older, InternalArray newer) {
            this.older = older;
            this.newer = newer;
            this.limit =
                    (int) Math.min((long) size, (older == null ? 0L : older.size()) + newer.size());
            DistinctBuffers cached = DISTINCT_BUFFER_CACHE.get();
            buffers = cached.inUse ? new DistinctBuffers() : cached;
            buffers.inUse = true;
        }

        private void release(boolean success) {
            buffers.inUse = false;
            // Failure may leave partial growth or occupied slots. Discard only this call's cache;
            // a reentrant call must not remove buffers still owned by its caller.
            if (!success || buffers.selectedPositions.length > MAX_RETAINED_POSITIONS) {
                if (DISTINCT_BUFFER_CACHE.get() == buffers) {
                    DISTINCT_BUFFER_CACHE.remove();
                }
                return;
            }
            Arrays.fill(buffers.hashTable, 0);
        }

        private void scan(@Nullable InternalArray source, boolean fromOlder) {
            if (source == null) {
                return;
            }
            for (int i = source.size() - 1; i >= 0 && count < limit; i--) {
                boolean isNull = primitiveDistinct && source.isNullAt(i);
                long key = primitiveDistinct && !isNull ? elementEquality.longKey(source, i) : 0;
                // Null has key zero but a nonzero hash, unlike the primitive zero value.
                int hash =
                        primitiveDistinct
                                ? (isNull
                                        ? ArrayElementEquality.NULL_HASH
                                        : ArrayElementEquality.hashLong(key))
                                : elementEquality.hash(source, i);
                int slot = bucketForHash(hash);
                while (buffers.hashTable[slot] != 0) {
                    int entry = buffers.hashTable[slot] - 1;
                    int position = buffers.selectedPositions[entry];
                    if (buffers.elementHashes[entry] == hash
                            && (primitiveDistinct
                                    ? buffers.primitiveKeys[entry] == key
                                    : elementEquality.equal(
                                            source,
                                            i,
                                            position < 0 ? older : newer,
                                            position < 0 ? ~position : position))) {
                        break;
                    }
                    slot = (slot + 1) & (buffers.hashTable.length - 1);
                }
                if (buffers.hashTable[slot] != 0) {
                    continue;
                }
                if (count == buffers.selectedPositions.length) {
                    int capacity =
                            (int) Math.min((long) limit, 2L * buffers.selectedPositions.length);
                    buffers.selectedPositions = Arrays.copyOf(buffers.selectedPositions, capacity);
                    buffers.elementHashes = Arrays.copyOf(buffers.elementHashes, capacity);
                }
                buffers.selectedPositions[count] = fromOlder ? ~i : i;
                buffers.elementHashes[count] = hash;
                if (primitiveDistinct) {
                    if (count == buffers.primitiveKeys.length) {
                        buffers.primitiveKeys =
                                Arrays.copyOf(
                                        buffers.primitiveKeys, buffers.selectedPositions.length);
                    }
                    buffers.primitiveKeys[count] = key;
                }
                buffers.hashTable[slot] = ++count;
                if (count == buffers.hashTable.length / 2 && count < limit) {
                    growHashTable();
                }
            }
        }

        private void growHashTable() {
            buffers.hashTable = new int[buffers.hashTable.length * 2];
            for (int i = 0; i < count; i++) {
                int target = bucketForHash(buffers.elementHashes[i]);
                while (buffers.hashTable[target] != 0) {
                    target = (target + 1) & (buffers.hashTable.length - 1);
                }
                buffers.hashTable[target] = i + 1;
            }
        }

        private int bucketForHash(int hash) {
            int mixed = hash * 0x9e3779b9;
            return (mixed ^ (mixed >>> 16)) & (buffers.hashTable.length - 1);
        }

        private Object build() {
            if (count == 0) {
                return newer;
            }
            if (isWhole(newer, false)) {
                return newer;
            }
            if (isWhole(older, true)) {
                return older;
            }
            // The new value equals the old tail, so count == older.size() proves older is unique.
            // OVERWRITE may leave raw arrays; tail equality alone does not justify reuse.
            if (older != null
                    && count == older.size()
                    && newer.size() == 1
                    && elementEquality.equal(older, count - 1, newer, 0)) {
                return older;
            }
            if (spliceable && BinaryArraySplicer.canSplice(older, newer)) {
                return buildBinaryResult();
            }
            Object[] result = new Object[count];
            for (int i = 0; i < count; i++) {
                int position = buffers.selectedPositions[count - i - 1];
                result[i] =
                        elementGetter.getElementOrNull(
                                position < 0 ? older : newer, position < 0 ? ~position : position);
            }
            return new GenericArray(result);
        }

        private BinaryArray buildBinaryResult() {
            BinaryArraySplicer splicer = new BinaryArraySplicer(elementType);
            int i = count - 1;
            while (i >= 0) {
                int position = buffers.selectedPositions[i];
                boolean fromOlder = position < 0;
                int start = fromOlder ? ~position : position;
                int run = 1;
                while (i - run >= 0
                        && (buffers.selectedPositions[i - run] < 0) == fromOlder
                        && (fromOlder
                                        ? ~buffers.selectedPositions[i - run]
                                        : buffers.selectedPositions[i - run])
                                == start + run) {
                    run++;
                }
                splicer.append((BinaryArray) (fromOlder ? older : newer), start, run);
                i -= run;
            }
            return splicer.build();
        }

        private boolean isWhole(@Nullable InternalArray source, boolean fromOlder) {
            if (source == null || source.size() != count) {
                return false;
            }
            for (int i = 0; i < count; i++) {
                if (buffers.selectedPositions[count - i - 1] != (fromOlder ? ~i : i)) {
                    return false;
                }
            }
            return true;
        }
    }

    /** Copies the kept elements as bytes, without reading each one. */
    private BinaryArray splice(
            @Nullable BinaryArray older, int olderSize, BinaryArray newer, int skip) {
        BinaryArraySplicer splicer = new BinaryArraySplicer(elementType);
        int skipOlder = Math.min(skip, olderSize);
        if (older != null) {
            splicer.append(older, skipOlder, olderSize - skipOlder);
        }
        int skipNewer = skip - skipOlder;
        return splicer.append(newer, skipNewer, newer.size() - skipNewer).build();
    }

    @Override
    public boolean requiresAggOnFirstWrite() {
        return true;
    }
}
