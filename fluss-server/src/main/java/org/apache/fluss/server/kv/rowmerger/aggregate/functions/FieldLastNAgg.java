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

/** Appends input arrays to the accumulator and keeps the last {@code size} elements. */
public class FieldLastNAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private static final int MAX_RETAINED_POSITIONS = 1024;
    private static final ThreadLocal<Workspace> WORKSPACE = ThreadLocal.withInitial(Workspace::new);

    private final int size;
    private final DataType elementType;
    private final boolean spliceable;
    private final InternalArray.ElementGetter elementGetter;
    private final boolean distinct;
    private final ArrayElementComparator comparator;

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
        this.comparator = distinct ? new ArrayElementComparator(elementType) : null;
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
        Selection selection = new Selection(older, newer);
        try {
            selection.scan(newer, false);
            selection.scan(older, true);
            return selection.build();
        } finally {
            selection.release();
        }
    }

    /** Reuses only primitive scratch space, shared across buckets on the same write thread. */
    private static final class Workspace {
        private int[] positions = new int[8];
        private int[] hashes = new int[8];
        private int[] table = new int[16];
        private boolean inUse;
    }

    /** Stores source positions rather than materialized values, in reverse occurrence order. */
    private final class Selection {

        private final InternalArray older;
        private final InternalArray newer;
        private final int limit;
        private final Workspace workspace;
        // Negative positions encode ~ordinal in older; non-negative positions index newer.
        private int[] positions;
        private int[] hashes;
        private int[] table;
        private int count;

        private Selection(@Nullable InternalArray older, InternalArray newer) {
            this.older = older;
            this.newer = newer;
            this.limit =
                    (int) Math.min((long) size, (older == null ? 0L : older.size()) + newer.size());
            Workspace cached = WORKSPACE.get();
            workspace = cached.inUse ? new Workspace() : cached;
            workspace.inUse = true;
            positions = workspace.positions;
            hashes = workspace.hashes;
            table = workspace.table;
        }

        private void release() {
            Arrays.fill(table, 0);
            workspace.positions = positions;
            workspace.hashes = hashes;
            workspace.table = table;
            workspace.inUse = false;
            // Large arrays must not leave unbounded scratch space on long-lived threads.
            if (positions.length > MAX_RETAINED_POSITIONS && WORKSPACE.get() == workspace) {
                WORKSPACE.remove();
            }
        }

        private void scan(@Nullable InternalArray source, boolean old) {
            if (source == null) {
                return;
            }
            for (int i = source.size() - 1; i >= 0 && count < limit; i--) {
                int hash = comparator.hash(source, i);
                int slot = slot(hash);
                while (table[slot] != 0) {
                    int entry = table[slot] - 1;
                    int position = positions[entry];
                    if (hashes[entry] == hash
                            && comparator.equal(
                                    source,
                                    i,
                                    position < 0 ? older : newer,
                                    position < 0 ? ~position : position)) {
                        break;
                    }
                    slot = (slot + 1) & (table.length - 1);
                }
                if (table[slot] != 0) {
                    continue;
                }
                if (count == positions.length) {
                    int capacity = (int) Math.min((long) limit, 2L * positions.length);
                    positions = Arrays.copyOf(positions, capacity);
                    hashes = Arrays.copyOf(hashes, capacity);
                }
                positions[count] = old ? ~i : i;
                hashes[count] = hash;
                table[slot] = ++count;
                if (count == table.length / 2 && count < limit) {
                    table = new int[table.length * 2];
                    for (int j = 0; j < count; j++) {
                        int target = slot(hashes[j]);
                        while (table[target] != 0) {
                            target = (target + 1) & (table.length - 1);
                        }
                        table[target] = j + 1;
                    }
                }
            }
        }

        private int slot(int hash) {
            int mixed = hash * 0x9e3779b9;
            return (mixed ^ (mixed >>> 16)) & (table.length - 1);
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
            // Check uniqueness before reusing a repeated tail: OVERWRITE may store raw arrays.
            if (older != null
                    && count == older.size()
                    && newer.size() == 1
                    && comparator.equal(older, count - 1, newer, 0)) {
                return older;
            }
            if (spliceable && BinaryArraySplicer.canSplice(older, newer)) {
                BinaryArraySplicer splicer = new BinaryArraySplicer(elementType);
                int i = count - 1;
                while (i >= 0) {
                    int position = positions[i];
                    boolean old = position < 0;
                    int start = old ? ~position : position;
                    int run = 1;
                    while (i - run >= 0
                            && (positions[i - run] < 0) == old
                            && (old ? ~positions[i - run] : positions[i - run]) == start + run) {
                        run++;
                    }
                    splicer.append((BinaryArray) (old ? older : newer), start, run);
                    i -= run;
                }
                return splicer.build();
            }
            Object[] result = new Object[count];
            for (int i = 0; i < count; i++) {
                int position = positions[count - i - 1];
                result[i] =
                        elementGetter.getElementOrNull(
                                position < 0 ? older : newer, position < 0 ? ~position : position);
            }
            return new GenericArray(result);
        }

        private boolean isWhole(@Nullable InternalArray source, boolean old) {
            if (source == null || source.size() != count) {
                return false;
            }
            for (int i = 0; i < count; i++) {
                if (positions[count - i - 1] != (old ? ~i : i)) {
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
