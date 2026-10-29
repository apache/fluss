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

package org.apache.fluss.row.array;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.BinaryArrayWriter;
import org.apache.fluss.row.BinarySection;
import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;

import javax.annotation.Nullable;

import java.util.Arrays;

import static org.apache.fluss.row.AbstractBinaryWriter.NON_COMPACT_DECIMAL_SIZE;
import static org.apache.fluss.row.AbstractBinaryWriter.NON_COMPACT_TIMESTAMP_SIZE;
import static org.apache.fluss.row.BinaryArrayWriter.roundNumberOfBytesToNearestWord;
import static org.apache.fluss.types.DataTypeChecks.getPrecision;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Builds a {@link BinaryArray} from runs of elements of other binary arrays, by copying bytes
 * instead of reading and rewriting each element.
 *
 * <p>The result has the bytes the {@link BinaryArrayWriter} would write, and the class of the
 * sources. Call {@link #supports(DataType)} and {@link #canSplice(InternalArray, InternalArray)}
 * first, and rebuild the array element by element when either returns false.
 */
@Internal
public final class BinaryArraySplicer {

    /** Where an element keeps its data besides its slot. */
    private enum DataLayout {
        /** Only in the slot. */
        SLOT,
        /** In the slot when 7 bytes or shorter, else in the variable-length part. */
        INLINE_OR_VARIABLE,
        /** In the variable-length part: nested arrays, maps and rows. */
        VARIABLE,
        /** In a fixed-size block: non-compact decimals. */
        DECIMAL,
        /** In a fixed-size block, with the nanoseconds in the slot: non-compact timestamps. */
        TIMESTAMP
    }

    private final DataType elementType;
    private final int elementSize;
    private final DataLayout layout;

    private BinaryArray[] sources = new BinaryArray[2];
    private int[] starts = new int[2];
    private int[] counts = new int[2];
    private int runs;

    /**
     * Creates a splicer for arrays of the given element type.
     *
     * @param elementType the element type of the source arrays and the result
     * @throws IllegalArgumentException if {@link #supports(DataType)} returns false for it
     */
    public BinaryArraySplicer(DataType elementType) {
        DataLayout layout = layoutOf(elementType);
        checkArgument(layout != null, "Unsupported element type: %s", elementType);
        this.elementType = elementType;
        this.elementSize = BinaryArray.calculateFixLengthPartSize(elementType);
        this.layout = layout;
    }

    /** Returns whether arrays of the given element type can be spliced. */
    public static boolean supports(DataType elementType) {
        return layoutOf(elementType) != null;
    }

    /** Returns whether two arrays are binary arrays of one class. A null array is ignored. */
    public static boolean canSplice(@Nullable InternalArray first, @Nullable InternalArray second) {
        if (first != null && !isSpliceable(first)) {
            return false;
        }
        if (second != null && !isSpliceable(second)) {
            return false;
        }
        return first == null || second == null || first.getClass() == second.getClass();
    }

    /**
     * Appends the elements {@code [start, start + count)} of the source to the result.
     *
     * @param source a binary array of the same class as the sources appended before
     * @param start the position of the first element to append
     * @param count the number of elements to append, may be 0
     * @return this splicer
     */
    public BinaryArraySplicer append(BinaryArray source, int start, int count) {
        // not checkArgument: it would box the arguments on every call
        if (!isSpliceable(source) || source.getSegments().length != 1) {
            throw new IllegalArgumentException("Cannot splice " + source.getClass().getName());
        }
        if (runs > 0 && sources[0].getClass() != source.getClass()) {
            throw new IllegalArgumentException(
                    "Cannot splice "
                            + sources[0].getClass().getName()
                            + " with "
                            + source.getClass().getName());
        }
        if (start < 0 || count < 0 || start > source.size() || count > source.size() - start) {
            throw new IllegalArgumentException(
                    String.format(
                            "Elements [%s, %s) are out of the bounds of an array of size %s",
                            start, (long) start + count, source.size()));
        }
        if (runs == sources.length) {
            sources = Arrays.copyOf(sources, runs * 2);
            starts = Arrays.copyOf(starts, runs * 2);
            counts = Arrays.copyOf(counts, runs * 2);
        }
        sources[runs] = source;
        starts[runs] = start;
        counts[runs] = count;
        runs++;
        return this;
    }

    /** Builds a new array from the appended runs, of the same class as their sources. */
    public BinaryArray build() {
        checkArgument(runs > 0, "Nothing was appended");
        int numElements = 0;
        int variableSize = 0;
        int[] runVariableSizes = new int[runs];
        int[] runBlockStarts = new int[runs];
        for (int i = 0; i < runs; i++) {
            numElements += counts[i];
            runVariableSizes[i] = planVariablePart(i, runBlockStarts);
            variableSize += runVariableSizes[i];
        }
        int headerSize = BinaryArray.calculateHeaderInBytes(numElements);
        int fixedSize = roundNumberOfBytesToNearestWord(headerSize + elementSize * numElements);
        byte[] bytes = new byte[fixedSize + variableSize];
        MemorySegment out = MemorySegment.wrap(bytes);
        out.putInt(0, numElements);

        int position = 0;
        int cursor = fixedSize;
        for (int i = 0; i < runs; i++) {
            cursor =
                    copyRun(
                            i,
                            runBlockStarts[i],
                            runVariableSizes[i],
                            bytes,
                            out,
                            headerSize,
                            position,
                            cursor);
            position += counts[i];
        }

        BinaryArray result = newArrayLike(sources[0]);
        result.pointTo(out, 0, bytes.length);
        return result;
    }

    /**
     * Returns the variable-length bytes of a run. Also records in {@code blockStarts} where this
     * data starts in the source when it is one block in element order, as the writer leaves it, or
     * -1.
     */
    private int planVariablePart(int run, int[] blockStarts) {
        blockStarts[run] = -1;
        if (layout == DataLayout.SLOT) {
            return 0;
        }
        BinaryArray source = sources[run];
        MemorySegment segment = source.getSegments()[0];
        int base = source.getOffset();
        int slots = base + BinaryArray.calculateHeaderInBytes(source.size());
        int bytes = 0;
        int blockStart = -1;
        int nextOffset = -1;
        boolean contiguous = true;
        for (int i = starts[run]; i < starts[run] + counts[run]; i++) {
            if (BinarySegmentUtils.bitGet(segment, base + 4, i)) {
                continue;
            }
            long offsetAndSize = segment.getLong(slots + i * elementSize);
            if (isInline(offsetAndSize)) {
                continue;
            }
            int reserved = reservedBytes(offsetAndSize);
            int offset = (int) (offsetAndSize >>> 32);
            if (blockStart < 0) {
                blockStart = offset;
            } else if (offset != nextOffset) {
                contiguous = false;
            }
            nextOffset = offset + reserved;
            bytes += reserved;
        }
        if (contiguous && blockStart >= 0 && blockStart + bytes <= source.getSizeInBytes()) {
            blockStarts[run] = blockStart;
        }
        return bytes;
    }

    /**
     * Copies a run to {@code position} of the result and returns the new end of the variable-length
     * part. Data in one block is copied at once and its offsets move by the same distance,
     * otherwise each element is copied on its own.
     *
     * @param blockStart where the data of the run starts in the source, or -1 if it is not one
     *     block
     * @param variableSize the variable-length bytes of the run
     */
    private int copyRun(
            int run,
            int blockStart,
            int variableSize,
            byte[] bytes,
            MemorySegment out,
            int headerSize,
            int position,
            int cursor) {
        BinaryArray source = sources[run];
        int start = starts[run];
        int count = counts[run];
        if (count == 0) {
            return cursor;
        }
        MemorySegment segment = source.getSegments()[0];
        int base = source.getOffset();
        int sourceSlots = base + BinaryArray.calculateHeaderInBytes(source.size());
        segment.get(
                sourceSlots + start * elementSize,
                bytes,
                headerSize + position * elementSize,
                count * elementSize);

        if (layout == DataLayout.SLOT) {
            copyNulls(segment, base, start, count, bytes, out, headerSize, position);
            return cursor;
        }
        if (blockStart >= 0) {
            segment.get(base + blockStart, bytes, cursor, variableSize);
            int shift = cursor - blockStart;
            for (int i = 0; i < count; i++) {
                int slot = headerSize + (position + i) * elementSize;
                if (BinarySegmentUtils.bitGet(segment, base + 4, start + i)) {
                    setNull(bytes, out, slot, position + i);
                    continue;
                }
                long offsetAndSize = out.getLong(slot);
                if (isInline(offsetAndSize)) {
                    continue;
                }
                int offset = (int) (offsetAndSize >>> 32) + shift;
                out.putLong(slot, ((long) offset << 32) | (offsetAndSize & 0xFFFFFFFFL));
            }
            return cursor + variableSize;
        }
        for (int i = 0; i < count; i++) {
            int slot = headerSize + (position + i) * elementSize;
            if (BinarySegmentUtils.bitGet(segment, base + 4, start + i)) {
                setNull(bytes, out, slot, position + i);
                continue;
            }
            long offsetAndSize = out.getLong(slot);
            if (isInline(offsetAndSize)) {
                continue;
            }
            int reserved = reservedBytes(offsetAndSize);
            // a string copies its length, a decimal or timestamp its whole block
            int length =
                    layout == DataLayout.INLINE_OR_VARIABLE || layout == DataLayout.VARIABLE
                            ? (int) offsetAndSize
                            : reserved;
            segment.get(base + (int) (offsetAndSize >>> 32), bytes, cursor, length);
            // keep the low 32 bits: the length, or a timestamp's nanoseconds
            out.putLong(slot, ((long) cursor << 32) | (offsetAndSize & 0xFFFFFFFFL));
            cursor += reserved;
        }
        return cursor;
    }

    /** Returns whether a non-null element keeps its data inside the slot. */
    private boolean isInline(long offsetAndSize) {
        return layout == DataLayout.INLINE_OR_VARIABLE
                && (offsetAndSize & BinarySection.HIGHEST_FIRST_BIT) != 0;
    }

    /** Returns the variable-length bytes of a non-null element that is not inline. */
    private int reservedBytes(long offsetAndSize) {
        switch (layout) {
            case INLINE_OR_VARIABLE:
            case VARIABLE:
                return roundNumberOfBytesToNearestWord((int) offsetAndSize);
            case DECIMAL:
                return NON_COMPACT_DECIMAL_SIZE;
            case TIMESTAMP:
                return NON_COMPACT_TIMESTAMP_SIZE;
            default:
                throw new IllegalStateException("Unexpected layout: " + layout);
        }
    }

    /** Marks an element of the result as null, with a zero slot as the writer leaves it. */
    private void setNull(byte[] bytes, MemorySegment out, int slot, int position) {
        BinarySegmentUtils.bitSet(out, 4, position);
        Arrays.fill(bytes, slot, slot + elementSize, (byte) 0);
    }

    /**
     * Marks the null elements of a run in the result, skipping 64 at a time while there are none.
     */
    private void copyNulls(
            MemorySegment segment,
            int base,
            int start,
            int count,
            byte[] bytes,
            MemorySegment out,
            int headerSize,
            int position) {
        int nullBits = base + 4;
        int end = start + count;
        int i = start;
        while (i < end) {
            // 64 aligned null bits are one long, and lie inside the header while i + 64 <= size
            if ((i & 63) == 0 && i + 64 <= end && segment.getLong(nullBits + (i >>> 3)) == 0L) {
                i += 64;
                continue;
            }
            if (BinarySegmentUtils.bitGet(segment, nullBits, i)) {
                int target = position + i - start;
                setNull(bytes, out, headerSize + target * elementSize, target);
            }
            i++;
        }
    }

    private BinaryArray newArrayLike(BinaryArray source) {
        Class<?> clazz = source.getClass();
        if (clazz == CompactedArray.class) {
            return new CompactedArray(elementType);
        } else if (clazz == IndexedArray.class) {
            return new IndexedArray(elementType);
        } else if (clazz == AlignedArray.class) {
            return new AlignedArray();
        } else if (clazz == PrimitiveBinaryArray.class) {
            return new PrimitiveBinaryArray();
        }
        throw new IllegalArgumentException("Cannot splice " + clazz.getName());
    }

    private static boolean isSpliceable(InternalArray array) {
        Class<?> clazz = array.getClass();
        return clazz == CompactedArray.class
                || clazz == IndexedArray.class
                || clazz == AlignedArray.class
                || clazz == PrimitiveBinaryArray.class;
    }

    @Nullable
    private static DataLayout layoutOf(DataType elementType) {
        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return DataLayout.SLOT;
            case CHAR:
            case STRING:
            case BINARY:
            case BYTES:
                return DataLayout.INLINE_OR_VARIABLE;
            case ARRAY:
            case MAP:
            case ROW:
                return DataLayout.VARIABLE;
            case DECIMAL:
                return Decimal.isCompact(getPrecision(elementType))
                        ? DataLayout.SLOT
                        : DataLayout.DECIMAL;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampNtz.isCompact(getPrecision(elementType))
                        ? DataLayout.SLOT
                        : DataLayout.TIMESTAMP;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampLtz.isCompact(getPrecision(elementType))
                        ? DataLayout.SLOT
                        : DataLayout.TIMESTAMP;
            default:
                // a type added later is not spliced until its layout is known
                return null;
        }
    }
}
