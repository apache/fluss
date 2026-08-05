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

package org.apache.fluss.flink.functions.bitmap;

import org.apache.fluss.exception.FlussRuntimeException;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.Objects;

/**
 * {@code rb_xor_agg(bitmap BYTES) -> BYTES}
 *
 * <p>Aggregates multiple serialized {@link RoaringBitmap} values using bitwise XOR across rows.
 * Returns elements that appear in an odd number of input bitmaps — useful for change detection and
 * symmetric difference analysis.
 *
 * <p>Unlike {@link RbOrAggFunction}, this function requires a custom {@link Accumulator} that
 * carries an {@code initialized} flag alongside the bitmap. This is necessary because XOR of
 * identical bitmaps cancels to an empty bitmap, which must be distinguishable from "no input
 * received yet". An uninitialized accumulator returns {@code null}; an initialized but empty
 * accumulator returns an empty serialized bitmap.
 *
 * <p>XOR is self-inverse, so retraction applies the same XOR operation, making this function
 * suitable for use on retractable streams, unlike {@link RbAndAggFunction}.
 *
 * <p>Note: there is no server-side {@code FieldRoaringBitmapXorAgg} counterpart. This function
 * executes entirely in Flink. Users should be aware that combining it with {@code
 * table.merge-engine=aggregation} may produce unexpected results during server-side compaction.
 */
@FunctionHint(
        accumulator = @DataTypeHint(value = "RAW", bridgedTo = RbXorAggFunction.Accumulator.class))
public class RbXorAggFunction extends AggregateFunction<byte[], RbXorAggFunction.Accumulator> {

    // -------------------------------------------------------------------------
    // Accumulator
    // -------------------------------------------------------------------------

    /** Mutable accumulator that tracks initialization state for XOR aggregation. */
    public static final class Accumulator {

        /** True after the first non-null input has been accumulated. */
        public boolean initialized = false;

        /** Current XOR result; meaningless if {@code initialized} is false. */
        public RoaringBitmap value = new RoaringBitmap();
    }

    // -------------------------------------------------------------------------
    // AggregateFunction implementation
    // -------------------------------------------------------------------------

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    /**
     * XORs the input bitmap into the accumulator.
     *
     * @param acc the running accumulator
     * @param bitmapBytes serialized RoaringBitmap bytes; null and empty arrays are ignored
     */
    public void accumulate(Accumulator acc, @Nullable byte[] bitmapBytes) throws IOException {
        if (bitmapBytes == null || bitmapBytes.length == 0) {
            return;
        }
        acc.value.xor(BitmapUtils.fromBytes(bitmapBytes));
        acc.initialized = true;
    }

    /**
     * XOR is self-inverse, so retraction applies the same XOR operation.
     *
     * @param acc the running accumulator
     * @param bitmapBytes serialized RoaringBitmap bytes; null and empty arrays are ignored
     */
    public void retract(Accumulator acc, @Nullable byte[] bitmapBytes) throws IOException {
        if (bitmapBytes == null || bitmapBytes.length == 0) {
            return;
        }
        acc.value.xor(BitmapUtils.fromBytes(bitmapBytes));
        acc.initialized = true;
    }

    /** Merges partial accumulators using XOR, required for two-phase aggregation. */
    public void merge(Accumulator acc, Iterable<Accumulator> it) {
        for (Accumulator other : it) {
            if (other.initialized) {
                acc.value.xor(other.value);
                acc.initialized = true;
            }
        }
    }

    public void resetAccumulator(Accumulator acc) {
        acc.initialized = false;
        acc.value.clear();
    }

    @Override
    @Nullable
    public byte[] getValue(Accumulator acc) {
        if (!acc.initialized) {
            return null;
        }
        try {
            return BitmapUtils.toBytes(acc.value);
        } catch (IOException e) {
            throw new FlussRuntimeException("Failed to serialize rb_xor_agg accumulator.", e);
        }
    }

    @Override
    public TypeInformation<Accumulator> getAccumulatorType() {
        return AccumulatorTypeInfo.INSTANCE;
    }

    // -------------------------------------------------------------------------
    // TypeSerializer and TypeInformation for Accumulator
    // -------------------------------------------------------------------------

    /** {@link TypeInformation} for {@link Accumulator}. */
    @ThreadSafe
    public static final class AccumulatorTypeInfo extends TypeInformation<Accumulator> {

        public static final AccumulatorTypeInfo INSTANCE = new AccumulatorTypeInfo();

        private static final long serialVersionUID = 1L;

        private AccumulatorTypeInfo() {}

        @Override
        public boolean isBasicType() {
            return false;
        }

        @Override
        public boolean isTupleType() {
            return false;
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        public int getTotalFields() {
            return 1;
        }

        @Override
        public Class<Accumulator> getTypeClass() {
            return Accumulator.class;
        }

        @Override
        public boolean isKeyType() {
            return false;
        }

        @Override
        public TypeSerializer<Accumulator> createSerializer(ExecutionConfig config) {
            return AccumulatorSerializer.INSTANCE;
        }

        @Override
        public String toString() {
            return "RbXorAccumulatorTypeInfo";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof AccumulatorTypeInfo && ((AccumulatorTypeInfo) obj).canEqual(this);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getTypeClass());
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof AccumulatorTypeInfo;
        }
    }

    /** {@link TypeSerializer} for {@link Accumulator}. */
    @ThreadSafe
    public static final class AccumulatorSerializer extends TypeSerializerSingleton<Accumulator> {

        public static final AccumulatorSerializer INSTANCE = new AccumulatorSerializer();

        private static final long serialVersionUID = 1L;

        private AccumulatorSerializer() {}

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public Accumulator createInstance() {
            return new Accumulator();
        }

        @Override
        public Accumulator copy(Accumulator from) {
            Accumulator copy = new Accumulator();
            copy.initialized = from.initialized;
            copy.value = from.value.clone();
            return copy;
        }

        @Override
        public Accumulator copy(Accumulator from, Accumulator reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(Accumulator record, DataOutputView target) throws IOException {
            target.writeBoolean(record.initialized);
            if (record.initialized) {
                byte[] bytes = BitmapUtils.toBytes(record.value);
                target.writeInt(bytes.length);
                target.write(bytes);
            }
        }

        @Override
        public Accumulator deserialize(DataInputView source) throws IOException {
            Accumulator acc = new Accumulator();
            acc.initialized = source.readBoolean();
            if (acc.initialized) {
                int size = source.readInt();
                byte[] bytes = new byte[size];
                source.readFully(bytes);
                acc.value = BitmapUtils.fromBytes(bytes);
            }
            return acc;
        }

        @Override
        public Accumulator deserialize(Accumulator reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            boolean initialized = source.readBoolean();
            target.writeBoolean(initialized);
            if (initialized) {
                int size = source.readInt();
                target.writeInt(size);
                byte[] buffer = new byte[size];
                source.readFully(buffer);
                target.write(buffer);
            }
        }

        @Override
        public TypeSerializerSnapshot<Accumulator> snapshotConfiguration() {
            return new AccumulatorSerializerSnapshot();
        }

        /** Snapshot for {@link AccumulatorSerializer}. */
        public static final class AccumulatorSerializerSnapshot
                extends SimpleTypeSerializerSnapshot<Accumulator> {

            public AccumulatorSerializerSnapshot() {
                super(() -> INSTANCE);
            }
        }
    }
}
