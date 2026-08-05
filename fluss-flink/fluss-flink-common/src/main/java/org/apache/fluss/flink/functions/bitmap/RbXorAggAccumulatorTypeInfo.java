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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.Objects;

/** {@link TypeInformation} for {@link RbXorAggFunction.Accumulator}. */
@ThreadSafe
public final class RbXorAggAccumulatorTypeInfo
        extends TypeInformation<RbXorAggFunction.Accumulator> {

    private static final long serialVersionUID = 1L;

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
    public Class<RbXorAggFunction.Accumulator> getTypeClass() {
        return RbXorAggFunction.Accumulator.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<RbXorAggFunction.Accumulator> createSerializer(ExecutionConfig config) {
        return RbXorAccumulatorSerializer.INSTANCE;
    }

    @Override
    public String toString() {
        return "RbXorAccumulatorTypeInfo";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof RbXorAggAccumulatorTypeInfo
                && ((RbXorAggAccumulatorTypeInfo) obj).canEqual(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTypeClass());
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof RbXorAggAccumulatorTypeInfo;
    }

    /** {@link TypeSerializer} for {@link RbXorAggFunction.Accumulator}. */
    @ThreadSafe
    public static final class RbXorAccumulatorSerializer
            extends TypeSerializerSingleton<RbXorAggFunction.Accumulator> {

        public static final RbXorAccumulatorSerializer INSTANCE = new RbXorAccumulatorSerializer();
        private static final long serialVersionUID = 1L;

        private RbXorAccumulatorSerializer() {}

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public RbXorAggFunction.Accumulator createInstance() {
            return new RbXorAggFunction.Accumulator();
        }

        @Override
        public RbXorAggFunction.Accumulator copy(RbXorAggFunction.Accumulator from) {
            RbXorAggFunction.Accumulator copy = new RbXorAggFunction.Accumulator();
            copy.initialized = from.initialized;
            copy.value = from.value.clone();
            return copy;
        }

        @Override
        public RbXorAggFunction.Accumulator copy(
                RbXorAggFunction.Accumulator from, RbXorAggFunction.Accumulator reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(RbXorAggFunction.Accumulator record, DataOutputView target)
                throws IOException {
            target.writeBoolean(record.initialized);
            if (record.initialized) {
                byte[] bytes = BitmapUtils.toBytes(record.value);
                target.writeInt(bytes.length);
                target.write(bytes);
            }
        }

        @Override
        public RbXorAggFunction.Accumulator deserialize(DataInputView source) throws IOException {
            RbXorAggFunction.Accumulator acc = new RbXorAggFunction.Accumulator();
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
        public RbXorAggFunction.Accumulator deserialize(
                RbXorAggFunction.Accumulator reuse, DataInputView source) throws IOException {
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
        public TypeSerializerSnapshot<RbXorAggFunction.Accumulator> snapshotConfiguration() {
            return new RbXorAccumulatorSerializerSnapshot();
        }

        /** Snapshot for {@link RbXorAccumulatorSerializer}. */
        public static final class RbXorAccumulatorSerializerSnapshot
                extends SimpleTypeSerializerSnapshot<RbXorAggFunction.Accumulator> {
            public RbXorAccumulatorSerializerSnapshot() {
                super(() -> INSTANCE);
            }
        }
    }
}
