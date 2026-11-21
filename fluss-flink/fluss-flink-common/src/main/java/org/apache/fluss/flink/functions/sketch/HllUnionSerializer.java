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

package org.apache.fluss.flink.functions.sketch;

import org.apache.datasketches.hll.Union;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** Serializes Union accumulators using the DataSketches binary format. */
public final class HllUnionSerializer extends TypeSerializerSingleton<Union> {
    public static final HllUnionSerializer INSTANCE = new HllUnionSerializer();
    private static final long serialVersionUID = 1L;

    /** Public constructor for Flink's RAW data type inference. */
    public HllUnionSerializer() {}

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public Union createInstance() {
        return new Union(12);
    }

    @Override
    public Union copy(Union from) {
        byte[] bytes = from.toCompactByteArray();
        return Union.heapify(bytes);
    }

    @Override
    public Union copy(Union from, Union reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(Union record, DataOutputView target) throws IOException {
        byte[] bytes = record.toCompactByteArray();
        target.writeInt(bytes.length);
        target.write(bytes);
    }

    @Override
    public Union deserialize(DataInputView source) throws IOException {
        int size = source.readInt();
        if (size < 0) {
            throw new IOException("Negative sketch length: " + size);
        }
        byte[] bytes = new byte[size];
        source.readFully(bytes);
        return Union.heapify(bytes);
    }

    @Override
    public Union deserialize(Union reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<Union> snapshotConfiguration() {
        return new Snapshot();
    }

    /** Serializer snapshot for persisted sketch accumulators. */
    public static final class Snapshot extends SimpleTypeSerializerSnapshot<Union> {
        public Snapshot() {
            super(() -> INSTANCE);
        }
    }
}
