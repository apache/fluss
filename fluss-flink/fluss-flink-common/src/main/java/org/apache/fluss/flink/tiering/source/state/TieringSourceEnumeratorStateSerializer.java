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

package org.apache.fluss.flink.tiering.source.state;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/**
 * Serializer for {@link TieringSourceEnumeratorState}.
 *
 * <h3>Version Evolution:</h3>
 *
 * <ul>
 *   <li><b>Version 0:</b> Empty state (stateless marker).
 *   <li><b>Version 1 (Current):</b> Stores the KV snapshot lease id so that it can be recovered
 *       after a checkpoint restore.
 * </ul>
 */
public class TieringSourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<TieringSourceEnumeratorState> {

    public static final TieringSourceEnumeratorStateSerializer INSTANCE =
            new TieringSourceEnumeratorStateSerializer();

    private static final int VERSION_0 = 0;
    private static final int VERSION_1 = 1;
    private static final int CURRENT_VERSION = VERSION_1;

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(TieringSourceEnumeratorState obj) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        String leaseId = obj.getKvSnapshotLeaseId();
        if (leaseId != null) {
            out.writeBoolean(true);
            out.writeUTF(leaseId);
        } else {
            out.writeBoolean(false);
        }
        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public TieringSourceEnumeratorState deserialize(int version, byte[] serialized)
            throws IOException {
        switch (version) {
            case VERSION_0:
                // v0 was an empty state; no lease id persisted
                return new TieringSourceEnumeratorState();
            case VERSION_1:
                return deserializeV1(serialized);
            default:
                throw new IOException(
                        String.format(
                                "The bytes are serialized with version %d, "
                                        + "while this deserializer only supports version up to %d",
                                version, CURRENT_VERSION));
        }
    }

    private TieringSourceEnumeratorState deserializeV1(byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        boolean hasLeaseId = in.readBoolean();
        String leaseId = hasLeaseId ? in.readUTF() : null;
        return new TieringSourceEnumeratorState(leaseId);
    }
}
