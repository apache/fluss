/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.paimon.sink.committable;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.LogOffsetCommittable;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.io.IOException;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Most is copied from Pamion's MultiTableCommittableSerializer. But can also serialize/deserialize
 * {@link FlussLogOffsetCommittable} which is used to notify Fluss the log offsets committed to
 * Paimon.
 */
public class PaimonMultiTableCommittableSerializer
        implements SimpleVersionedSerializer<MultiTableCommittable> {

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private final CommitMessageSerializer commitMessageSerializer;

    public PaimonMultiTableCommittableSerializer(CommitMessageSerializer commitMessageSerializer) {
        this.commitMessageSerializer = commitMessageSerializer;
    }

    @Override
    public int getVersion() {
        return 2;
    }

    @Override
    public byte[] serialize(MultiTableCommittable committable) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        // first serialize all metadata
        out.writeUTF(committable.getDatabase());
        out.writeUTF(committable.getTable());

        serializeCommittable(committable, out);
        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    private void serializeCommittable(MultiTableCommittable committable, DataOutputSerializer out)
            throws IOException {
        byte[] wrapped;
        int version;
        byte committableKind;
        Object wrappedCommittable = committable.wrappedCommittable();

        // check whether it is our custom FlussLogOffsetCommittable or not,
        // if is, set committableKind to -1
        if (wrappedCommittable instanceof FlussLogOffsetCommittable) {
            version = 1;
            wrapped = ((FlussLogOffsetCommittable) wrappedCommittable).toBytes();
            committableKind = -1;
        } else {
            switch (committable.kind()) {
                case FILE:
                    version = commitMessageSerializer.getVersion();
                    wrapped =
                            commitMessageSerializer.serialize(
                                    (CommitMessage) committable.wrappedCommittable());
                    break;
                case LOG_OFFSET:
                    version = 1;
                    wrapped = ((LogOffsetCommittable) committable.wrappedCommittable()).toBytes();
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported kind: " + committable.kind());
            }
            committableKind = committable.kind().toByteValue();
        }

        out.writeLong(committable.checkpointId());
        out.writeByte(committableKind);
        out.writeInt(wrapped.length);
        out.write(wrapped);
        out.writeInt(version);
    }

    @Override
    public MultiTableCommittable deserialize(int committableVersion, byte[] serialized)
            throws IOException {
        if (committableVersion != getVersion()) {
            throw new RuntimeException("Can not deserialize version: " + committableVersion);
        }

        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        // first deserialize all metadata
        String database = in.readUTF();
        String table = in.readUTF();
        Committable committable = deserializeCommittable(committableVersion, in);

        return MultiTableCommittable.fromCommittable(
                Identifier.create(database, table), committable);
    }

    private Committable deserializeCommittable(int committableVersion, DataInputDeserializer in)
            throws IOException {
        if (committableVersion != getVersion()) {
            throw new RuntimeException("Can not deserialize version: " + committableVersion);
        }

        // deserialize checkpointId/committableKind, wrappedCommittable and version
        long checkpointId = in.readLong();
        byte committableKind = in.readByte();

        int commitMessageSize = in.readInt();
        byte[] wrapped = new byte[commitMessageSize];
        in.read(wrapped);
        int version = in.readInt();

        Object wrappedCommittable;
        Committable.Kind kind;
        if (committableKind == -1) {
            // it's our custom FlussLogOffsetCommittable, set the kind to LOG_OFFSET
            kind = Committable.Kind.LOG_OFFSET;
            wrappedCommittable = FlussLogOffsetCommittable.fromBytes(wrapped);
        } else {
            kind = Committable.Kind.fromByteValue(committableKind);
            switch (kind) {
                case FILE:
                    wrappedCommittable = commitMessageSerializer.deserialize(version, wrapped);
                    break;
                case LOG_OFFSET:
                    wrappedCommittable = LogOffsetCommittable.fromBytes(wrapped);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported kind: " + kind);
            }
        }
        return new Committable(checkpointId, kind, wrappedCommittable);
    }
}
