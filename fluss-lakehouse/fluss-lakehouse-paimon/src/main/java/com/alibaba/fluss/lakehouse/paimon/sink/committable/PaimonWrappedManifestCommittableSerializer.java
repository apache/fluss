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

import com.alibaba.fluss.metadata.TableBucket;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.serializer.VersionedSerializer;
import org.apache.paimon.flink.sink.WrappedManifestCommittableSerializer;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.manifest.WrappedManifestCommittable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Serializer of {@link PaimonWrapperManifestCommittable} . */
public class PaimonWrappedManifestCommittableSerializer
        implements VersionedSerializer<PaimonWrapperManifestCommittable> {

    protected static final int CURRENT_VERSION = 1;

    private final WrappedManifestCommittableSerializer wrappedManifestCommittableSerializer;

    public PaimonWrappedManifestCommittableSerializer(
            WrappedManifestCommittableSerializer wrappedManifestCommittableSerializer) {
        this.wrappedManifestCommittableSerializer = wrappedManifestCommittableSerializer;
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PaimonWrapperManifestCommittable paimonWrapperManifestCommittable)
            throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out)) {
            // write table id by table path
            byte[] tableIdByIdentifierBytes =
                    convertIdByIdentifierToBytes(
                            paimonWrapperManifestCommittable.getTableIdByIdentifier());
            view.writeInt(tableIdByIdentifierBytes.length);
            view.write(tableIdByIdentifierBytes);

            // write partition name by id
            byte[] partitionNameByBytes =
                    convertPartitionNameByBytes(
                            paimonWrapperManifestCommittable.getPartitionNameById());
            view.writeInt(partitionNameByBytes.length);
            view.write(partitionNameByBytes);

            // write log end offset
            byte[] tableLogEndOffsetBytes =
                    convertLogOffsetsToBytes(
                            paimonWrapperManifestCommittable.getTableLogEndOffset());
            view.writeInt(tableLogEndOffsetBytes.length);
            view.write(tableLogEndOffsetBytes);

            byte[] wrappedManifestCommittableBytes =
                    wrappedManifestCommittableSerializer.serialize(
                            paimonWrapperManifestCommittable);
            view.writeInt(wrappedManifestCommittableBytes.length);
            view.write(wrappedManifestCommittableBytes);
            return out.toByteArray();
        }
    }

    @Override
    public PaimonWrapperManifestCommittable deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting PaimonWrapperManifestCommittable version to be "
                            + CURRENT_VERSION
                            + ", but found "
                            + version
                            + ".\nPaimonWrapperManifestCommittable is not a compatible data structure. "
                            + "Please restart the job afresh (do not recover from savepoint).");
        }

        try (ByteArrayInputStream in = new ByteArrayInputStream(serialized);
                DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in)) {

            // deserialize table id by table path
            int tableIdByIdentifierLen = view.readInt();
            byte[] tableIdByIdentifierBytes = new byte[tableIdByIdentifierLen];
            view.read(tableIdByIdentifierBytes);
            Map<Identifier, Long> tableIdByIdentifiers =
                    fromBytesToTableIdByIdentifier(tableIdByIdentifierBytes);

            // deserialize partition name by id
            int partitionNameByIdLen = view.readInt();
            byte[] partitionNameByIdBytes = new byte[partitionNameByIdLen];
            view.read(partitionNameByIdBytes);
            Map<Long, String> partitionNameById = fromBytesToPartitionById(partitionNameByIdBytes);

            int tableEndLogOffsetLen = view.readInt();
            byte[] tableEndLogOffsetBytes = new byte[tableEndLogOffsetLen];
            view.read(tableEndLogOffsetBytes);
            Map<Long, Map<TableBucket, Long>> tableEndLogOffsets =
                    fromBytesToLogOffset(tableEndLogOffsetBytes);

            int wrappedManifestCommittableLen = view.readInt();
            byte[] wrappedManifestCommittableBytes = new byte[wrappedManifestCommittableLen];
            view.read(wrappedManifestCommittableBytes);

            WrappedManifestCommittable wrappedManifestCommittable =
                    wrappedManifestCommittableSerializer.deserialize(
                            version, wrappedManifestCommittableBytes);

            return new PaimonWrapperManifestCommittable(
                    wrappedManifestCommittable,
                    tableIdByIdentifiers,
                    partitionNameById,
                    tableEndLogOffsets);
        }
    }

    private byte[] convertIdByIdentifierToBytes(Map<Identifier, Long> tableIdByIdentifier)
            throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out)) {
            view.writeInt(tableIdByIdentifier.size());
            for (Map.Entry<Identifier, Long> tableIdByIdentifierEntry :
                    tableIdByIdentifier.entrySet()) {
                view.writeUTF(tableIdByIdentifierEntry.getKey().getFullName());
                view.writeLong(tableIdByIdentifierEntry.getValue());
            }
            return out.toByteArray();
        }
    }

    private byte[] convertPartitionNameByBytes(Map<Long, String> partitionNameById)
            throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out)) {
            view.writeInt(partitionNameById.size());

            for (Map.Entry<Long, String> partitionNameByIdEntry : partitionNameById.entrySet()) {
                view.writeLong(partitionNameByIdEntry.getKey());
                view.writeUTF(partitionNameByIdEntry.getValue());
            }
            return out.toByteArray();
        }
    }

    private byte[] convertLogOffsetsToBytes(Map<Long, Map<TableBucket, Long>> tableLogOffsets)
            throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out)) {
            view.writeInt(tableLogOffsets.size());

            for (Map.Entry<Long, Map<TableBucket, Long>> tableLogOffset :
                    tableLogOffsets.entrySet()) {
                long tableId = tableLogOffset.getKey();
                view.writeLong(tableId);

                byte[] bucketLogOffsetBytes =
                        BucketOffsetsSerdeUtils.serialize(tableLogOffset.getValue());
                view.writeInt(bucketLogOffsetBytes.length);
                view.write(bucketLogOffsetBytes);
            }
            return out.toByteArray();
        }
    }

    private Map<Identifier, Long> fromBytesToTableIdByIdentifier(byte[] tableIdByIdentifierBytes)
            throws IOException {
        try (ByteArrayInputStream in = new ByteArrayInputStream(tableIdByIdentifierBytes);
                DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in)) {
            int tableIdByIdentifierSize = view.readInt();
            Map<Identifier, Long> tableIdByIdentifiers = new HashMap<>(tableIdByIdentifierSize);
            for (int i = 0; i < tableIdByIdentifierSize; i++) {
                tableIdByIdentifiers.put(Identifier.fromString(view.readUTF()), view.readLong());
            }
            return tableIdByIdentifiers;
        }
    }

    private Map<Long, Map<TableBucket, Long>> fromBytesToLogOffset(byte[] tableLogOffsetsBytes)
            throws IOException {
        try (ByteArrayInputStream in = new ByteArrayInputStream(tableLogOffsetsBytes);
                DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in)) {
            int tableLogOffsetsSize = view.readInt();
            Map<Long, Map<TableBucket, Long>> tableLogOffsets = new HashMap<>(tableLogOffsetsSize);
            for (int i = 0; i < tableLogOffsetsSize; i++) {
                long tableId = view.readLong();
                int bucketLogOffsetBytesLen = view.readInt();
                byte[] bucketLogOffsetBytes = new byte[bucketLogOffsetBytesLen];
                view.read(bucketLogOffsetBytes);
                tableLogOffsets.put(
                        tableId,
                        BucketOffsetsSerdeUtils.deserialize(tableId, bucketLogOffsetBytes));
            }
            return tableLogOffsets;
        }
    }

    private Map<Long, String> fromBytesToPartitionById(byte[] partitionByIdBytes)
            throws IOException {
        try (ByteArrayInputStream in = new ByteArrayInputStream(partitionByIdBytes);
                DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in)) {
            int partitionByIdSize = view.readInt();
            Map<Long, String> partitionById = new HashMap<>(partitionByIdSize);
            for (int i = 0; i < partitionByIdSize; i++) {
                partitionById.put(view.readLong(), view.readUTF());
            }
            return partitionById;
        }
    }
}
