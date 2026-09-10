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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.KvSnapshotFileMetadata;
import org.apache.fluss.metadata.KvSnapshotFileMetadataJsonSerde;
import org.apache.fluss.server.kv.autoinc.AutoIncIDRange;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Adapter between server snapshot lifecycle objects and standard snapshot file metadata. */
public class CompletedSnapshotJsonSerde
        implements JsonSerializer<CompletedSnapshot>, JsonDeserializer<CompletedSnapshot> {

    public static final CompletedSnapshotJsonSerde INSTANCE = new CompletedSnapshotJsonSerde();

    /** Creates a serde for completed snapshots. */
    public CompletedSnapshotJsonSerde() {}

    @Override
    public void serialize(CompletedSnapshot completedSnapshot, JsonGenerator generator)
            throws IOException {
        KvSnapshotFileMetadataJsonSerde.INSTANCE.serialize(
                toFileMetadata(completedSnapshot), generator);
    }

    @Override
    public CompletedSnapshot deserialize(JsonNode node) {
        return toCompletedSnapshot(KvSnapshotFileMetadataJsonSerde.INSTANCE.deserialize(node));
    }

    /** Serializes a completed snapshot to standard metadata JSON bytes. */
    public static byte[] toJson(CompletedSnapshot completedSnapshot) {
        return JsonSerdeUtils.writeValueAsBytes(completedSnapshot, INSTANCE);
    }

    /** Deserializes standard metadata JSON bytes into a completed server snapshot. */
    public static CompletedSnapshot fromJson(byte[] json) {
        return JsonSerdeUtils.readValue(json, INSTANCE);
    }

    private static KvSnapshotFileMetadata toFileMetadata(CompletedSnapshot completedSnapshot) {
        KvSnapshotHandle snapshotHandle = completedSnapshot.getKvSnapshotHandle();
        List<KvSnapshotFileMetadata.AutoIncrementRange> ranges = null;
        if (completedSnapshot.getAutoIncIDRanges() != null) {
            ranges = new ArrayList<>();
            for (AutoIncIDRange range : completedSnapshot.getAutoIncIDRanges()) {
                ranges.add(
                        new KvSnapshotFileMetadata.AutoIncrementRange(
                                range.getColumnId(), range.getStart(), range.getEnd()));
            }
        }
        return new KvSnapshotFileMetadata(
                completedSnapshot.getTableBucket(),
                completedSnapshot.getSnapshotID(),
                completedSnapshot.getSnapshotLocation().toString(),
                toFileHandles(snapshotHandle.getSharedKvFileHandles()),
                toFileHandles(snapshotHandle.getPrivateFileHandles()),
                snapshotHandle.getIncrementalSize(),
                completedSnapshot.getLogOffset(),
                completedSnapshot.getRowCount(),
                ranges);
    }

    /** Converts already-parsed standard metadata into a completed server snapshot. */
    public static CompletedSnapshot toCompletedSnapshot(KvSnapshotFileMetadata metadata) {
        List<AutoIncIDRange> ranges = null;
        if (metadata.getAutoIncrementRanges() != null) {
            ranges = new ArrayList<>();
            for (KvSnapshotFileMetadata.AutoIncrementRange range :
                    metadata.getAutoIncrementRanges()) {
                ranges.add(
                        new AutoIncIDRange(range.getColumnId(), range.getStart(), range.getEnd()));
            }
        }
        return new CompletedSnapshot(
                metadata.getTableBucket(),
                metadata.getSnapshotId(),
                new FsPath(metadata.getSnapshotLocation()),
                KvSnapshotHandle.restore(
                        toServerFileHandles(metadata.getSharedFiles()),
                        toServerFileHandles(metadata.getPrivateFiles()),
                        metadata.getIncrementalSize()),
                metadata.getLogOffset(),
                metadata.getRowCount(),
                ranges);
    }

    private static List<KvSnapshotFileMetadata.FileHandle> toFileHandles(
            List<KvFileHandleAndLocalPath> serverHandles) {
        List<KvSnapshotFileMetadata.FileHandle> handles = new ArrayList<>(serverHandles.size());
        for (KvFileHandleAndLocalPath serverHandle : serverHandles) {
            handles.add(
                    new KvSnapshotFileMetadata.FileHandle(
                            serverHandle.getKvFileHandle().getFilePath(),
                            serverHandle.getKvFileHandle().getSize(),
                            serverHandle.getLocalPath()));
        }
        return handles;
    }

    private static List<KvFileHandleAndLocalPath> toServerFileHandles(
            List<KvSnapshotFileMetadata.FileHandle> metadataHandles) {
        List<KvFileHandleAndLocalPath> handles = new ArrayList<>(metadataHandles.size());
        for (KvSnapshotFileMetadata.FileHandle metadataHandle : metadataHandles) {
            handles.add(
                    KvFileHandleAndLocalPath.of(
                            new KvFileHandle(metadataHandle.getPath(), metadataHandle.getSize()),
                            metadataHandle.getLocalPath()));
        }
        return handles;
    }
}
