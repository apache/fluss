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

package com.alibaba.fluss.flink.source.split;

import com.alibaba.fluss.flink.lakehouse.LakeSplitSerializer;
import com.alibaba.fluss.metadata.TableBucket;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/** A serializer for the {@link SourceSplitBase}. */
public class SourceSplitSerializer implements SimpleVersionedSerializer<SourceSplitBase> {

    public static final SourceSplitSerializer INSTANCE = new SourceSplitSerializer();

    private static final int VERSION_0 = 0;

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final byte HYBRID_SNAPSHOT_SPLIT_FLAG = 1;
    private static final byte LOG_SPLIT_FLAG = 2;

    private static final int CURRENT_VERSION = VERSION_0;

    private LakeSplitSerializer lakeSplitSerializer;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(SourceSplitBase split) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        byte splitKind = split.splitKind();
        out.writeByte(splitKind);
        // write common part
        serializeSourceSplitBase(out, split);

        if (!split.isLakeSplit()) {
            if (split.isHybridSnapshotLogSplit()) {
                HybridSnapshotLogSplit hybridSnapshotLogSplit = split.asHybridSnapshotLogSplit();
                // write snapshot id
                out.writeLong(hybridSnapshotLogSplit.getSnapshotId());
                // write records to skip
                out.writeLong(hybridSnapshotLogSplit.recordsToSkip());
                // write is snapshot finished
                out.writeBoolean(hybridSnapshotLogSplit.isSnapshotFinished());
                // write log starting offset
                out.writeLong(hybridSnapshotLogSplit.getLogStartingOffset());
            } else {
                LogSplit logSplit = split.asLogSplit();
                // write starting offset
                out.writeLong(logSplit.getStartingOffset());
                // write stopping offset
                out.writeLong(logSplit.getStoppingOffset().orElse(LogSplit.NO_STOPPING_OFFSET));
            }
        } else {
            getLakeSplitSerializer().serialize(out, split);
        }

        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    private void serializeSourceSplitBase(DataOutputSerializer out, SourceSplitBase sourceSplitBase)
            throws IOException {
        // write bucket
        TableBucket tableBucket = sourceSplitBase.getTableBucket();
        out.writeLong(tableBucket.getTableId());
        // write partition
        if (sourceSplitBase.getTableBucket().getPartitionId() != null) {
            out.writeBoolean(true);
            out.writeLong(sourceSplitBase.getTableBucket().getPartitionId());
            out.writeUTF(sourceSplitBase.getPartitionName());
        } else {
            out.writeBoolean(false);
        }
        out.writeInt(tableBucket.getBucket());
    }

    @Override
    public SourceSplitBase deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION_0) {
            throw new IOException("Unknown version " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        byte splitKind = in.readByte();

        // deserialize split bucket
        long tableId = in.readLong();
        Long partitionId = null;
        String partitionName = null;
        if (in.readBoolean()) {
            partitionId = in.readLong();
            partitionName = in.readUTF();
        }
        int bucketId = in.readInt();
        TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);

        if (splitKind == HYBRID_SNAPSHOT_SPLIT_FLAG) {
            long snapshotId = in.readLong();
            long recordsToSkip = in.readLong();
            boolean isSnapshotFinished = in.readBoolean();
            long logStartingOffset = in.readLong();
            return new HybridSnapshotLogSplit(
                    tableBucket,
                    partitionName,
                    snapshotId,
                    recordsToSkip,
                    isSnapshotFinished,
                    logStartingOffset);
        } else if (splitKind == LOG_SPLIT_FLAG) {
            long startingOffset = in.readLong();
            long stoppingOffset = in.readLong();
            return new LogSplit(tableBucket, partitionName, startingOffset, stoppingOffset);
        } else {
            return getLakeSplitSerializer().deserialize(splitKind, tableBucket, partitionName, in);
        }
    }

    private LakeSplitSerializer getLakeSplitSerializer() {
        if (lakeSplitSerializer == null) {
            lakeSplitSerializer = new LakeSplitSerializer();
        }
        return lakeSplitSerializer;
    }
}
