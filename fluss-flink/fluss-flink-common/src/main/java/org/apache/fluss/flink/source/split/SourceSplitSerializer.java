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

package org.apache.fluss.flink.source.split;

import org.apache.fluss.flink.lake.LakeSplitSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.fluss.flink.source.split.LogSplit.NO_STOPPING_OFFSET;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A serializer for the {@link SourceSplitBase}. */
public class SourceSplitSerializer implements SimpleVersionedSerializer<SourceSplitBase> {

    private static final int VERSION_0 = 0;
    // Version 1 adds backlogMarkedOffset/backlogMarkedStoppingOffset for
    // LogSplit/HybridSnapshotLogSplit.
    private static final int VERSION_1 = 1;

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final byte HYBRID_SNAPSHOT_SPLIT_FLAG = 1;
    private static final byte LOG_SPLIT_FLAG = 2;

    private static final int CURRENT_VERSION = VERSION_1;

    @Nullable private final LakeSource<LakeSplit> lakeSource;

    public SourceSplitSerializer(LakeSource<LakeSplit> lakeSource) {
        this.lakeSource = lakeSource;
    }

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
                out.writeLong(hybridSnapshotLogSplit.getSnapshotId());
                out.writeLong(hybridSnapshotLogSplit.recordsToSkip());
                out.writeBoolean(hybridSnapshotLogSplit.isSnapshotFinished());
                out.writeLong(hybridSnapshotLogSplit.getLogStartingOffset());
                // V1 field
                out.writeLong(
                        hybridSnapshotLogSplit
                                .getBacklogMarkedStoppingOffset()
                                .orElse(NO_STOPPING_OFFSET));
            } else {
                LogSplit logSplit = split.asLogSplit();
                out.writeLong(logSplit.getStartingOffset());
                out.writeLong(logSplit.getStoppingOffset().orElse(NO_STOPPING_OFFSET));
                // V1 field
                out.writeLong(logSplit.getBacklogMarkedOffset().orElse(NO_STOPPING_OFFSET));
            }
        } else {
            LakeSplitSerializer lakeSplitSerializer =
                    new LakeSplitSerializer(checkNotNull(lakeSource).getSplitSerializer());
            lakeSplitSerializer.serialize(out, split);
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
        if (version > VERSION_1) {
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

            if (version > VERSION_0) {
                long backlogMarkedStoppingOffset = in.readLong();
                return new HybridSnapshotLogSplit(
                        tableBucket,
                        partitionName,
                        snapshotId,
                        recordsToSkip,
                        isSnapshotFinished,
                        logStartingOffset,
                        backlogMarkedStoppingOffset);
            } else {
                return new HybridSnapshotLogSplit(
                        tableBucket,
                        partitionName,
                        snapshotId,
                        recordsToSkip,
                        isSnapshotFinished,
                        logStartingOffset);
            }

        } else if (splitKind == LOG_SPLIT_FLAG) {
            long startingOffset = in.readLong();
            long stoppingOffset = in.readLong();

            if (version > VERSION_0) {
                long backlogMarkedOffset = in.readLong();
                return new LogSplit(
                        tableBucket,
                        partitionName,
                        startingOffset,
                        stoppingOffset,
                        backlogMarkedOffset);
            } else {
                return new LogSplit(tableBucket, partitionName, startingOffset, stoppingOffset);
            }

        } else {
            LakeSplitSerializer lakeSplitSerializer =
                    new LakeSplitSerializer(checkNotNull(lakeSource).getSplitSerializer());
            return lakeSplitSerializer.deserialize(splitKind, tableBucket, partitionName, in);
        }
    }
}
