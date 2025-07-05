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

package org.apache.fluss.flink.tiering.source.split;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/** A serializer for the {@link TieringSplit}. */
public class TieringSplitSerializer implements SimpleVersionedSerializer<TieringSplit> {

    public static final TieringSplitSerializer INSTANCE = new TieringSplitSerializer();

    private static final int VERSION_0 = 0;

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final byte TIERING_SNAPSHOT_SPLIT_FLAG = 1;
    private static final byte TIERING_LOG_SPLIT_FLAG = 2;

    private static final int CURRENT_VERSION = VERSION_0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(TieringSplit split) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();

        byte splitKind = split.splitKind();
        out.writeByte(splitKind);

        // write table path
        TablePath tablePath = split.getTablePath();
        out.writeUTF(tablePath.getDatabaseName());
        out.writeUTF(tablePath.getTableName());

        // write table id
        TableBucket tableBucket = split.getTableBucket();
        out.writeLong(tableBucket.getTableId());

        // write bucket
        out.writeInt(tableBucket.getBucket());

        // write partition
        if (split.getTableBucket().getPartitionId() != null) {
            out.writeBoolean(true);
            out.writeLong(split.getTableBucket().getPartitionId());
            out.writeUTF(split.getPartitionName());
        } else {
            out.writeBoolean(false);
        }

        // write number of splits
        out.writeInt(split.getNumberOfSplits());
        if (split.isTieringSnapshotSplit()) {
            // Snapshot split
            TieringSnapshotSplit tieringSnapshotSplit = split.asTieringSnapshotSplit();
            // write snapshot id
            out.writeLong(tieringSnapshotSplit.getSnapshotId());
            // write log offset of snapshot
            out.writeLong(tieringSnapshotSplit.getLogOffsetOfSnapshot());
        } else {
            // Log split
            TieringLogSplit tieringLogSplit = split.asTieringLogSplit();
            // write starting offset
            out.writeLong(tieringLogSplit.getStartingOffset());
            // write stopping offset
            out.writeLong(tieringLogSplit.getStoppingOffset());
        }

        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public TieringSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION_0) {
            throw new IOException("Unknown version " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        byte splitKind = in.readByte();

        // deserialize table path
        String databaseName = in.readUTF();
        String tableName = in.readUTF();
        TablePath tablePath = new TablePath(databaseName, tableName);

        // deserialize table id
        long tableId = in.readLong();

        // deserialize table bucket
        int bucketId = in.readInt();

        // deserialize table partition
        Long partitionId = null;
        String partitionName = null;
        if (in.readBoolean()) {
            partitionId = in.readLong();
            partitionName = in.readUTF();
        }
        TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);

        // deserialize number of splits
        int numberOfSplits = in.readInt();

        if (splitKind == TIERING_SNAPSHOT_SPLIT_FLAG) {
            // deserialize snapshot id
            long snapshotId = in.readLong();
            // deserialize log offset of snapshot
            long logOffsetOfSnapshot = in.readLong();
            return new TieringSnapshotSplit(
                    tablePath,
                    tableBucket,
                    partitionName,
                    snapshotId,
                    logOffsetOfSnapshot,
                    numberOfSplits);
        } else {
            // deserialize starting offset
            long startingOffset = in.readLong();
            // deserialize starting offset
            long stoppingOffset = in.readLong();
            return new TieringLogSplit(
                    tablePath,
                    tableBucket,
                    partitionName,
                    startingOffset,
                    stoppingOffset,
                    numberOfSplits);
        }
    }
}
