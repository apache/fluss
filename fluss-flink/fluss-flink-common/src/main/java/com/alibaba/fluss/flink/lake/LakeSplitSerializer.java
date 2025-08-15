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

package com.alibaba.fluss.flink.lake;

import com.alibaba.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import com.alibaba.fluss.flink.lake.split.LakeSnapshotSplit;
import com.alibaba.fluss.flink.source.split.LogSplit;
import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.lake.source.LakeSplit;
import com.alibaba.fluss.metadata.TableBucket;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit.LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND;
import static com.alibaba.fluss.flink.lake.split.LakeSnapshotSplit.LAKE_SNAPSHOT_SPLIT_KIND;

/** A serializer for lake split. */
public class LakeSplitSerializer {

    private final SimpleVersionedSerializer<LakeSplit> sourceSplitSerializer;

    public LakeSplitSerializer(SimpleVersionedSerializer<LakeSplit> sourceSplitSerializer) {
        this.sourceSplitSerializer = sourceSplitSerializer;
    }

    public void serialize(DataOutputSerializer out, SourceSplitBase split) throws IOException {
        if (split instanceof LakeSnapshotSplit) {
            byte[] serializeBytes =
                    sourceSplitSerializer.serialize(((LakeSnapshotSplit) split).getLakeSplit());
            out.writeInt(serializeBytes.length);
            out.write(serializeBytes);
        } else if (split instanceof LakeSnapshotAndFlussLogSplit) {
            // writing file store source split
            LakeSnapshotAndFlussLogSplit lakeSnapshotAndFlussLogSplit =
                    ((LakeSnapshotAndFlussLogSplit) split);
            List<LakeSplit> lakeSplits = lakeSnapshotAndFlussLogSplit.getLakeSplits();
            if (lakeSplits == null) {
                // no snapshot data for the bucket
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeInt(lakeSplits.size());
                for (LakeSplit lakeSplit : lakeSplits) {
                    byte[] serializeBytes = sourceSplitSerializer.serialize(lakeSplit);
                    out.writeInt(serializeBytes.length);
                    out.write(serializeBytes);
                }
            }
            // writing starting/stopping offset
            out.writeLong(lakeSnapshotAndFlussLogSplit.getStartingOffset());
            out.writeLong(
                    lakeSnapshotAndFlussLogSplit
                            .getStoppingOffset()
                            .orElse(LogSplit.NO_STOPPING_OFFSET));
            out.writeLong(lakeSnapshotAndFlussLogSplit.getRecordsToSkip());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported split type: " + split.getClass().getName());
        }
    }

    public SourceSplitBase deserialize(
            byte splitKind,
            TableBucket tableBucket,
            @Nullable String partition,
            DataInputDeserializer input)
            throws IOException {
        if (splitKind == LAKE_SNAPSHOT_SPLIT_KIND) {
            byte[] serializeBytes = new byte[input.readInt()];
            input.read(serializeBytes);
            LakeSplit fileStoreSourceSplit =
                    sourceSplitSerializer.deserialize(
                            sourceSplitSerializer.getVersion(), serializeBytes);
            return new LakeSnapshotSplit(tableBucket, partition, fileStoreSourceSplit);
        } else if (splitKind == LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND) {
            List<LakeSplit> lakeSplits = null;
            if (input.readBoolean()) {
                int lakeSplitSize = input.readInt();
                lakeSplits = new ArrayList<>(lakeSplitSize);
                for (int i = 0; i < lakeSplitSize; i++) {
                    byte[] serializeBytes = new byte[input.readInt()];
                    input.read(serializeBytes);
                    lakeSplits.add(
                            sourceSplitSerializer.deserialize(
                                    sourceSplitSerializer.getVersion(), serializeBytes));
                }
            }
            long startingOffset = input.readLong();
            long stoppingOffset = input.readLong();
            long recordsToSkip = input.readLong();
            return new LakeSnapshotAndFlussLogSplit(
                    tableBucket,
                    partition,
                    lakeSplits,
                    startingOffset,
                    stoppingOffset,
                    recordsToSkip);
        } else {
            throw new UnsupportedOperationException("Unsupported split kind: " + splitKind);
        }
    }
}
