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

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.flink.lake.reader.LakeSnapshotAndLogSplitScanner;
import com.alibaba.fluss.flink.lake.reader.LakeSnapshotScanner;
import com.alibaba.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import com.alibaba.fluss.flink.lake.split.LakeSnapshotSplit;
import com.alibaba.fluss.flink.source.reader.BoundedSplitReader;
import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.lake.source.LakeSource;
import com.alibaba.fluss.lake.source.LakeSplit;

import javax.annotation.Nullable;

import java.util.Queue;

/** A generator to generate reader for lake split. */
public class LakeSplitReaderGenerator {

    private final Table table;

    private final @Nullable int[] projectedFields;
    private final @Nullable LakeSource<LakeSplit> lakeSource;

    public LakeSplitReaderGenerator(
            Table table,
            @Nullable int[] projectedFields,
            @Nullable LakeSource<LakeSplit> lakeSource) {
        this.table = table;
        this.projectedFields = projectedFields;
        this.lakeSource = lakeSource;
    }

    public void addSplit(SourceSplitBase split, Queue<SourceSplitBase> boundedSplits) {
        if (split instanceof LakeSnapshotSplit) {
            boundedSplits.add(split);
        } else if (split instanceof LakeSnapshotAndFlussLogSplit) {
            boundedSplits.add(split);
        } else {
            throw new UnsupportedOperationException(
                    String.format("The split type of %s is not supported.", split.getClass()));
        }
    }

    public BoundedSplitReader getBoundedSplitScanner(SourceSplitBase split) {
        if (split instanceof LakeSnapshotSplit) {
            LakeSnapshotSplit lakeSnapshotSplit = (LakeSnapshotSplit) split;
            LakeSnapshotScanner lakeSnapshotScanner =
                    new LakeSnapshotScanner(lakeSource, lakeSnapshotSplit);
            return new BoundedSplitReader(
                    lakeSnapshotScanner, lakeSnapshotSplit.getRecordsToSplit());
        } else if (split instanceof LakeSnapshotAndFlussLogSplit) {
            LakeSnapshotAndFlussLogSplit lakeSnapshotAndFlussLogSplit =
                    (LakeSnapshotAndFlussLogSplit) split;
            LakeSnapshotAndLogSplitScanner lakeSnapshotAndLogSplitScanner =
                    new LakeSnapshotAndLogSplitScanner(
                            table, lakeSource, lakeSnapshotAndFlussLogSplit, projectedFields);
            return new BoundedSplitReader(
                    lakeSnapshotAndLogSplitScanner,
                    lakeSnapshotAndFlussLogSplit.getRecordsToSkip());
        } else {
            throw new UnsupportedOperationException(
                    String.format("The split type of %s is not supported.", split.getClass()));
        }
    }
}
