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

package com.alibaba.fluss.lake.paimon.flink;

import com.alibaba.fluss.flink.lake.hybrid.LakeSplitAssigner;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.FileStoreSourceSplitGenerator;
import org.apache.paimon.flink.source.PendingSplitsCheckpoint;
import org.apache.paimon.flink.source.StaticFileStoreSource;
import org.apache.paimon.flink.source.StaticFileStoreSplitEnumerator;
import org.apache.paimon.flink.source.assigners.PreAssignSplitAssigner;
import org.apache.paimon.flink.source.assigners.SplitAssigner;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** FlinkStaticFIleStoreSource is a flink source for paimon table. */
public class FlinkStaticFIleStoreSource extends StaticFileStoreSource {

    private static final long serialVersionUID = 3L;

    private final int splitBatchSize;

    private final LakeSplitAssigner lakeSplitAssigner;

    public FlinkStaticFIleStoreSource(
            ReadBuilder readBuilder, int splitBatchSize, LakeSplitAssigner lakeSplitAssigner) {
        super(
                readBuilder,
                null,
                splitBatchSize,
                // don't care about assign mode
                null);
        this.splitBatchSize = splitBatchSize;
        this.lakeSplitAssigner = lakeSplitAssigner;
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            PendingSplitsCheckpoint checkpoint) {
        Collection<FileStoreSourceSplit> splits =
                checkpoint == null ? getSplits(context) : checkpoint.splits();
        SplitAssigner splitAssigner =
                new PreAssignSplitAssigner(splitBatchSize, context, Collections.emptyList());
        for (FileStoreSourceSplit split : splits) {
            Integer suggestedTask = assignSuggestedTask(split, context);
            if (suggestedTask != null) {
                splitAssigner.addSplit(suggestedTask, split);
            }
        }
        return new StaticFileStoreSplitEnumerator(context, null, splitAssigner, null);
    }

    private List<FileStoreSourceSplit> getSplits(SplitEnumeratorContext context) {
        FileStoreSourceSplitGenerator splitGenerator = new FileStoreSourceSplitGenerator();
        TableScan scan = readBuilder.newScan();
        // register scan metrics
        if (context.metricGroup() != null) {
            ((InnerTableScan) scan)
                    .withMetricRegistry(new FlinkMetricRegistry(context.metricGroup()));
        }
        return splitGenerator.createSplits(scan.plan());
    }

    @Nullable
    private Integer assignSuggestedTask(
            FileStoreSourceSplit split, SplitEnumeratorContext<FileStoreSourceSplit> context) {
        DataSplit dataSplit = ((DataSplit) split.split());
        String partition = null;
        BinaryRow partitionRow = dataSplit.partition();
        if (partitionRow.getFieldCount() > 0) {
            partition = partitionRow.getString(0).toString();
        }
        return lakeSplitAssigner.assignSplit(
                partition, dataSplit.bucket(), context.currentParallelism());
    }
}
