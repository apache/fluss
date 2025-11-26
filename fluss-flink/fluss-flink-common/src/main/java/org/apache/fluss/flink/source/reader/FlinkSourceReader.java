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

package org.apache.fluss.flink.source.reader;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.adapter.SingleThreadMultiplexSourceReaderBaseAdapter;
import org.apache.fluss.flink.lake.LakeSplitStateInitializer;
import org.apache.fluss.flink.source.emitter.FlinkRecordEmitter;
import org.apache.fluss.flink.source.event.PartitionBucketsUnsubscribedEvent;
import org.apache.fluss.flink.source.event.PartitionsRemovedEvent;
import org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import org.apache.fluss.flink.source.reader.fetcher.FlinkSourceFetcherManager;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplitState;
import org.apache.fluss.flink.source.split.LogSplitState;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitState;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/** The source reader for Fluss. */
public class FlinkSourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBaseAdapter<
                RecordAndPos, OUT, SourceSplitBase, SourceSplitState> {

    public FlinkSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordAndPos>> elementsQueue,
            Configuration flussConfig,
            TablePath tablePath,
            RowType sourceOutputType,
            SourceReaderContext context,
            FlinkSourceReaderMetrics flinkSourceReaderMetrics,
            FlinkRecordEmitter<OUT> recordEmitter,
            LakeSource<LakeSplit> lakeSource) {
        super(
                elementsQueue,
                new FlinkSourceFetcherManager(
                        elementsQueue,
                        () ->
                                new FlinkSourceSplitReader(
                                        flussConfig,
                                        tablePath,
                                        sourceOutputType,
                                        flinkSourceReaderMetrics,
                                        lakeSource),
                        (ignore) -> {}),
                recordEmitter,
                context.getConfiguration(),
                context);
    }

    @Override
    protected void onSplitFinished(Map<String, SourceSplitState> map) {
        // do nothing
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof PartitionsRemovedEvent) {
            PartitionsRemovedEvent partitionsRemovedEvent = (PartitionsRemovedEvent) sourceEvent;
            Consumer<Set<TableBucket>> unsubscribeTableBucketsCallback =
                    (unsubscribedTableBuckets) -> {
                        // send remove partitions ack event to coordinator
                        context.sendSourceEventToCoordinator(
                                new PartitionBucketsUnsubscribedEvent(unsubscribedTableBuckets));
                    };
            ((FlinkSourceFetcherManager) splitFetcherManager)
                    .removePartitions(
                            partitionsRemovedEvent.getRemovedPartitions(),
                            unsubscribeTableBucketsCallback);
        }
    }

    @Override
    protected SourceSplitState initializedState(SourceSplitBase split) {
        if (split.isHybridSnapshotLogSplit()) {
            return new HybridSnapshotLogSplitState(split.asHybridSnapshotLogSplit());
        } else if (split.isLogSplit()) {
            return new LogSplitState(split.asLogSplit());
        } else if (split.isLakeSplit()) {
            return LakeSplitStateInitializer.initializedState(split);
        } else {
            throw new UnsupportedOperationException("Unsupported split type: " + split);
        }
    }

    @Override
    protected SourceSplitBase toSplitType(String splitId, SourceSplitState splitState) {
        return splitState.toSourceSplit();
    }
}
