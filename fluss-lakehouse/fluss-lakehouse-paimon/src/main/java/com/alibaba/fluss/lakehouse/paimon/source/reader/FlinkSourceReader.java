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

package com.alibaba.fluss.lakehouse.paimon.source.reader;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lakehouse.paimon.record.MultiplexCdcRecord;
import com.alibaba.fluss.lakehouse.paimon.source.emitter.FlinkRecordEmitter;
import com.alibaba.fluss.lakehouse.paimon.source.event.PartitionsRemovedEvent;
import com.alibaba.fluss.lakehouse.paimon.source.event.TableBucketsUnsubscribedEvent;
import com.alibaba.fluss.lakehouse.paimon.source.event.TablesRemovedEvent;
import com.alibaba.fluss.lakehouse.paimon.source.fetcher.FlinkSourceFetcherManager;
import com.alibaba.fluss.lakehouse.paimon.source.metrics.FlinkSourceReaderMetrics;
import com.alibaba.fluss.lakehouse.paimon.source.split.HybridSnapshotLogSplit;
import com.alibaba.fluss.lakehouse.paimon.source.split.HybridSnapshotLogSplitState;
import com.alibaba.fluss.lakehouse.paimon.source.split.LogSplit;
import com.alibaba.fluss.lakehouse.paimon.source.split.LogSplitState;
import com.alibaba.fluss.lakehouse.paimon.source.split.SourceSplitBase;
import com.alibaba.fluss.lakehouse.paimon.source.split.SourceSplitState;
import com.alibaba.fluss.metadata.TableBucket;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/** The source reader for Fluss. */
public class FlinkSourceReader
        extends SingleThreadMultiplexSourceReaderBase<
                MultiplexCdcRecordAndPos, MultiplexCdcRecord, SourceSplitBase, SourceSplitState> {

    public FlinkSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<MultiplexCdcRecordAndPos>>
                    elementsQueue,
            Configuration flussConfig,
            SourceReaderContext context,
            FlinkSourceReaderMetrics flinkSourceReaderMetrics) {
        super(
                elementsQueue,
                new FlinkSourceFetcherManager(
                        elementsQueue,
                        () -> new FlinkSourceSplitReader(flussConfig, flinkSourceReaderMetrics),
                        (ignore) -> {}),
                new FlinkRecordEmitter(),
                context.getConfiguration(),
                context);
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof TablesRemovedEvent) {
            TablesRemovedEvent tablesRemovedEvent = (TablesRemovedEvent) sourceEvent;
            Consumer<Set<TableBucket>> unsubscribeTableBucketsCallback =
                    (unsubscribedTableBuckets) -> {
                        // send remove partitions ack event to coordinator
                        context.sendSourceEventToCoordinator(
                                new TableBucketsUnsubscribedEvent(unsubscribedTableBuckets));
                    };
            ((FlinkSourceFetcherManager) splitFetcherManager)
                    .removeTables(
                            tablesRemovedEvent.getRemovedTables(), unsubscribeTableBucketsCallback);
        } else if (sourceEvent instanceof PartitionsRemovedEvent) {
            PartitionsRemovedEvent partitionsRemovedEvent = (PartitionsRemovedEvent) sourceEvent;
            Consumer<Set<TableBucket>> unsubscribeTableBucketsCallback =
                    (unsubscribedTableBuckets) -> {
                        // send remove partitions ack event to coordinator
                        context.sendSourceEventToCoordinator(
                                new TableBucketsUnsubscribedEvent(unsubscribedTableBuckets));
                    };
            ((FlinkSourceFetcherManager) splitFetcherManager)
                    .removePartitions(
                            partitionsRemovedEvent.getRemovedPartitionsByTableId(),
                            unsubscribeTableBucketsCallback);
        }
    }

    @Override
    protected void onSplitFinished(Map<String, SourceSplitState> map) {
        // do nothing
    }

    @Override
    protected SourceSplitState initializedState(SourceSplitBase split) {
        if (split instanceof HybridSnapshotLogSplit) {
            return new HybridSnapshotLogSplitState((HybridSnapshotLogSplit) split);
        } else if (split instanceof LogSplit) {
            return new LogSplitState(split.asLogSplit());
        } else {
            throw new UnsupportedOperationException("Unsupported split type: " + split);
        }
    }

    @Override
    protected SourceSplitBase toSplitType(String splitId, SourceSplitState splitState) {
        return splitState.toSourceSplit();
    }
}
