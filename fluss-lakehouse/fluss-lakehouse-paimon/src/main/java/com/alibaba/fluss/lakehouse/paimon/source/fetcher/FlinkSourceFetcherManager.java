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

package com.alibaba.fluss.lakehouse.paimon.source.fetcher;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.lakehouse.paimon.source.reader.FlinkSourceSplitReader;
import com.alibaba.fluss.lakehouse.paimon.source.reader.MultiplexCdcRecordAndPos;
import com.alibaba.fluss.lakehouse.paimon.source.split.SourceSplitBase;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherTask;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The SplitFetcherManager for Fluss source. This class is needed to help remove the partition
 * buckets to read inside the {@link FlinkSourceSplitReader}.
 */
@Internal
public class FlinkSourceFetcherManager
        extends SingleThreadFetcherManager<MultiplexCdcRecordAndPos, SourceSplitBase> {

    /**
     * Creates a new SplitFetcherManager with a single I/O threads.
     *
     * @param elementsQueue The queue that is used to hand over data from the I/O thread (the
     *     fetchers) to the reader (which emits the records and book-keeps the state. This must be
     *     the same queue instance that is also passed to the {@link SourceReaderBase}.
     * @param splitReaderSupplier The factory for the split reader that connects to the source
     *     system.
     * @param splitFinishedHook Hook for handling finished splits in split fetchers.
     */
    public FlinkSourceFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<MultiplexCdcRecordAndPos>>
                    elementsQueue,
            Supplier<SplitReader<MultiplexCdcRecordAndPos, SourceSplitBase>> splitReaderSupplier,
            Consumer<Collection<String>> splitFinishedHook) {
        super(elementsQueue, splitReaderSupplier, new Configuration(), splitFinishedHook);
    }

    public void removeTables(
            Map<Long, TablePath> removeTables,
            Consumer<Set<TableBucket>> unsubscribeTableBucketsCallback) {
        Function<FlinkSourceSplitReader, Set<TableBucket>> removeBucketFunc =
                (sourceSplitReader) -> sourceSplitReader.removeTables(removeTables);
        enqueueRemoveTableBucketTask(removeBucketFunc, unsubscribeTableBucketsCallback);
    }

    public void removePartitions(
            Map<Long, Map<Long, String>> removedPartitionsByTableId,
            Consumer<Set<TableBucket>> unsubscribeTableBucketsCallback) {
        Function<FlinkSourceSplitReader, Set<TableBucket>> removeBucketFunc =
                (sourceSplitReader) ->
                        sourceSplitReader.removePartitions(removedPartitionsByTableId);
        enqueueRemoveTableBucketTask(removeBucketFunc, unsubscribeTableBucketsCallback);
    }

    public void enqueueRemoveTableBucketTask(
            Function<FlinkSourceSplitReader, Set<TableBucket>> removeBucketFunc,
            Consumer<Set<TableBucket>> unsubscribeTableBucketsCallback) {
        SplitFetcher<MultiplexCdcRecordAndPos, SourceSplitBase> splitFetcher = fetchers.get(0);
        if (splitFetcher != null) {
            // The fetcher thread is still running. This should be the majority of the cases.
            enqueueRemoveTableBucketTask(
                    splitFetcher, removeBucketFunc, unsubscribeTableBucketsCallback);
        } else {
            splitFetcher = createSplitFetcher();
            enqueueRemoveTableBucketTask(
                    splitFetcher, removeBucketFunc, unsubscribeTableBucketsCallback);
            startFetcher(splitFetcher);
        }
    }

    private void enqueueRemoveTableBucketTask(
            SplitFetcher<MultiplexCdcRecordAndPos, SourceSplitBase> splitFetcher,
            Function<FlinkSourceSplitReader, Set<TableBucket>> removeBucketFunc,
            Consumer<Set<TableBucket>> unsubscribeTableBucketsCallback) {
        FlinkSourceSplitReader sourceSplitReader =
                (FlinkSourceSplitReader) splitFetcher.getSplitReader();
        splitFetcher.enqueueTask(
                new SplitFetcherTask() {
                    @Override
                    public boolean run() {
                        Set<TableBucket> unsubscribedBuckets =
                                removeBucketFunc.apply(sourceSplitReader);
                        unsubscribeTableBucketsCallback.accept(unsubscribedBuckets);
                        return true;
                    }

                    @Override
                    public void wakeUp() {}
                });
    }
}
