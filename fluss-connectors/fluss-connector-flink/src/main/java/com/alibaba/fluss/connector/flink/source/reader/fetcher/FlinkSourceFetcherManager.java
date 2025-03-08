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

package com.alibaba.fluss.connector.flink.source.reader.fetcher;

import com.alibaba.fluss.connector.flink.source.reader.FlinkSourceSplitReader;
import com.alibaba.fluss.connector.flink.source.reader.RecordAndPos;
import com.alibaba.fluss.connector.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.metadata.TableBucket;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherTask;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The SplitFetcherManager for Fluss source. This class is needed to help remove the partition
 * buckets to read inside the {@link FlinkSourceSplitReader}.
 */
@Internal
public class FlinkSourceFetcherManager
        extends SingleThreadFetcherManager<RecordAndPos, SourceSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSourceFetcherManager.class);

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
            FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordAndPos>> elementsQueue,
            Supplier<SplitReader<RecordAndPos, SourceSplitBase>> splitReaderSupplier,
            Consumer<Collection<String>> splitFinishedHook) {
        super(elementsQueue, splitReaderSupplier, new Configuration(), splitFinishedHook);
    }

    public void removePartitions(
            Map<Long, String> removedPartitions,
            Consumer<Set<TableBucket>> unsubscribeTableBucketsCallback) {
        SplitFetcher<RecordAndPos, SourceSplitBase> splitFetcher = fetchers.get(0);
        if (splitFetcher != null) {
            // The fetcher thread is still running. This should be the majority of the cases.
            enqueuePartitionsRemovedTask(
                    splitFetcher, removedPartitions, unsubscribeTableBucketsCallback);
        } else {
            splitFetcher = createSplitFetcher();
            enqueuePartitionsRemovedTask(
                    splitFetcher, removedPartitions, unsubscribeTableBucketsCallback);
            startFetcher(splitFetcher);
        }
    }

    private void enqueuePartitionsRemovedTask(
            SplitFetcher<RecordAndPos, SourceSplitBase> splitFetcher,
            Map<Long, String> removedPartitions,
            Consumer<Set<TableBucket>> unsubscribeTableBucketsCallback) {
        FlinkSourceSplitReader sourceSplitReader =
                (FlinkSourceSplitReader) splitFetcher.getSplitReader();

        splitFetcher.enqueueTask(
                new SplitFetcherTask() {
                    @Override
                    public boolean run() {
                        Set<TableBucket> unsubscribedBuckets =
                                sourceSplitReader.removePartitions(removedPartitions);
                        unsubscribeTableBucketsCallback.accept(unsubscribedBuckets);
                        return true;
                    }

                    @Override
                    public void wakeUp() {}
                });
    }
}
