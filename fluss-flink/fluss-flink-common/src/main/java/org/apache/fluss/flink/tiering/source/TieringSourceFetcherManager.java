/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.tiering.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherTask;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.fluss.flink.adapter.SingleThreadFetcherManagerAdapter;
import org.apache.fluss.flink.tiering.source.split.TieringSplit;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The SplitFetcherManager for tiering source. This class is needed to help notify a table reaches
 * the max duration of tiering to {@link TieringSplitReader}.
 */
public class TieringSourceFetcherManager<WriteResult>
        extends SingleThreadFetcherManagerAdapter<
                TableBucketWriteResult<WriteResult>, TieringSplit> {

    public TieringSourceFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<TableBucketWriteResult<WriteResult>>>
                    elementsQueue,
            Supplier<SplitReader<TableBucketWriteResult<WriteResult>, TieringSplit>>
                    splitReaderSupplier,
            Configuration configuration,
            Consumer<Collection<String>> splitFinishedHook) {
        super(elementsQueue, splitReaderSupplier, configuration, splitFinishedHook);
    }

    public void markTableReachTieringMaxDuration(long tableId) {
        if (!fetchers.isEmpty()) {
            // The fetcher thread is still running. This should be the majority of the cases.
            fetchers.values()
                    .forEach(
                            splitFetcher ->
                                    enqueueMarkTableReachTieringMaxDurationTask(
                                            splitFetcher, tableId));
        } else {
            SplitFetcher<TableBucketWriteResult<WriteResult>, TieringSplit> splitFetcher =
                    createSplitFetcher();
            enqueueMarkTableReachTieringMaxDurationTask(splitFetcher, tableId);
            startFetcher(splitFetcher);
        }
    }

    private void enqueueMarkTableReachTieringMaxDurationTask(
            SplitFetcher<TableBucketWriteResult<WriteResult>, TieringSplit> splitFetcher,
            long reachTieringDeadlineTable) {
        splitFetcher.enqueueTask(
                new SplitFetcherTask() {
                    @Override
                    public boolean run() {
                        ((TieringSplitReader<WriteResult>) splitFetcher.getSplitReader())
                                .handleTableReachTieringMaxDuration(reachTieringDeadlineTable);
                        return true;
                    }

                    @Override
                    public void wakeUp() {
                        // do nothing
                    }
                });
    }
}
