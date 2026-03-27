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

import org.apache.fluss.flink.adapter.SingleThreadFetcherManagerAdapter;
import org.apache.fluss.flink.tiering.source.split.TieringSplit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherTask;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(TieringSourceFetcherManager.class);

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
            LOG.info("fetchers is not empty, marking tiering max duration for table {}", tableId);
            fetchers.values()
                    .forEach(
                            splitFetcher ->
                                    enqueueMarkTableReachTieringMaxDurationTask(
                                            splitFetcher, tableId));
        } else {
            SplitFetcher<TableBucketWriteResult<WriteResult>, TieringSplit> splitFetcher =
                    createSplitFetcher();
            LOG.info(
                    "fetchers is empty, enqueue marking tiering max duration for table {}",
                    tableId);
            enqueueMarkTableReachTieringMaxDurationTask(splitFetcher, tableId);
            startFetcher(splitFetcher);
        }
    }

    /** Notify the SplitReader that a table tiering has failed and should be cleaned up. */
    public void notifyTableTieringFailed(long tableId) {
        if (!fetchers.isEmpty()) {
            LOG.info("Notifying SplitReader that table {} tiering has failed", tableId);
            fetchers.values()
                    .forEach(
                            splitFetcher ->
                                    enqueueNotifyTableTieringFailedTask(splitFetcher, tableId));
        } else {
            SplitFetcher<TableBucketWriteResult<WriteResult>, TieringSplit> splitFetcher =
                    createSplitFetcher();
            LOG.info(
                    "fetchers is empty, enqueue notify table tiering failed for table {}", tableId);
            enqueueNotifyTableTieringFailedTask(splitFetcher, tableId);
            startFetcher(splitFetcher);
        }
    }

    /**
     * Poll all failed table infos from the SplitReaders.
     *
     * @param consumer the consumer to process each failed table info
     */
    public void pollFailedTableInfos(
            java.util.function.Consumer<TieringSplitReader.FailedTableInfo> consumer) {
        for (SplitFetcher<TableBucketWriteResult<WriteResult>, TieringSplit> fetcher :
                fetchers.values()) {
            TieringSplitReader<WriteResult> splitReader =
                    (TieringSplitReader<WriteResult>) fetcher.getSplitReader();
            TieringSplitReader.FailedTableInfo failedInfo;
            while ((failedInfo = splitReader.pollFailedTableInfo()) != null) {
                consumer.accept(failedInfo);
            }
        }
    }

    private void enqueueNotifyTableTieringFailedTask(
            SplitFetcher<TableBucketWriteResult<WriteResult>, TieringSplit> splitFetcher,
            long failedTableId) {
        splitFetcher.enqueueTask(
                new SplitFetcherTask() {
                    @Override
                    public boolean run() {
                        ((TieringSplitReader<WriteResult>) splitFetcher.getSplitReader())
                                .notifyTableTieringFailed(failedTableId);
                        return true;
                    }

                    @Override
                    public void wakeUp() {
                        // do nothing
                    }
                });
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
