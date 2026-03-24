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
        LOG.info("Enqueueing handleTableReachTieringMaxDuration task for table {}", tableId);
        enqueueTaskForTable(
                tableId,
                reader -> {
                    LOG.debug(
                            "Executing handleTableReachTieringMaxDuration in split reader for table {}",
                            tableId);
                    reader.handleTableReachTieringMaxDuration(tableId);
                },
                "handleTableReachTieringMaxDuration");
    }

    public void markTableDropped(long tableId) {
        LOG.info("Enqueueing handleTableDropped task for table {}", tableId);
        enqueueTaskForTable(
                tableId,
                reader -> {
                    LOG.debug("Executing handleTableDropped in split reader for table {}", tableId);
                    reader.handleTableDropped(tableId);
                },
                "handleTableDropped");
    }

    private void enqueueTaskForTable(
            long tableId, Consumer<TieringSplitReader<WriteResult>> action, String actionDesc) {
        SplitFetcher<TableBucketWriteResult<WriteResult>, TieringSplit> splitFetcher;
        if (!fetchers.isEmpty()) {
            LOG.info("Fetchers are active, enqueueing {} task for table {}", actionDesc, tableId);
            fetchers.values().forEach(f -> enqueueReaderTask(f, action));
        } else {
            LOG.info(
                    "No active fetchers, creating new fetcher and enqueueing {} task for table {}",
                    actionDesc,
                    tableId);
            splitFetcher = createSplitFetcher();
            enqueueReaderTask(splitFetcher, action);
            startFetcher(splitFetcher);
        }
    }

    @SuppressWarnings("unchecked")
    private void enqueueReaderTask(
            SplitFetcher<TableBucketWriteResult<WriteResult>, TieringSplit> splitFetcher,
            Consumer<TieringSplitReader<WriteResult>> action) {
        splitFetcher.enqueueTask(
                new SplitFetcherTask() {
                    @Override
                    public boolean run() {
                        action.accept(
                                (TieringSplitReader<WriteResult>) splitFetcher.getSplitReader());
                        return true;
                    }

                    @Override
                    public void wakeUp() {}
                });
    }
}
