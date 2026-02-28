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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.Connection;
import org.apache.fluss.flink.adapter.SingleThreadMultiplexSourceReaderBaseAdapter;
import org.apache.fluss.flink.tiering.event.FailedTieringEvent;
import org.apache.fluss.flink.tiering.event.TieringReachMaxDurationEvent;
import org.apache.fluss.flink.tiering.source.split.TieringSplit;
import org.apache.fluss.flink.tiering.source.state.TieringSplitState;
import org.apache.fluss.lake.writer.LakeTieringFactory;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.fluss.flink.tiering.source.TieringSplitReader.DEFAULT_POLL_TIMEOUT;

/** A {@link SourceReader} that read records from Fluss and write to lake. */
@Internal
public final class TieringSourceReader<WriteResult>
        extends SingleThreadMultiplexSourceReaderBaseAdapter<
                TableBucketWriteResult<WriteResult>,
                TableBucketWriteResult<WriteResult>,
                TieringSplit,
                TieringSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(TieringSourceReader.class);

    private final Connection connection;

    // Queue to store failure markers that need to be sent to downstream Committer
    // These markers are generated when receiving FailedTieringEvent from Enumerator
    private final Queue<TableBucketWriteResult<WriteResult>> failedMarkersForCommitter;

    public TieringSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<TableBucketWriteResult<WriteResult>>>
                    elementsQueue,
            SourceReaderContext context,
            Connection connection,
            LakeTieringFactory<WriteResult, ?> lakeTieringFactory) {
        this(elementsQueue, context, connection, lakeTieringFactory, DEFAULT_POLL_TIMEOUT);
    }

    @VisibleForTesting
    TieringSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<TableBucketWriteResult<WriteResult>>>
                    elementsQueue,
            SourceReaderContext context,
            Connection connection,
            LakeTieringFactory<WriteResult, ?> lakeTieringFactory,
            Duration pollTimeout) {
        super(
                elementsQueue,
                new TieringSourceFetcherManager<>(
                        elementsQueue,
                        () -> new TieringSplitReader<>(connection, lakeTieringFactory, pollTimeout),
                        context.getConfiguration(),
                        (ignore) -> {}),
                new TableBucketWriteResultEmitter<>(),
                context.getConfiguration(),
                context);
        this.connection = connection;
        this.failedMarkersForCommitter = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void start() {
        // we request a split only if we did not get splits during the checkpoint restore
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<TableBucketWriteResult<WriteResult>> output)
            throws Exception {
        // Check for failed tables and send events to Enumerator
        processFailedTables();

        // Emit any pending failure markers to the downstream Committer
        emitFailedMarkersToCommitter(output);

        return super.pollNext(output);
    }

    /**
     * Processes any failed tables detected from the SplitReader and sends FailedTieringEvent to the
     * Enumerator.
     */
    private void processFailedTables() {
        ((TieringSourceFetcherManager<WriteResult>) splitFetcherManager)
                .pollFailedTableInfos(
                        failedTable -> {
                            LOG.info(
                                    "Detected table {} tiering failure, sending FailedTieringEvent "
                                            + "to Enumerator. Reason: {}",
                                    failedTable.getTableId(),
                                    failedTable.getFailReason());
                            context.sendSourceEventToCoordinator(
                                    new FailedTieringEvent(
                                            failedTable.getTableId(), failedTable.getFailReason()));
                        });
    }

    /** Emits any pending failure markers to the downstream Committer. */
    private void emitFailedMarkersToCommitter(
            ReaderOutput<TableBucketWriteResult<WriteResult>> output) {
        TableBucketWriteResult<WriteResult> failedMarker;
        while ((failedMarker = failedMarkersForCommitter.poll()) != null) {
            LOG.info(
                    "Emitting failure marker for table {} to downstream Committer.",
                    failedMarker.tableBucket().getTableId());
            output.collect(failedMarker);
        }
    }

    @Override
    protected void onSplitFinished(Map<String, TieringSplitState> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    public List<TieringSplit> snapshotState(long checkpointId) {
        // we return empty list to make source reader be stateless
        return Collections.emptyList();
    }

    @Override
    protected TieringSplitState initializedState(TieringSplit split) {
        if (split.isTieringSnapshotSplit()) {
            return new TieringSplitState(split);
        } else if (split.isTieringLogSplit()) {
            return new TieringSplitState(split);
        } else {
            throw new UnsupportedOperationException("Unsupported split type: " + split);
        }
    }

    @Override
    protected TieringSplit toSplitType(String splitId, TieringSplitState splitState) {
        return splitState.toSourceSplit();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof TieringReachMaxDurationEvent) {
            TieringReachMaxDurationEvent reachMaxDurationEvent =
                    (TieringReachMaxDurationEvent) sourceEvent;
            long tableId = reachMaxDurationEvent.getTableId();
            LOG.info("Received reach max duration for table {}", tableId);
            ((TieringSourceFetcherManager<WriteResult>) splitFetcherManager)
                    .markTableReachTieringMaxDuration(tableId);
        } else if (sourceEvent instanceof FailedTieringEvent) {
            FailedTieringEvent failedEvent = (FailedTieringEvent) sourceEvent;
            long failedTableId = failedEvent.getTableId();
            String failReason = failedEvent.failReason();

            LOG.info(
                    "Received FailedTieringEvent from Enumerator for table {}. Reason: {}",
                    failedTableId,
                    failReason);

            // Notify the SplitReader to clean up state for the failed table
            ((TieringSourceFetcherManager<WriteResult>) splitFetcherManager)
                    .notifyTableTieringFailed(failedTableId);

            // Create a failure marker and queue it for sending to downstream Committer
            TableBucketWriteResult<WriteResult> failedMarker =
                    TableBucketWriteResult.failedMarker(failedTableId, failReason);
            failedMarkersForCommitter.offer(failedMarker);
            LOG.info(
                    "Queued failure marker for table {} to be sent to downstream Committer.",
                    failedTableId);
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
