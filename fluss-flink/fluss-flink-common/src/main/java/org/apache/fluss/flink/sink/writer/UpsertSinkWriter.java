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

package org.apache.fluss.flink.sink.writer;

import org.apache.fluss.client.table.writer.DeleteResult;
import org.apache.fluss.client.table.writer.TableWriter;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.client.table.writer.UpsertResult;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.row.OperationType;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.undo.ProducerOffsetReporter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** An upsert sink writer or fluss primary key table. */
public class UpsertSinkWriter<InputT> extends FlinkSinkWriter<InputT> {

    private transient UpsertWriter upsertWriter;

    /**
     * Optional context for reporting offsets to the upstream UndoRecoveryOperator.
     *
     * <p>When provided (non-null), the writer will report offsets after each successful write
     * operation. This is used when the UndoRecoveryOperator manages state for aggregation tables.
     *
     * <p>When null, offset reporting is skipped (for non-aggregation tables).
     */
    @Nullable private final ProducerOffsetReporter offsetReporter;

    /**
     * Creates a new UpsertSinkWriter with ProducerOffsetReporter for UndoRecoveryOperator
     * integration.
     *
     * @param tablePath the path of the table to write to
     * @param flussConfig the Fluss configuration
     * @param tableRowType the row type of the table
     * @param targetColumnIndexes optional column indexes for partial updates
     * @param mailboxExecutor the mailbox executor for async operations
     * @param flussSerializationSchema the serialization schema for input records
     * @param offsetReporter optional reporter for reporting offsets to upstream operator
     */
    public UpsertSinkWriter(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            @Nullable int[] targetColumnIndexes,
            MailboxExecutor mailboxExecutor,
            FlussSerializationSchema<InputT> flussSerializationSchema,
            @Nullable ProducerOffsetReporter offsetReporter) {
        super(
                tablePath,
                flussConfig,
                tableRowType,
                targetColumnIndexes,
                mailboxExecutor,
                flussSerializationSchema);
        this.offsetReporter = offsetReporter;
    }

    @Override
    public void initialize(SinkWriterMetricGroup metricGroup) {
        super.initialize(metricGroup);
        Upsert upsert = table.newUpsert();
        if (targetColumnIndexes != null) {
            upsert = upsert.partialUpdate(targetColumnIndexes);
        }
        upsertWriter = upsert.createWriter();
        LOG.info("Finished opening Fluss {}.", this.getClass().getSimpleName());
    }

    @Override
    CompletableFuture<?> writeRow(OperationType opType, InternalRow internalRow) {
        if (opType == OperationType.UPSERT) {
            CompletableFuture<UpsertResult> future = upsertWriter.upsert(internalRow);
            // Report offset to upstream UndoRecoveryOperator if reporter provided
            if (offsetReporter != null) {
                return future.thenAccept(
                        result -> {
                            if (result.getBucket() != null && result.getLogEndOffset() >= 0) {
                                offsetReporter.reportOffset(
                                        result.getBucket(), result.getLogEndOffset());
                            }
                        });
            }
            return future;
        } else if (opType == OperationType.DELETE) {
            CompletableFuture<DeleteResult> future = upsertWriter.delete(internalRow);
            // Report offset to upstream UndoRecoveryOperator if reporter provided
            if (offsetReporter != null) {
                return future.thenAccept(
                        result -> {
                            if (result.getBucket() != null && result.getLogEndOffset() >= 0) {
                                offsetReporter.reportOffset(
                                        result.getBucket(), result.getLogEndOffset());
                            }
                        });
            }
            return future;
        } else {
            throw new UnsupportedOperationException("Unsupported operation type: " + opType);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        upsertWriter.flush();
        checkAsyncException();
    }

    @Override
    TableWriter getTableWriter() {
        return upsertWriter;
    }
}
