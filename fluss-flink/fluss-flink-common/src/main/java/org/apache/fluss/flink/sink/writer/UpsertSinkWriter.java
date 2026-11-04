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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.table.writer.DeleteResult;
import org.apache.fluss.client.table.writer.RetractResult;
import org.apache.fluss.client.table.writer.TableWriter;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.client.table.writer.UpsertResult;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.row.OperationType;
import org.apache.fluss.flink.row.RowWithOp;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.undo.ProducerOffsetReporter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

/** An upsert sink writer or fluss primary key table. */
public class UpsertSinkWriter<InputT> extends FlinkSinkWriter<InputT> {

    @VisibleForTesting transient UpsertWriter upsertWriter;
    @Nullable private transient UpsertWriter deleteWriter;
    @Nullable private transient UpsertWriter retractWriter;
    @Nullable private transient int[] retractInputColumns;
    private final boolean removeRecordOnDelete;

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
     * Whether this writer supports RETRACT operations. For aggregation tables this may be enabled
     * either for the full schema or for a retract-safe partial-update column subset.
     */
    private final boolean schemaSupportsRetract;

    /** Creates a writer with the existing upsert and delete behavior. */
    public UpsertSinkWriter(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            @Nullable int[] targetColumnIndexes,
            MailboxExecutor mailboxExecutor,
            FlussSerializationSchema<InputT> flussSerializationSchema,
            @Nullable ProducerOffsetReporter offsetReporter) {
        this(
                tablePath,
                flussConfig,
                tableRowType,
                targetColumnIndexes,
                mailboxExecutor,
                flussSerializationSchema,
                offsetReporter,
                false,
                false);
    }

    /** Creates a writer with an explicit whole-row policy for changelog DELETE. */
    public UpsertSinkWriter(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            @Nullable int[] targetColumnIndexes,
            MailboxExecutor mailboxExecutor,
            FlussSerializationSchema<InputT> flussSerializationSchema,
            @Nullable ProducerOffsetReporter offsetReporter,
            boolean schemaSupportsRetract,
            boolean removeRecordOnDelete) {
        super(
                tablePath,
                flussConfig,
                tableRowType,
                targetColumnIndexes,
                mailboxExecutor,
                flussSerializationSchema);
        this.offsetReporter = offsetReporter;
        this.schemaSupportsRetract = schemaSupportsRetract;
        this.removeRecordOnDelete = removeRecordOnDelete;
    }

    @Override
    public void initialize(SinkWriterMetricGroup metricGroup) {
        super.initialize(metricGroup);
        Upsert upsert = table.newUpsert();
        if (targetColumnIndexes != null) {
            upsert = upsert.partialUpdate(targetColumnIndexes);
        }
        upsertWriter = upsert.createWriter();
        // Both writers share the connection's WriterClient and its per-bucket sequence. A full
        // delete must not inherit partial-update target columns from the aggregate writer.
        if (removeRecordOnDelete && targetColumnIndexes != null) {
            deleteWriter = table.newUpsert().createWriter();
        }
        LOG.info("Finished opening Fluss {}.", this.getClass().getSimpleName());
    }

    @Override
    CompletableFuture<?> writeRow(OperationType opType, InternalRow internalRow) {
        return writeRow(opType, internalRow, upsertWriter);
    }

    @Override
    CompletableFuture<?> writeRow(RowWithOp row) {
        if (row.getOperationType() != OperationType.RETRACT || row.getTargetColumns() == null) {
            return super.writeRow(row);
        }
        int[] columns = row.getTargetColumns();
        if (retractWriter == null || !Arrays.equals(columns, retractInputColumns)) {
            retractInputColumns = columns;
            if (targetColumnIndexes != null) {
                columns =
                        Arrays.stream(columns)
                                .filter(
                                        column ->
                                                Arrays.stream(targetColumnIndexes)
                                                        .anyMatch(target -> target == column))
                                .toArray();
            }
            retractWriter = table.newUpsert().partialUpdate(columns).createWriter();
        }
        return writeRow(row.getOperationType(), row.getRow(), retractWriter);
    }

    private CompletableFuture<?> writeRow(
            OperationType opType, InternalRow internalRow, UpsertWriter writer) {
        if (opType == OperationType.RETRACT) {
            if (!schemaSupportsRetract) {
                throw new UnsupportedOperationException(
                        "Received RETRACT operation but the schema does not support retract. "
                                + "This should have been rejected at plan time or serialization.");
            }
            CompletableFuture<RetractResult> future = writer.retract(internalRow);
            if (offsetReporter != null) {
                return future.thenAccept(
                        result ->
                                reportOffsetIfAvailable(
                                        result.getBucket(), result.getLogEndOffset()));
            }
            return future;
        } else if (opType == OperationType.UPSERT) {
            CompletableFuture<UpsertResult> future = upsertWriter.upsert(internalRow);
            if (offsetReporter != null) {
                return future.thenAccept(
                        result ->
                                reportOffsetIfAvailable(
                                        result.getBucket(), result.getLogEndOffset()));
            }
            return future;
        } else if (opType == OperationType.DELETE) {
            CompletableFuture<DeleteResult> future =
                    (deleteWriter == null ? upsertWriter : deleteWriter).delete(internalRow);
            if (offsetReporter != null) {
                return future.thenAccept(
                        result ->
                                reportOffsetIfAvailable(
                                        result.getBucket(), result.getLogEndOffset()));
            }
            return future;
        } else {
            throw new UnsupportedOperationException("Unsupported operation type: " + opType);
        }
    }

    /**
     * Reports the offset to the upstream UndoRecoveryOperator if the bucket and offset are
     * available.
     */
    private void reportOffsetIfAvailable(@Nullable TableBucket bucket, long logEndOffset) {
        if (offsetReporter != null && bucket != null && logEndOffset >= 0) {
            offsetReporter.reportOffset(bucket, logEndOffset);
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
