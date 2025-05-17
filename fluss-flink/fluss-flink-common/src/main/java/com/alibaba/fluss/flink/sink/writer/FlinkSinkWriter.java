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

package com.alibaba.fluss.flink.sink.writer;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.TableWriter;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.flink.metrics.FlinkMetricRegistry;
import com.alibaba.fluss.flink.row.OperationType;
import com.alibaba.fluss.flink.row.RowWithOp;
import com.alibaba.fluss.flink.sink.serializer.FlussSerializationSchema;
import com.alibaba.fluss.flink.sink.serializer.SerializerInitContextImpl;
import com.alibaba.fluss.flink.utils.FlinkConversions;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.Metric;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.row.InternalRow;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/** Base class for Flink {@link SinkWriter} implementations in Fluss. */
public abstract class FlinkSinkWriter<InputT> implements SinkWriter<InputT> {

    protected static final Logger LOG = LoggerFactory.getLogger(FlinkSinkWriter.class);

    private final TablePath tablePath;
    private final Configuration flussConfig;
    protected final RowType tableRowType;
    protected final @Nullable int[] targetColumnIndexes;
    private final MailboxExecutor mailboxExecutor;
    private final FlussSerializationSchema<InputT> serializationSchema;

    private transient Connection connection;
    protected transient Table table;
    protected transient FlinkMetricRegistry flinkMetricRegistry;

    protected transient SinkWriterMetricGroup metricGroup;

    private transient Counter numRecordsOutCounter;
    private transient Counter numRecordsOutErrorsCounter;
    private volatile Throwable asyncWriterException;

    public FlinkSinkWriter(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            MailboxExecutor mailboxExecutor,
            FlussSerializationSchema<InputT> serializationSchema) {
        this(tablePath, flussConfig, tableRowType, null, mailboxExecutor, serializationSchema);
    }

    public FlinkSinkWriter(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            @Nullable int[] targetColumns,
            MailboxExecutor mailboxExecutor,
            FlussSerializationSchema<InputT> serializationSchema) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.targetColumnIndexes = targetColumns;
        this.tableRowType = tableRowType;
        this.mailboxExecutor = mailboxExecutor;
        this.serializationSchema = serializationSchema;
    }

    public void initialize(SinkWriterMetricGroup metricGroup) {
        LOG.info(
                "Opening Fluss {}, database: {} and table: {}",
                this.getClass().getSimpleName(),
                tablePath.getDatabaseName(),
                tablePath.getTableName());
        this.metricGroup = metricGroup;
        flinkMetricRegistry =
                new FlinkMetricRegistry(
                        metricGroup, Collections.singleton(MetricNames.WRITER_SEND_LATENCY_MS));
        connection = ConnectionFactory.createConnection(flussConfig, flinkMetricRegistry);
        table = connection.getTable(tablePath);
        sanityCheck(table.getTableInfo());

        try {
            this.serializationSchema.open(
                    new SerializerInitContextImpl(table.getTableInfo().getRowType()));
        } catch (Exception e) {
            throw new FlussRuntimeException(e);
        }

        initMetrics();
    }

    protected void initMetrics() {
        numRecordsOutCounter = metricGroup.getNumRecordsSendCounter();
        numRecordsOutErrorsCounter = metricGroup.getNumRecordsOutErrorsCounter();
        metricGroup.setCurrentSendTimeGauge(this::computeSendTime);
    }

    @Override
    public void write(InputT inputValue, Context context) throws IOException, InterruptedException {
        checkAsyncException();

        try {
            RowWithOp rowWithOp = serializationSchema.serialize(inputValue);
            OperationType opType = rowWithOp.getOperationType();
            InternalRow row = rowWithOp.getRow();
            if (opType == OperationType.IGNORE) {
                // skip writing the row
                return;
            }
            CompletableFuture<?> writeFuture = writeRow(opType, row);
            writeFuture.whenComplete(
                    (ignored, throwable) -> {
                        if (throwable != null) {
                            if (this.asyncWriterException == null) {
                                this.asyncWriterException = throwable;
                            }

                            // Checking for exceptions from previous writes
                            mailboxExecutor.execute(
                                    this::checkAsyncException, "Update error metric");
                        }
                    });

            numRecordsOutCounter.inc();
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public abstract void flush(boolean endOfInput) throws IOException, InterruptedException;

    abstract CompletableFuture<?> writeRow(OperationType opType, InternalRow internalRow);

    @Override
    public void close() throws Exception {
        try {
            if (table != null) {
                table.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception occurs while closing Fluss Table.", e);
        }
        table = null;

        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception occurs while closing Fluss Connection.", e);
        }
        connection = null;

        if (flinkMetricRegistry != null) {
            flinkMetricRegistry.close();
        }
        flinkMetricRegistry = null;

        // Rethrow exception for the case in which close is called before writer() and flush().
        checkAsyncException();

        LOG.info("Finished closing Fluss sink function.");
    }

    private void sanityCheck(TableInfo flussTableInfo) {
        // when it's UpsertSinkWriter, it means it has primary key got from Flink's metadata
        boolean hasPrimaryKey = this instanceof UpsertSinkWriter;
        if (flussTableInfo.hasPrimaryKey() != hasPrimaryKey) {
            throw new ValidationException(
                    String.format(
                            "Primary key constraint is not matched between metadata in Fluss (%s) and Flink (%s).",
                            flussTableInfo.hasPrimaryKey(), hasPrimaryKey));
        }
        RowType currentTableRowType = FlinkConversions.toFlinkRowType(flussTableInfo.getRowType());
        if (!this.tableRowType.copy(false).equals(currentTableRowType.copy(false))) {
            // The default nullability of Flink row type and Fluss row type might be not the same,
            // thus we need to compare the row type without nullability here.

            // Throw exception if the schema is the not same, this should rarely happen because we
            // only allow fluss tables derived from fluss catalog. But this can happen if an ALTER
            // TABLE command executed on the fluss table, after the job is submitted but before the
            // SinkFunction is opened.
            throw new ValidationException(
                    "The Flink query schema is not matched to current Fluss table schema. "
                            + "\nFlink query schema: "
                            + this.tableRowType
                            + "\nFluss table schema: "
                            + currentTableRowType);
        }
    }

    private long computeSendTime() {
        if (flinkMetricRegistry == null) {
            return -1;
        }

        Metric writerSendLatencyMs =
                flinkMetricRegistry.getFlussMetric(MetricNames.WRITER_SEND_LATENCY_MS);
        if (writerSendLatencyMs == null) {
            return -1;
        }

        return ((Gauge<Long>) writerSendLatencyMs).getValue();
    }

    /**
     * This method should only be invoked in the mailbox thread since the counter is not volatile.
     * Logic needs to be invoked by write AND flush since we support various semantics.
     */
    protected void checkAsyncException() throws IOException {
        // reset this exception since we could close the writer later on
        Throwable throwable = asyncWriterException;
        if (throwable != null) {
            asyncWriterException = null;
            numRecordsOutErrorsCounter.inc();
            LOG.error("Exception occurs while write row to fluss.", throwable);
            throw new IOException(
                    "One or more Fluss Writer send requests have encountered exception", throwable);
        }
    }

    @VisibleForTesting
    abstract TableWriter getTableWriter();
}
