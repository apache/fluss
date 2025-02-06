/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.flink.sink;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.flink.metrics.FlinkMetricRegistry;
import com.alibaba.fluss.connector.flink.utils.FlinkConversions;
import com.alibaba.fluss.connector.flink.utils.FlinkRowToFlussRowConverter;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.Metric;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.row.InternalRow;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/** Flink's {@link SinkFunction} implementation for Fluss. */
abstract class FlinkSinkFunction extends RichSinkFunction<RowData>
        implements CheckpointedFunction, Serializable {

    private static final long serialVersionUID = 1L;
    protected static final Logger LOG = LoggerFactory.getLogger(FlinkSinkFunction.class);

    private final TablePath tablePath;
    private final Configuration flussConfig;
    protected final RowType tableRowType;
    protected final @Nullable int[] targetColumnIndexes;
    private final boolean ignoreDelete;

    private transient Connection connection;
    protected transient Table table;
    protected transient FlinkRowToFlussRowConverter dataConverter;
    protected transient FlinkMetricRegistry flinkMetricRegistry;

    protected transient SinkWriterMetricGroup metricGroup;

    private transient Counter numRecordsOutCounter;
    private transient Counter numRecordsOutErrorsCounter;
    private volatile Throwable asyncWriterException;

    public FlinkSinkFunction(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            boolean ignoreDelete) {
        this(tablePath, flussConfig, tableRowType, null, ignoreDelete);
    }

    public FlinkSinkFunction(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            @Nullable int[] targetColumns,
            boolean ignoreDelete) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.targetColumnIndexes = targetColumns;
        this.tableRowType = tableRowType;
        this.ignoreDelete = ignoreDelete;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration config) {
        LOG.info(
                "Opening Fluss {}, database: {} and table: {}",
                this.getClass().getSimpleName(),
                tablePath.getDatabaseName(),
                tablePath.getTableName());
        metricGroup = InternalSinkWriterMetricGroup.wrap(getRuntimeContext().getMetricGroup());
        flinkMetricRegistry =
                new FlinkMetricRegistry(
                        metricGroup, Collections.singleton(MetricNames.WRITER_SEND_LATENCY_MS));
        connection = ConnectionFactory.createConnection(flussConfig, flinkMetricRegistry);
        table = connection.getTable(tablePath);
        sanityCheck(table.getDescriptor());
        dataConverter = createFlinkRowToFlussRowConverter();
        initMetrics();
    }

    protected void initMetrics() {
        numRecordsOutCounter = metricGroup.getNumRecordsSendCounter();
        numRecordsOutErrorsCounter = metricGroup.getNumRecordsOutErrorsCounter();
        metricGroup.setCurrentSendTimeGauge(this::computeSendTime);
    }

    @Override
    public void invoke(RowData value, SinkFunction.Context context) throws IOException {
        checkAsyncException();
        if (ignoreDelete
                && (value.getRowKind() == RowKind.UPDATE_BEFORE
                        || value.getRowKind() == RowKind.DELETE)) {
            return;
        }

        InternalRow internalRow = dataConverter.toInternalRow(value);
        CompletableFuture<Void> writeFuture = writeRow(value.getRowKind(), internalRow);
        writeFuture.exceptionally(
                exception -> {
                    if (this.asyncWriterException == null) {
                        this.asyncWriterException = exception;
                    }
                    return null;
                });

        numRecordsOutCounter.inc();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws IOException {
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) {}

    @Override
    public void finish() throws IOException {
        flush();
    }

    abstract void flush() throws IOException;

    abstract FlinkRowToFlussRowConverter createFlinkRowToFlussRowConverter();

    abstract CompletableFuture<Void> writeRow(RowKind rowKind, InternalRow internalRow);

    @Override
    public void close() throws Exception {
        super.close();

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

        try {
            if (dataConverter != null) {
                dataConverter.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception occurs while closing Fluss RowData Converter.", e);
        }
        dataConverter = null;

        // Rethrow exception for the case in which close is called before writer() and flush().
        checkAsyncException();

        LOG.info("Finished closing Fluss sink function.");
    }

    private void sanityCheck(TableDescriptor flussTableDescriptor) {
        // when it's UpsertSinkFunction, it means it has primary key got from Flink's metadata
        boolean hasPrimaryKey = this instanceof UpsertSinkFunction;
        if (flussTableDescriptor.hasPrimaryKey() != hasPrimaryKey) {
            throw new ValidationException(
                    String.format(
                            "Primary key constraint is not matched between metadata in Fluss (%s) and Flink (%s).",
                            flussTableDescriptor.hasPrimaryKey(), hasPrimaryKey));
        }
        RowType currentTableRowType =
                FlinkConversions.toFlinkRowType(flussTableDescriptor.getSchema().toRowType());
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
}
