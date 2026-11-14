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

package org.apache.fluss.lake.delta.tiering;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.delta.DeltaConfig;
import org.apache.fluss.lake.delta.utils.DeltaColumnarBatch;
import org.apache.fluss.lake.delta.utils.FlussRowToDeltaValues;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.types.RowType;

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Implementation of {@link LakeWriter} for Delta Lake log tables.
 *
 * <p>The writer copies records into an in-memory buffer and writes the buffer out as Parquet files
 * with the Delta Kernel Parquet writer whenever {@link DeltaConfig#WRITE_BATCH_SIZE} rows are
 * buffered, and once more on {@link #complete()}. Files are written under the Delta table but are
 * not visible until {@link DeltaLakeCommitter} adds them in a commit.
 */
public class DeltaLakeWriter implements LakeWriter<DeltaWriteResult> {

    private final Engine engine;
    private final Row transactionState;
    private final StructType schema;
    private final Map<String, Literal> partitionLiterals;
    private final Map<String, String> partitionValues;
    private final FlussRowToDeltaValues rowConverter;
    private final int batchSize;

    private final List<Object[]> bufferedRows = new ArrayList<>();
    private final List<DeltaDataFile> dataFiles = new ArrayList<>();

    public DeltaLakeWriter(Configuration options, WriterInitContext writerInitContext)
            throws IOException {
        this.engine = DeltaConfig.createEngine(options);
        String tablePath = DeltaConfig.tablePath(options, writerInitContext.tablePath());
        Snapshot snapshot = TableManager.loadSnapshot(tablePath).build(engine);
        // The transaction is never committed, it only tells how to lay out the data files.
        Transaction transaction =
                snapshot.buildUpdateTableTransaction(DeltaConfig.ENGINE_INFO, Operation.WRITE)
                        .build(engine);
        this.transactionState = transaction.getTransactionState(engine);
        this.schema = transaction.getSchema(engine);

        RowType rowType = writerInitContext.tableInfo().getRowType();
        checkState(
                schema.fieldNames().equals(rowType.getFieldNames()),
                "The Delta table %s has columns %s, but the Fluss table has columns %s.",
                tablePath,
                schema.fieldNames(),
                rowType.getFieldNames());
        this.rowConverter = new FlussRowToDeltaValues(rowType);
        this.partitionValues =
                partitionValues(
                        writerInitContext.tableInfo().getPartitionKeys(),
                        writerInitContext.partition());
        this.partitionLiterals = new HashMap<>();
        for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
            partitionLiterals.put(entry.getKey(), Literal.ofString(entry.getValue()));
        }
        this.batchSize =
                DeltaConfig.writeBatchSize(
                        options, writerInitContext.tableInfo().getCustomProperties().toMap());
    }

    @Override
    public void write(LogRecord record) throws IOException {
        bufferedRows.add(rowConverter.convert(record.getRow()));
        if (bufferedRows.size() >= batchSize) {
            flush();
        }
    }

    @Override
    public DeltaWriteResult complete() throws IOException {
        flush();
        return new DeltaWriteResult(dataFiles);
    }

    @Override
    public void close() throws IOException {
        bufferedRows.clear();
    }

    private void flush() throws IOException {
        if (bufferedRows.isEmpty()) {
            return;
        }
        FilteredColumnarBatch batch =
                new FilteredColumnarBatch(
                        DeltaColumnarBatch.of(schema, bufferedRows), Optional.empty());
        CloseableIterator<FilteredColumnarBatch> physicalData =
                Transaction.transformLogicalData(
                        engine,
                        transactionState,
                        CloseableIterators.of(Collections.singletonList(batch).iterator()),
                        partitionLiterals);
        DataWriteContext writeContext =
                Transaction.getWriteContext(engine, transactionState, partitionLiterals);
        try (CloseableIterator<DataFileStatus> files =
                engine.getParquetHandler()
                        .writeParquetFiles(
                                writeContext.getTargetDirectory(),
                                physicalData,
                                writeContext.getStatisticsColumns())) {
            while (files.hasNext()) {
                dataFiles.add(DeltaDataFile.of(files.next(), schema, partitionValues));
            }
        }
        bufferedRows.clear();
    }

    private static Map<String, String> partitionValues(
            List<String> partitionKeys, @Nullable String partitionName) {
        if (partitionName == null) {
            return Collections.emptyMap();
        }
        List<String> values =
                ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName)
                        .getPartitionValues();
        Map<String, String> partitionValues = new HashMap<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            partitionValues.put(partitionKeys.get(i), values.get(i));
        }
        return partitionValues;
    }
}
