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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.lake.iceberg.maintenance.IcebergRewriteDataFiles;
import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.lake.iceberg.tiering.writer.AppendOnlyTaskWriter;
import org.apache.fluss.lake.iceberg.tiering.writer.DeltaTaskWriter;
import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;

/** Implementation of {@link LakeWriter} for Iceberg. */
public class IcebergLakeWriter implements LakeWriter<IcebergWriteResult> {

    protected static final Logger LOG = LoggerFactory.getLogger(IcebergLakeWriter.class);

    private static final String AUTO_MAINTENANCE_KEY = "table.datalake.auto-maintenance";

    private final Catalog icebergCatalog;
    private final Table icebergTable;
    private final RecordWriter recordWriter;
    private final boolean autoMaintenanceEnabled;

    @Nullable private final ExecutorService compactionExecutor;
    @Nullable private CompletableFuture<RewriteDataFileResult> compactionFuture;

    public IcebergLakeWriter(
            IcebergCatalogProvider icebergCatalogProvider, WriterInitContext writerInitContext)
            throws IOException {
        this.icebergCatalog = icebergCatalogProvider.get();
        this.icebergTable = getTable(writerInitContext.tablePath());

        // Check auto-maintenance from table properties
        this.autoMaintenanceEnabled =
                Boolean.parseBoolean(
                        icebergTable.properties().getOrDefault(AUTO_MAINTENANCE_KEY, "false"));

        // Create a record writer
        this.recordWriter = createRecordWriter(writerInitContext);

        if (autoMaintenanceEnabled) {
            this.compactionExecutor =
                    Executors.newSingleThreadExecutor(
                            new ExecutorThreadFactory(
                                    "iceberg-compact-" + writerInitContext.tableBucket()));
            scheduleCompaction(writerInitContext);
        } else {
            this.compactionExecutor = null;
        }
    }

    private RecordWriter createRecordWriter(WriterInitContext writerInitContext) {
        List<Integer> equalityFieldIds =
                new ArrayList<>(icebergTable.schema().identifierFieldIds());
        TaskWriter<Record> taskWriter =
                TaskWriterFactory.createTaskWriter(
                        icebergTable,
                        writerInitContext.partition(),
                        writerInitContext.tableBucket().getBucket());
        if (equalityFieldIds.isEmpty()) {
            return new AppendOnlyTaskWriter(icebergTable, writerInitContext, taskWriter);
        } else {
            return new DeltaTaskWriter(icebergTable, writerInitContext, taskWriter);
        }
    }

    @Override
    public void write(LogRecord record) throws IOException {
        try {
            recordWriter.write(record);
        } catch (Exception e) {
            throw new IOException("Failed to write Fluss record to Iceberg.", e);
        }
    }

    @Override
    public IcebergWriteResult complete() throws IOException {
        try {
            WriteResult writeResult = recordWriter.complete();

            RewriteDataFileResult rewriteDataFileResult = null;
            if (compactionFuture != null) {
                rewriteDataFileResult = compactionFuture.get();
            }
            return new IcebergWriteResult(writeResult, rewriteDataFileResult);
        } catch (Exception e) {
            throw new IOException("Failed to complete Iceberg write.", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (compactionFuture != null && !compactionFuture.isDone()) {
                compactionFuture.cancel(true);
            }

            if (compactionExecutor != null
                    && !compactionExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warn("Fail to close compactionExecutor.");
            }

            if (recordWriter != null) {
                recordWriter.close();
            }
            if (icebergCatalog != null && icebergCatalog instanceof Closeable) {
                ((Closeable) icebergCatalog).close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close IcebergLakeWriter.", e);
        }
    }

    private Table getTable(TablePath tablePath) throws IOException {
        try {
            return icebergCatalog.loadTable(toIceberg(tablePath));
        } catch (Exception e) {
            throw new IOException("Failed to get table " + tablePath + " in Iceberg.", e);
        }
    }

    private void scheduleCompaction(WriterInitContext writerInitContext) {
        compactionFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                Table table = icebergTable;
                                IcebergRewriteDataFiles rewriter =
                                        new IcebergRewriteDataFiles(
                                                        table,
                                                        writerInitContext.partition(),
                                                        writerInitContext.tableBucket())
                                                .targetSizeInBytes(compactionTargetSize(table));
                                return rewriter.execute();
                            } catch (Exception e) {
                                LOG.info("Fail to compact iceberg data files.", e);
                                // Swallow and return null to avoid failing the write path
                                return null;
                            }
                        },
                        compactionExecutor);
    }

    private static long compactionTargetSize(Table icebergTable) {
        long splitSize =
                PropertyUtil.propertyAsLong(
                        icebergTable.properties(),
                        TableProperties.SPLIT_SIZE,
                        TableProperties.SPLIT_SIZE_DEFAULT);
        long targetFileSize =
                PropertyUtil.propertyAsLong(
                        icebergTable.properties(),
                        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
                        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
        return Math.min(splitSize, targetFileSize);
    }
}
