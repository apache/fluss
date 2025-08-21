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

package com.alibaba.fluss.lake.iceberg.tiering;

import com.alibaba.fluss.lake.iceberg.actions.IcebergRewriteDataFiles;
import com.alibaba.fluss.lake.iceberg.tiering.writer.AppendOnlyTaskWriter;
import com.alibaba.fluss.lake.iceberg.tiering.writer.DeltaTaskWriter;
import com.alibaba.fluss.lake.writer.LakeWriter;
import com.alibaba.fluss.lake.writer.WriterInitContext;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.utils.concurrent.ExecutorThreadFactory;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.alibaba.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

/** Implementation of {@link LakeWriter} for Iceberg. */
public class IcebergLakeWriter implements LakeWriter<IcebergWriteResult> {

    protected static final Logger LOG = LoggerFactory.getLogger(IcebergLakeWriter.class);

    private static final String AUTO_MAINTENANCE_KEY = "table.datalake.auto-maintenance";
    private static final long SMALL_FILE_THRESHOLD = 50 * 1024 * 1024; // 50MB

    private final Catalog icebergCatalog;
    private final Table icebergTable;
    private final RecordWriter recordWriter;
    private final TableBucket tableBucket;
    private final boolean autoMaintenanceEnabled;
    @Nullable private final String bucketPartitionFieldName;

    @Nullable private final ExecutorService compactionExecutor;
    @Nullable private CompletableFuture<RewriteDataFilesActionResult> compactionFuture;

    public IcebergLakeWriter(
            IcebergCatalogProvider icebergCatalogProvider, WriterInitContext writerInitContext)
            throws IOException {
        this.icebergCatalog = icebergCatalogProvider.get();
        this.icebergTable = getTable(writerInitContext.tablePath());
        this.tableBucket = writerInitContext.tableBucket();
        this.bucketPartitionFieldName = resolveBucketPartitionFieldName(icebergTable);

        // Check auto-maintenance from table properties
        this.autoMaintenanceEnabled =
                Boolean.parseBoolean(
                        icebergTable.properties().getOrDefault(AUTO_MAINTENANCE_KEY, "false"));

        // Create a record writer
        this.recordWriter = createRecordWriter(writerInitContext);

        if (autoMaintenanceEnabled) {
            this.compactionExecutor =
                    Executors.newSingleThreadExecutor(
                            new ExecutorThreadFactory("iceberg-compact-" + tableBucket));
            scheduleCompactionIfNeeded(tableBucket.getBucket());
        } else {
            this.compactionExecutor = null;
        }
    }

    private RecordWriter createRecordWriter(WriterInitContext writerInitContext) {
        Schema schema = icebergTable.schema();
        List<Integer> equalityFieldIds = new ArrayList<>(schema.identifierFieldIds());

        // Get target file size from table properties
        long targetFileSize = targetFileSize(icebergTable);
        FileFormat format = fileFormat(icebergTable);
        OutputFileFactory outputFileFactory =
                OutputFileFactory.builderFor(
                                icebergTable,
                                writerInitContext.tableBucket().getBucket(),
                                // task id always 0
                                0)
                        .format(format)
                        .build();

        if (equalityFieldIds.isEmpty()) {
            return new AppendOnlyTaskWriter(
                    icebergTable, writerInitContext, format, outputFileFactory, targetFileSize);
        } else {
            return new DeltaTaskWriter(
                    icebergTable, writerInitContext, format, outputFileFactory, targetFileSize);
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

            RewriteDataFilesActionResult rewriteDataFilesActionResult;
            try {
                rewriteDataFilesActionResult = compactionFuture.get();
            } catch (CancellationException e) {
                rewriteDataFilesActionResult = null;
            }

            return new IcebergWriteResult(writeResult, rewriteDataFilesActionResult);
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

            if (!compactionExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
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

    private void scheduleCompactionIfNeeded(int bucketId) {
        if (bucketPartitionFieldName == null) {
            return;
        }

        try {
            compactionFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    // Scan files for this bucket
                                    List<DataFile> bucketFiles = scanBucketFiles(bucketId);

                                    // Check if compaction needed
                                    if (shouldCompact(bucketFiles)) {
                                        return compactFiles(bucketFiles, bucketId);
                                    }
                                    return null;
                                } catch (Exception e) {
                                    // Swallow and return null to avoid failing the write path
                                    return null;
                                }
                            },
                            compactionExecutor);
        } catch (Exception e) {
            throw new RuntimeException("Failed to schedule compaction for bucket: " + bucketId, e);
        }
    }

    private List<DataFile> scanBucketFiles(int bucketId) {
        List<DataFile> bucketFiles = new ArrayList<>();

        try {
            if (bucketPartitionFieldName == null) {
                return bucketFiles;
            }
            // Scan files filtered by Iceberg bucket partition field
            CloseableIterable<FileScanTask> tasks =
                    icebergTable
                            .newScan()
                            .filter(Expressions.equal(bucketPartitionFieldName, bucketId))
                            .planFiles();

            try (CloseableIterable<FileScanTask> scanTasks = tasks) {
                for (FileScanTask task : scanTasks) {
                    bucketFiles.add(task.file());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to scan files for bucket: " + bucketId, e);
        }

        return bucketFiles;
    }

    private boolean shouldCompact(List<DataFile> files) {
        if (files.size() > 1) {
            return true;
        }
        return false;
    }

    private RewriteDataFilesActionResult compactFiles(List<DataFile> files, int bucketId) {
        if (files.size() < 2) {
            // No effective rewrite necessary
            return null;
        }

        try {
            // Use IcebergRewriteDataFiles to perform the actual compaction
            IcebergRewriteDataFiles rewriter = new IcebergRewriteDataFiles(icebergTable);

            RewriteDataFilesActionResult result =
                    rewriter.filter(Expressions.equal(bucketPartitionFieldName, bucketId))
                            .targetSizeInBytes(compactionTargetSize(icebergTable))
                            .binPack()
                            .execute();

            return result;

        } catch (Exception e) {
            return null;
        }
    }

    @Nullable
    private static String resolveBucketPartitionFieldName(Table table) {
        try {
            for (PartitionField f : table.spec().fields()) {
                String name = f.name();
                if (name != null) {
                    return name;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to resolve bucket partition field name from table spec", e);
        }
        return null;
    }

    private static FileFormat fileFormat(Table icebergTable) {
        String formatString =
                PropertyUtil.propertyAsString(
                        icebergTable.properties(),
                        TableProperties.DEFAULT_FILE_FORMAT,
                        TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
        return FileFormat.fromString(formatString);
    }

    private static long targetFileSize(Table icebergTable) {
        return PropertyUtil.propertyAsLong(
                icebergTable.properties(),
                WRITE_TARGET_FILE_SIZE_BYTES,
                WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
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
