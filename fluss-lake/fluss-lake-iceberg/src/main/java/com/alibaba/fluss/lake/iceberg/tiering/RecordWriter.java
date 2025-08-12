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

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.LogRecord;

import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/** A base interface to write {@link LogRecord} to Iceberg. */
public abstract class RecordWriter implements AutoCloseable {

    protected final TaskWriter<RowData> taskWriter;
    protected final Schema icebergSchema;
    protected final int bucket;
    @Nullable protected final String partition;
    protected final FlussRecordAsIcebergRow flussRecordAsIcebergRow;
    protected final ExecutorService compactionExecutor;
    protected CompletableFuture<Void> compactionFuture;

    public RecordWriter(
            TaskWriter<RowData> taskWriter,
            Schema icebergSchema,
            TableBucket tableBucket,
            @Nullable String partition,
            List<String> partitionKeys,
            ExecutorService compactionExecutor) {
        this.taskWriter = taskWriter;
        this.icebergSchema = icebergSchema;
        this.bucket = tableBucket.getBucket();
        this.partition = partition; // null for non-partitioned tables
        this.flussRecordAsIcebergRow =
                new FlussRecordAsIcebergRow(tableBucket.getBucket(), icebergSchema);
        this.compactionExecutor = compactionExecutor;
    }

    public abstract void write(LogRecord record) throws Exception;

    public WriteResult complete() throws Exception {
        // Wait for background compaction to complete if running
        if (compactionFuture != null && !compactionFuture.isDone()) {
            compactionFuture.join();
        }

        // Complete the task writer and get write result
        WriteResult writeResult = taskWriter.complete();
        return writeResult;
    }

    @Override
    public void close() throws Exception {
        if (compactionFuture != null && !compactionFuture.isDone()) {
            compactionFuture.cancel(true);
        }
        if (taskWriter != null) {
            taskWriter.close();
        }
    }

    protected void scheduleCompactionIfNeeded(boolean autoMaintenance) {
        if (!autoMaintenance || compactionExecutor == null) {
            return;
        }

        compactionFuture =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                // TODO: Implement compaction logic
                                // 1. Scan manifests for files in this bucket
                                // 2. Check if compaction is beneficial (multiple small files)
                                // 3. Rewrite files if needed
                                performCompaction();
                            } catch (Exception e) {
                                System.err.println(
                                        "Background compaction failed: " + e.getMessage());
                            }
                        },
                        compactionExecutor);
    }

    protected abstract void performCompaction() throws Exception;
}
