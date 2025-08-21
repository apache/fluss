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

package com.alibaba.fluss.lake.iceberg.actions;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.IcebergGenericReader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Concrete implementation for Fluss's Iceberg integration. Handles bin-packing compaction of small
 * files into larger ones.
 */
public class IcebergRewriteDataFiles {

    private final Table table;
    private Expression filter = Expressions.alwaysTrue();
    private long targetSizeInBytes = 128 * 1024 * 1024; // 128MB default

    public IcebergRewriteDataFiles(Table table) {
        this.table = table;
    }

    public IcebergRewriteDataFiles filter(Expression expression) {
        this.filter = expression;
        return this;
    }

    public IcebergRewriteDataFiles targetSizeInBytes(long targetSize) {
        this.targetSizeInBytes = targetSize;
        return this;
    }

    public IcebergRewriteDataFiles binPack() {
        return this;
    }

    public RewriteDataFilesActionResult execute() {
        try {
            // Scan files matching the filter
            List<FileScanTask> tasksToRewrite = scanFilesToRewrite();

            if (tasksToRewrite.size() < 2) {
                return new RewriteDataFilesActionResult(
                        Collections.emptyList(), Collections.emptyList());
            }

            // Group files using bin-packing strategy
            List<List<FileScanTask>> fileGroups = binPackFiles(tasksToRewrite);

            List<DataFile> allNewFiles = new ArrayList<>();
            List<DataFile> allOldFiles = new ArrayList<>();

            // Rewrite each group
            for (List<FileScanTask> group : fileGroups) {
                if (group.size() < 2) {
                    continue;
                }

                DataFile newFile = rewriteFileGroup(group);
                if (newFile != null) {
                    allNewFiles.add(newFile);
                    for (FileScanTask task : group) {
                        allOldFiles.add(task.file());
                    }
                }
            }

            if (!allNewFiles.isEmpty()) {
                commitChanges(allOldFiles, allNewFiles);
            }

            return new RewriteDataFilesActionResult(allNewFiles, allOldFiles);

        } catch (Exception e) {
            throw new RuntimeException("Compaction failed", e);
        }
    }

    private List<FileScanTask> scanFilesToRewrite() throws IOException {
        List<FileScanTask> tasks = new ArrayList<>();

        try (CloseableIterable<FileScanTask> scanTasks =
                table.newScan().filter(filter).planFiles()) {

            for (FileScanTask task : scanTasks) {
                tasks.add(task);
            }
        } catch (IOException e) {
            throw new IOException("Failed to scan files", e);
        }
        return tasks;
    }

    private List<List<FileScanTask>> binPackFiles(List<FileScanTask> tasks) {
        List<List<FileScanTask>> groups = new ArrayList<>();
        List<FileScanTask> currentGroup = new ArrayList<>();
        long currentSize = 0;

        // Sort files by size for better bin-packing (smallest first)
        tasks.sort(
                (t1, t2) -> Long.compare(t1.file().fileSizeInBytes(), t2.file().fileSizeInBytes()));

        for (FileScanTask task : tasks) {
            long size = task.file().fileSizeInBytes();
            if (currentSize + size > targetSizeInBytes && !currentGroup.isEmpty()) {
                // Start a new group
                groups.add(new ArrayList<>(currentGroup));
                currentGroup.clear();
                currentSize = 0;
            }
            currentGroup.add(task);
            currentSize += size;
        }

        // Add the last group
        if (!currentGroup.isEmpty()) {
            groups.add(currentGroup);
        }
        return groups;
    }

    private DataFile rewriteFileGroup(List<FileScanTask> group) {
        try {
            // Generate output file path
            DataFile sample = group.get(0).file();
            FileFormat fileFormat = sample.format();
            String fileName = String.format("%s.%s", UUID.randomUUID(), fileFormat.toString());
            OutputFile outputFile =
                    table.io().newOutputFile(table.locationProvider().newDataLocation(fileName));

            FileAppenderFactory<Record> appenderFactory =
                    new GenericAppenderFactory(table.schema());

            // Use Iceberg FileAppender to write, then build a DataFile manually
            Metrics metrics;
            long recordCount = 0L;
            FileAppender<Record> writer = null;
            try {
                writer = appenderFactory.newAppender(outputFile, fileFormat);

                for (FileScanTask task : group) {
                    try (CloseableIterable<Record> records = readDataFile(task)) {
                        for (Record record : records) {
                            writer.add(record);
                            recordCount++;
                        }
                    }
                }
            } finally {
                if (writer != null) {
                    writer.close();
                }
            }
            metrics = writer.metrics();

            // Assumes all files in group share the same partition
            PartitionSpec spec = table.spec();
            StructLike partition = sample.partition();

            String location = outputFile.location();
            InputFile inputFile = table.io().newInputFile(location);
            long fileSizeInBytes = inputFile.getLength();

            DataFile newFile =
                    DataFiles.builder(spec)
                            .withPath(location)
                            .withFileSizeInBytes(fileSizeInBytes)
                            .withFormat(fileFormat)
                            .withRecordCount(recordCount)
                            .withMetrics(metrics)
                            .withPartition(partition) // no-op for unpartitioned specs
                            .build();

            return newFile;

        } catch (Exception e) {
            throw new RuntimeException("Failed to rewrite file group", e);
        }
    }

    private CloseableIterable<Record> readDataFile(FileScanTask task) throws IOException {
        IcebergGenericReader reader = new IcebergGenericReader(table.newScan(), true);
        return reader.open(task);
    }

    private void commitChanges(List<DataFile> oldFiles, List<DataFile> newFiles) {
        RewriteFiles rewrite = table.newRewrite();

        for (DataFile oldFile : oldFiles) {
            rewrite.deleteFile(oldFile);
        }

        for (DataFile newFile : newFiles) {
            rewrite.addFile(newFile);
        }

        rewrite.commit();
    }
}
