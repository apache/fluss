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
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Concrete implementation of {@link RewriteDataFiles} for Fluss's Iceberg integration. Handles
 * bin-packing compaction of small files into larger ones.
 */
public class IcebergRewriteDataFiles implements RewriteDataFiles {

    private final Table table;
    private Expression filter = Expressions.alwaysTrue();
    private long targetSizeInBytes = 128 * 1024 * 1024; // 128MB default

    public IcebergRewriteDataFiles(Table table) {
        this.table = table;
    }

    @Override
    public RewriteDataFiles filter(Expression expression) {
        this.filter = expression;
        return this;
    }

    @Override
    public RewriteDataFiles targetSizeInBytes(long targetSize) {
        this.targetSizeInBytes = targetSize;
        return this;
    }

    @Override
    public RewriteDataFiles binPack() {
        return this;
    }

    @Override
    public RewriteDataFilesActionResult execute() {
        try {
            // Scan files matching the filter
            List<DataFile> filesToRewrite = scanFilesToRewrite();

            if (filesToRewrite.size() < 2) {
                return new RewriteDataFilesActionResult(new ArrayList<>(), new ArrayList<>());
            }

            // Group files using bin-packing strategy
            List<List<DataFile>> fileGroups = binPackFiles(filesToRewrite);

            List<DataFile> allNewFiles = new ArrayList<>();
            List<DataFile> allOldFiles = new ArrayList<>();

            // Rewrite each group
            for (List<DataFile> group : fileGroups) {
                if (group.size() < 2) {
                    continue;
                }

                DataFile newFile = rewriteFileGroup(group);
                if (newFile != null) {
                    allNewFiles.add(newFile);
                    allOldFiles.addAll(group);
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

    private List<DataFile> scanFilesToRewrite() throws IOException {
        List<DataFile> files = new ArrayList<>();

        try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(filter).planFiles()) {

            for (FileScanTask task : tasks) {
                files.add(task.file());
            }
        } catch (IOException e) {
            throw new IOException("Failed to scan files", e);
        }
        return files;
    }

    private List<List<DataFile>> binPackFiles(List<DataFile> files) {
        List<List<DataFile>> groups = new ArrayList<>();
        List<DataFile> currentGroup = new ArrayList<>();
        long currentSize = 0;

        // Sort files by size for better bin-packing (smallest first)
        files.sort((f1, f2) -> Long.compare(f1.fileSizeInBytes(), f2.fileSizeInBytes()));

        for (DataFile file : files) {
            if (currentSize + file.fileSizeInBytes() > targetSizeInBytes
                    && !currentGroup.isEmpty()) {
                // Start a new group
                groups.add(new ArrayList<>(currentGroup));
                currentGroup.clear();
                currentSize = 0;
            }
            currentGroup.add(file);
            currentSize += file.fileSizeInBytes();
        }

        // Add the last group
        if (!currentGroup.isEmpty()) {
            groups.add(currentGroup);
        }
        return groups;
    }

    private DataFile rewriteFileGroup(List<DataFile> group) {
        try {
            // Generate output file path
            String fileName = String.format("%s.parquet", UUID.randomUUID());
            OutputFile outputFile =
                    table.io().newOutputFile(table.locationProvider().newDataLocation(fileName));

            // Create Parquet writer
            FileAppenderFactory<Record> appenderFactory =
                    new GenericAppenderFactory(table.schema());

            Parquet.WriteBuilder writeBuilder =
                    Parquet.write(outputFile)
                            .schema(table.schema())
                            .createWriterFunc(GenericParquetWriter::buildWriter)
                            .overwrite();

            // Use Iceberg FileAppender to write, then build a DataFile manually
            Metrics metrics;
            long recordCount = 0L;
            org.apache.iceberg.io.FileAppender<Record> writer = null;
            try {
                writer = writeBuilder.build();
                for (DataFile file : group) {
                    try (CloseableIterable<Record> records = readDataFile(file)) {
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
            DataFile sample = group.get(0);

            String location = outputFile.location();
            InputFile inputFile = table.io().newInputFile(location);
            long fileSizeInBytes = inputFile.getLength();

            DataFile newFile =
                    DataFiles.builder(spec)
                            .withPath(location)
                            .withFileSizeInBytes(fileSizeInBytes)
                            .withFormat(FileFormat.PARQUET)
                            .withRecordCount(recordCount)
                            .withMetrics(metrics)
                            .withPartition(sample.partition()) // no-op for unpartitioned specs
                            .build();

            return newFile;

        } catch (Exception e) {
            throw new RuntimeException("Failed to rewrite file group", e);
        }
    }

    private CloseableIterable<Record> readDataFile(DataFile file) throws IOException {
        return Parquet.read(table.io().newInputFile(file.path().toString()))
                .project(table.schema())
                .createReaderFunc(
                        fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema))
                .build();
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
