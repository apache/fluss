/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.maintenance;

import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.metadata.TableBucket;

import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenericReader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.BinPacking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toFilterExpression;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Concrete implementation for Fluss's Iceberg integration. Handles bin-packing compaction of small
 * files into larger ones.
 */
public class IcebergRewriteDataFiles {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergRewriteDataFiles.class);

    private static final int MIN_FILES_TO_COMPACT = 3;

    private final Table table;
    private final String partition;
    private final TableBucket bucket;
    private final Expression filter;
    private long targetSizeInBytes = 128 * 1024 * 1024; // 128MB default

    public IcebergRewriteDataFiles(Table table, @Nullable String partition, TableBucket bucket) {
        this.table = table;
        this.partition = partition;
        this.bucket = bucket;
        this.filter = toFilterExpression(table, partition, bucket.getBucket());
    }

    public IcebergRewriteDataFiles targetSizeInBytes(long targetSize) {
        this.targetSizeInBytes = targetSize;
        return this;
    }

    private List<CombinedScanTask> planRewriteFileGroups() throws IOException {
        List<FileScanTask> fileScanTasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks =
                table.newScan().includeColumnStats().filter(filter).ignoreResiduals().planFiles()) {
            tasks.forEach(fileScanTasks::add);
        }

        // the files < targetSizeInBytes is less than MIN_FILES_TO_COMPACT, don't compact
        if (fileScanTasks.stream()
                        .filter(fileScanTask -> fileScanTask.length() < targetSizeInBytes)
                        .count()
                < MIN_FILES_TO_COMPACT) {
            // return empty file group
            return Collections.emptyList();
        }

        // then, pack the fileScanTasks into compaction units which contains compactable
        // fileScanTasks, after compaction, we want to it still keep order by __offset column,
        // so, let's first sort by __offset column
        int offsetFieldId = table.schema().findField(OFFSET_COLUMN_NAME).fieldId();
        fileScanTasks.sort(sortFileScanTask(offsetFieldId));

        // do package now
        BinPacking.ListPacker<FileScanTask> packer =
                new BinPacking.ListPacker<>(targetSizeInBytes, 1, false);
        return packer.pack(fileScanTasks, ContentScanTask::length).stream()
                .filter(tasks -> tasks.size() > 1)
                .map(BaseCombinedScanTask::new)
                .collect(Collectors.toList());
    }

    private Comparator<FileScanTask> sortFileScanTask(int sortFiledId) {
        return (f1, f2) -> {
            ByteBuffer buffer1 =
                    f1.file()
                            .lowerBounds()
                            .get(sortFiledId)
                            .order(ByteOrder.LITTLE_ENDIAN)
                            .rewind();
            long offset1 = buffer1.getLong();
            ByteBuffer buffer2 =
                    f2.file()
                            .lowerBounds()
                            .get(sortFiledId)
                            .order(ByteOrder.LITTLE_ENDIAN)
                            .rewind();
            long offset2 = buffer2.getLong();
            return Long.compare(offset1, offset2);
        };
    }

    @Nullable
    public RewriteDataFileResult execute() {
        try {
            // plan the file groups to be rewrite
            List<CombinedScanTask> tasksToRewrite = planRewriteFileGroups();
            if (tasksToRewrite.isEmpty()) {
                return null;
            }
            LOG.info("Start to rewrite files {}.", tasksToRewrite);
            List<DataFile> deletedDataFiles = new ArrayList<>();
            List<DataFile> addedDataFiles = new ArrayList<>();
            for (CombinedScanTask combinedScanTask : tasksToRewrite) {
                addedDataFiles.addAll(rewriteFileGroup(combinedScanTask));
                deletedDataFiles.addAll(
                        combinedScanTask.files().stream()
                                .map(ContentScanTask::file)
                                .collect(Collectors.toList()));
            }
            LOG.info("Finish rewriting files from {} to {}.", deletedDataFiles, addedDataFiles);
            return new RewriteDataFileResult(deletedDataFiles, addedDataFiles);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Fail to compact bucket %s of table %s.", bucket, table.name()),
                    e);
        }
    }

    private List<DataFile> rewriteFileGroup(CombinedScanTask combinedScanTask) throws IOException {
        try (CloseableIterable<Record> records = readDataFile(combinedScanTask);
                TaskWriter<Record> taskWriter =
                        TaskWriterFactory.createTaskWriter(table, partition, bucket.getBucket())) {
            for (Record record : records) {
                taskWriter.write(record);
            }
            WriteResult rewriteResult = taskWriter.complete();
            checkState(
                    rewriteResult.deleteFiles().length == 0,
                    "the delete files should be empty, but got "
                            + Arrays.toString(rewriteResult.deleteFiles()));
            return Arrays.asList(rewriteResult.dataFiles());
        }
    }

    private CloseableIterable<Record> readDataFile(CombinedScanTask combinedScanTask) {
        IcebergGenericReader reader = new IcebergGenericReader(table.newScan(), true);
        return reader.open(combinedScanTask);
    }
}
