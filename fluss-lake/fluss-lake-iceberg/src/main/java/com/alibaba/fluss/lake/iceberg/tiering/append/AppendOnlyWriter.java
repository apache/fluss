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

package com.alibaba.fluss.lake.iceberg.tiering.append;

import com.alibaba.fluss.lake.iceberg.tiering.RecordWriter;
import com.alibaba.fluss.lake.iceberg.utils.IcebergConversions;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.LogRecord;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BaseRewriteDataFilesAction;
import org.apache.iceberg.flink.sink.FlinkAppenderFactory;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/** A {@link RecordWriter} to write to Iceberg's append-only table. */
public class AppendOnlyWriter extends RecordWriter {

    private static final String TARGET_FILE_SIZE_PROPERTY = "write.target-file-size-bytes";
    private static final long DEFAULT_TARGET_FILE_SIZE = 128L * 1024 * 1024; // 128MB

    private final Table icebergTable;
    private final boolean autoMaintenance;

    public AppendOnlyWriter(
            Table icebergTable,
            TableBucket tableBucket,
            @Nullable String partition,
            List<String> partitionKeys,
            boolean autoMaintenance,
            ExecutorService compactionExecutor) {
        super(
                createTaskWriter(icebergTable, tableBucket),
                icebergTable.schema(),
                tableBucket,
                partition,
                partitionKeys,
                compactionExecutor);
        this.icebergTable = icebergTable;
        this.autoMaintenance = autoMaintenance;

        // Schedule background compaction if enabled
        scheduleCompactionIfNeeded(autoMaintenance);
    }

    private static TaskWriter<RowData> createTaskWriter(
            Table icebergTable, TableBucket tableBucket) {
        // Get target file size from table properties
        long targetFileSize =
                PropertyUtil.propertyAsLong(
                        icebergTable.properties(),
                        TARGET_FILE_SIZE_PROPERTY,
                        DEFAULT_TARGET_FILE_SIZE);

        // Create Flink row type from Iceberg schema
        RowType flinkRowType = createFlinkRowType(icebergTable.schema());

        FileAppenderFactory<RowData> appenderFactory =
                new FlinkAppenderFactory(
                        icebergTable.schema(),
                        flinkRowType,
                        icebergTable.properties(),
                        icebergTable.spec(),
                        null, // No equality fields for append-only
                        null, // No equality delete schema
                        null); // No equality field names

        OutputFileFactory outputFileFactory =
                OutputFileFactory.builderFor(
                                icebergTable,
                                tableBucket.getBucket(), // Use bucket as task ID
                                0) // attempt ID
                        .format(FileFormat.PARQUET)
                        .build();

        return new UnpartitionedWriter<>(
                icebergTable.spec(),
                FileFormat.PARQUET,
                appenderFactory,
                outputFileFactory,
                icebergTable.io(),
                targetFileSize);
    }

    @Override
    public void write(LogRecord record) throws Exception {
        flussRecordAsIcebergRow.setFlussRecord(record);
        taskWriter.write(flussRecordAsIcebergRow);
    }

    @Override
    protected void performCompaction() throws Exception {
        if (!autoMaintenance) {
            return;
        }

        try {
            BaseRewriteDataFilesAction<?> rewriteAction =
                    (BaseRewriteDataFilesAction<?>) icebergTable.newRewrite();

            // TODO: Filter files by bucket and check if compaction is needed
            // rewriteAction.filter(Expressions.equal("__bucket", bucket));

            // TODO: Configure compaction strategy
            // rewriteAction.option("target-file-size-bytes", String.valueOf(targetFileSize));
            // rewriteAction.execute();

        } catch (Exception e) {
            System.err.println(
                    "Background compaction failed for bucket " + bucket + ": " + e.getMessage());
        }
    }

    private static RowType createFlinkRowType(Schema icebergSchema) {
        // Convert Iceberg schema to Flink RowType using existing conversion utility
        List<RowType.RowField> fields = new ArrayList<>();

        for (Types.NestedField column : icebergSchema.columns()) {
            if (!isSystemColumn(column.name())) {
                LogicalType flinkType = IcebergConversions.toFlinkLogicalType(column.type());
                fields.add(new RowType.RowField(column.name(), flinkType));
            }
        }

        fields.add(new RowType.RowField("__bucket", new IntType()));
        fields.add(new RowType.RowField("__offset", new BigIntType()));
        fields.add(new RowType.RowField("__timestamp", new TimestampType(3)));

        return new RowType(fields);
    }

    private static boolean isSystemColumn(String columnName) {
        return "__bucket".equals(columnName)
                || "__offset".equals(columnName)
                || "__timestamp".equals(columnName);
    }
}
