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

import com.alibaba.fluss.lake.iceberg.tiering.writer.AppendOnlyWriter;
import com.alibaba.fluss.lake.iceberg.tiering.writer.DeltaTaskWriter;
import com.alibaba.fluss.lake.writer.LakeWriter;
import com.alibaba.fluss.lake.writer.WriterInitContext;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.LogRecord;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.PropertyUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.alibaba.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

/** Implementation of {@link LakeWriter} for Iceberg. */
public class IcebergLakeWriter implements LakeWriter<IcebergWriteResult> {

    private final Catalog icebergCatalog;
    private final Table icebergTable;
    private final RecordWriter recordWriter;

    public IcebergLakeWriter(
            IcebergCatalogProvider icebergCatalogProvider, WriterInitContext writerInitContext)
            throws IOException {
        this.icebergCatalog = icebergCatalogProvider.get();
        this.icebergTable = getTable(writerInitContext.tablePath());

        // Create record writer based on table type
        // For now, only supporting non-partitioned append-only tables
        this.recordWriter = createRecordWriter(writerInitContext);
    }

    private RecordWriter createRecordWriter(WriterInitContext writerInitContext) {
        Schema schema = icebergTable.schema();
        List<Integer> equalityFieldIds = new ArrayList<>(schema.identifierFieldIds());
        PartitionSpec spec = icebergTable.spec();

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

        if (equalityFieldIds == null || equalityFieldIds.isEmpty()) {
            if (spec.isUnpartitioned()) {
                return new AppendOnlyWriter(
                        icebergTable,
                        writerInitContext.schema().getRowType(),
                        writerInitContext.tableBucket(),
                        null, // No partition for non-partitioned table
                        Collections.emptyList(), // No partition keys
                        format,
                        outputFileFactory,
                        targetFileSize);
            } else {
                return null;
            }
        } else {
            if (spec.isUnpartitioned()) {
                return new DeltaTaskWriter(
                        icebergTable,
                        writerInitContext.schema().getRowType(),
                        writerInitContext.tableBucket(),
                        null, // No partition for non-partitioned table
                        Collections.emptyList(), // No partition keys);
                        format,
                        outputFileFactory,
                        targetFileSize);
            } else {
                return null;
            }
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
            return new IcebergWriteResult(writeResult);
        } catch (Exception e) {
            throw new IOException("Failed to complete Iceberg write.", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
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
}
