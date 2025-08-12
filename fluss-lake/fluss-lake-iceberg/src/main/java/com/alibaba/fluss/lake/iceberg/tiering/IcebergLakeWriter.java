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

import com.alibaba.fluss.lake.iceberg.tiering.append.AppendOnlyWriter;
import com.alibaba.fluss.lake.writer.LakeWriter;
import com.alibaba.fluss.lake.writer.WriterInitContext;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.LogRecord;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.PropertyUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.alibaba.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;

/** Implementation of {@link LakeWriter} for Iceberg. */
public class IcebergLakeWriter implements LakeWriter<IcebergWriteResult> {

    private static final String AUTO_MAINTENANCE_PROPERTY = "table.datalake.auto-maintenance";

    private final Catalog icebergCatalog;
    private final Table icebergTable;
    private final RecordWriter recordWriter;
    private final boolean autoMaintenance;
    private final ExecutorService compactionExecutor;

    public IcebergLakeWriter(
            IcebergCatalogProvider icebergCatalogProvider, WriterInitContext writerInitContext)
            throws IOException {
        this.icebergCatalog = icebergCatalogProvider.get();
        this.icebergTable = getTable(writerInitContext.tablePath());

        // Check if auto-maintenance is enabled
        this.autoMaintenance =
                PropertyUtil.propertyAsBoolean(
                        icebergTable.properties(), AUTO_MAINTENANCE_PROPERTY, false);

        // Initialize compaction executor if needed
        this.compactionExecutor =
                autoMaintenance
                        ? Executors.newSingleThreadExecutor(
                                r -> new Thread(r, "iceberg-compaction"))
                        : null;

        // Create record writer based on table type
        // For now, only supporting non-partitioned append-only tables
        this.recordWriter = createRecordWriter(writerInitContext);
    }

    private RecordWriter createRecordWriter(WriterInitContext writerInitContext)
            throws IOException {
        if (!icebergTable.spec().isUnpartitioned()) {
            throw new UnsupportedOperationException("Partitioned tables are not yet supported");
        }

        // For now, assume append-only (no primary keys)

        return new AppendOnlyWriter(
                icebergTable,
                writerInitContext.tableBucket(),
                null, // No partition for non-partitioned table
                Collections.emptyList(), // No partition keys
                autoMaintenance,
                compactionExecutor);
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
            if (compactionExecutor != null) {
                compactionExecutor.shutdown();
            }
            if (icebergCatalog != null && icebergCatalog instanceof AutoCloseable) {
                ((AutoCloseable) icebergCatalog).close();
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
}
