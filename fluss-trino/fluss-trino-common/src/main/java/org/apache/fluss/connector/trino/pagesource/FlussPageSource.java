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

package org.apache.fluss.connector.trino.pagesource;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScan;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.connector.trino.connection.FlussClientManager;
import org.apache.fluss.connector.trino.handle.FlussColumnHandle;
import org.apache.fluss.connector.trino.handle.FlussTableHandle;
import org.apache.fluss.connector.trino.split.FlussSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.InternalRow;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Page source for reading data from Fluss.
 * 
 * <p>This class implements the Trino ConnectorPageSource interface to read data
 * from Fluss tables. It supports reading from both LogStore and KvStore.
 */
public class FlussPageSource implements ConnectorPageSource {

    private static final Logger log = Logger.get(FlussPageSource.class);
    private static final int ROWS_PER_REQUEST = 1024;

    private final FlussClientManager clientManager;
    private final FlussTableHandle tableHandle;
    private final FlussSplit split;
    private final List<FlussColumnHandle> columns;
    private final PageBuilder pageBuilder;

    private Table table;
    private LogScanner scanner;
    private boolean finished;
    private long completedBytes;
    private long readTimeNanos;

    public FlussPageSource(
            FlussClientManager clientManager,
            FlussTableHandle tableHandle,
            FlussSplit split,
            List<FlussColumnHandle> columns) {
        this.clientManager = requireNonNull(clientManager, "clientManager is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.split = requireNonNull(split, "split is null");
        this.columns = requireNonNull(columns, "columns is null");
        
        // Initialize page builder with column types
        List<Type> types = columns.stream()
                .map(FlussColumnHandle::getType)
                .collect(java.util.stream.Collectors.toList());
        this.pageBuilder = new PageBuilder(types);
        
        this.finished = false;
        this.completedBytes = 0;
        this.readTimeNanos = 0;
        
        initializeScanner();
    }

    private void initializeScanner() {
        try {
            // Get table client from Fluss
            table = clientManager.getTable(split.getTablePath());
            
            // Create log scanner for the bucket
            TableBucket tableBucket = split.getTableBucket();
            LogScan logScan = new LogScan();
            
            scanner = table.getLogScanner(logScan);
            scanner.subscribeFromBeginning(tableBucket.getBucket());
            
            log.debug("Initialized scanner for table: %s, bucket: %s",
                    split.getTablePath(), tableBucket);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Fluss scanner", e);
        }
    }

    @Override
    public long getCompletedBytes() {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos() {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public Page getNextPage() {
        if (finished) {
            return null;
        }

        long start = System.nanoTime();
        
        try {
            // Poll records from Fluss
            ScanRecords records = scanner.poll(Duration.ofMillis(100));
            
            if (records.isEmpty()) {
                // No more data available
                finished = true;
                return null;
            }
            
            // Process records and build page
            for (ScanRecord record : records) {
                processRecord(record);
                
                // Build page when we have enough rows
                if (pageBuilder.isFull()) {
                    break;
                }
            }
            
            // Return the completed page
            if (!pageBuilder.isEmpty()) {
                Page page = pageBuilder.build();
                pageBuilder.reset();
                
                // Update metrics
                completedBytes += page.getSizeInBytes();
                readTimeNanos += System.nanoTime() - start;
                
                return page;
            }
            
            return null;
        } catch (Exception e) {
            throw new RuntimeException("Error reading from Fluss", e);
        }
    }

    private void processRecord(ScanRecord record) {
        pageBuilder.declarePosition();
        
        InternalRow row = record.getRow();
        
        for (int i = 0; i < columns.size(); i++) {
            FlussColumnHandle column = columns.get(i);
            int fieldIndex = column.getOrdinalPosition();
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
            Type type = column.getType();
            
            // Check if value is null
            if (row.isNullAt(fieldIndex)) {
                blockBuilder.appendNull();
                continue;
            }
            
            // Write value based on type
            writeValue(blockBuilder, type, row, fieldIndex);
        }
    }

    private void writeValue(BlockBuilder blockBuilder, Type type, InternalRow row, int fieldIndex) {
        String typeName = type.getDisplayName().toLowerCase();
        
        try {
            if (typeName.equals("boolean")) {
                type.writeBoolean(blockBuilder, row.getBoolean(fieldIndex));
            } else if (typeName.equals("tinyint")) {
                type.writeLong(blockBuilder, row.getByte(fieldIndex));
            } else if (typeName.equals("smallint")) {
                type.writeLong(blockBuilder, row.getShort(fieldIndex));
            } else if (typeName.equals("integer")) {
                type.writeLong(blockBuilder, row.getInt(fieldIndex));
            } else if (typeName.equals("bigint")) {
                type.writeLong(blockBuilder, row.getLong(fieldIndex));
            } else if (typeName.equals("real")) {
                type.writeLong(blockBuilder, Float.floatToIntBits(row.getFloat(fieldIndex)));
            } else if (typeName.equals("double")) {
                type.writeDouble(blockBuilder, row.getDouble(fieldIndex));
            } else if (typeName.startsWith("varchar")) {
                String value = row.getString(fieldIndex).toString();
                Slice slice = Slices.utf8Slice(value);
                type.writeSlice(blockBuilder, slice);
            } else if (typeName.startsWith("varbinary")) {
                byte[] bytes = row.getBytes(fieldIndex);
                Slice slice = Slices.wrappedBuffer(bytes);
                type.writeSlice(blockBuilder, slice);
            } else {
                // Fallback to string representation
                String value = row.getString(fieldIndex).toString();
                Slice slice = Slices.utf8Slice(value);
                type.writeSlice(blockBuilder, slice);
            }
        } catch (Exception e) {
            log.warn(e, "Error writing value for column at index %d, type %s", fieldIndex, typeName);
            blockBuilder.appendNull();
        }
    }

    @Override
    public long getMemoryUsage() {
        return pageBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void close() throws IOException {
        try {
            if (scanner != null) {
                scanner.close();
            }
        } catch (Exception e) {
            log.warn(e, "Error closing scanner");
        }
    }
}
