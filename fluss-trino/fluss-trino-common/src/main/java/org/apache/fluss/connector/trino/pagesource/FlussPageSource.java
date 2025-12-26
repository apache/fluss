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
    private static final int INITIAL_PAGE_SIZE = 1024;
    private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);

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
            
            // Apply optimizations from table handle if available
            if (tableHandle.getConstraint().isPresent() && 
                !tableHandle.getConstraint().get().isAll()) {
                // Apply predicate pushdown
                logScan = logScan.withFilter(tableHandle.getConstraint().get());
            }
            
            // Apply limit pushdown if available
            if (tableHandle.getLimit().isPresent()) {
                long limit = tableHandle.getLimit().get();
                if (limit > 0 && limit < Integer.MAX_VALUE) {
                    logScan = logScan.withLimit((int) limit);
                }
            }
            
            scanner = table.getLogScanner(logScan);
            scanner.subscribeFromBeginning(tableBucket.getBucket());
            
            log.debug("Initialized scanner for table: %s, bucket: %s with optimizations",
                    split.getTablePath(), tableBucket);
        } catch (Exception e) {
            log.error(e, "Failed to initialize Fluss scanner for table: %s, bucket: %s",
                    split.getTablePath(), split.getTableBucket());
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
            // Poll records from Fluss with adaptive timeout
            Duration pollTimeout = calculatePollTimeout();
            ScanRecords records = scanner.poll(pollTimeout);
            
            if (records.isEmpty()) {
                // Check if we've reached end of stream
                if (scanner.isFinished()) {
                    finished = true;
                    log.debug("Scanner finished for table: %s, bucket: %s",
                            split.getTablePath(), split.getTableBucket());
                    return null;
                }
                
                // No data available right now, but stream may continue
                return null;
            }
            
            int recordsProcessed = 0;
            
            // Process records and build page
            for (ScanRecord record : records) {
                if (record == null) {
                    continue;
                }
                
                try {
                    processRecord(record);
                    recordsProcessed++;
                } catch (Exception e) {
                    log.warn(e, "Error processing record, skipping");
                    // Continue with next record instead of failing completely
                    continue;
                }
                
                // Build page when we have enough rows or page is full
                if (pageBuilder.isFull() || recordsProcessed >= ROWS_PER_REQUEST) {
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
                
                log.debug("Read page with %d rows, %d bytes for table: %s, bucket: %s",
                        page.getPositionCount(), page.getSizeInBytes(),
                        split.getTablePath(), split.getTableBucket());
                
                return page;
            }
            
            return null;
        } catch (Exception e) {
            log.error(e, "Error reading from Fluss for table: %s, bucket: %s",
                    split.getTablePath(), split.getTableBucket());
            throw new RuntimeException("Error reading from Fluss", e);
        }
    }
    
    /**
     * Calculate adaptive poll timeout based on current performance.
     */
    private Duration calculatePollTimeout() {
        // Start with default timeout
        long timeoutMs = 100;
        
        // Adjust based on read performance
        if (completedBytes > 0 && readTimeNanos > 0) {
            // Calculate average bytes per millisecond
            double bytesPerMs = (double) completedBytes / (readTimeNanos / 1_000_000.0);
            
            // If we're reading slowly, increase timeout to reduce polling overhead
            if (bytesPerMs < 100) {  // Less than 100 bytes/ms
                timeoutMs = Math.min(1000, timeoutMs * 2); // Up to 1 second
            }
        }
        
        return Duration.ofMillis(timeoutMs);
    }

    private void processRecord(ScanRecord record) {
        if (record == null || record.getRow() == null) {
            log.warn("Received null record or row, skipping");
            return;
        }
        
        pageBuilder.declarePosition();
        
        InternalRow row = record.getRow();
        
        for (int i = 0; i < columns.size(); i++) {
            FlussColumnHandle column = columns.get(i);
            if (column == null) {
                log.warn("Null column handle at index %d, appending null", i);
                pageBuilder.getBlockBuilder(i).appendNull();
                continue;
            }
            
            int fieldIndex = column.getOrdinalPosition();
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
            Type type = column.getType();
            
            // Validate field index
            if (fieldIndex < 0 || fieldIndex >= row.getArity()) {
                log.warn("Invalid field index %d for row with arity %d, appending null", 
                        fieldIndex, row.getArity());
                blockBuilder.appendNull();
                continue;
            }
            
            // Check if value is null
            if (row.isNullAt(fieldIndex)) {
                blockBuilder.appendNull();
                continue;
            }
            
            // Write value based on type
            try {
                writeValue(blockBuilder, type, row, fieldIndex);
            } catch (Exception e) {
                log.warn(e, "Error writing value for column '%s' at index %d, type %s, appending null", 
                        column.getName(), fieldIndex, type.getDisplayName());
                blockBuilder.appendNull();
            }
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
            } else if (typeName.startsWith("decimal")) {
                // Decimal types need special handling
                io.trino.spi.type.DecimalType decimalType = (io.trino.spi.type.DecimalType) type;
                org.apache.fluss.row.Decimal decimal = row.getDecimal(fieldIndex, 
                    decimalType.getPrecision(), decimalType.getScale());
                if (decimalType.isShort()) {
                    type.writeLong(blockBuilder, decimal.toUnscaledLong());
                } else {
                    type.writeObject(blockBuilder, decimal.toBigDecimal().unscaledValue());
                }
            } else if (typeName.startsWith("char")) {
                org.apache.fluss.utils.StringUtils flussString = row.getString(fieldIndex);
                if (flussString != null) {
                    String value = flussString.toString();
                    Slice slice = Slices.utf8Slice(value);
                    type.writeSlice(blockBuilder, slice);
                } else {
                    blockBuilder.appendNull();
                }
            } else if (typeName.startsWith("varchar")) {
                org.apache.fluss.utils.StringUtils flussString = row.getString(fieldIndex);
                if (flussString != null) {
                    String value = flussString.toString();
                    Slice slice = Slices.utf8Slice(value);
                    type.writeSlice(blockBuilder, slice);
                } else {
                    blockBuilder.appendNull();
                }
            } else if (typeName.startsWith("varbinary") || typeName.equals("binary")) {
                byte[] bytes = row.getBytes(fieldIndex);
                if (bytes != null) {
                    Slice slice = Slices.wrappedBuffer(bytes);
                    type.writeSlice(blockBuilder, slice);
                } else {
                    blockBuilder.appendNull();
                }
            } else if (typeName.equals("date")) {
                // Date is stored as days since epoch in both Fluss and Trino
                int days = row.getInt(fieldIndex);
                type.writeLong(blockBuilder, days);
            } else if (typeName.startsWith("time")) {
                // Time is stored as microseconds since midnight
                long timeValue = row.getLong(fieldIndex);
                type.writeLong(blockBuilder, timeValue);
            } else if (typeName.startsWith("timestamp")) {
                // Timestamp handling
                io.trino.spi.type.TimestampType timestampType = (io.trino.spi.type.TimestampType) type;
                org.apache.fluss.row.TimestampNtz timestamp = row.getTimestampNtz(fieldIndex, 
                    timestampType.getPrecision());
                long epochMicros = timestamp.getMillisecond() * 1000 + timestamp.getNanoOfMillisecond() / 1000;
                type.writeLong(blockBuilder, epochMicros);
            } else if (typeName.startsWith("array")) {
                // Array type handling
                io.trino.spi.type.ArrayType arrayType = (io.trino.spi.type.ArrayType) type;
                org.apache.fluss.row.InternalArray array = row.getArray(fieldIndex);
                if (array != null) {
                    BlockBuilder arrayBlockBuilder = blockBuilder.beginBlockEntry();
                    for (int j = 0; j < array.size(); j++) {
                        if (array.isNullAt(j)) {
                            arrayBlockBuilder.appendNull();
                        } else {
                            writeArrayElement(arrayBlockBuilder, arrayType.getElementType(), array, j);
                        }
                    }
                    blockBuilder.closeEntry();
                } else {
                    blockBuilder.appendNull();
                }
            } else if (typeName.startsWith("map")) {
                // Map type handling
                io.trino.spi.type.MapType mapType = (io.trino.spi.type.MapType) type;
                org.apache.fluss.row.InternalMap map = row.getMap(fieldIndex);
                if (map != null) {
                    BlockBuilder mapBlockBuilder = blockBuilder.beginBlockEntry();
                    org.apache.fluss.row.InternalArray keyArray = map.keyArray();
                    org.apache.fluss.row.InternalArray valueArray = map.valueArray();
                    for (int j = 0; j < map.size(); j++) {
                        writeArrayElement(mapBlockBuilder, mapType.getKeyType(), keyArray, j);
                        if (valueArray.isNullAt(j)) {
                            mapBlockBuilder.appendNull();
                        } else {
                            writeArrayElement(mapBlockBuilder, mapType.getValueType(), valueArray, j);
                        }
                    }
                    blockBuilder.closeEntry();
                } else {
                    blockBuilder.appendNull();
                }
            } else if (typeName.startsWith("row")) {
                // Row type handling
                io.trino.spi.type.RowType rowType = (io.trino.spi.type.RowType) type;
                org.apache.fluss.row.InternalRow nestedRow = row.getRow(fieldIndex, rowType.getFields().size());
                if (nestedRow != null) {
                    BlockBuilder rowBlockBuilder = blockBuilder.beginBlockEntry();
                    for (int j = 0; j < rowType.getFields().size(); j++) {
                        if (nestedRow.isNullAt(j)) {
                            rowBlockBuilder.appendNull();
                        } else {
                            writeValue(rowBlockBuilder, rowType.getFields().get(j).getType(), nestedRow, j);
                        }
                    }
                    blockBuilder.closeEntry();
                } else {
                    blockBuilder.appendNull();
                }
            } else {
                // Fallback to string representation
                log.warn("Unsupported type: %s, using string fallback", typeName);
                org.apache.fluss.utils.StringUtils flussString = row.getString(fieldIndex);
                if (flussString != null) {
                    String value = flussString.toString();
                    Slice slice = Slices.utf8Slice(value);
                    type.writeSlice(blockBuilder, slice);
                } else {
                    blockBuilder.appendNull();
                }
            }
        } catch (Exception e) {
            log.warn(e, "Error writing value for column at index %d, type %s", fieldIndex, typeName);
            blockBuilder.appendNull();
        }
    }

    private void writeArrayElement(BlockBuilder blockBuilder, Type elementType, 
                                   org.apache.fluss.row.InternalArray array, int index) {
        if (array == null || index < 0 || index >= array.size()) {
            blockBuilder.appendNull();
            return;
        }
        
        String typeName = elementType.getDisplayName().toLowerCase();
        
        try {
            if (typeName.equals("boolean")) {
                if (!array.isNullAt(index)) {
                    elementType.writeBoolean(blockBuilder, array.getBoolean(index));
                } else {
                    blockBuilder.appendNull();
                }
            } else if (typeName.equals("tinyint")) {
                if (!array.isNullAt(index)) {
                    elementType.writeLong(blockBuilder, array.getByte(index));
                } else {
                    blockBuilder.appendNull();
                }
            } else if (typeName.equals("smallint")) {
                if (!array.isNullAt(index)) {
                    elementType.writeLong(blockBuilder, array.getShort(index));
                } else {
                    blockBuilder.appendNull();
                }
            } else if (typeName.equals("integer")) {
                if (!array.isNullAt(index)) {
                    elementType.writeLong(blockBuilder, array.getInt(index));
                } else {
                    blockBuilder.appendNull();
                }
            } else if (typeName.equals("bigint")) {
                if (!array.isNullAt(index)) {
                    elementType.writeLong(blockBuilder, array.getLong(index));
                } else {
                    blockBuilder.appendNull();
                }
            } else if (typeName.equals("real")) {
                if (!array.isNullAt(index)) {
                    elementType.writeLong(blockBuilder, Float.floatToIntBits(array.getFloat(index)));
                } else {
                    blockBuilder.appendNull();
                }
            } else if (typeName.equals("double")) {
                if (!array.isNullAt(index)) {
                    elementType.writeDouble(blockBuilder, array.getDouble(index));
                } else {
                    blockBuilder.appendNull();
                }
            } else if (typeName.startsWith("varchar")) {
                if (!array.isNullAt(index)) {
                    org.apache.fluss.utils.StringUtils flussString = array.getString(index);
                    if (flussString != null) {
                        String value = flussString.toString();
                        Slice slice = Slices.utf8Slice(value);
                        elementType.writeSlice(blockBuilder, slice);
                    } else {
                        blockBuilder.appendNull();
                    }
                } else {
                    blockBuilder.appendNull();
                }
            } else if (typeName.startsWith("varbinary")) {
                if (!array.isNullAt(index)) {
                    byte[] bytes = array.getBinary(index);
                    if (bytes != null) {
                        Slice slice = Slices.wrappedBuffer(bytes);
                        elementType.writeSlice(blockBuilder, slice);
                    } else {
                        blockBuilder.appendNull();
                    }
                } else {
                    blockBuilder.appendNull();
                }
            } else {
                // For complex nested types, recursively handle
                log.warn("Complex array element type: %s", typeName);
                if (!array.isNullAt(index)) {
                    org.apache.fluss.utils.StringUtils flussString = array.getString(index);
                    if (flussString != null) {
                        String value = flussString.toString();
                        Slice slice = Slices.utf8Slice(value);
                        elementType.writeSlice(blockBuilder, slice);
                    } else {
                        blockBuilder.appendNull();
                    }
                } else {
                    blockBuilder.appendNull();
                }
            }
        } catch (Exception e) {
            log.warn(e, "Error writing array element at index %d, type %s", index, typeName);
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
            log.debug("Closing Fluss page source for table: %s, bucket: %s",
                    split.getTablePath(), split.getTableBucket());
            
            // Close scanner
            if (scanner != null) {
                try {
                    scanner.close();
                    log.debug("Scanner closed successfully");
                } catch (Exception e) {
                    log.warn(e, "Error closing scanner");
                } finally {
                    scanner = null;
                }
            }
            
            // Close table client
            if (table != null) {
                try {
                    table.close();
                    log.debug("Table client closed successfully");
                } catch (Exception e) {
                    log.warn(e, "Error closing table client");
                } finally {
                    table = null;
                }
            }
            
            // Reset page builder
            if (pageBuilder != null) {
                pageBuilder.reset();
            }
            
            log.debug("Fluss page source closed successfully");
        } catch (Exception e) {
            log.error(e, "Error closing Fluss page source");
            // Don't throw exception as this is a cleanup method
        }
    }
}
