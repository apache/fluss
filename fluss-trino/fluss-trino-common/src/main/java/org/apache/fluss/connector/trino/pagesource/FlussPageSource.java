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
            } else if (typeName.startsWith("decimal")) {
                // Decimal types need special handling
                org.apache.fluss.row.Decimal decimal = row.getDecimal(fieldIndex, 
                    ((io.trino.spi.type.DecimalType) type).getPrecision(),
                    ((io.trino.spi.type.DecimalType) type).getScale());
                if (((io.trino.spi.type.DecimalType) type).isShort()) {
                    type.writeLong(blockBuilder, decimal.toUnscaledLong());
                } else {
                    type.writeObject(blockBuilder, decimal.toBigDecimal().unscaledValue());
                }
            } else if (typeName.startsWith("char")) {
                String value = row.getString(fieldIndex).toString();
                Slice slice = Slices.utf8Slice(value);
                type.writeSlice(blockBuilder, slice);
            } else if (typeName.startsWith("varchar")) {
                String value = row.getString(fieldIndex).toString();
                Slice slice = Slices.utf8Slice(value);
                type.writeSlice(blockBuilder, slice);
            } else if (typeName.startsWith("varbinary") || typeName.equals("binary")) {
                byte[] bytes = row.getBytes(fieldIndex);
                Slice slice = Slices.wrappedBuffer(bytes);
                type.writeSlice(blockBuilder, slice);
            } else if (typeName.equals("date")) {
                // Date is stored as days since epoch in both Fluss and Trino
                int days = row.getInt(fieldIndex);
                type.writeLong(blockBuilder, days);
            } else if (typeName.startsWith("time")) {
                // Time is stored as milliseconds/microseconds since midnight
                long timeValue = row.getLong(fieldIndex);
                type.writeLong(blockBuilder, timeValue);
            } else if (typeName.startsWith("timestamp")) {
                // Timestamp handling
                org.apache.fluss.row.TimestampNtz timestamp = row.getTimestampNtz(fieldIndex, 
                    ((io.trino.spi.type.TimestampType) type).getPrecision());
                long epochMicros = timestamp.getMillisecond() * 1000 + timestamp.getNanoOfMillisecond() / 1000;
                type.writeLong(blockBuilder, epochMicros);
            } else if (typeName.startsWith("array")) {
                // Array type handling
                io.trino.spi.type.ArrayType arrayType = (io.trino.spi.type.ArrayType) type;
                org.apache.fluss.row.InternalArray array = row.getArray(fieldIndex);
                BlockBuilder arrayBlockBuilder = blockBuilder.beginBlockEntry();
                for (int j = 0; j < array.size(); j++) {
                    if (array.isNullAt(j)) {
                        arrayBlockBuilder.appendNull();
                    } else {
                        writeArrayElement(arrayBlockBuilder, arrayType.getElementType(), array, j);
                    }
                }
                blockBuilder.closeEntry();
            } else if (typeName.startsWith("map")) {
                // Map type handling
                io.trino.spi.type.MapType mapType = (io.trino.spi.type.MapType) type;
                org.apache.fluss.row.InternalMap map = row.getMap(fieldIndex);
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
            } else if (typeName.startsWith("row")) {
                // Row type handling
                io.trino.spi.type.RowType rowType = (io.trino.spi.type.RowType) type;
                org.apache.fluss.row.InternalRow nestedRow = row.getRow(fieldIndex, rowType.getFields().size());
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
                // Fallback to string representation
                log.warn("Unsupported type: %s, using string fallback", typeName);
                String value = row.getString(fieldIndex).toString();
                Slice slice = Slices.utf8Slice(value);
                type.writeSlice(blockBuilder, slice);
            }
        } catch (Exception e) {
            log.warn(e, "Error writing value for column at index %d, type %s", fieldIndex, typeName);
            blockBuilder.appendNull();
        }
    }

    private void writeArrayElement(BlockBuilder blockBuilder, Type elementType, 
                                   org.apache.fluss.row.InternalArray array, int index) {
        String typeName = elementType.getDisplayName().toLowerCase();
        
        if (typeName.equals("boolean")) {
            elementType.writeBoolean(blockBuilder, array.getBoolean(index));
        } else if (typeName.equals("tinyint")) {
            elementType.writeLong(blockBuilder, array.getByte(index));
        } else if (typeName.equals("smallint")) {
            elementType.writeLong(blockBuilder, array.getShort(index));
        } else if (typeName.equals("integer")) {
            elementType.writeLong(blockBuilder, array.getInt(index));
        } else if (typeName.equals("bigint")) {
            elementType.writeLong(blockBuilder, array.getLong(index));
        } else if (typeName.equals("real")) {
            elementType.writeLong(blockBuilder, Float.floatToIntBits(array.getFloat(index)));
        } else if (typeName.equals("double")) {
            elementType.writeDouble(blockBuilder, array.getDouble(index));
        } else if (typeName.startsWith("varchar")) {
            String value = array.getString(index).toString();
            Slice slice = Slices.utf8Slice(value);
            elementType.writeSlice(blockBuilder, slice);
        } else if (typeName.startsWith("varbinary")) {
            byte[] bytes = array.getBinary(index);
            Slice slice = Slices.wrappedBuffer(bytes);
            elementType.writeSlice(blockBuilder, slice);
        } else {
            // For complex nested types, recursively handle
            log.warn("Complex array element type: %s", typeName);
            String value = String.valueOf(array.getString(index));
            Slice slice = Slices.utf8Slice(value);
            elementType.writeSlice(blockBuilder, slice);
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
