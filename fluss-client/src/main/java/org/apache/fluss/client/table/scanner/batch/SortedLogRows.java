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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.rocksdb.RocksDBHandle;
import org.apache.fluss.rocksdb.RocksIteratorWrapper;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.KeyValueRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.serializer.RowSerializer;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.INDEXED;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Sorted, deduplicated log rows for KV snapshot-and-log batch merge.
 *
 * <p>Primary keys are ordered by unsigned lexicographical comparison of their encoded bytes. This
 * matches both the KV snapshot scan order and RocksDB's default bytewise comparator.
 */
@NotThreadSafe
final class SortedLogRows implements Closeable {
    static final int DEFAULT_SPILL_THRESHOLD = 8192;

    private static final byte NORMAL_ROW = 0;
    private static final byte TOMBSTONE_ROW = 1;
    private static final int VALUE_FLAG_LENGTH = 1;

    private final int[] keyIndexes;
    private final KeyEncoder primaryKeyEncoder;
    private final Map<byte[], KeyValueRow> memoryRows;
    private final ProjectedRow keyProjectedRow;
    private final RowSerializer valueSerializer;
    private final RowDecoder valueDecoder;
    private final LogScanner logScanner;
    private final TableBucket tableBucket;
    private final Path scannerTmpDirectory;
    private final long stoppingOffset;
    private final int spillThreshold;

    private boolean loaded;
    private boolean closed;

    @Nullable private Path spillDirectory;
    @Nullable private DBOptions dbOptions;
    @Nullable private RocksDBHandle rocksDBHandle;
    @Nullable private WriteOptions writeOptions;

    @VisibleForTesting
    SortedLogRows(
            RowType rowType,
            int[] keyIndexes,
            KeyEncoder primaryKeyEncoder,
            LogScanner logScanner,
            TableBucket tableBucket,
            long stoppingOffset,
            String scannerTmpDir,
            int spillThreshold) {
        checkArgument(spillThreshold > 0, "Spill threshold must be positive.");
        this.keyIndexes = Arrays.copyOf(keyIndexes, keyIndexes.length);
        this.primaryKeyEncoder = primaryKeyEncoder;
        this.memoryRows = new TreeMap<>(SortedLogRows::compareKeys);
        this.keyProjectedRow = ProjectedRow.from(this.keyIndexes);

        this.valueSerializer = new RowSerializer(toDataTypes(rowType), INDEXED);
        this.valueDecoder = RowDecoder.create(KvFormat.INDEXED, toDataTypes(rowType));
        this.logScanner = logScanner;
        this.tableBucket = tableBucket;
        this.scannerTmpDirectory = Paths.get(scannerTmpDir);
        this.stoppingOffset = stoppingOffset;
        this.spillThreshold = spillThreshold;
        this.loaded = stoppingOffset <= 0;
    }

    boolean load(Duration timeout) throws IOException {
        checkNotClosed();
        if (loaded) {
            return true;
        }

        ScanRecords scanRecords = logScanner.poll(timeout);
        for (ScanRecord scanRecord : scanRecords.records(tableBucket)) {
            long logOffset = scanRecord.logOffset();
            if (logOffset >= stoppingOffset) {
                loaded = true;
                break;
            }

            put(scanRecord);
            if (logOffset >= stoppingOffset - 1) {
                loaded = true;
                break;
            }
        }

        Long consumedUpToOffset = scanRecords.consumedUpToOffset(tableBucket);
        if (consumedUpToOffset != null && consumedUpToOffset >= stoppingOffset) {
            loaded = true;
        }
        if (loaded) {
            IOUtils.closeQuietly(logScanner);
        }
        return loaded;
    }

    private void put(ScanRecord scanRecord) throws IOException {
        ChangeType changeType = scanRecord.getChangeType();
        boolean isDelete =
                changeType == ChangeType.DELETE || changeType == ChangeType.UPDATE_BEFORE;
        put(scanRecord.getRow(), isDelete);
    }

    private void put(InternalRow row, boolean isDelete) throws IOException {
        checkNotClosed();

        if (rocksDBHandle == null) {
            putToMemory(row, isDelete);
            if (memoryRows.size() > spillThreshold) {
                spillToRocksDB();
            }
        } else {
            putToRocksDB(row, isDelete);
        }
    }

    CloseableIterator<KeyValueRow> newIterator() throws IOException {
        checkNotClosed();
        checkState(loaded, "Log rows are not ready for iteration.");
        if (rocksDBHandle == null) {
            return CloseableIterator.wrap(memoryRows.values().iterator());
        }

        RocksIterator rocksIterator =
                rocksDBHandle.getDb().newIterator(rocksDBHandle.getDefaultColumnFamilyHandle());
        RocksIteratorWrapper rocksIteratorWrapper = new RocksIteratorWrapper(rocksIterator);
        rocksIteratorWrapper.seekToFirst();
        return new RocksDBLogRowsIterator(rocksIteratorWrapper);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        closed = true;
        IOUtils.closeQuietly(logScanner);
        memoryRows.clear();
        IOUtils.closeQuietly(writeOptions);
        IOUtils.closeQuietly(rocksDBHandle);
        IOUtils.closeQuietly(dbOptions);
        if (spillDirectory != null) {
            FileUtils.deleteDirectoryQuietly(spillDirectory.toFile());
        }
    }

    @VisibleForTesting
    boolean isSpilled() {
        return rocksDBHandle != null;
    }

    @VisibleForTesting
    @Nullable
    Path spillDirectory() {
        return spillDirectory;
    }

    private void putToMemory(InternalRow row, boolean isDelete) {
        BinaryRow copiedRow = copyRow(row);
        KeyValueRow keyValueRow = new KeyValueRow(keyIndexes, copiedRow, isDelete);
        memoryRows.put(encodeKey(keyValueRow.keyRow()), keyValueRow);
    }

    private BinaryRow copyRow(InternalRow row) {
        if (row instanceof BinaryRow) {
            return ((BinaryRow) row).copy();
        }
        return valueSerializer.toBinaryRow(row).copy();
    }

    private void spillToRocksDB() throws IOException {
        checkState(rocksDBHandle == null, "Log rows have already been spilled.");

        try {
            Files.createDirectories(scannerTmpDirectory);
            spillDirectory = Files.createTempDirectory(scannerTmpDirectory, "sorted-log-rows-");
            dbOptions = new DBOptions().setCreateIfMissing(true);
            ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
            rocksDBHandle =
                    new RocksDBHandle(spillDirectory.toFile(), dbOptions, columnFamilyOptions);
            rocksDBHandle.openDB();
            writeOptions = new WriteOptions().setDisableWAL(true);

            for (Map.Entry<byte[], KeyValueRow> entry : memoryRows.entrySet()) {
                KeyValueRow keyValueRow = entry.getValue();
                putToRocksDB(entry.getKey(), keyValueRow.valueRow(), keyValueRow.isDelete());
            }
            memoryRows.clear();
        } catch (Exception e) {
            close();
            throw new IOException("Failed to spill log rows to RocksDB.", e);
        }
    }

    private void putToRocksDB(InternalRow row, boolean isDelete) throws IOException {
        putToRocksDB(encodeKey(keyProjectedRow.replaceRow(row)), row, isDelete);
    }

    private void putToRocksDB(byte[] key, InternalRow row, boolean isDelete) throws IOException {
        try {
            rocksDBHandle
                    .getDb()
                    .put(
                            rocksDBHandle.getDefaultColumnFamilyHandle(),
                            writeOptions,
                            key,
                            serializeValue(row, isDelete));
        } catch (RocksDBException e) {
            throw new IOException("Failed to write log row to RocksDB.", e);
        }
    }

    private byte[] encodeKey(InternalRow keyRow) {
        return primaryKeyEncoder.encodeKey(keyRow);
    }

    private byte[] serializeValue(InternalRow row, boolean isDelete) {
        BinaryRow binaryRow = valueSerializer.toBinaryRow(row);
        byte[] valueBytes = new byte[VALUE_FLAG_LENGTH + binaryRow.getSizeInBytes()];
        valueBytes[0] = isDelete ? TOMBSTONE_ROW : NORMAL_ROW;
        binaryRow.copyTo(valueBytes, VALUE_FLAG_LENGTH);
        return valueBytes;
    }

    private KeyValueRow deserializeValue(byte[] valueBytes) {
        boolean isDelete = valueBytes[0] == TOMBSTONE_ROW;
        InternalRow valueRow =
                valueDecoder.decode(
                        MemorySegment.wrap(valueBytes),
                        VALUE_FLAG_LENGTH,
                        valueBytes.length - VALUE_FLAG_LENGTH);
        return new KeyValueRow(keyIndexes, valueRow, isDelete);
    }

    private void checkNotClosed() {
        checkState(!closed, "Sorted log rows has already been closed.");
    }

    private static DataType[] toDataTypes(RowType rowType) {
        return rowType.getChildren().toArray(new DataType[0]);
    }

    private static int compareKeys(byte[] left, byte[] right) {
        return MemorySegment.wrap(left)
                .compare(MemorySegment.wrap(right), 0, 0, left.length, right.length);
    }

    private class RocksDBLogRowsIterator implements CloseableIterator<KeyValueRow> {

        private final RocksIteratorWrapper rocksIteratorWrapper;
        private boolean closed;

        private RocksDBLogRowsIterator(RocksIteratorWrapper rocksIteratorWrapper) {
            this.rocksIteratorWrapper = rocksIteratorWrapper;
        }

        @Override
        public boolean hasNext() {
            return !closed && rocksIteratorWrapper.isValid();
        }

        @Override
        public KeyValueRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            KeyValueRow keyValueRow = deserializeValue(rocksIteratorWrapper.value());
            rocksIteratorWrapper.next();
            return keyValueRow;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            rocksIteratorWrapper.close();
        }
    }
}
