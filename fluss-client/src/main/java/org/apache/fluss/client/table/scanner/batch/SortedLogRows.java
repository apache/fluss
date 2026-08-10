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
import org.apache.fluss.row.serializer.RowSerializer;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;

import org.rocksdb.AbstractComparator;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.INDEXED;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Sorted, deduplicated log rows for snapshot-and-log batch merge. */
@NotThreadSafe
final class SortedLogRows implements Closeable {
    static final int DEFAULT_SPILL_THRESHOLD = 8192;

    private static final byte NORMAL_ROW = 0;
    private static final byte TOMBSTONE_ROW = 1;
    private static final int VALUE_FLAG_LENGTH = 1;

    private final int[] keyIndexes;
    private final Comparator<InternalRow> rowComparator;
    private final Map<InternalRow, KeyValueRow> memoryRows;
    private final ProjectedRow keyProjectedRow;
    private final RowSerializer keySerializer;
    private final RowSerializer valueSerializer;
    private final RowDecoder keyDecoder;
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
    @Nullable private ComparatorOptions comparatorOptions;
    @Nullable private InternalRowComparator rocksComparator;
    @Nullable private RocksDBHandle rocksDBHandle;
    @Nullable private WriteOptions writeOptions;

    SortedLogRows(
            RowType rowType,
            int[] keyIndexes,
            Comparator<InternalRow> rowComparator,
            LogScanner logScanner,
            TableBucket tableBucket,
            long stoppingOffset,
            String scannerTmpDir) {
        this(
                rowType,
                keyIndexes,
                rowComparator,
                logScanner,
                tableBucket,
                stoppingOffset,
                scannerTmpDir,
                DEFAULT_SPILL_THRESHOLD);
    }

    @VisibleForTesting
    SortedLogRows(
            RowType rowType,
            int[] keyIndexes,
            Comparator<InternalRow> rowComparator,
            LogScanner logScanner,
            TableBucket tableBucket,
            long stoppingOffset,
            String scannerTmpDir,
            int spillThreshold) {
        checkArgument(spillThreshold > 0, "Spill threshold must be positive.");
        this.keyIndexes = Arrays.copyOf(keyIndexes, keyIndexes.length);
        this.rowComparator = rowComparator;
        this.memoryRows = new TreeMap<>(rowComparator);
        this.keyProjectedRow = ProjectedRow.from(this.keyIndexes);

        RowType keyRowType = rowType.project(this.keyIndexes);
        this.keySerializer = new RowSerializer(toDataTypes(keyRowType), INDEXED);
        this.valueSerializer = new RowSerializer(toDataTypes(rowType), INDEXED);
        this.keyDecoder = RowDecoder.create(KvFormat.INDEXED, toDataTypes(keyRowType));
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
        IOUtils.closeQuietly(rocksComparator);
        IOUtils.closeQuietly(comparatorOptions);
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
        BinaryRow copiedRow = valueSerializer.toBinaryRow(row).copy();
        KeyValueRow keyValueRow = new KeyValueRow(keyIndexes, copiedRow, isDelete);
        memoryRows.put(keyValueRow.keyRow(), keyValueRow);
    }

    private void spillToRocksDB() throws IOException {
        checkState(rocksDBHandle == null, "Log rows have already been spilled.");

        try {
            Files.createDirectories(scannerTmpDirectory);
            spillDirectory = Files.createTempDirectory(scannerTmpDirectory, "sorted-log-rows-");
            dbOptions = new DBOptions().setCreateIfMissing(true);
            comparatorOptions = new ComparatorOptions().setUseDirectBuffer(false);
            rocksComparator =
                    new InternalRowComparator(comparatorOptions, keyDecoder, rowComparator);
            ColumnFamilyOptions columnFamilyOptions =
                    new ColumnFamilyOptions().setComparator(rocksComparator);
            rocksDBHandle =
                    new RocksDBHandle(spillDirectory.toFile(), dbOptions, columnFamilyOptions);
            rocksDBHandle.openDB();
            writeOptions = new WriteOptions().setDisableWAL(true);

            for (KeyValueRow keyValueRow : memoryRows.values()) {
                putToRocksDB(keyValueRow.valueRow(), keyValueRow.isDelete());
            }
            memoryRows.clear();
        } catch (Exception e) {
            close();
            throw new IOException("Failed to spill log rows to RocksDB.", e);
        }
    }

    private void putToRocksDB(InternalRow row, boolean isDelete) throws IOException {
        try {
            rocksDBHandle
                    .getDb()
                    .put(
                            rocksDBHandle.getDefaultColumnFamilyHandle(),
                            writeOptions,
                            serializeKey(row),
                            serializeValue(row, isDelete));
        } catch (RocksDBException e) {
            throw new IOException("Failed to write log row to RocksDB.", e);
        }
    }

    private byte[] serializeKey(InternalRow row) {
        return toBytes(keySerializer.toBinaryRow(keyProjectedRow.replaceRow(row)));
    }

    private byte[] serializeValue(InternalRow row, boolean isDelete) {
        byte[] rowBytes = toBytes(valueSerializer.toBinaryRow(row));
        byte[] valueBytes = new byte[VALUE_FLAG_LENGTH + rowBytes.length];
        valueBytes[0] = isDelete ? TOMBSTONE_ROW : NORMAL_ROW;
        System.arraycopy(rowBytes, 0, valueBytes, VALUE_FLAG_LENGTH, rowBytes.length);
        return valueBytes;
    }

    private KeyValueRow deserializeValue(byte[] valueBytes) {
        boolean isDelete = valueBytes[0] == TOMBSTONE_ROW;
        byte[] rowBytes = Arrays.copyOfRange(valueBytes, VALUE_FLAG_LENGTH, valueBytes.length);
        InternalRow valueRow = valueDecoder.decode(rowBytes);
        return new KeyValueRow(keyIndexes, valueRow, isDelete);
    }

    private void checkNotClosed() {
        checkState(!closed, "Sorted log rows has already been closed.");
    }

    private static DataType[] toDataTypes(RowType rowType) {
        return rowType.getChildren().toArray(new DataType[0]);
    }

    private static byte[] toBytes(BinaryRow row) {
        byte[] bytes = new byte[row.getSizeInBytes()];
        row.copyTo(bytes, 0);
        return bytes;
    }

    private static byte[] toBytes(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    private static class InternalRowComparator extends AbstractComparator {

        private final RowDecoder keyDecoder;
        private final Comparator<InternalRow> rowComparator;

        InternalRowComparator(
                ComparatorOptions comparatorOptions,
                RowDecoder keyDecoder,
                Comparator<InternalRow> rowComparator) {
            super(comparatorOptions);
            this.keyDecoder = keyDecoder;
            this.rowComparator = rowComparator;
        }

        @Override
        public String name() {
            return "fluss-sorted-log-rows-comparator";
        }

        @Override
        public int compare(ByteBuffer left, ByteBuffer right) {
            return rowComparator.compare(
                    keyDecoder.decode(toBytes(left)), keyDecoder.decode(toBytes(right)));
        }
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
