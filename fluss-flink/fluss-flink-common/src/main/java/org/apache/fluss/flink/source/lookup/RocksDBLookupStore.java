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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.rocksdb.RocksDBOperationUtils;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;

import io.github.fluss_contrib.rocksdb.ColumnFamilyDescriptor;
import io.github.fluss_contrib.rocksdb.ColumnFamilyHandle;
import io.github.fluss_contrib.rocksdb.ColumnFamilyOptions;
import io.github.fluss_contrib.rocksdb.DBOptions;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.RocksIterator;
import io.github.fluss_contrib.rocksdb.Slice;
import io.github.fluss_contrib.rocksdb.WriteOptions;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Local RocksDB-backed key-value store for the full lookup cache.
 *
 * <p>Uses a single column family. Snapshot rows and CDC changes are written sequentially (not
 * concurrently), so no overlay merge is needed.
 *
 * <p>This class is <b>not thread-safe</b>. Callers must synchronize externally (e.g. via {@code
 * ReadWriteLock}).
 */
@NotThreadSafe
final class RocksDBLookupStore implements AutoCloseable {

    private final File dbDirectory;
    private final DBOptions dbOptions;
    private final ColumnFamilyOptions columnFamilyOptions;
    private final WriteOptions writeOptions;
    private final RocksDB rocksDB;
    private final ColumnFamilyHandle defaultColumnFamilyHandle;

    RocksDBLookupStore(File dbDirectory) throws IOException {
        this.dbDirectory = dbDirectory;
        if (!dbDirectory.mkdirs() && !dbDirectory.isDirectory()) {
            throw new IOException("Unable to create full cache directory " + dbDirectory);
        }

        DBOptions openedDbOptions = null;
        ColumnFamilyOptions openedColumnFamilyOptions = null;
        WriteOptions openedWriteOptions = null;
        RocksDB openedDb = null;
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
        try {
            RocksDB.loadLibrary();
            openedDbOptions = new DBOptions().setCreateIfMissing(true);
            openedColumnFamilyOptions = new ColumnFamilyOptions();
            // The cache is rebuilt on startup, so writes do not need WAL recovery.
            openedWriteOptions = new WriteOptions().setDisableWAL(true);
            List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                    Collections.singletonList(
                            new ColumnFamilyDescriptor(
                                    RocksDB.DEFAULT_COLUMN_FAMILY, openedColumnFamilyOptions));
            openedDb =
                    RocksDBOperationUtils.openDB(
                            dbDirectory.getAbsolutePath(),
                            columnFamilyDescriptors,
                            columnFamilyHandles,
                            openedDbOptions,
                            false);
            defaultColumnFamilyHandle = columnFamilyHandles.get(0);
            rocksDB = openedDb;
            dbOptions = openedDbOptions;
            columnFamilyOptions = openedColumnFamilyOptions;
            writeOptions = openedWriteOptions;
        } catch (Throwable t) {
            columnFamilyHandles.forEach(IOUtils::closeQuietly);
            IOUtils.closeQuietly(openedDb);
            IOUtils.closeQuietly(openedWriteOptions);
            IOUtils.closeQuietly(openedColumnFamilyOptions);
            IOUtils.closeQuietly(openedDbOptions);
            FileUtils.deleteDirectoryQuietly(dbDirectory);
            throw t;
        }
    }

    /** Writes or overwrites a key-value pair. */
    void put(byte[] key, byte[] value) throws RocksDBException {
        rocksDB.put(defaultColumnFamilyHandle, writeOptions, key, value);
    }

    /** Deletes a key. */
    void delete(byte[] key) throws RocksDBException {
        rocksDB.delete(defaultColumnFamilyHandle, writeOptions, key);
    }

    /** Point lookup. Returns null if the key does not exist. */
    @Nullable
    byte[] get(byte[] key) throws RocksDBException {
        return rocksDB.get(defaultColumnFamilyHandle, key);
    }

    /**
     * Returns all values whose keys start with the given prefix.
     *
     * <p>Uses a bounded iterator, aligned with the server-side {@code RocksDBKv.prefixLookup}.
     */
    List<byte[]> prefixLookup(byte[] prefixKey) throws RocksDBException {
        byte[] upperBound = prefixUpperBound(prefixKey);
        // The bound must outlive both the read options and the iterator that reference it.
        try (Slice upperBoundSlice = upperBound == null ? null : new Slice(upperBound);
                ReadOptions readOptions = new ReadOptions().setTotalOrderSeek(true)) {
            if (upperBoundSlice != null) {
                readOptions.setIterateUpperBound(upperBoundSlice);
            }
            try (RocksIterator iterator =
                    rocksDB.newIterator(defaultColumnFamilyHandle, readOptions)) {
                return readPrefix(iterator, prefixKey);
            }
        }
    }

    /** Reads a bounded prefix scan and propagates iterator errors instead of partial results. */
    @VisibleForTesting
    static List<byte[]> readPrefix(RocksIterator iterator, byte[] prefixKey)
            throws RocksDBException {
        List<byte[]> values = new ArrayList<>();
        iterator.seek(prefixKey);
        while (iterator.isValid()) {
            values.add(iterator.value());
            iterator.next();
        }
        iterator.status();
        return values;
    }

    /** Returns the exclusive bytewise bound, or null if the prefix has no finite upper bound. */
    @Nullable
    private static byte[] prefixUpperBound(byte[] prefix) {
        for (int i = prefix.length - 1; i >= 0; i--) {
            if ((prefix[i] & 0xff) != 0xff) {
                byte[] upperBound = Arrays.copyOf(prefix, i + 1);
                upperBound[i]++;
                return upperBound;
            }
        }
        // Empty and all-0xff prefixes match every key at or after the seek position.
        return null;
    }

    /** Returns the total SST file size from RocksDB metadata, excluding WAL and metadata files. */
    long diskSizeBytes() throws RocksDBException {
        return rocksDB.getLongProperty("rocksdb.total-sst-files-size");
    }

    /** Closes RocksDB and deletes the directory. */
    @Override
    public void close() {
        IOUtils.closeQuietly(defaultColumnFamilyHandle);
        IOUtils.closeQuietly(rocksDB);
        IOUtils.closeQuietly(writeOptions);
        IOUtils.closeQuietly(columnFamilyOptions);
        IOUtils.closeQuietly(dbOptions);
        FileUtils.deleteDirectoryQuietly(dbDirectory);
    }
}
