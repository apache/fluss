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
import org.apache.fluss.utils.BytesUtils;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
    private final RocksDB rocksDB;
    private final ColumnFamilyHandle defaultColumnFamilyHandle;

    RocksDBLookupStore(File dbDirectory) throws IOException {
        this.dbDirectory = dbDirectory;
        if (!dbDirectory.mkdirs() && !dbDirectory.isDirectory()) {
            throw new IOException("Unable to create full cache directory " + dbDirectory);
        }

        RocksDB.loadLibrary();
        dbOptions = new DBOptions().setCreateIfMissing(true);
        columnFamilyOptions = new ColumnFamilyOptions();
        List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                Collections.singletonList(
                        new ColumnFamilyDescriptor(
                                RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
        rocksDB =
                RocksDBOperationUtils.openDB(
                        dbDirectory.getAbsolutePath(),
                        columnFamilyDescriptors,
                        columnFamilyHandles,
                        dbOptions,
                        false);
        defaultColumnFamilyHandle = columnFamilyHandles.get(0);
    }

    /** Writes or overwrites a key-value pair. */
    void put(byte[] key, byte[] value) throws RocksDBException {
        rocksDB.put(defaultColumnFamilyHandle, key, value);
    }

    /** Deletes a key. */
    void delete(byte[] key) throws RocksDBException {
        rocksDB.delete(defaultColumnFamilyHandle, key);
    }

    /** Point lookup. Returns null if the key does not exist. */
    @Nullable
    byte[] get(byte[] key) throws RocksDBException {
        return rocksDB.get(defaultColumnFamilyHandle, key);
    }

    /**
     * Returns all values whose keys start with the given prefix.
     *
     * <p>Uses seek + prefix iteration, aligned with the server-side {@code RocksDBKv.prefixLookup}.
     */
    List<byte[]> prefixLookup(byte[] prefixKey) throws RocksDBException {
        try (RocksIterator iterator = rocksDB.newIterator(defaultColumnFamilyHandle)) {
            return readPrefix(iterator, prefixKey);
        }
    }

    /** Reads a prefix and propagates iterator errors instead of returning partial results. */
    @VisibleForTesting
    static List<byte[]> readPrefix(RocksIterator iterator, byte[] prefixKey)
            throws RocksDBException {
        List<byte[]> values = new ArrayList<>();
        iterator.seek(prefixKey);
        while (iterator.isValid() && BytesUtils.prefixEquals(prefixKey, iterator.key())) {
            values.add(iterator.value());
            iterator.next();
        }
        iterator.status();
        return values;
    }

    /** Returns the approximate on-disk size in bytes. */
    long diskSizeBytes() {
        return directorySize(dbDirectory);
    }

    /** Closes RocksDB and deletes the directory. */
    @Override
    public void close() {
        IOUtils.closeQuietly(defaultColumnFamilyHandle);
        IOUtils.closeQuietly(rocksDB);
        IOUtils.closeQuietly(columnFamilyOptions);
        IOUtils.closeQuietly(dbOptions);
        FileUtils.deleteDirectoryQuietly(dbDirectory);
    }

    private static long directorySize(File file) {
        if (!file.exists()) {
            return 0L;
        }
        if (file.isFile()) {
            return file.length();
        }
        File[] children = file.listFiles();
        if (children == null) {
            return 0L;
        }
        long size = 0L;
        for (File child : children) {
            size += directorySize(child);
        }
        return size;
    }
}
