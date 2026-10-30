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

import io.github.fluss_contrib.rocksdb.BlockBasedTableConfig;
import io.github.fluss_contrib.rocksdb.BloomFilter;
import io.github.fluss_contrib.rocksdb.ColumnFamilyDescriptor;
import io.github.fluss_contrib.rocksdb.ColumnFamilyHandle;
import io.github.fluss_contrib.rocksdb.ColumnFamilyOptions;
import io.github.fluss_contrib.rocksdb.DBOptions;
import io.github.fluss_contrib.rocksdb.LRUCache;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.RocksIterator;
import io.github.fluss_contrib.rocksdb.Slice;
import io.github.fluss_contrib.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Local RocksDB-backed key-value store shared by all buckets assigned to a lookup subtask.
 *
 * <p>This class is <b>not thread-safe</b>. Callers must synchronize externally (e.g. via {@code
 * ReadWriteLock}).
 */
@NotThreadSafe
final class RocksDBLookupStore implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBLookupStore.class);

    private final File dbDirectory;
    private final RocksDBResources resources;
    private boolean closed;

    RocksDBLookupStore(File dbDirectory, FullCacheLookupOptions options) throws IOException {
        this.dbDirectory = dbDirectory;
        if (!dbDirectory.mkdirs() && !dbDirectory.isDirectory()) {
            throw new IOException("Unable to create full cache directory " + dbDirectory);
        }

        LOG.info("Opening bucket-sharded full lookup cache at {}.", dbDirectory.getAbsolutePath());
        resources = openRocksDB(dbDirectory, options);
    }

    /** Creates the database and releases every allocated native resource if opening fails. */
    private static RocksDBResources openRocksDB(File dbDirectory, FullCacheLookupOptions options)
            throws IOException {
        RocksDBResources resources = new RocksDBResources();
        try {
            RocksDB.loadLibrary();
            resources.dbOptions = new DBOptions();
            resources
                    .dbOptions
                    .setCreateIfMissing(true)
                    .setMaxBackgroundJobs(options.maxBackgroundJobs)
                    .setMaxOpenFiles(options.maxOpenFiles);
            resources.blockCache = new LRUCache(options.blockCacheSize, -1, false, 0.1);
            resources.bloomFilter = new BloomFilter(10, false);
            BlockBasedTableConfig tableConfig =
                    new BlockBasedTableConfig()
                            .setBlockCache(resources.blockCache)
                            .setCacheIndexAndFilterBlocks(true)
                            .setCacheIndexAndFilterBlocksWithHighPriority(true)
                            .setPinL0FilterAndIndexBlocksInCache(true)
                            .setFilterPolicy(resources.bloomFilter);
            resources.columnFamilyOptions = new ColumnFamilyOptions();
            resources
                    .columnFamilyOptions
                    .setWriteBufferSize(options.writeBufferSize)
                    .setMaxWriteBufferNumber(options.writeBufferCount)
                    .setTableFormatConfig(tableConfig);
            // The cache is rebuilt on startup, so writes do not need WAL recovery.
            resources.writeOptions = new WriteOptions().setDisableWAL(true);
            List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                    Collections.singletonList(
                            new ColumnFamilyDescriptor(
                                    RocksDB.DEFAULT_COLUMN_FAMILY, resources.columnFamilyOptions));
            resources.rocksDB =
                    RocksDBOperationUtils.openDB(
                            dbDirectory.getAbsolutePath(),
                            columnFamilyDescriptors,
                            resources.columnFamilyHandles,
                            resources.dbOptions,
                            false);
            resources.defaultColumnFamilyHandle = resources.columnFamilyHandles.get(0);
            return resources;
        } catch (Throwable t) {
            resources.close();
            LOG.warn(
                    "Failed to open full lookup cache at {}; cleaning up.",
                    dbDirectory.getAbsolutePath(),
                    t);
            FileUtils.deleteDirectoryQuietly(dbDirectory);
            throw t;
        }
    }

    /** Writes or overwrites a key-value pair. */
    void put(byte[] key, byte[] value) throws RocksDBException {
        resources.rocksDB.put(
                resources.defaultColumnFamilyHandle, resources.writeOptions, key, value);
    }

    /** Deletes a key. */
    void delete(byte[] key) throws RocksDBException {
        resources.rocksDB.delete(resources.defaultColumnFamilyHandle, resources.writeOptions, key);
    }

    /** Point lookup. Returns null if the key does not exist. */
    @Nullable
    byte[] get(byte[] key) throws RocksDBException {
        return resources.rocksDB.get(resources.defaultColumnFamilyHandle, key);
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
                    resources.rocksDB.newIterator(
                            resources.defaultColumnFamilyHandle, readOptions)) {
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
        return resources.rocksDB.getLongProperty("rocksdb.total-sst-files-size");
    }

    /** Estimates native memtable, table-reader and block-cache memory, excluding other overhead. */
    long estimatedMemoryBytes() throws RocksDBException {
        return resources.rocksDB.getLongProperty("rocksdb.cur-size-all-mem-tables")
                + resources.rocksDB.getLongProperty("rocksdb.estimate-table-readers-mem")
                + resources.blockCache.getUsage();
    }

    /** Closes RocksDB and deletes the directory. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        LOG.info("Closing and removing full lookup cache at {}.", dbDirectory.getAbsolutePath());
        resources.close();
        FileUtils.deleteDirectoryQuietly(dbDirectory);
        if (dbDirectory.exists()) {
            LOG.warn(
                    "Full lookup cache directory could not be removed: {}.",
                    dbDirectory.getAbsolutePath());
        }
    }

    /**
     * Native resources with the same lifetime as one database, also used during partial opening.
     */
    private static final class RocksDBResources implements AutoCloseable {

        private final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
        private LRUCache blockCache;
        private BloomFilter bloomFilter;
        private DBOptions dbOptions;
        private ColumnFamilyOptions columnFamilyOptions;
        private WriteOptions writeOptions;
        private RocksDB rocksDB;
        private ColumnFamilyHandle defaultColumnFamilyHandle;

        @Override
        public void close() {
            columnFamilyHandles.forEach(IOUtils::closeQuietly);
            IOUtils.closeQuietly(rocksDB);
            IOUtils.closeQuietly(writeOptions);
            IOUtils.closeQuietly(columnFamilyOptions);
            IOUtils.closeQuietly(dbOptions);
            IOUtils.closeQuietly(bloomFilter);
            IOUtils.closeQuietly(blockCache);
        }
    }
}
