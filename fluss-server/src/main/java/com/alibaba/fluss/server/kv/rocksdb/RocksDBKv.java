/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.kv.rocksdb;

import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.rocksdb.RocksDBOperationUtils;
import com.alibaba.fluss.server.utils.ResourceGuard;
import com.alibaba.fluss.utils.IOUtils;
import com.alibaba.fluss.utils.crc.Java;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** A wrapper for the operation of {@link org.rocksdb.RocksDB}. */
public class RocksDBKv implements AutoCloseable {

    /** The container of RocksDB option factory and predefined options. */
    private final RocksDBResourceContainer optionsContainer;

    /**
     * Protects access to RocksDB in other threads, like the checkpointing thread from parallel call
     * that disposes the RocksDB object.
     */
    private final ResourceGuard rocksDBResourceGuard;

    /** The write options to use in the states. We disable write ahead logging. */
    private final WriteOptions writeOptions;

    private final PrefixComparer prefixComparer;

    /**
     * We are not using the default column family for KV ops, but we still need to remember this
     * handle so that we can close it properly when the kv is closed. Note that the one returned by
     * {@link RocksDB#open(String)} is different from that by {@link
     * RocksDB#getDefaultColumnFamily()}, probably it's a bug of RocksDB java API.
     */
    private final ColumnFamilyHandle defaultColumnFamilyHandle;

    /** Our RocksDB database. Currently, one kv tablet, one RocksDB instance. */
    protected final RocksDB db;

    // mark whether this kv is already closed and prevent duplicate closing
    private volatile boolean closed = false;

    public RocksDBKv(
            RocksDBResourceContainer optionsContainer,
            RocksDB db,
            ResourceGuard rocksDBResourceGuard,
            ColumnFamilyHandle defaultColumnFamilyHandle) {
        this.optionsContainer = optionsContainer;
        this.db = db;
        this.rocksDBResourceGuard = rocksDBResourceGuard;
        this.writeOptions = optionsContainer.getWriteOptions();
        this.defaultColumnFamilyHandle = defaultColumnFamilyHandle;
        this.prefixComparer = new PrefixComparer();
    }

    public ResourceGuard getResourceGuard() {
        return rocksDBResourceGuard;
    }

    public RocksDBWriteBatchWrapper newWriteBatch(long writeBatchSize) {
        return new RocksDBWriteBatchWrapper(db, writeBatchSize);
    }

    public @Nullable byte[] get(byte[] key) throws IOException {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new IOException("Fail to get key.", e);
        }
    }

    public List<byte[]> multiGet(List<byte[]> keys) throws IOException {
        try {
            return db.multiGetAsList(keys);
        } catch (RocksDBException e) {
            throw new IOException("Fail to get keys.", e);
        }
    }

    public List<byte[]> prefixLookup(byte[] prefixKey) {
        List<byte[]> pkList = new ArrayList<>();
        ReadOptions readOptions = new ReadOptions();
        RocksIterator iterator = db.newIterator(defaultColumnFamilyHandle, readOptions);
        try {
            iterator.seek(prefixKey);
            while (iterator.isValid() && prefixComparer.isPrefixEquals(prefixKey, iterator.key())) {
                pkList.add(iterator.value());
                iterator.next();
            }
        } finally {
            readOptions.close();
            iterator.close();
        }

        return pkList;
    }

    public List<byte[]> limitScan(Integer limit) {
        List<byte[]> pkList = new ArrayList<>();
        ReadOptions readOptions = new ReadOptions();
        RocksIterator iterator = db.newIterator(defaultColumnFamilyHandle, readOptions);

        int count = 0;
        try {
            iterator.seekToFirst();
            while (iterator.isValid() && count < limit) {
                pkList.add(iterator.value());
                iterator.next();
                count++;
            }
        } finally {
            readOptions.close();
            iterator.close();
        }

        return pkList;
    }

    public void put(byte[] key, byte[] value) throws IOException {
        try {
            db.put(writeOptions, key, value);
        } catch (RocksDBException e) {
            throw new IOException("Fail to put key.", e);
        }
    }

    public void delete(byte[] key) throws IOException {
        try {
            db.delete(key);
        } catch (RocksDBException e) {
            throw new IOException("Fail to delete key.", e);
        }
    }

    public void checkIfRocksDBClosed() {
        if (this.closed) {
            throw new FlussRuntimeException(
                    "The RocksDb for kv in "
                            + optionsContainer.getInstanceRocksDBPath()
                            + " is already closed");
        }
    }

    @Override
    public void close() throws Exception {
        if (this.closed) {
            return;
        }

        // This call will block until all clients that still acquire access to the RocksDB instance
        // have released it,
        // so that we cannot release the native resources while clients are still working with it in
        // parallel.
        rocksDBResourceGuard.close();

        // IMPORTANT: null reference to signal potential async checkpoint workers that the db was
        // disposed, as
        // working on the disposed object results in SEGFAULTS.
        if (db != null) {

            // RocksDB's native memory management requires that *all* CFs (including default) are
            // closed before the
            // DB is closed. See:
            // https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
            // Start with default CF ...
            List<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>();
            RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(
                    columnFamilyOptions, defaultColumnFamilyHandle);
            IOUtils.closeQuietly(defaultColumnFamilyHandle);

            // ... and finally close the DB instance ...
            IOUtils.closeQuietly(db);

            columnFamilyOptions.forEach(IOUtils::closeQuietly);

            IOUtils.closeQuietly(optionsContainer);
        }
        this.closed = true;
    }

    public RocksDB getDb() {
        return db;
    }

    private static class PrefixComparer {
        private static final boolean isJava9OrAbove;
        private static Method compareMethod;

        static {
            if (Java.IS_JAVA9_COMPATIBLE) {
                isJava9OrAbove = true;
                try {
                    compareMethod =
                            Arrays.class.getMethod(
                                    "compare",
                                    byte[].class,
                                    int.class,
                                    int.class,
                                    byte[].class,
                                    int.class,
                                    int.class);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException("Failed to get compare method for byte arrays", e);
                }
            } else {
                isJava9OrAbove = false;
            }
        }

        public boolean isPrefixEquals(byte[] prefix, byte[] bytes) {
            if (isJava9OrAbove) {
                return isPrefixEqualsJava9(prefix, bytes);
            } else {
                return isPrefixEqualsBelowJava9(prefix, bytes);
            }
        }

        /**
         * Check if the given first byte array ({@code prefix}) is a prefix of the second byte array
         * ({@code bytes}) upper java9.
         *
         * @param prefix The prefix byte array
         * @param bytes The byte array to check if it has the prefix
         * @return true if the given bytes has the given prefix, false otherwise
         */
        private boolean isPrefixEqualsJava9(byte[] prefix, byte[] bytes) {
            try {
                if (prefix.length > bytes.length) {
                    return false;
                }

                int result =
                        (int)
                                compareMethod.invoke(
                                        null, prefix, 0, prefix.length, bytes, 0, prefix.length);
                return result == 0;
            } catch (Exception e) {
                throw new RuntimeException("Error invoking compare method", e);
            }
        }

        /**
         * Check if the given first byte array ({@code prefix}) is a prefix of the second byte array
         * ({@code bytes}) below java9.
         *
         * @param prefix The prefix byte array
         * @param bytes The byte array to check if it has the prefix
         * @return true if the given bytes has the given prefix, false otherwise
         */
        private static boolean isPrefixEqualsBelowJava9(byte[] prefix, byte[] bytes) {
            if (prefix.length > bytes.length) {
                return false;
            }
            for (int i = 0; i < prefix.length; i++) {
                if (prefix[i] != bytes[i]) {
                    return false;
                }
            }
            return true;
        }
    }
}
