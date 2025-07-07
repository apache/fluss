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

package org.apache.fluss.server.kv.rocksdb;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.rocksdb.RocksDBHandle;
import org.apache.fluss.server.exception.KvBuildingException;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.UUID;
import java.util.function.Supplier;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Builder for {@link RocksDBKv} . */
public class RocksDBKvBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBKvBuilder.class);

    public static final String DB_INSTANCE_DIR_STRING = "db";

    /** column family options for default column family . */
    private final ColumnFamilyOptions columnFamilyOptions;

    /** The container of RocksDB option factory and predefined options. */
    private final RocksDBResourceContainer optionsContainer;

    /** Path where this configured instance stores its data directory. */
    private final File instanceBasePath;

    /** Path where this configured instance stores its RocksDB database. */
    private final File instanceRocksDBPath;

    /** The number of (re)tries for loading the RocksDB JNI library. */
    private static final int ROCKSDB_LIB_LOADING_ATTEMPTS = 3;

    /** Flag whether the native library has been loaded. */
    private static boolean rocksDbInitialized = false;

    public RocksDBKvBuilder(
            File instanceBasePath,
            RocksDBResourceContainer rocksDBResourceContainer,
            ColumnFamilyOptions columnFamilyOptions) {
        this.columnFamilyOptions = columnFamilyOptions;
        this.optionsContainer = rocksDBResourceContainer;
        this.instanceBasePath = instanceBasePath;
        this.instanceRocksDBPath = getInstanceRocksDBPath(instanceBasePath);
    }

    public RocksDBKv build() throws KvBuildingException {
        ColumnFamilyHandle defaultColumnFamilyHandle = null;
        RocksDB db = null;
        ResourceGuard rocksDBResourceGuard = new ResourceGuard();
        RocksDBHandle rocksDBHandle = null;

        try {
            ensureRocksDBIsLoaded(System.getProperty("java.io.tmpdir"));
            prepareDirectories();
            rocksDBHandle =
                    new RocksDBHandle(
                            instanceRocksDBPath,
                            optionsContainer.getDbOptions(),
                            columnFamilyOptions);
            rocksDBHandle.openDB();
            db = rocksDBHandle.getDb();
            defaultColumnFamilyHandle = rocksDBHandle.getDefaultColumnFamilyHandle();
        } catch (Throwable t) {
            IOUtils.closeQuietly(defaultColumnFamilyHandle);
            IOUtils.closeQuietly(db);
            IOUtils.closeQuietly(rocksDBHandle);
            IOUtils.closeQuietly(columnFamilyOptions);
            IOUtils.closeQuietly(optionsContainer);

            // Log and throw
            String errMsg = "Caught unexpected exception. Fail to build RocksDB kv.";
            LOG.error(errMsg, t);

            throw new KvBuildingException(errMsg, t);
        }
        LOG.info("Finished building RocksDB kv at {}.", instanceBasePath);
        return new RocksDBKv(optionsContainer, db, rocksDBResourceGuard, defaultColumnFamilyHandle);
    }

    void prepareDirectories() throws IOException {
        checkAndCreateDirectory(instanceBasePath);
    }

    private static void checkAndCreateDirectory(File directory) throws IOException {
        if (directory.exists()) {
            if (!directory.isDirectory()) {
                throw new IOException("Not a directory: " + directory);
            }
        } else if (!directory.mkdirs()) {
            throw new IOException(
                    String.format("Could not create RocksDB data directory at %s.", directory));
        }
    }

    public static File getInstanceRocksDBPath(File instanceBasePath) {
        return new File(instanceBasePath, DB_INSTANCE_DIR_STRING);
    }

    // ------------------------------------------------------------------------
    //  static library loading utilities
    // ------------------------------------------------------------------------

    @VisibleForTesting
    static void ensureRocksDBIsLoaded(String tempDirectory) throws IOException {
        ensureRocksDBIsLoaded(tempDirectory, NativeLibraryLoader::getInstance);
    }

    @VisibleForTesting
    static void ensureRocksDBIsLoaded(
            String tempDirectory, Supplier<NativeLibraryLoader> nativeLibraryLoaderSupplier)
            throws IOException {
        synchronized (RocksDBKvBuilder.class) {
            if (!rocksDbInitialized) {

                final File tempDirParent = new File(tempDirectory).getAbsoluteFile();
                LOG.info(
                        "Attempting to load RocksDB native library and store it under '{}'",
                        tempDirParent);

                Throwable lastException = null;
                for (int attempt = 1; attempt <= ROCKSDB_LIB_LOADING_ATTEMPTS; attempt++) {
                    File rocksLibFolder = null;
                    try {
                        // when multiple instances of this class and RocksDB exist in different
                        // class loaders, then we can see the following exception:
                        // "java.lang.UnsatisfiedLinkError: Native Library
                        // /path/to/temp/dir/librocksdbjni-linux64.so
                        // already loaded in another class loader"

                        // to avoid that, we need to add a random element to the library file path
                        // (I know, seems like an unnecessary hack, since the JVM obviously can
                        // handle multiple
                        //  instances of the same JNI library being loaded in different class
                        // loaders, but
                        //  apparently not when coming from the same file path, so there we go)

                        rocksLibFolder =
                                new File(tempDirParent, "rocksdb-lib-" + UUID.randomUUID());

                        // make sure the temp path exists
                        LOG.debug(
                                "Attempting to create RocksDB native library folder {}",
                                rocksLibFolder);
                        // noinspection ResultOfMethodCallIgnored
                        rocksLibFolder.mkdirs();

                        // explicitly load the JNI dependency if it has not been loaded before
                        nativeLibraryLoaderSupplier
                                .get()
                                .loadLibrary(rocksLibFolder.getAbsolutePath());

                        // this initialization here should validate that the loading succeeded
                        RocksDB.loadLibrary();

                        // seems to have worked
                        LOG.info("Successfully loaded RocksDB native library");
                        rocksDbInitialized = true;
                        return;
                    } catch (Throwable t) {
                        lastException = t;
                        LOG.debug("RocksDB JNI library loading attempt {} failed", attempt, t);

                        // try to force RocksDB to attempt reloading the library
                        try {
                            resetRocksDBLoadedFlag();
                        } catch (Throwable tt) {
                            LOG.debug(
                                    "Failed to reset 'initialized' flag in RocksDB native code loader",
                                    tt);
                        }

                        FileUtils.deleteDirectoryQuietly(rocksLibFolder);
                    }
                }

                throw new IOException("Could not load the native RocksDB library", lastException);
            }
        }
    }

    @VisibleForTesting
    static void resetRocksDBLoadedFlag() throws Exception {
        final Field initField =
                org.rocksdb.NativeLibraryLoader.class.getDeclaredField("initialized");
        initField.setAccessible(true);
        initField.setBoolean(null, false);
    }

    @VisibleForTesting
    static void resetRocksDbInitialized() {
        rocksDbInitialized = false;
    }
}
