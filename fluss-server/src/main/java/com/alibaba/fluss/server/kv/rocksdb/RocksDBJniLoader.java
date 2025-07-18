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

package com.alibaba.fluss.server.kv.rocksdb;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.utils.FileUtils;

import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * A utility class for loading RocksDB JNI library.
 *
 * <p>This class provides a centralized way to load RocksDB native library and ensures that:
 *
 * <ul>
 *   <li>The library is loaded only once across the entire JVM
 *   <li>Multiple loading attempts are handled gracefully
 *   <li>Library loading failures are properly reported
 *   <li>Thread safety is maintained during loading
 * </ul>
 *
 * <p>Note: This class should be the only entry point for RocksDB JNI library loading in the
 * project. Direct calls to {@link RocksDB#loadLibrary()} are not allowed.
 */
public class RocksDBJniLoader {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBJniLoader.class);

    /** The number of (re)tries for loading the RocksDB JNI library. */
    private static final int ROCKSDB_LIB_LOADING_ATTEMPTS = 3;

    /** Flag whether the native library has been loaded. */
    private static volatile boolean rocksDbInitialized = false;

    private RocksDBJniLoader() {
        // Utility class, prevent instantiation
    }

    /**
     * Ensures that RocksDB JNI library is loaded.
     *
     * @param tempDirectory the temporary directory to store the native library
     * @throws IOException if the library cannot be loaded after all attempts
     */
    public static void ensureRocksDBIsLoaded(String tempDirectory) throws IOException {
        ensureRocksDBIsLoaded(tempDirectory, NativeLibraryLoader::getInstance);
    }

    /**
     * Ensures that RocksDB JNI library is loaded with a custom library loader supplier.
     *
     * @param tempDirectory the temporary directory to store the native library
     * @param nativeLibraryLoaderSupplier supplier for the native library loader
     * @throws IOException if the library cannot be loaded after all attempts
     */
    @VisibleForTesting
    public static void ensureRocksDBIsLoaded(
            String tempDirectory, Supplier<NativeLibraryLoader> nativeLibraryLoaderSupplier)
            throws IOException {
        synchronized (RocksDBJniLoader.class) {
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
                        // handle multiple instances of the same JNI library being loaded in
                        // different class
                        // loaders, but apparently not when coming from the same file path, so there
                        // we go)

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

    /**
     * Checks if RocksDB JNI library has been loaded.
     *
     * @return true if the library is loaded, false otherwise
     */
    public static boolean isRocksDBLoaded() {
        return rocksDbInitialized;
    }

    /**
     * Resets the RocksDB loaded flag in the native library loader.
     *
     * @throws Exception if the reset operation fails
     */
    @VisibleForTesting
    public static void resetRocksDBLoadedFlag() throws Exception {
        final Field initField =
                org.rocksdb.NativeLibraryLoader.class.getDeclaredField("initialized");
        initField.setAccessible(true);
        initField.setBoolean(null, false);
    }

    /** Resets the initialization flag for testing purposes. */
    @VisibleForTesting
    public static void resetRocksDbInitialized() {
        rocksDbInitialized = false;
    }
}
