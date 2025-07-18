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

import com.alibaba.fluss.rocksdb.RocksDBHandle;
import com.alibaba.fluss.server.exception.KvBuildingException;
import com.alibaba.fluss.server.utils.ResourceGuard;
import com.alibaba.fluss.utils.IOUtils;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

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
            // Ensure RocksDB JNI library is loaded using the centralized loader
            RocksDBJniLoader.ensureRocksDBIsLoaded(System.getProperty("java.io.tmpdir"));
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
}
