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

package org.apache.fluss.rocksdb;

import org.apache.fluss.utils.IOUtils;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Utility for creating a RocksDB instance either from scratch or from restored local data. */
public class RocksDBHandle implements AutoCloseable {

    private final boolean isReadOnly;
    private final DBOptions dbOptions;

    private final String dbPath;

    private RocksDB db;

    private ColumnFamilyHandle defaultColumnFamilyHandle;

    private final ColumnFamilyOptions defaultColumnFamilyOptions;

    public RocksDBHandle(
            File instanceRocksDBPath,
            DBOptions dbOptions,
            ColumnFamilyOptions defaultColumnFamilyOptions,
            boolean isReadOnly) {
        this.dbPath = instanceRocksDBPath.getAbsolutePath();
        this.dbOptions = dbOptions;
        this.defaultColumnFamilyOptions = defaultColumnFamilyOptions;
        this.isReadOnly = isReadOnly;
    }

    public RocksDBHandle(
            File instanceRocksDBPath,
            DBOptions dbOptions,
            ColumnFamilyOptions defaultColumnFamilyOptions) {
        this(instanceRocksDBPath, dbOptions, defaultColumnFamilyOptions, false);
    }

    public void openDB() throws IOException {
        loadDb();
    }

    private void loadDb() throws IOException {
        // we only have one column family, default column family
        List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                Collections.singletonList(
                        new ColumnFamilyDescriptor(
                                RocksDB.DEFAULT_COLUMN_FAMILY, defaultColumnFamilyOptions));
        List<ColumnFamilyHandle> defaultCfHandle = new ArrayList<>(1);
        db =
                RocksDBOperationUtils.openDB(
                        dbPath, columnFamilyDescriptors, defaultCfHandle, dbOptions, isReadOnly);
        // remove the default column family which is located at the first index
        defaultColumnFamilyHandle = defaultCfHandle.remove(0);
    }

    public RocksDB getDb() {
        return db;
    }

    public ColumnFamilyHandle getDefaultColumnFamilyHandle() {
        return defaultColumnFamilyHandle;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(defaultColumnFamilyHandle);
        IOUtils.closeQuietly(db);
        // Making sure the already created column family options will be closed
        IOUtils.closeQuietly(defaultColumnFamilyOptions);
    }
}
