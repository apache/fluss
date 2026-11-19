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

import io.github.fluss_contrib.rocksdb.ColumnFamilyDescriptor;
import io.github.fluss_contrib.rocksdb.ColumnFamilyHandle;
import io.github.fluss_contrib.rocksdb.ColumnFamilyOptions;
import io.github.fluss_contrib.rocksdb.DBOptions;
import io.github.fluss_contrib.rocksdb.RocksDB;

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

    private final List<ColumnFamilyDescriptor> columnFamilies;
    private final List<ColumnFamilyHandle> handles = new ArrayList<>();

    public RocksDBHandle(
            File instanceRocksDBPath,
            DBOptions dbOptions,
            ColumnFamilyOptions defaultColumnFamilyOptions,
            boolean isReadOnly) {
        this(
                instanceRocksDBPath,
                dbOptions,
                Collections.singletonList(
                        new ColumnFamilyDescriptor(
                                RocksDB.DEFAULT_COLUMN_FAMILY, defaultColumnFamilyOptions)),
                isReadOnly);
    }

    public RocksDBHandle(
            File instanceRocksDBPath,
            DBOptions dbOptions,
            ColumnFamilyOptions defaultColumnFamilyOptions) {
        this(instanceRocksDBPath, dbOptions, defaultColumnFamilyOptions, false);
    }

    /** Creates a handle for a database whose first descriptor is the default column family. */
    public RocksDBHandle(
            File instanceRocksDBPath,
            DBOptions dbOptions,
            List<ColumnFamilyDescriptor> columnFamilies,
            boolean isReadOnly) {
        this.dbPath = instanceRocksDBPath.getAbsolutePath();
        this.dbOptions = dbOptions;
        this.columnFamilies = columnFamilies;
        this.isReadOnly = isReadOnly;
    }

    public void openDB() throws IOException {
        db = RocksDBOperationUtils.openDB(dbPath, columnFamilies, handles, dbOptions, isReadOnly);
    }

    public RocksDB getDb() {
        return db;
    }

    public ColumnFamilyHandle getDefaultColumnFamilyHandle() {
        return handles.get(0);
    }

    @Override
    public void close() {
        handles.forEach(IOUtils::closeQuietly);
        IOUtils.closeQuietly(db);
        columnFamilies.forEach(descriptor -> IOUtils.closeQuietly(descriptor.getOptions()));
    }
}
