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

package com.alibaba.fluss.server.lakehouse;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.TableAlreadyExistException;
import com.alibaba.fluss.lake.lakestorage.LakeCatalog;
import com.alibaba.fluss.lake.lakestorage.LakeStorage;
import com.alibaba.fluss.lake.lakestorage.LakeStoragePlugin;
import com.alibaba.fluss.lake.source.LakeSource;
import com.alibaba.fluss.lake.writer.LakeTieringFactory;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;

import java.util.HashMap;
import java.util.Map;

/** A plugin of paimon just for testing purpose. */
public class TestingPaimonStoragePlugin implements LakeStoragePlugin {

    public static final String IDENTIFIER = DataLakeFormat.PAIMON.toString();

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public LakeStorage createLakeStorage(Configuration configuration) {
        return new TestingPaimonLakeStorage();
    }

    /** Paimon implementation of LakeStorage for testing purpose. */
    public static class TestingPaimonLakeStorage implements LakeStorage {

        @Override
        public LakeTieringFactory<?, ?> createLakeTieringFactory() {
            throw new UnsupportedOperationException("createLakeTieringFactory is not supported.");
        }

        @Override
        public LakeCatalog createLakeCatalog() {
            return new TestingPaimonCatalog();
        }

        @Override
        public LakeSource<?> createLakeSource(TablePath tablePath) {
            throw new UnsupportedOperationException("Not implemented");
        }
    }

    /** Paimon implementation of LakeCatalog for testing purpose. */
    public static class TestingPaimonCatalog implements LakeCatalog {

        private final Map<TablePath, TableDescriptor> tableByPath = new HashMap<>();

        @Override
        public void createTable(TablePath tablePath, TableDescriptor tableDescriptor)
                throws TableAlreadyExistException {
            if (tableByPath.containsKey(tablePath)) {
                throw new TableAlreadyExistException("Table " + tablePath + " already exists");
            }
            tableByPath.put(tablePath, tableDescriptor);
        }

        public TableDescriptor getTable(TablePath tablePath) {
            return tableByPath.get(tablePath);
        }
    }
}
