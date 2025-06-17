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

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.utils.IOUtils;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;

/** External extension for tests that require an instance of RocksDB. */
public class RocksDBExtension implements BeforeEachCallback, AfterEachCallback {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBExtension.class);

    /** provides the working directory for the RocksDB instance. */
    private File rockDbDir;

    private RocksDBResourceContainer rocksDBResourceContainer;
    /** The RocksDB instance object. */
    private RocksDBKv rocksDBKv;

    public RocksDBExtension() {}

    private void before() throws Exception {
        rockDbDir = Files.createTempDirectory("rocksdbDir").toFile();
        rocksDBResourceContainer = new RocksDBResourceContainer(new Configuration(), rockDbDir);
        this.rocksDBKv =
                new RocksDBKvBuilder(
                                rockDbDir,
                                rocksDBResourceContainer,
                                rocksDBResourceContainer.getColumnOptions())
                        .build();
    }

    private void after() {
        // destruct in reversed order of creation.
        IOUtils.closeQuietly(rocksDBKv);
        IOUtils.closeQuietly(rocksDBResourceContainer);
        if (rockDbDir != null) {
            rockDbDir.delete();
        }
    }

    public RocksDB getRocksDb() {
        return rocksDBKv.db;
    }

    public File getRockDbDir() {
        return rockDbDir;
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        after();
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        before();
    }
}
