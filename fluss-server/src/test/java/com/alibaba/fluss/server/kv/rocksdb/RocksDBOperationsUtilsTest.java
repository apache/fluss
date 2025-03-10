/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

import com.alibaba.fluss.rocksdb.RocksDBOperationUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link RocksDBOperationUtils}. */
class RocksDBOperationsUtilsTest {

    @Test
    void testOpenDBFail(@TempDir Path temporaryFolder) throws Exception {
        final File rocksDir = new File(temporaryFolder.toFile(), "db");

        Files.createDirectories(rocksDir.toPath());

        try (DBOptions dbOptions = new DBOptions().setCreateIfMissing(false)) {
            assertThatThrownBy(
                            () -> {
                                RocksDB rocks =
                                        RocksDBOperationUtils.openDB(
                                                rocksDir.getAbsolutePath(),
                                                Collections.emptyList(),
                                                Collections.emptyList(),
                                                dbOptions,
                                                false);
                                rocks.close();
                            })
                    .isInstanceOf(IOException.class)
                    .hasMessage("Error while opening RocksDB instance.")
                    .cause()
                    .isInstanceOf(RocksDBException.class)
                    .hasMessageContaining("does not exist (create_if_missing is false)");
        }
    }
}
