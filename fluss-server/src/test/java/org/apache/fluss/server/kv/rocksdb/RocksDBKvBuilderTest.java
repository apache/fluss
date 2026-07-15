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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.server.exception.KvBuildingException;
import org.apache.fluss.server.kv.RowTtlCompactionFilterFactory;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.FlinkCompactionFilter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link org.apache.fluss.server.kv.rocksdb.RocksDBKvBuilder} . */
class RocksDBKvBuilderTest {

    /**
     * This test checks that the RocksDB native code loader still responds to resetting the init
     * flag.
     */
    @Test
    void testResetInitFlag() throws Exception {
        RocksDBKvBuilder.resetRocksDBLoadedFlag();
    }

    @Test
    void testTempLibFolderDeletedOnFail(@TempDir Path tempDir) {
        RocksDBKvBuilder.resetRocksDbInitialized();
        assertThatThrownBy(
                        () ->
                                RocksDBKvBuilder.ensureRocksDBIsLoaded(
                                        tempDir.toString(),
                                        () -> {
                                            throw new FlussRuntimeException("expected exception");
                                        }))
                .isInstanceOf(IOException.class);
        File[] files = tempDir.toFile().listFiles();
        assertThat(files).isNotNull();
        assertThat(files).isEmpty();
    }

    @Test
    void testCompactionFilterFactoryClosedWithKv(@TempDir Path tempDir) throws Exception {
        FlinkCompactionFilter.FlinkCompactionFilterFactory filterFactory =
                RowTtlCompactionFilterFactory.create(Duration.ofHours(1L), new ManualClock(0L));
        RocksDBResourceContainer rocksDBResourceContainer =
                new RocksDBResourceContainer(new Configuration(), tempDir.toFile());
        RocksDBKvBuilder rocksDBKvBuilder =
                new RocksDBKvBuilder(
                                tempDir.toFile(),
                                rocksDBResourceContainer,
                                rocksDBResourceContainer.getColumnOptions())
                        .setCompactionFilterFactory(filterFactory);

        try {
            try (RocksDBKv ignored = rocksDBKvBuilder.build()) {
                assertThat(filterFactory.isOwningHandle()).isTrue();
            }
            assertThat(filterFactory.isOwningHandle()).isFalse();
        } finally {
            filterFactory.close();
        }
    }

    @Test
    void testCompactionFilterFactoryClosedWhenBuildFails() {
        FlinkCompactionFilter.FlinkCompactionFilterFactory filterFactory =
                RowTtlCompactionFilterFactory.create(Duration.ofHours(1L), new ManualClock(0L));
        RocksDBResourceContainer rocksDBResourceContainer = new RocksDBResourceContainer();
        RocksDBKvBuilder rocksDBKvBuilder =
                new RocksDBKvBuilder(
                                null,
                                rocksDBResourceContainer,
                                rocksDBResourceContainer.getColumnOptions())
                        .setCompactionFilterFactory(filterFactory);

        try {
            assertThatThrownBy(rocksDBKvBuilder::build).isInstanceOf(KvBuildingException.class);
            assertThat(filterFactory.isOwningHandle()).isFalse();
        } finally {
            filterFactory.close();
        }
    }
}
