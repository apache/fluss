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

package org.apache.fluss.flink.source.lookup;

import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.RocksIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests local lookup storage and propagation of prefix-read failures. */
class RocksDBLookupStoreTest {

    @TempDir private Path tempDir;

    @Test
    void testPointAndPrefixReads() throws Exception {
        Path directory = tempDir.resolve("cache");
        try (RocksDBLookupStore store = new RocksDBLookupStore(directory.toFile())) {
            store.put(new byte[] {1, 1}, new byte[] {10});
            store.put(new byte[] {1, 2}, new byte[] {20});
            store.put(new byte[] {2, 1}, new byte[] {30});
            assertThat(store.get(new byte[] {1, 1})).containsExactly((byte) 10);
            assertThat(store.prefixLookup(new byte[] {1}))
                    .containsExactly(new byte[] {10}, new byte[] {20});
            assertThat(store.prefixLookup(new byte[] {1, 1})).containsExactly(new byte[] {10});
            assertThat(store.prefixLookup(new byte[] {1, 1, 0})).isEmpty();
            assertThat(store.prefixLookup(new byte[0]))
                    .containsExactly(new byte[] {10}, new byte[] {20}, new byte[] {30});
            store.delete(new byte[] {1, 1});
            assertThat(store.get(new byte[] {1, 1})).isNull();
            assertThat(store.prefixLookup(new byte[] {1})).containsExactly(new byte[] {20});
            assertThat(store.prefixLookup(new byte[] {3})).isEmpty();
        }
        assertThat(directory).doesNotExist();
    }

    @Test
    void testPrefixReadsWithUnsignedByteBounds() throws Exception {
        try (RocksDBLookupStore store = new RocksDBLookupStore(tempDir.resolve("cache").toFile())) {
            store.put(new byte[] {1, (byte) 0xff}, new byte[] {10});
            store.put(new byte[] {1, (byte) 0xff, 0}, new byte[] {20});
            store.put(new byte[] {2}, new byte[] {30});
            store.put(new byte[] {0x7f, (byte) 0xff}, new byte[] {40});
            store.put(new byte[] {(byte) 0x80}, new byte[] {50});
            store.put(new byte[] {(byte) 0xff}, new byte[] {60});
            store.put(new byte[] {(byte) 0xff, (byte) 0xff}, new byte[] {70});

            assertThat(store.prefixLookup(new byte[] {1, (byte) 0xff}))
                    .containsExactly(new byte[] {10}, new byte[] {20});
            assertThat(store.prefixLookup(new byte[] {0x7f, (byte) 0xff}))
                    .containsExactly(new byte[] {40});
            assertThat(store.prefixLookup(new byte[] {(byte) 0xff}))
                    .containsExactly(new byte[] {60}, new byte[] {70});
            assertThat(store.prefixLookup(new byte[] {(byte) 0xff, (byte) 0xff}))
                    .containsExactly(new byte[] {70});
        }
    }

    @Test
    void testFailedOpenCleansDirectoryAndAllowsRetry() throws Exception {
        Path directory = Files.createDirectory(tempDir.resolve("broken-cache"));
        // An invalid CURRENT file forces RocksDB.open to fail after allocating native options.
        Files.write(directory.resolve("CURRENT"), "invalid".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> new RocksDBLookupStore(directory.toFile()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Error while opening RocksDB");
        assertThat(directory).doesNotExist();

        try (RocksDBLookupStore store = new RocksDBLookupStore(directory.toFile())) {
            store.put(new byte[] {1}, new byte[] {10});
            assertThat(store.get(new byte[] {1})).containsExactly((byte) 10);
        }
        assertThat(directory).doesNotExist();
    }

    @Test
    void testDiskSizeUsesSstFilesOnly() throws Exception {
        Path directory = tempDir.resolve("cache");
        try (RocksDBLookupStore store = new RocksDBLookupStore(directory.toFile())) {
            Files.write(directory.resolve("unrelated-file"), new byte[1024]);
            assertThat(store.diskSizeBytes()).isZero();
        }
    }

    @Test
    void testPrefixReadDoesNotReturnPartialResultsOnIteratorFailure() throws Exception {
        RocksIterator iterator = mock(RocksIterator.class);
        when(iterator.isValid()).thenReturn(true, false);
        when(iterator.value()).thenReturn(new byte[] {10});
        RocksDBException failure = new RocksDBException("injected iterator read failure");
        doThrow(failure).when(iterator).status();
        assertThatThrownBy(() -> RocksDBLookupStore.readPrefix(iterator, new byte[] {1}))
                .isSameAs(failure);
    }
}
