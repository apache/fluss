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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

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
            store.delete(new byte[] {1, 1});
            assertThat(store.get(new byte[] {1, 1})).isNull();
            assertThat(store.prefixLookup(new byte[] {1})).containsExactly(new byte[] {20});
            assertThat(store.prefixLookup(new byte[] {3})).isEmpty();
        }
        assertThat(directory).doesNotExist();
    }

    @Test
    void testPrefixReadDoesNotReturnPartialResultsOnIteratorFailure() throws Exception {
        RocksIterator iterator = mock(RocksIterator.class);
        when(iterator.isValid()).thenReturn(true, false);
        when(iterator.key()).thenReturn(new byte[] {1, 1});
        when(iterator.value()).thenReturn(new byte[] {10});
        RocksDBException failure = new RocksDBException("injected iterator read failure");
        doThrow(failure).when(iterator).status();
        assertThatThrownBy(() -> RocksDBLookupStore.readPrefix(iterator, new byte[] {1}))
                .isSameAs(failure);
    }
}
