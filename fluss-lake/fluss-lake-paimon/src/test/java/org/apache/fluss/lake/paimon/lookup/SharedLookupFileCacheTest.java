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

package org.apache.fluss.lake.paimon.lookup;

import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Duration;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SharedLookupFileCache}. */
class SharedLookupFileCacheTest {

    @TempDir private File tempDir;

    @Test
    void testNamespaceIsolationAndGlobalLimit() throws Exception {
        File firstFile = lookupFile("first.lookup");
        File secondFile = lookupFile("second.lookup");
        File thirdFile = lookupFile("third.lookup");

        try (SharedLookupFileCache sharedCache =
                new SharedLookupFileCache(Duration.ofHours(1), MemorySize.ofKibiBytes(2))) {
            Cache<String, LookupFile> firstNamespace = sharedCache.namespaced("first");
            Cache<String, LookupFile> secondNamespace = sharedCache.namespaced("second");
            LookupFile firstLookupFile = lookupFile(firstFile);
            LookupFile secondLookupFile = lookupFile(secondFile);

            firstNamespace.put("same-file-name", firstLookupFile);
            secondNamespace.put("same-file-name", secondLookupFile);
            assertThat(firstNamespace.getIfPresent("same-file-name")).isSameAs(firstLookupFile);
            assertThat(secondNamespace.getIfPresent("same-file-name")).isSameAs(secondLookupFile);

            firstNamespace.invalidateAll();
            assertThat(firstFile).doesNotExist();
            assertThat(secondFile).exists();

            sharedCache.updateMaxDiskSize(MemorySize.ofKibiBytes(1));
            secondNamespace.put("third-file", lookupFile(thirdFile));
            assertThat(Arrays.asList(secondFile, thirdFile).stream().filter(File::exists).count())
                    .isLessThanOrEqualTo(1L);
        }

        assertThat(secondFile).doesNotExist();
        assertThat(thirdFile).doesNotExist();
    }

    private File lookupFile(String name) throws IOException {
        File file = new File(tempDir, name);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.setLength(1024L);
        }
        return file;
    }

    private static LookupFile lookupFile(File file) {
        return new LookupFile(file, 1, 0L, "v1", new NoOpLookupStoreReader(), () -> {});
    }

    private static final class NoOpLookupStoreReader implements LookupStoreReader {
        @Override
        public byte[] lookup(byte[] key) {
            return null;
        }

        @Override
        public void close() {}
    }
}
