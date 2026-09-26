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
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
                    .isEqualTo(1L);
            assertThat(sharedCache.capacityEvictions()).isOne();
        }

        assertThat(secondFile).doesNotExist();
        assertThat(thirdFile).doesNotExist();
    }

    @Test
    void testGlobalBudgetAcrossNamespaces() throws Exception {
        File firstFile = lookupFile("first.lookup");
        File secondFile = lookupFile("second.lookup");
        try (SharedLookupFileCache sharedCache =
                new SharedLookupFileCache(Duration.ofHours(1), MemorySize.ofKibiBytes(2))) {
            Cache<String, LookupFile> first = sharedCache.namespaced("first");
            Cache<String, LookupFile> second = sharedCache.namespaced("second");
            first.put("file", lookupFile(firstFile));
            second.put("file", lookupFile(secondFile));
            first.cleanUp();
            assertThat(first.estimatedSize() + second.estimatedSize()).isEqualTo(2L);

            sharedCache.updateMaxDiskSize(MemorySize.ofKibiBytes(1));
            first.cleanUp();
            assertThat(first.estimatedSize() + second.estimatedSize()).isOne();
            assertThat(Arrays.asList(firstFile, secondFile)).filteredOn(File::exists).hasSize(1);
            assertThat(sharedCache.capacityEvictions()).isOne();
            sharedCache.close();
            assertThat(firstFile).doesNotExist();
            assertThat(secondFile).doesNotExist();
            assertThat(sharedCache.capacityEvictions()).isOne();
        }
    }

    @Test
    void testDynamicExpirationAndIndependentFileAccess() throws Exception {
        AtomicLong time = new AtomicLong();
        File firstFile = lookupFile("first.lookup");
        File secondFile = lookupFile("second.lookup");
        try (SharedLookupFileCache sharedCache =
                new SharedLookupFileCache(
                        Duration.ofHours(3), MemorySize.ofKibiBytes(2), time::get)) {
            Cache<String, LookupFile> cache = sharedCache.namespaced("table");
            cache.put("first", lookupFile(firstFile));
            cache.put("second", lookupFile(secondFile));
            time.set(Duration.ofMinutes(20).toNanos());
            assertThat(cache.getIfPresent("second")).isNotNull();
            sharedCache.updateExpireAfterAccess(Duration.ofMinutes(30));
            time.set(Duration.ofMinutes(31).toNanos());
            cache.cleanUp();
            assertThat(firstFile).doesNotExist();
            assertThat(secondFile).exists();
            assertThat(sharedCache.capacityEvictions()).isZero();

            sharedCache.updateExpireAfterAccess(Duration.ofHours(2));
            time.set(Duration.ofHours(1).toNanos());
            cache.cleanUp();
            assertThat(secondFile).exists();
            time.set(Duration.ofHours(3).toNanos());
            cache.cleanUp();
            assertThat(secondFile).doesNotExist();
            assertThat(sharedCache.capacityEvictions()).isZero();
        }
    }

    @Test
    void testCapacityEvictionWaitsForActiveFileRead() throws Exception {
        CountDownLatch readersStarted = new CountDownLatch(2);
        CountDownLatch releaseReaders = new CountDownLatch(1);
        CountDownLatch evictionStarted = new CountDownLatch(1);
        AtomicInteger readersClosed = new AtomicInteger();
        File firstFile = lookupFile("first.lookup");
        File secondFile = lookupFile("second.lookup");
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try (SharedLookupFileCache sharedCache =
                new SharedLookupFileCache(Duration.ofHours(1), MemorySize.ofKibiBytes(2))) {
            LookupFile first =
                    blockingLookupFile(
                            firstFile,
                            readersStarted,
                            releaseReaders,
                            evictionStarted,
                            readersClosed);
            LookupFile second =
                    blockingLookupFile(
                            secondFile,
                            readersStarted,
                            releaseReaders,
                            evictionStarted,
                            readersClosed);
            sharedCache.namespaced("first").put("file", first);
            sharedCache.namespaced("second").put("file", second);
            Future<byte[]> firstRead = executor.submit(() -> first.get(new byte[] {1}));
            Future<byte[]> secondRead = executor.submit(() -> second.get(new byte[] {2}));
            try {
                assertThat(readersStarted.await(30, TimeUnit.SECONDS)).isTrue();
                Future<?> eviction =
                        executor.submit(
                                () -> sharedCache.updateMaxDiskSize(MemorySize.ofKibiBytes(1)));
                assertThat(evictionStarted.await(30, TimeUnit.SECONDS)).isTrue();
                assertThat(readersClosed.get()).isZero();
                assertThat(firstFile).exists();
                assertThat(secondFile).exists();
                releaseReaders.countDown();
                assertThat(firstRead.get(30, TimeUnit.SECONDS)).containsExactly((byte) 1);
                assertThat(secondRead.get(30, TimeUnit.SECONDS)).containsExactly((byte) 2);
                eviction.get(30, TimeUnit.SECONDS);
                assertThat(readersClosed.get()).isOne();
                assertThat(sharedCache.capacityEvictions()).isOne();
            } finally {
                releaseReaders.countDown();
            }
        } finally {
            releaseReaders.countDown();
            executor.shutdownNow();
        }
    }

    private static LookupFile blockingLookupFile(
            File file,
            CountDownLatch started,
            CountDownLatch release,
            CountDownLatch evictionStarted,
            AtomicInteger closed) {
        LookupStoreReader reader =
                new LookupStoreReader() {
                    @Override
                    public byte[] lookup(byte[] key) throws IOException {
                        started.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        }
                        return key;
                    }

                    @Override
                    public void close() {
                        closed.incrementAndGet();
                    }
                };
        return new LookupFile(file, 1, 0L, "v1", reader, () -> {}) {
            @Override
            public void close(RemovalCause cause) throws IOException {
                evictionStarted.countDown();
                super.close(cause);
            }
        };
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
