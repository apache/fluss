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

package org.apache.fluss.server.kv;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.ByteArraySlice;

import io.github.fluss_contrib.rocksdb.FlushOptions;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests prefix lookup isolation and lifetime while KV writes and close run concurrently. */
class KvTabletPrefixLookupTest extends ReplicaTestBase {
    private final ExecutorService executor = Executors.newFixedThreadPool(3);
    private KvTablet kvTablet;

    @AfterEach
    void stopExecutor() throws InterruptedException {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS))
                .as("Prefix lookup test executor must terminate before tablet cleanup")
                .isTrue();
    }

    @Test
    void testPrefixLookupAllowsWriteAndFlushBeforeSeek() throws Exception {
        createTablet();
        byte[] key = getKeyValuePairs(genKvRecords(new Object[] {1, "old"})).get(0).f0;
        kvTablet.putAsLeader(genKvRecordBatch(new Object[] {1, "old"}), null);
        flushAndWait(kvTablet, Long.MAX_VALUE);
        BlockingReadWriteLock lock = pauseAfterReadUnlock();
        Future<List<ByteArraySlice>> scan = executor.submit(() -> kvTablet.prefixLookup(key));
        try {
            assertThat(lock.readUnlocked.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(kvTablet.getRocksDBKv().getResourceGuard().getLeaseCount()).isEqualTo(1);
            Future<?> write =
                    executor.submit(
                            () -> {
                                kvTablet.putAsLeader(
                                        genKvRecordBatch(new Object[] {1, "new"}), null);
                                flushAndWait(kvTablet, Long.MAX_VALUE);
                                return null;
                            });
            write.get(10, TimeUnit.SECONDS);
            assertThat(scan).isNotDone();
        } finally {
            lock.continueRead.countDown();
        }
        List<ByteArraySlice> values = scan.get(10, TimeUnit.SECONDS);
        assertThat(values).hasSize(1);
        assertThat(values.get(0).toByteArray())
                .containsExactly(getKeyValuePairs(genKvRecords(new Object[] {1, "old"})).get(0).f1);
        assertThat(kvTablet.getRocksDBKv().getResourceGuard().getLeaseCount()).isZero();
    }

    @Test
    void testPrefixLookupCorruptionReleasesReadLease() throws Exception {
        createTablet();
        byte[] key = getKeyValuePairs(genKvRecords(new Object[] {1, "value"})).get(0).f0;
        kvTablet.putAsLeader(genKvRecordBatch(new Object[] {1, "value"}), null);
        flushAndWait(kvTablet, Long.MAX_VALUE);
        try (FlushOptions options = new FlushOptions().setWaitForFlush(true)) {
            kvTablet.getRocksDBKv().getDb().flush(options);
        }
        try (Stream<Path> files = Files.walk(kvTablet.getKvTabletDir().toPath())) {
            Path sst = files.filter(path -> path.toString().endsWith(".sst")).findFirst().get();
            try (RandomAccessFile file = new RandomAccessFile(sst.toFile(), "rw")) {
                int firstByte = file.readUnsignedByte();
                file.seek(0);
                file.writeByte(firstByte ^ 0xff);
            }
        }
        assertThatThrownBy(() -> kvTablet.prefixLookup(key))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to iterate prefix lookup.")
                .hasCauseInstanceOf(RocksDBException.class)
                .satisfies(error -> assertThat(error.getCause()).hasMessageContaining("checksum"));
        assertThat(kvTablet.getRocksDBKv().getResourceGuard().getLeaseCount()).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConcurrentCloseOrDropWaitsForPrefixRead(boolean drop) throws Exception {
        createTablet();
        RocksDBKv rocks = kvTablet.getRocksDBKv();
        RocksDBKv.PrefixLookup read = rocks.openPrefixLookup(new byte[] {1});
        Future<?> firstClose =
                executor.submit(
                        () -> {
                            kvTablet.close();
                            return null;
                        });
        Future<?> secondClose = null;
        try {
            waitUntil(
                    rocks.getResourceGuard()::isClosed,
                    Duration.ofSeconds(10),
                    "Close must wait for the read lease.");
            AtomicReference<Thread> closingThread = new AtomicReference<>();
            secondClose =
                    executor.submit(
                            () -> {
                                closingThread.set(Thread.currentThread());
                                if (drop) {
                                    kvTablet.drop();
                                } else {
                                    kvTablet.close();
                                }
                                return null;
                            });
            Future<?> closing = secondClose;
            waitUntil(
                    () ->
                            closing.isDone()
                                    || (closingThread.get() != null
                                            && closingThread.get().getState()
                                                    == Thread.State.WAITING),
                    Duration.ofSeconds(10),
                    "The second closer must wait for physical close.");
            assertThat(secondClose).isNotDone();
            assertThat(kvTablet.getKvTabletDir()).exists();
            assertThatThrownBy(() -> kvTablet.prefixLookup(new byte[] {1}))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("closing");
            assertThat(read.readAll()).isEmpty();
        } finally {
            read.close();
            firstClose.get(10, TimeUnit.SECONDS);
            if (secondClose != null) {
                secondClose.get(10, TimeUnit.SECONDS);
            }
        }
        assertThat(rocks.getResourceGuard().getLeaseCount()).isZero();
        if (drop) {
            assertThat(kvTablet.getKvTabletDir()).doesNotExist();
        }
    }

    private void createTablet() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        Replica replica = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, bucket);
        replica.makeLeader(
                new NotifyLeaderAndIsrData(
                        DATA1_PHYSICAL_TABLE_PATH_PK,
                        bucket,
                        Arrays.asList(TABLET_SERVER_ID, TABLET_SERVER_ID + 1),
                        new LeaderAndIsr(
                                TABLET_SERVER_ID,
                                0,
                                Collections.singletonList(TABLET_SERVER_ID),
                                Collections.emptyList(),
                                INITIAL_COORDINATOR_EPOCH,
                                0)));
        kvTablet = replica.getKvTablet();
    }

    private BlockingReadWriteLock pauseAfterReadUnlock() throws Exception {
        BlockingReadWriteLock lock = new BlockingReadWriteLock();
        Field field = KvTablet.class.getDeclaredField("kvLock");
        field.setAccessible(true);
        field.set(kvTablet, lock);
        return lock;
    }

    private static class BlockingReadWriteLock extends ReentrantReadWriteLock {
        private final CountDownLatch readUnlocked = new CountDownLatch(1);
        private final CountDownLatch continueRead = new CountDownLatch(1);
        private final AtomicBoolean pauseNextRead = new AtomicBoolean(true);
        private final ReadLock readLock =
                new ReadLock(this) {
                    @Override
                    public void unlock() {
                        super.unlock();
                        if (pauseNextRead.compareAndSet(true, false)) {
                            readUnlocked.countDown();
                            try {
                                assertThat(continueRead.await(10, TimeUnit.SECONDS)).isTrue();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new AssertionError(e);
                            }
                        }
                    }
                };

        @Override
        public ReadLock readLock() {
            return readLock;
        }
    }
}
