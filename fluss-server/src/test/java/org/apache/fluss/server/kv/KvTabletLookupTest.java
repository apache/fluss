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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

/** Tests lookup isolation and resource lifetime while writes and close run concurrently. */
class KvTabletLookupTest extends ReplicaTestBase {
    private final ExecutorService executor = Executors.newFixedThreadPool(3);
    private KvTablet kvTablet;

    @AfterEach
    void stopExecutor() throws InterruptedException {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 15, 16, 32})
    void testMultiGetPreservesReadViewAcrossFlush(int keyCount) throws Exception {
        createTablet();
        kvTablet.putAsLeader(genKvRecordBatch(new Object[] {1, "old"}), null);
        flushAndWait(kvTablet, Long.MAX_VALUE);
        byte[] key = getKeyValuePairs(genKvRecords(new Object[] {1, "old"})).get(0).f0;
        byte[] missing = getKeyValuePairs(genKvRecords(new Object[] {2, "missing"})).get(0).f0;
        List<byte[]> keys = new ArrayList<>(Collections.nCopies(keyCount, key));
        if (keyCount > 1) {
            keys.set(1, missing);
        }
        BlockingReadWriteLock lock = pauseAfterReadUnlock();
        RocksDBKv reader = kvTablet.getRocksDBKv();
        Future<List<ByteArraySlice>> read = executor.submit(() -> kvTablet.multiGet(keys));
        try {
            assertThat(lock.readUnlocked.await(10, TimeUnit.SECONDS)).isTrue();
            int expectedViews = keyCount < 16 ? 0 : 1;
            assertThat(reader.getResourceGuard().getLeaseCount()).isEqualTo(expectedViews);
            assertThat(reader.getDb().getLongProperty("rocksdb.num-snapshots"))
                    .isEqualTo(expectedViews);
            executor.submit(
                            () -> {
                                kvTablet.putAsLeader(
                                        genKvRecordBatch(
                                                new Object[] {1, "new"},
                                                new Object[] {2, "inserted"}),
                                        null);
                                flushAndWait(kvTablet, Long.MAX_VALUE);
                                return null;
                            })
                    .get(10, TimeUnit.SECONDS);
            assertThat(read).isNotDone();
        } finally {
            lock.continueRead.countDown();
        }
        List<ByteArraySlice> values = read.get(10, TimeUnit.SECONDS);
        byte[] expected = getKeyValuePairs(genKvRecords(new Object[] {1, "old"})).get(0).f1;
        assertThat(values).hasSize(keyCount);
        for (int i = 0; i < keyCount; i++) {
            if (i == 1) {
                assertThat(values.get(i)).isNull();
            } else {
                assertThat(values.get(i).toByteArray()).containsExactly(expected);
            }
        }
        assertThat(reader.getResourceGuard().getLeaseCount()).isZero();
        assertThat(reader.getDb().getLongProperty("rocksdb.num-snapshots")).isZero();
    }

    @ParameterizedTest
    @ValueSource(ints = {15, 16})
    void testInvalidMultiGetReleasesResources(int keyCount) throws Exception {
        createTablet();
        RocksDBKv reader = kvTablet.getRocksDBKv();
        assertThatThrownBy(() -> kvTablet.multiGet(Collections.nCopies(keyCount, null)))
                .isInstanceOf(NullPointerException.class);
        assertThat(reader.getResourceGuard().getLeaseCount()).isZero();
        assertThat(reader.getDb().getLongProperty("rocksdb.num-snapshots")).isZero();
        assertThat(kvTablet.multiGet(Collections.nCopies(keyCount, new byte[] {1})))
                .containsOnlyNulls();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCloseOrDropWaitsForMultiGet(boolean drop) throws Exception {
        createTablet();
        RocksDBKv rocks = kvTablet.getRocksDBKv();
        RocksDBKv.MultiGetLookup read = rocks.openMultiGet();
        Future<?> closing =
                executor.submit(
                        () -> {
                            if (drop) {
                                kvTablet.drop();
                            } else {
                                kvTablet.close();
                            }
                            return null;
                        });
        try {
            waitUntil(
                    rocks.getResourceGuard()::isClosed,
                    Duration.ofSeconds(10),
                    "Close must wait for the lookup lease.");
            assertThat(closing).isNotDone();
            assertThat(kvTablet.getKvTabletDir()).exists();
            for (int keyCount : new int[] {1, 16}) {
                assertThatThrownBy(
                                () ->
                                        kvTablet.multiGet(
                                                Collections.nCopies(keyCount, new byte[] {1})))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("closing");
            }
            assertThat(read.read(Collections.singletonList(new byte[] {1})))
                    .containsExactly((byte[]) null);
        } finally {
            read.close();
            closing.get(10, TimeUnit.SECONDS);
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
