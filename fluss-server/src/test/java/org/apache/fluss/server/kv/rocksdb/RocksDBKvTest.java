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
import org.apache.fluss.metrics.SimpleCounter;
import org.apache.fluss.metrics.util.TestHistogram;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.fluss.server.kv.rocksdb.RocksDBKv}. */
class RocksDBKvTest {

    @Test
    void testRocksDbKv(@TempDir Path tempDir) throws Exception {
        File instanceBasePath = tempDir.toFile();
        RocksDBResourceContainer rocksDBResourceContainer =
                new RocksDBResourceContainer(new Configuration(), instanceBasePath);
        RocksDBKvBuilder rocksDBKvBuilder =
                new RocksDBKvBuilder(
                        instanceBasePath,
                        rocksDBResourceContainer,
                        rocksDBResourceContainer.getColumnOptions());

        try (RocksDBKv rocksDBKv = rocksDBKvBuilder.build()) {
            // put the k/v
            byte[] key = new byte[] {1, 2, 3};
            byte[] val = new byte[] {1, 2};
            rocksDBKv.put(key, val);
            assertThat(rocksDBKv.get(key)).isEqualTo(val);
            // put with a different value
            byte[] val1 = new byte[] {1};
            rocksDBKv.put(key, val1);
            assertThat(rocksDBKv.get(key)).isEqualTo(val1);
            // delete the key
            rocksDBKv.delete(key);
            assertThat(rocksDBKv.get(key)).isNull();

            // test multi get
            byte[] key2 = new byte[] {1, 2, 3, 4};
            byte[] val2 = new byte[] {1, 2, 3};
            rocksDBKv.put(key2, val2);

            assertThat(rocksDBKv.multiGet(Arrays.asList(key, key2))).containsExactly(null, val2);
        }
    }

    // ------------------------------------------------------------------
    //  Backpressure tests
    // ------------------------------------------------------------------

    @Test
    void testDefaultWriteBatchAllowsRocksDbSlowdown(@TempDir Path tempDir) throws Exception {
        File instanceBasePath = tempDir.toFile();
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(new Configuration(), instanceBasePath);
        RocksDBKvBuilder builder =
                new RocksDBKvBuilder(instanceBasePath, container, container.getColumnOptions());

        try (RocksDBKv kv = builder.build();
                RocksDBWriteBatchWrapper writer =
                        kv.newWriteBatch(1024, new SimpleCounter(), new TestHistogram())) {
            assertThat(noSlowdown(writer)).isFalse();
        }
    }

    @Test
    void testNoSlowdownWriteBatchFailsFastOnRocksDbDelay(@TempDir Path tempDir) throws Exception {
        File instanceBasePath = tempDir.toFile();
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(new Configuration(), instanceBasePath);
        RocksDBKvBuilder builder =
                new RocksDBKvBuilder(instanceBasePath, container, container.getColumnOptions());

        try (RocksDBKv kv = builder.build();
                RocksDBWriteBatchWrapper writer =
                        kv.newNoSlowdownWriteBatch(
                                1024, new SimpleCounter(), new TestHistogram())) {
            assertThat(noSlowdown(writer)).isTrue();
        }
    }

    /**
     * Verifies that the L0 property is actually readable on frocksdbjni 6.20.3-ververica-2.0.
     *
     * <p>This is a regression test: {@code getLongProperty("rocksdb.num-files-at-level0")} throws
     * {@code RocksDBException: NotFound} because the property is parametric (string-type), not an
     * int-type property. The fix uses {@code getProperty} + {@code Long.parseLong} instead.
     */
    @Test
    void testCurrentPressure_l0PropertyReadable(@TempDir Path tempDir) throws Exception {
        File instanceBasePath = tempDir.toFile();
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(new Configuration(), instanceBasePath);
        ColumnFamilyOptions cfOpts = container.getColumnOptions();

        RocksDBKvBuilder builder =
                new RocksDBKvBuilder(instanceBasePath, container, cfOpts)
                        .setFlussL0SlowdownTrigger(2);

        try (RocksDBKv kv = builder.build()) {
            // Initially 0 L0 files, should return 0 pressure.
            assertThat(kv.currentPressure()).isEqualTo(0f);

            // Write + flush to produce exactly 1 L0 SST.
            kv.put(new byte[] {1}, new byte[] {1});
            try (FlushOptions flushOpts = new FlushOptions()) {
                flushOpts.setWaitForFlush(true);
                kv.db.flush(flushOpts);
            }
            kv.refreshL0FileCount();

            // L0 = 1, still below flussL0SlowdownTrigger (2), should still be 0.
            assertThat(kv.currentPressure()).isEqualTo(0f);
        }
    }

    /**
     * Verifies the proactive backpressure pressure curve. Configuration: flussTrigger=2,
     * rocksdbSlowdownTrigger=5.
     */
    @Test
    void testPressureCurve(@TempDir Path tempDir) throws Exception {
        File instanceBasePath = tempDir.toFile();
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(new Configuration(), instanceBasePath);
        ColumnFamilyOptions cfOpts = container.getColumnOptions();
        // slowdownTrigger = 6; with default maxWriteBufferNumber = 2 the gate closes when
        // L0 + 2 >= 6, i.e. L0 >= 4. High compaction/stop triggers prevent RocksDB from
        // interfering during the test.
        cfOpts.setLevel0SlowdownWritesTrigger(6);
        cfOpts.setLevel0FileNumCompactionTrigger(100);
        cfOpts.setLevel0StopWritesTrigger(100);

        RocksDBKvBuilder builder =
                new RocksDBKvBuilder(instanceBasePath, container, cfOpts)
                        .setFlussL0SlowdownTrigger(2);

        try (RocksDBKv kv = builder.build()) {
            // --- L0 = 0: no pressure ---
            assertThat(kv.currentPressure()).isEqualTo(0f);

            // Flush to L0 = 2 (reaches flussTrigger). p = (2-2)/(6-2) = 0.
            flushNTimes(kv, 2);
            assertThat(kv.currentPressure()).isEqualTo(0f);

            // Flush to L0 = 3: proactive throttle signal kicks in. p = (3-2)/(6-2) = 1/4.
            flushNTimes(kv, 1);
            float p = kv.currentPressure();
            assertThat(p).isCloseTo(1f / 4f, org.assertj.core.data.Offset.offset(0.01f));

            // Flush to L0 = 4: p = (4-2)/(6-2) = 2/4 = 0.5.
            flushNTimes(kv, 1);
            p = kv.currentPressure();
            assertThat(p).isCloseTo(0.5f, org.assertj.core.data.Offset.offset(0.01f));

            // Flush to L0 = 5: p clamped to (window-1)/window = 3/4.
            flushNTimes(kv, 1);
            p = kv.currentPressure();
            assertThat(p).isLessThan(1f);
            assertThat(p).isCloseTo(3f / 4f, org.assertj.core.data.Offset.offset(0.01f));
        }
    }

    @Test
    void testWriteRejectionStaysRaisedUntilWriteSuccess(@TempDir Path tempDir) throws Exception {
        File instanceBasePath = tempDir.toFile();
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(new Configuration(), instanceBasePath);
        ColumnFamilyOptions cfOpts = container.getColumnOptions();
        cfOpts.setLevel0SlowdownWritesTrigger(6);
        cfOpts.setLevel0FileNumCompactionTrigger(100);
        cfOpts.setLevel0StopWritesTrigger(100);

        RocksDBKvBuilder builder =
                new RocksDBKvBuilder(instanceBasePath, container, cfOpts)
                        .setFlussL0SlowdownTrigger(2);

        try (RocksDBKv kv = builder.build()) {
            assertThat(kv.currentPressure()).isEqualTo(0f);

            kv.recordWriteRejected();

            assertThat(kv.currentPressure())
                    .isCloseTo(0.8f, org.assertj.core.data.Offset.offset(0.01f));
            assertThat(kv.wouldExceedFlushBudget(0)).isTrue();

            Thread.sleep(150L);

            assertThat(kv.currentPressure())
                    .isCloseTo(0.8f, org.assertj.core.data.Offset.offset(0.01f));
            assertThat(kv.wouldExceedFlushBudget(0)).isTrue();

            kv.recordWriteSucceeded();

            assertThat(kv.currentPressure()).isEqualTo(0f);
            assertThat(kv.wouldExceedFlushBudget(0)).isFalse();
        }
    }

    /** Writes a unique key and flushes the memtable N times to produce N L0 SST files. */
    private static void flushNTimes(RocksDBKv kv, int n) throws Exception {
        for (int i = 0; i < n; i++) {
            // Write a unique key to dirty the memtable.
            byte[] key = ("flush-key-" + System.nanoTime() + "-" + i).getBytes();
            kv.put(key, key);
            try (FlushOptions flushOpts = new FlushOptions()) {
                flushOpts.setWaitForFlush(true);
                kv.db.flush(flushOpts);
            }
        }
        kv.refreshL0FileCount();
    }

    private static boolean noSlowdown(RocksDBWriteBatchWrapper writer) throws Exception {
        Field field = RocksDBWriteBatchWrapper.class.getDeclaredField("options");
        field.setAccessible(true);
        return ((WriteOptions) field.get(writer)).noSlowdown();
    }

    /**
     * Verifies that {@link RocksDBKv#wouldExceedFlushBudget(long)} correctly rejects writes when
     * the pending buffer bytes would produce enough L0 SSTs to breach the slowdown trigger.
     *
     * <p>Configuration: slowdownTrigger=6, maxWriteBufferNumber=2, writeBufferSize=1024. The
     * admission gate rejects when {@code currentL0 + maxWriteBufferNumber + estimatedFlushFiles >=
     * slowdownTrigger}.
     *
     * <ul>
     *   <li>L0=0 → 3 estimated files is safe, 4 estimated files rejected.
     *   <li>L0=2 → 1 estimated file is safe, 2 estimated files rejected.
     *   <li>L0=4 → remainingSlots=0 → always rejected (L0 gate).
     * </ul>
     */
    @Test
    void testFlushBudgetRejectsLargeBuffer(@TempDir Path tempDir) throws Exception {
        File instanceBasePath = tempDir.toFile();
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(new Configuration(), instanceBasePath);
        ColumnFamilyOptions cfOpts = container.getColumnOptions();
        cfOpts.setLevel0SlowdownWritesTrigger(6);
        cfOpts.setWriteBufferSize(1024);
        cfOpts.setMaxWriteBufferNumber(2);
        cfOpts.setLevel0FileNumCompactionTrigger(100);
        cfOpts.setLevel0StopWritesTrigger(100);

        RocksDBKvBuilder builder =
                new RocksDBKvBuilder(instanceBasePath, container, cfOpts)
                        .setFlussL0SlowdownTrigger(2);

        try (RocksDBKv kv = builder.build()) {
            assertThat(kv.wouldExceedFlushBudget(0)).isFalse();
            assertThat(kv.wouldExceedFlushBudget(3072)).isFalse();
            assertThat(kv.wouldExceedFlushBudget(3073)).isTrue();
            assertThat(kv.wouldExceedFlushBudget(8192)).isTrue();

            flushNTimes(kv, 2);
            assertThat(kv.wouldExceedFlushBudget(0)).isFalse();
            assertThat(kv.wouldExceedFlushBudget(1024)).isFalse();
            assertThat(kv.wouldExceedFlushBudget(1025)).isTrue();

            // Flush to L0=4: remainingSlots = 6 - 4 - 2 = 0 → L0 gate always rejects
            flushNTimes(kv, 2);
            assertThat(kv.wouldExceedFlushBudget(0)).isTrue();
        }
    }

    /**
     * Regression test for the JVM crash (exit 134 / SIGABRT) observed in CI for {@code
     * KvReplicaRestoreITCase}: after {@link RocksDBKv#close()} releases the native handle, late
     * callers from {@code DelayedWrite#onComplete} → {@code Replica#samplePressureForCompletion}
     * could still reach {@link RocksDBKv#currentPressure()} → {@code db.getProperty(...)} and touch
     * the disposed native handle. The pressure-sampling fence must turn those late calls into a
     * benign "no pressure" reading instead of a native crash.
     */
    @Test
    void testPressureQueriesAfterCloseAreSafe(@TempDir Path tempDir) throws Exception {
        File instanceBasePath = tempDir.toFile();
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(new Configuration(), instanceBasePath);
        ColumnFamilyOptions cfOpts = container.getColumnOptions();
        cfOpts.setLevel0SlowdownWritesTrigger(5);
        cfOpts.setLevel0FileNumCompactionTrigger(100);
        cfOpts.setLevel0StopWritesTrigger(100);

        RocksDBKvBuilder builder =
                new RocksDBKvBuilder(instanceBasePath, container, cfOpts)
                        .setFlussL0SlowdownTrigger(2);

        RocksDBKv kv = builder.build();
        // Produce a couple of L0 SSTs so a non-fenced query would otherwise observe non-zero L0.
        flushNTimes(kv, 2);

        kv.close();

        // Pressure sampling must degrade gracefully once the native handle is gone.
        assertThat(kv.currentPressure()).isEqualTo(0f);
    }
}
