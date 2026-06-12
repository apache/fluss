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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.FlushOptions;

import java.io.File;
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

            // L0 = 1, still below flussL0SlowdownTrigger (2), should still be 0.
            assertThat(kv.currentPressure()).isEqualTo(0f);
        }
    }

    /**
     * Verifies the full backpressure logic across the proactive throttle signal and the predictive
     * flush gate. Configuration: flussTrigger=2, rocksdbSlowdownTrigger=5.
     */
    @Test
    void testPressureCurveAndFlushGate(@TempDir Path tempDir) throws Exception {
        File instanceBasePath = tempDir.toFile();
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(new Configuration(), instanceBasePath);
        ColumnFamilyOptions cfOpts = container.getColumnOptions();
        // Low slowdown trigger for testing; very high compaction/stop triggers to prevent
        // RocksDB from interfering (auto-compacting or stalling) during the test.
        cfOpts.setLevel0SlowdownWritesTrigger(5);
        cfOpts.setLevel0FileNumCompactionTrigger(100);
        cfOpts.setLevel0StopWritesTrigger(100);

        RocksDBKvBuilder builder =
                new RocksDBKvBuilder(instanceBasePath, container, cfOpts)
                        .setFlussL0SlowdownTrigger(2);

        try (RocksDBKv kv = builder.build()) {
            // --- L0 = 0: no pressure, flush gate open ---
            assertThat(kv.currentPressure()).isEqualTo(0f);
            assertThat(kv.wouldExceedSlowdownTriggerOnFlush()).isFalse();

            // Flush to L0 = 2 (reaches flussTrigger). p = (2-2)/(5-2) = 0.
            flushNTimes(kv, 2);
            assertThat(kv.currentPressure()).isEqualTo(0f);
            assertThat(kv.wouldExceedSlowdownTriggerOnFlush()).isFalse();

            // Flush to L0 = 3: proactive throttle signal kicks in. p = (3-2)/(5-2) = 1/3.
            flushNTimes(kv, 1);
            float p = kv.currentPressure();
            assertThat(p).isCloseTo(1f / 3f, org.assertj.core.data.Offset.offset(0.01f));
            assertThat(kv.wouldExceedSlowdownTriggerOnFlush()).isFalse();

            // Flush to L0 = 4: predictive flush gate now closes (4 + 1 == 5 == slowdownTrigger).
            // p = min(L0 - flussTrigger, window-1) / window = min(2, 2) / 3 = 2/3.
            flushNTimes(kv, 1);
            p = kv.currentPressure();
            assertThat(p).isCloseTo(2f / 3f, org.assertj.core.data.Offset.offset(0.01f));
            assertThat(kv.wouldExceedSlowdownTriggerOnFlush()).isTrue();

            // Flush to L0 = 5: storage engine has reached its slowdown trigger. Pressure stays
            // clamped strictly below 1 and the flush gate remains closed.
            flushNTimes(kv, 1);
            p = kv.currentPressure();
            assertThat(p).isLessThan(1f);
            assertThat(p).isCloseTo(2f / 3f, org.assertj.core.data.Offset.offset(0.01f));
            assertThat(kv.wouldExceedSlowdownTriggerOnFlush()).isTrue();
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
    }

    /**
     * Regression test for the JVM crash (exit 134 / SIGABRT) observed in CI for {@code
     * KvReplicaRestoreITCase}: after {@link RocksDBKv#close()} releases the native handle, late
     * callers from {@code DelayedWrite#onComplete} → {@code Replica#samplePressureForCompletion}
     * could still reach {@link RocksDBKv#currentPressure()} → {@code db.getProperty(...)} and touch
     * the disposed native handle. The fence inside {@code currentL0FileCount} must turn those late
     * calls into a benign "no pressure" reading instead of a native crash.
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

        // Both pressure paths must degrade gracefully once the native handle is gone.
        assertThat(kv.currentPressure()).isEqualTo(0f);
        assertThat(kv.wouldExceedSlowdownTriggerOnFlush()).isFalse();
    }
}
