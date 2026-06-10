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
import org.apache.fluss.exception.StorageBackpressureException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.FlushOptions;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void testCheckBackpressure_l0PropertyReadable(@TempDir Path tempDir) throws Exception {
        File instanceBasePath = tempDir.toFile();
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(new Configuration(), instanceBasePath);
        ColumnFamilyOptions cfOpts = container.getColumnOptions();

        RocksDBKvBuilder builder =
                new RocksDBKvBuilder(instanceBasePath, container, cfOpts)
                        .setFlussL0SlowdownTrigger(2);

        try (RocksDBKv kv = builder.build()) {
            // Initially 0 L0 files, should return 0 pressure and NOT throw.
            float pressure = kv.checkBackpressure();
            assertThat(pressure).isEqualTo(0f);

            // Write + flush to produce exactly 1 L0 SST.
            kv.put(new byte[] {1}, new byte[] {1});
            try (FlushOptions flushOpts = new FlushOptions()) {
                flushOpts.setWaitForFlush(true);
                kv.db.flush(flushOpts);
            }

            // L0 = 1, still below flussL0SlowdownTrigger (2), should still be 0.
            pressure = kv.checkBackpressure();
            assertThat(pressure).isEqualTo(0f);
        }
    }

    /**
     * Verifies the full backpressure logic (proactive throttle signal plus hard rejection) using
     * low thresholds.
     *
     * <p>Configuration: flussTrigger=2, rocksdbSlowdownTrigger=5. We generate L0 files by
     * repeatedly flushing and verify the pressure transitions.
     */
    @Test
    void testCheckBackpressure_throttleAndHardRejection(@TempDir Path tempDir) throws Exception {
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
            // --- L0 = 0: no pressure ---
            assertThat(kv.checkBackpressure()).isEqualTo(0f);

            // Flush to L0 = 2 (reaches flussTrigger)
            flushNTimes(kv, 2);
            float p = kv.checkBackpressure();
            // p = (2-2)/(5-2) = 0. Still boundary, equals trigger.
            assertThat(p).isEqualTo(0f);

            // Flush to L0 = 3: proactive throttle signal kicks in
            flushNTimes(kv, 1);
            p = kv.checkBackpressure();
            // p = (3-2)/(5-2) = 1/3
            assertThat(p).isGreaterThan(0f).isLessThan(1f);
            assertThat(p).isCloseTo(1f / 3f, org.assertj.core.data.Offset.offset(0.01f));

            // Flush to L0 = 4: stronger throttle signal
            flushNTimes(kv, 1);
            p = kv.checkBackpressure();
            // p = (4-2)/(5-2) = 2/3, but clamped to (window-1)/window = 2/3 (same here)
            assertThat(p).isCloseTo(2f / 3f, org.assertj.core.data.Offset.offset(0.01f));

            // Flush to L0 = 5: storage engine reaches its slowdown trigger -> hard rejection
            flushNTimes(kv, 1);
            assertThatThrownBy(kv::checkBackpressure)
                    .isInstanceOf(StorageBackpressureException.class)
                    .hasMessageContaining("slowdown trigger");
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
}
