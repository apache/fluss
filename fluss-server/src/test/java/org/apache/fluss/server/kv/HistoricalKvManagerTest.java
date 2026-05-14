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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RateLimiter;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HistoricalKvManager}. */
class HistoricalKvManagerTest {

    @TempDir Path tempDir;

    private ManualClock clock;
    private HistoricalKvManager manager;

    @BeforeEach
    void setUp() {
        clock = new ManualClock();
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KV_HISTORICAL_IDLE_TIMEOUT, Duration.ofMinutes(30));
        RateLimiter rateLimiter = KvManager.getDefaultRateLimiter();
        manager = new HistoricalKvManager(conf, rateLimiter, clock);
    }

    @AfterEach
    void tearDown() {
        manager.close();
    }

    @Test
    void testPutGetAndDelete() throws IOException {
        TableBucket bucket = new TableBucket(1L, 100L, 0);
        File dataDir = newBucketDir("bucket-0");

        // get before put returns null
        assertThat(manager.get(bucket, key("k1"))).isNull();

        // put then get
        manager.put(bucket, dataDir, key("k1"), value("v1"));
        assertThat(manager.get(bucket, key("k1"))).isEqualTo(value("v1"));

        // delete then get returns null
        manager.delete(bucket, dataDir, key("k1"));
        assertThat(manager.get(bucket, key("k1"))).isNull();
    }

    @Test
    void testMultipleBucketsIsolated() throws IOException {
        TableBucket bucket0 = new TableBucket(1L, 100L, 0);
        TableBucket bucket1 = new TableBucket(1L, 100L, 1);
        File dir0 = newBucketDir("bucket-0");
        File dir1 = newBucketDir("bucket-1");

        manager.put(bucket0, dir0, key("k1"), value("v-from-0"));
        manager.put(bucket1, dir1, key("k1"), value("v-from-1"));

        assertThat(manager.get(bucket0, key("k1"))).isEqualTo(value("v-from-0"));
        assertThat(manager.get(bucket1, key("k1"))).isEqualTo(value("v-from-1"));
    }

    @Test
    void testIdleTimeoutCleanup() throws IOException {
        TableBucket bucket = new TableBucket(1L, 100L, 0);
        File dataDir = newBucketDir("bucket-0");

        ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();
        manager.startCleanupScheduler(scheduler);

        manager.put(bucket, dataDir, key("k1"), value("v1"));
        assertThat(manager.openInstanceCount()).isEqualTo(1);

        // Trigger before timeout — nothing should be cleaned
        clock.advanceTime(Duration.ofMinutes(10));
        scheduler.triggerPeriodicScheduledTasks();
        assertThat(manager.openInstanceCount()).isEqualTo(1);
        assertThat(manager.get(bucket, key("k1"))).isEqualTo(value("v1"));

        // Access resets the idle timer — advance 20 min (not timed out from last access)
        clock.advanceTime(Duration.ofMinutes(20));
        scheduler.triggerPeriodicScheduledTasks();
        assertThat(manager.get(bucket, key("k1"))).isEqualTo(value("v1"));

        // Advance past timeout from last access — instance should be evicted
        clock.advanceTime(Duration.ofMinutes(31));
        scheduler.triggerPeriodicScheduledTasks();
        assertThat(manager.openInstanceCount()).isEqualTo(0);
        assertThat(manager.get(bucket, key("k1"))).isNull();
    }

    @Test
    void testDropInstanceAndClose() throws IOException {
        TableBucket bucket0 = new TableBucket(1L, 100L, 0);
        TableBucket bucket1 = new TableBucket(1L, 100L, 1);
        File dir0 = newBucketDir("bucket-0");
        File dir1 = newBucketDir("bucket-1");

        manager.put(bucket0, dir0, key("k1"), value("v1"));
        manager.put(bucket1, dir1, key("k2"), value("v2"));
        assertThat(manager.openInstanceCount()).isEqualTo(2);

        // drop a single instance
        manager.dropInstance(bucket0);
        assertThat(manager.openInstanceCount()).isEqualTo(1);

        // close cleans up the remaining instances
        manager.close();
        assertThat(manager.openInstanceCount()).isEqualTo(0);
    }

    private File newBucketDir(String name) {
        File dir = tempDir.resolve(name).toFile();
        dir.mkdirs();
        return dir;
    }

    private static byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] value(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
