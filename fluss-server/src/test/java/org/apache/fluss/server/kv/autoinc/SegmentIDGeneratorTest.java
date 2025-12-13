/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.autoinc;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.SequenceIDCounter;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link SegmentIDGenerator}. */
class SegmentIDGeneratorTest {

    private static final TablePath TABLE_PATH = new TablePath("test_db", "test_table");
    private static final int SCHEMA_ID = 1;
    private static final int COLUMN_IDX = 0;
    private static final String COLUMN_NAME = "id";
    private static final long BATCH_SIZE = 100;
    private static final double LOW_WATER_MARK_RATIO = 0.5;

    private static class TestingSnapshotIDCounter implements SequenceIDCounter {

        private AtomicLong snapshotIdGenerator;

        public TestingSnapshotIDCounter(AtomicLong snapshotIdGenerator) {
            this.snapshotIdGenerator = snapshotIdGenerator;
        }

        @Override
        public long getAndIncrement() {
            return snapshotIdGenerator.getAndIncrement();
        }

        @Override
        public long getAndAdd(Long delta) {
            return snapshotIdGenerator.getAndAdd(delta);
        }
    }

    @Test
    void testNextVal_BasicContinuousId() throws InterruptedException {
        AtomicLong snapshotIdGenerator = new AtomicLong(0);

        Map<String, String> map = new HashMap<>();
        map.put(
                ConfigOptions.TABLE_AUTO_INC_PREFETCH_LOW_WATER_MARK_RATIO.key(),
                String.valueOf(LOW_WATER_MARK_RATIO));
        map.put(ConfigOptions.TABLE_AUTO_INC_BATCH_SIZE.key(), String.valueOf(BATCH_SIZE));
        Configuration configuration = Configuration.fromMap(map);
        FlussScheduler scheduler = new FlussScheduler(1);
        scheduler.startup();

        SegmentIDGenerator generator =
                new SegmentIDGenerator(
                        TABLE_PATH,
                        SCHEMA_ID,
                        COLUMN_IDX,
                        COLUMN_NAME,
                        new TestingSnapshotIDCounter(snapshotIdGenerator),
                        scheduler,
                        configuration);
        // first segment id generator
        for (long i = 0; i < BATCH_SIZE; i++) {
            assertThat(generator.nextVal()).isEqualTo(i);
        }

        // global snapshot id generator incremented twice
        Thread.sleep(100);
        assertThat(snapshotIdGenerator.get()).isEqualTo(2 * BATCH_SIZE);

        // second segment id generator
        for (long i = BATCH_SIZE; i < 2 * BATCH_SIZE; i++) {
            assertThat(generator.nextVal()).isEqualTo(i);
        }

        // global snapshot id generator incremented three times
        Thread.sleep(100);
        assertThat(snapshotIdGenerator.get()).isEqualTo(3 * BATCH_SIZE);
    }

    @Test
    void testMultiGenerator() throws InterruptedException {
        Map<String, String> map = new HashMap<>();
        map.put(
                ConfigOptions.TABLE_AUTO_INC_PREFETCH_LOW_WATER_MARK_RATIO.key(),
                String.valueOf(LOW_WATER_MARK_RATIO));
        map.put(ConfigOptions.TABLE_AUTO_INC_BATCH_SIZE.key(), String.valueOf(BATCH_SIZE));
        Configuration configuration = Configuration.fromMap(map);

        AtomicLong globalSnapshotIdGenerator = new AtomicLong(0);
        FlussScheduler scheduler = new FlussScheduler(2); // 增加调度器线程数适配多实例
        scheduler.startup();

        TablePath tablePath = new TablePath("test_db", "table1");

        SegmentIDGenerator generator1 =
                new SegmentIDGenerator(
                        tablePath,
                        SCHEMA_ID,
                        COLUMN_IDX,
                        COLUMN_NAME,
                        new TestingSnapshotIDCounter(globalSnapshotIdGenerator),
                        scheduler,
                        configuration);
        SegmentIDGenerator generator2 =
                new SegmentIDGenerator(
                        tablePath,
                        SCHEMA_ID,
                        COLUMN_IDX,
                        COLUMN_NAME,
                        new TestingSnapshotIDCounter(globalSnapshotIdGenerator),
                        scheduler,
                        configuration);
        List<Long> ids = Collections.synchronizedList(new ArrayList<>());
        Thread t1 =
                new Thread(
                        () -> {
                            for (int i = 0; i < 2 * BATCH_SIZE; i++) {
                                ids.add(generator1.nextVal());
                            }
                        });
        Thread t2 =
                new Thread(
                        () -> {
                            for (int i = 0; i < 2 * BATCH_SIZE; i++) {
                                ids.add(generator2.nextVal());
                            }
                        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        // wait for prefetch
        Thread.sleep(100);

        assertThat(globalSnapshotIdGenerator.get()).isEqualTo(6 * BATCH_SIZE);
        assertThat(ids.size()).isEqualTo(4 * BATCH_SIZE);
        assertThat(new HashSet<>(ids).size()).isEqualTo(4 * BATCH_SIZE);
        scheduler.shutdown();
    }
}
