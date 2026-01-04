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
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.SequenceIDCounter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test class for {@link SegmentSequenceGenerator}. */
class SegmentSequenceGeneratorTest {

    private static final TablePath TABLE_PATH = new TablePath("test_db", "test_table");
    private static final int COLUMN_IDX = 0;
    private static final String COLUMN_NAME = "id";
    private static final long BATCH_SIZE = 100;

    private AtomicLong snapshotIdGenerator;
    private Configuration configuration;

    @BeforeEach
    void setUp() {
        snapshotIdGenerator = new AtomicLong(0);
        Map<String, String> map = new HashMap<>();
        map.put(ConfigOptions.TABLE_AUTO_INC_BATCH_SIZE.key(), String.valueOf(BATCH_SIZE));
        configuration = Configuration.fromMap(map);
    }

    @Test
    void testNextValBasicContinuousId() {
        SegmentSequenceGenerator generator =
                new SegmentSequenceGenerator(
                        TABLE_PATH,
                        COLUMN_IDX,
                        COLUMN_NAME,
                        new TestingSnapshotIDCounter(snapshotIdGenerator),
                        configuration);
        for (long i = 0; i < BATCH_SIZE; i++) {
            assertThat(generator.nextVal()).isEqualTo(i);
        }

        for (long i = BATCH_SIZE; i < 2 * BATCH_SIZE; i++) {
            assertThat(generator.nextVal()).isEqualTo(i);
        }
    }

    @Test
    void testMultiGenerator() {
        ConcurrentLinkedDeque<Long> linkedDeque = new ConcurrentLinkedDeque<>();

        for (int i = 0; i < 20; i++) {
            new Thread(
                            () -> {
                                SegmentSequenceGenerator generator =
                                        new SegmentSequenceGenerator(
                                                new TablePath("test_db", "table1"),
                                                COLUMN_IDX,
                                                COLUMN_NAME + "_table1",
                                                new TestingSnapshotIDCounter(snapshotIdGenerator),
                                                configuration);
                                for (int j = 0; j < 130; j++) {
                                    linkedDeque.add(generator.nextVal());
                                }
                            })
                    .start();
        }

        assertThat(linkedDeque.stream().mapToLong(Long::longValue).max().orElse(0))
                .isLessThanOrEqualTo(40 * BATCH_SIZE);
    }

    @Test
    void testFetchFailed() {
        SegmentSequenceGenerator generator =
                new SegmentSequenceGenerator(
                        new TablePath("test_db", "table1"),
                        COLUMN_IDX,
                        COLUMN_NAME + "_table1",
                        new TestingSnapshotIDCounter(snapshotIdGenerator, 2),
                        configuration);
        for (int j = 0; j < BATCH_SIZE; j++) {
            assertThat(generator.nextVal()).isEqualTo(j);
        }
        assertThatThrownBy(generator::nextVal)
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessage(
                        String.format(
                                "Failed to fetch auto-increment values, table_path=%s, column_idx=%d, column_name=%s.",
                                "test_db.table1", COLUMN_IDX, COLUMN_NAME + "_table1"));
    }

    private static class TestingSnapshotIDCounter implements SequenceIDCounter {

        private final AtomicLong snapshotIdGenerator;
        private int fetchTime;
        private final int failedTrigger;

        public TestingSnapshotIDCounter(AtomicLong snapshotIdGenerator) {
            this(snapshotIdGenerator, Integer.MAX_VALUE);
        }

        public TestingSnapshotIDCounter(AtomicLong snapshotIdGenerator, int failedTrigger) {
            this.snapshotIdGenerator = snapshotIdGenerator;
            fetchTime = 0;
            this.failedTrigger = failedTrigger;
        }

        @Override
        public long getAndIncrement() {
            return snapshotIdGenerator.getAndIncrement();
        }

        @Override
        public long getAndAdd(Long delta) {
            if (++fetchTime < failedTrigger) {
                return snapshotIdGenerator.getAndAdd(delta);
            }
            throw new RuntimeException("Failed to get snapshot ID");
        }
    }
}
