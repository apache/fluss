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

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.SequenceOverflowException;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test class for {@link BoundedSegmentSequenceGenerator}. */
class SegmentSequenceGeneratorTest {

    private static final TablePath TABLE_PATH = new TablePath("test_db", "test_table");
    private static final String COLUMN_NAME = "id";
    private static final long CACHE_SIZE = 100;

    private AtomicLong snapshotIdGenerator;

    @BeforeEach
    void setUp() {
        snapshotIdGenerator = new AtomicLong(0);
    }

    @Test
    void testNextValBasicContinuousId() {
        BoundedSegmentSequenceGenerator generator =
                new BoundedSegmentSequenceGenerator(
                        TABLE_PATH,
                        COLUMN_NAME,
                        new TestingSequenceIDCounter(snapshotIdGenerator),
                        CACHE_SIZE,
                        Long.MAX_VALUE);
        for (long i = 1; i <= CACHE_SIZE; i++) {
            assertThat(generator.nextVal()).isEqualTo(i);
        }

        for (long i = CACHE_SIZE + 1; i <= 2 * CACHE_SIZE; i++) {
            assertThat(generator.nextVal()).isEqualTo(i);
        }
    }

    @Test
    void testMultiGenerator() throws InterruptedException {
        ConcurrentLinkedDeque<Long> linkedDeque = new ConcurrentLinkedDeque<>();
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            Thread thread =
                    new Thread(
                            () -> {
                                BoundedSegmentSequenceGenerator generator =
                                        new BoundedSegmentSequenceGenerator(
                                                new TablePath("test_db", "table1"),
                                                COLUMN_NAME,
                                                new TestingSequenceIDCounter(snapshotIdGenerator),
                                                CACHE_SIZE,
                                                Long.MAX_VALUE);
                                for (int j = 0; j < 130; j++) {
                                    linkedDeque.add(generator.nextVal());
                                }
                            });
            threads.add(thread);
            thread.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        assertThat(linkedDeque.stream().mapToLong(Long::longValue).max().orElse(0))
                .isLessThanOrEqualTo(40 * CACHE_SIZE);
        assertThat(linkedDeque.stream().distinct().count()).isEqualTo(130 * 20);
    }

    @Test
    void testFetchFailed() {
        BoundedSegmentSequenceGenerator generator =
                new BoundedSegmentSequenceGenerator(
                        new TablePath("test_db", "table1"),
                        COLUMN_NAME,
                        new TestingSequenceIDCounter(snapshotIdGenerator, 2),
                        CACHE_SIZE,
                        Long.MAX_VALUE);
        for (int j = 1; j <= CACHE_SIZE; j++) {
            assertThat(generator.nextVal()).isEqualTo(j);
        }
        assertThatThrownBy(generator::nextVal)
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessage(
                        String.format(
                                "Failed to fetch auto-increment values, table_path=%s, column_name=%s.",
                                "test_db.table1", COLUMN_NAME));
    }

    @Test
    void testFetchIdOverFlow() {
        int initialValue = Integer.MAX_VALUE - 10;
        snapshotIdGenerator = new AtomicLong(initialValue);
        BoundedSegmentSequenceGenerator generator =
                new BoundedSegmentSequenceGenerator(
                        new TablePath("test_db", "table1"),
                        COLUMN_NAME,
                        new TestingSequenceIDCounter(snapshotIdGenerator),
                        CACHE_SIZE,
                        Integer.MAX_VALUE);

        int lastValue = 0;
        for (int j = 1; j <= 10; j++) {
            lastValue = (int) generator.nextVal();
            assertThat(lastValue).isEqualTo(initialValue + j);
        }
        assertThat(lastValue).isEqualTo(Integer.MAX_VALUE);

        assertThatThrownBy(generator::nextVal)
                .isInstanceOf(SequenceOverflowException.class)
                .hasMessage(
                        String.format(
                                "Reached maximum value of sequence \"<%s>\" (%d).",
                                COLUMN_NAME, Integer.MAX_VALUE));
    }
}
