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

package org.apache.fluss.server.kv.prewrite;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KvPreWriteBufferMemoryManager}. */
class KvPreWriteBufferMemoryManagerTest {

    @Test
    void testClientPressureAndResumeHysteresis() {
        KvPreWriteBufferMemoryManager manager = new KvPreWriteBufferMemoryManager(100, 80);

        assertThat(manager.highWatermarkBytes()).isEqualTo(100);
        assertThat(manager.lowWatermarkBytes()).isEqualTo(80);
        assertThat(manager.tryReserve(100)).isTrue();
        assertThat(manager.isUnderPressure()).isTrue();
        assertThat(manager.tryReserve(1)).isFalse();

        manager.release(19);
        assertThat(manager.usedBytes()).isEqualTo(81);
        assertThat(manager.isUnderPressure()).isTrue();
        assertThat(manager.tryReserve(1)).isFalse();

        manager.release(1);
        assertThat(manager.isUnderPressure()).isFalse();
        assertThat(manager.tryReserve(20)).isTrue();
        assertThat(manager.usedBytes()).isEqualTo(100);
        assertThat(manager.isUnderPressure()).isTrue();
    }

    @Test
    void testClientReservationFailureDoesNotChangeUsage() {
        KvPreWriteBufferMemoryManager manager = new KvPreWriteBufferMemoryManager(100, 80);

        assertThat(manager.tryReserve(101)).isFalse();
        assertThat(manager.usedBytes()).isZero();
        assertThat(manager.isUnderPressure()).isFalse();
    }

    @Test
    void testRejectedLargeReservationEntersPressureOnlyAboveLowWatermark() {
        KvPreWriteBufferMemoryManager manager = new KvPreWriteBufferMemoryManager(100, 80);

        assertThat(manager.tryReserve(80)).isTrue();
        assertThat(manager.tryReserve(21)).isFalse();
        assertThat(manager.isUnderPressure()).isFalse();

        assertThat(manager.tryReserve(1)).isTrue();
        assertThat(manager.tryReserve(20)).isFalse();
        assertThat(manager.isUnderPressure()).isTrue();

        manager.release(1);
        assertThat(manager.usedBytes()).isEqualTo(80);
        assertThat(manager.isUnderPressure()).isFalse();
    }

    @Test
    void testInvalidReservationAndRelease() {
        assertThatThrownBy(() -> new KvPreWriteBufferMemoryManager(0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KvPreWriteBufferMemoryManager(100, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KvPreWriteBufferMemoryManager(100, 100))
                .isInstanceOf(IllegalArgumentException.class);

        KvPreWriteBufferMemoryManager manager = new KvPreWriteBufferMemoryManager(100, 80);
        assertThatThrownBy(() -> manager.tryReserve(-1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> manager.release(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> manager.release(1)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testConcurrentReservationsDoNotExceedLimit() throws Exception {
        final int threadCount = 8;
        final KvPreWriteBufferMemoryManager manager =
                new KvPreWriteBufferMemoryManager(10_000, 8_000);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        try {
            List<Callable<Integer>> reservationTasks = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                reservationTasks.add(
                        () -> {
                            int reservations = 0;
                            while (manager.tryReserve(1)) {
                                reservations++;
                            }
                            return reservations;
                        });
            }

            List<Future<Integer>> results = executor.invokeAll(reservationTasks);
            int successfulReservations = 0;
            for (Future<Integer> result : results) {
                successfulReservations += result.get();
            }

            assertThat(successfulReservations).isEqualTo(10_000);
            assertThat(manager.usedBytes()).isEqualTo(10_000);
            assertThat(manager.isUnderPressure()).isTrue();
        } finally {
            executor.shutdownNow();
        }
    }
}
