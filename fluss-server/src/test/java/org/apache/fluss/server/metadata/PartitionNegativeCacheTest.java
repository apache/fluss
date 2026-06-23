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

package org.apache.fluss.server.metadata;

import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartitionNegativeCache}. */
class PartitionNegativeCacheTest {

    private static final Duration TTL = Duration.ofMinutes(10);
    private static final long TTL_MS = TTL.toMillis();

    @Test
    void testMarkAndQueryNonExistent() {
        ManualClock clock = new ManualClock();
        PartitionNegativeCache cache = new PartitionNegativeCache(TTL, clock);

        // Initially, partition is not known as non-existent
        assertThat(cache.isKnownNonExistent(100L)).isFalse();

        // Mark it as non-existent
        cache.markNonExistent(100L);

        // Now it should be known as non-existent
        assertThat(cache.isKnownNonExistent(100L)).isTrue();
        assertThat(cache.size()).isEqualTo(1);
    }

    @Test
    void testUnknownPartitionReturnsFalse() {
        ManualClock clock = new ManualClock();
        PartitionNegativeCache cache = new PartitionNegativeCache(TTL, clock);

        // Never marked partition should return false
        assertThat(cache.isKnownNonExistent(999L)).isFalse();
        assertThat(cache.isKnownNonExistent(0L)).isFalse();
        assertThat(cache.isKnownNonExistent(-1L)).isFalse();
    }

    @Test
    void testMultiplePartitions() {
        ManualClock clock = new ManualClock();
        PartitionNegativeCache cache = new PartitionNegativeCache(TTL, clock);

        cache.markNonExistent(1L);
        cache.markNonExistent(2L);
        cache.markNonExistent(3L);

        assertThat(cache.isKnownNonExistent(1L)).isTrue();
        assertThat(cache.isKnownNonExistent(2L)).isTrue();
        assertThat(cache.isKnownNonExistent(3L)).isTrue();
        assertThat(cache.isKnownNonExistent(4L)).isFalse();
        assertThat(cache.size()).isEqualTo(3);
    }

    @Test
    void testTtlExpiration() {
        ManualClock clock = new ManualClock();
        PartitionNegativeCache cache = new PartitionNegativeCache(TTL, clock);

        cache.markNonExistent(100L);
        assertThat(cache.isKnownNonExistent(100L)).isTrue();

        // Advance time past TTL from last access time (which is 0)
        clock.advanceTime(TTL_MS + 1, TimeUnit.MILLISECONDS);
        assertThat(cache.isKnownNonExistent(100L)).isFalse();

        // Entry should be removed from cache
        assertThat(cache.size()).isEqualTo(0);
    }

    @Test
    void testNotExpiredAtExactBoundary() {
        ManualClock clock = new ManualClock();
        PartitionNegativeCache cache = new PartitionNegativeCache(TTL, clock);

        cache.markNonExistent(100L);

        // At exactly TTL_MS, condition is "now - lastAccess > ttl" (strict >), so not expired
        clock.advanceTime(TTL_MS, TimeUnit.MILLISECONDS);
        assertThat(cache.isKnownNonExistent(100L)).isTrue();
    }

    @Test
    void testAccessTimeRefreshKeepsEntryAlive() {
        ManualClock clock = new ManualClock();
        PartitionNegativeCache cache = new PartitionNegativeCache(TTL, clock);

        cache.markNonExistent(100L);

        // Access at t = TTL - 1 (within TTL window), refreshes access time to TTL - 1
        clock.advanceTime(TTL_MS - 1, TimeUnit.MILLISECONDS);
        assertThat(cache.isKnownNonExistent(100L)).isTrue();

        // Now TTL is measured from last access (TTL_MS - 1).
        // At t = 2*TTL - 2, elapsed = TTL - 1, NOT > TTL, still valid.
        clock.advanceTime(TTL_MS - 1, TimeUnit.MILLISECONDS);
        assertThat(cache.isKnownNonExistent(100L)).isTrue(); // refreshes to 2*TTL - 2

        // Access again at t = 3*TTL - 3 to prove indefinite survival
        clock.advanceTime(TTL_MS - 1, TimeUnit.MILLISECONDS);
        assertThat(cache.isKnownNonExistent(100L)).isTrue(); // refreshes to 3*TTL - 3

        // Now stop accessing. Advance past TTL from last access (3*TTL - 3).
        clock.advanceTime(TTL_MS + 1, TimeUnit.MILLISECONDS);
        assertThat(cache.isKnownNonExistent(100L)).isFalse();
    }

    @Test
    void testClear() {
        ManualClock clock = new ManualClock();
        PartitionNegativeCache cache = new PartitionNegativeCache(TTL, clock);

        cache.markNonExistent(1L);
        cache.markNonExistent(2L);
        cache.markNonExistent(3L);
        assertThat(cache.size()).isEqualTo(3);

        cache.clear();
        assertThat(cache.size()).isEqualTo(0);
        assertThat(cache.isKnownNonExistent(1L)).isFalse();
        assertThat(cache.isKnownNonExistent(2L)).isFalse();
        assertThat(cache.isKnownNonExistent(3L)).isFalse();
    }

    @Test
    void testRemarkAfterExpiration() {
        ManualClock clock = new ManualClock();
        PartitionNegativeCache cache = new PartitionNegativeCache(TTL, clock);

        cache.markNonExistent(100L);
        assertThat(cache.isKnownNonExistent(100L)).isTrue();

        // Expire the entry
        clock.advanceTime(TTL_MS + 1, TimeUnit.MILLISECONDS);
        assertThat(cache.isKnownNonExistent(100L)).isFalse();

        // Re-mark at new time should work
        cache.markNonExistent(100L);
        assertThat(cache.isKnownNonExistent(100L)).isTrue();

        // The new entry expires TTL after re-mark time (TTL_MS + 1)
        clock.advanceTime(TTL_MS + 1, TimeUnit.MILLISECONDS);
        assertThat(cache.isKnownNonExistent(100L)).isFalse();
    }

    @Test
    void testIndependentExpiration() {
        ManualClock clock = new ManualClock();
        PartitionNegativeCache cache = new PartitionNegativeCache(TTL, clock);

        // Mark partition 1 at t=0
        cache.markNonExistent(1L);

        // Mark partition 2 at t=5min
        clock.advanceTime(TTL_MS / 2, TimeUnit.MILLISECONDS);
        cache.markNonExistent(2L);

        // At t=TTL+1: partition 1 should expire, partition 2 should still be alive
        clock.advanceTime(TTL_MS / 2 + 1, TimeUnit.MILLISECONDS);
        assertThat(cache.isKnownNonExistent(1L)).isFalse();
        assertThat(cache.isKnownNonExistent(2L)).isTrue(); // refreshes access time to TTL_MS+1

        // Partition 2's access time was refreshed to TTL_MS+1 above.
        // To expire it, advance past TTL from that refreshed time.
        clock.advanceTime(TTL_MS + 1, TimeUnit.MILLISECONDS);
        assertThat(cache.isKnownNonExistent(2L)).isFalse();
    }
}
