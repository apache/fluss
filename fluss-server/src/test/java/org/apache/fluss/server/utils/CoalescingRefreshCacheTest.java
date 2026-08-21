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

package org.apache.fluss.server.utils;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CoalescingRefreshCache}, exercised with a trivial {@code Integer} value so the
 * coalescing/urgency mechanics are verified independently of any real caller (e.g. {@code
 * CoordinatorHealthCache}).
 */
class CoalescingRefreshCacheTest {

    private static final long URGENT_MAX_DELAY_MS = 100;

    @Test
    void testFreshCacheStartsDirtyWithTheSeedValue() {
        CoalescingRefreshCache<Integer> cache =
                new CoalescingRefreshCache<>(0, URGENT_MAX_DELAY_MS);
        assertThat(cache.get()).isEqualTo(0);
        // hasn't computed a real value yet -- starts dirty so a warm-up can force it through.
        assertThat(cache.isDirty()).isTrue();
    }

    @Test
    void testRefreshIsNoOpWhenNotDirtyEvenIfForced() {
        CoalescingRefreshCache<Integer> cache =
                new CoalescingRefreshCache<>(0, URGENT_MAX_DELAY_MS);
        cache.refresh(() -> 1, true); // clears the initial dirty state
        AtomicInteger computeCalls = new AtomicInteger();

        cache.refresh(() -> computeCalls.incrementAndGet(), true);

        // force overrides the timing gate, never the dirty gate -- nothing changed, so
        // compute() must not even be invoked, force notwithstanding.
        assertThat(computeCalls.get()).isZero();
        assertThat(cache.get()).isEqualTo(1);
    }

    @Test
    void testNonUrgentChangeWaitsForForce() {
        CoalescingRefreshCache<Integer> cache =
                new CoalescingRefreshCache<>(0, URGENT_MAX_DELAY_MS);
        AtomicInteger source = new AtomicInteger(1);
        cache.markDirty(false);

        cache.refresh(source::get, false); // not forced -- must wait
        assertThat(cache.get()).isEqualTo(0);
        assertThat(cache.isDirty()).isTrue();

        cache.refresh(source::get, true); // forced -- now it should recompute
        assertThat(cache.get()).isEqualTo(1);
        assertThat(cache.isDirty()).isFalse();
    }

    @Test
    void testUrgentChangeIsBoundedByMaxDelayEvenIfNeverForced() throws InterruptedException {
        CoalescingRefreshCache<Integer> cache =
                new CoalescingRefreshCache<>(0, URGENT_MAX_DELAY_MS);
        AtomicInteger source = new AtomicInteger(1);
        cache.refresh(source::get, true); // establishes a real lastRefreshTimeMs baseline
        source.set(2);

        cache.markDirty(true);
        cache.refresh(source::get, false); // urgent, but delay not yet elapsed
        assertThat(cache.get()).isEqualTo(1);

        Thread.sleep(URGENT_MAX_DELAY_MS + 50);

        cache.refresh(source::get, false); // still not forced, but the bound is up
        assertThat(cache.get()).isEqualTo(2);
        assertThat(cache.isDirty()).isFalse();
        assertThat(cache.isUrgentlyDirty()).isFalse();
    }

    @Test
    void testManyMarkDirtyCallsCoalesceIntoOneRecompute() {
        CoalescingRefreshCache<Integer> cache =
                new CoalescingRefreshCache<>(0, URGENT_MAX_DELAY_MS);
        AtomicInteger computeCalls = new AtomicInteger();
        AtomicInteger source = new AtomicInteger();

        for (int i = 0; i < 50; i++) {
            cache.markDirty(false);
            cache.refresh(
                    () -> {
                        computeCalls.incrementAndGet();
                        return source.incrementAndGet();
                    },
                    false); // never forced -- simulates a burst arriving while the queue is busy
        }
        assertThat(computeCalls.get()).isZero();

        cache.refresh(
                () -> {
                    computeCalls.incrementAndGet();
                    return source.incrementAndGet();
                },
                true); // forced now -- exactly one recompute for the whole burst

        assertThat(computeCalls.get()).isEqualTo(1);
        assertThat(cache.get()).isEqualTo(1);
    }

    @Test
    void testForceClearsDirtyFlagsAfterRecomputing() {
        CoalescingRefreshCache<Integer> cache =
                new CoalescingRefreshCache<>(0, URGENT_MAX_DELAY_MS);
        cache.markDirty(true);

        cache.refresh(() -> 7, true);

        assertThat(cache.get()).isEqualTo(7);
        // a forced refresh still recomputed the current truth, so nothing is left
        // unreflected -- force participates fully in the dirty-tracking system, it doesn't
        // sidestep it.
        assertThat(cache.isDirty()).isFalse();
        assertThat(cache.isUrgentlyDirty()).isFalse();
    }
}
