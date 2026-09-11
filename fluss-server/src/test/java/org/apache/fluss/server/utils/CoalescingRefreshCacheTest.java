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
 * coalescing/urgency/coverage mechanics are verified independently of any real caller (e.g. {@code
 * CoordinatorHealthCache}).
 */
class CoalescingRefreshCacheTest {

    private static final long URGENT_MAX_DELAY_MS = 50;
    private static final long NORMAL_MAX_DELAY_MS = 150;
    private static final long SAFETY_NET_MAX_DELAY_MS = 400;

    @Test
    void testFreshCacheStartsDirtyWithTheSeedValue() {
        CoalescingRefreshCache<Integer> cache = newCache(0);
        assertThat(cache.get()).isEqualTo(0);
        // hasn't computed a real value yet -- starts dirty so a warm-up can go through.
        assertThat(cache.isDirty()).isTrue();
    }

    @Test
    void testFirstRefreshIsAlwaysDueRegardlessOfElapsed() {
        CoalescingRefreshCache<Integer> cache = newCache(0);
        AtomicInteger computeCalls = new AtomicInteger();

        // called immediately after construction -- elapsed time is near zero, yet the very
        // first refresh() must still go through: a caller's warm-up call must never be a
        // silent no-op just because it happened to run right after construction.
        cache.refresh(() -> computeCalls.incrementAndGet());

        assertThat(computeCalls.get()).isEqualTo(1);
        assertThat(cache.isDirty()).isFalse();
    }

    @Test
    void testRefreshIsNoOpWhenNothingChangedAndNoBoundHasElapsed() {
        CoalescingRefreshCache<Integer> cache = newCache(0);
        cache.refresh(() -> 1); // warm-up: due unconditionally on the first call
        AtomicInteger computeCalls = new AtomicInteger();

        cache.refresh(() -> computeCalls.incrementAndGet());

        // called again immediately: nothing marked dirty, and no bound (urgent/normal/safety
        // net) has elapsed -- compute() must not even be invoked.
        assertThat(computeCalls.get()).isZero();
        assertThat(cache.get()).isEqualTo(1);
    }

    @Test
    void testNonUrgentChangeFiresAtNormalMaxDelayNotBefore() throws InterruptedException {
        CoalescingRefreshCache<Integer> cache = newCache(0);
        AtomicInteger source = new AtomicInteger(1);
        cache.refresh(source::get); // establishes a real lastRefreshTimeMs baseline
        source.set(2);
        cache.markDirty(false);

        cache.refresh(source::get); // normal delay not yet elapsed -- must wait
        assertThat(cache.get()).isEqualTo(1);
        assertThat(cache.isDirty()).isTrue();

        Thread.sleep(NORMAL_MAX_DELAY_MS + 50);

        cache.refresh(source::get); // normal bound is now up
        assertThat(cache.get()).isEqualTo(2);
        assertThat(cache.isDirty()).isFalse();
    }

    @Test
    void testUrgentChangeFiresAtUrgentMaxDelayNotBefore() throws InterruptedException {
        CoalescingRefreshCache<Integer> cache = newCache(0);
        AtomicInteger source = new AtomicInteger(1);
        cache.refresh(source::get); // establishes a real lastRefreshTimeMs baseline
        source.set(2);
        cache.markDirty(true);

        cache.refresh(source::get); // urgent, but delay not yet elapsed
        assertThat(cache.get()).isEqualTo(1);

        Thread.sleep(URGENT_MAX_DELAY_MS + 20);

        cache.refresh(source::get); // urgent bound is now up, well before the normal one
        assertThat(cache.get()).isEqualTo(2);
        assertThat(cache.isDirty()).isFalse();
        assertThat(cache.isUrgentlyDirty()).isFalse();
    }

    @Test
    void testSafetyNetFiresEvenWithoutAnyMarkDirtyCall() throws InterruptedException {
        CoalescingRefreshCache<Integer> cache = newCache(0);
        AtomicInteger source = new AtomicInteger(1);
        cache.refresh(source::get); // establishes a real lastRefreshTimeMs baseline
        source.set(2);
        // deliberately never call markDirty -- proves the coverage guarantee is independent of
        // whether a caller remembered to report the change at all.

        Thread.sleep(SAFETY_NET_MAX_DELAY_MS + 50);

        cache.refresh(source::get);
        assertThat(cache.get()).isEqualTo(2);
    }

    @Test
    void testManyMarkDirtyCallsCoalesceIntoOneRecompute() throws InterruptedException {
        CoalescingRefreshCache<Integer> cache = newCache(0);
        cache.refresh(() -> 0); // warm-up
        AtomicInteger computeCalls = new AtomicInteger();
        AtomicInteger source = new AtomicInteger();

        for (int i = 0; i < 50; i++) {
            cache.markDirty(false);
            cache.refresh(
                    () -> {
                        computeCalls.incrementAndGet();
                        return source.incrementAndGet();
                    }); // each call lands well within normalMaxDelayMs -- none is due yet
        }
        assertThat(computeCalls.get()).isZero();

        Thread.sleep(NORMAL_MAX_DELAY_MS + 50);
        cache.refresh(
                () -> {
                    computeCalls.incrementAndGet();
                    return source.incrementAndGet();
                }); // now due -- exactly one recompute for the whole burst

        assertThat(computeCalls.get()).isEqualTo(1);
        assertThat(cache.get()).isEqualTo(1);
    }

    @Test
    void testDirtyFlagsClearAfterADueRecompute() {
        CoalescingRefreshCache<Integer> cache = newCache(0);
        cache.markDirty(true);

        cache.refresh(() -> 7); // first call is unconditionally due

        assertThat(cache.get()).isEqualTo(7);
        assertThat(cache.isDirty()).isFalse();
        assertThat(cache.isUrgentlyDirty()).isFalse();
    }

    private static CoalescingRefreshCache<Integer> newCache(int initialValue) {
        return new CoalescingRefreshCache<>(
                initialValue, URGENT_MAX_DELAY_MS, NORMAL_MAX_DELAY_MS, SAFETY_NET_MAX_DELAY_MS);
    }
}
