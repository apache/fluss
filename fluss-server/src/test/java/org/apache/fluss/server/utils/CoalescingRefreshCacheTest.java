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
    void testInitialValueIsReturnedUntouched() {
        CoalescingRefreshCache<Integer> cache =
                new CoalescingRefreshCache<>(0, URGENT_MAX_DELAY_MS);
        assertThat(cache.get()).isEqualTo(0);
        assertThat(cache.isDirty()).isFalse();
    }

    @Test
    void testRefreshIfNeededIsNoOpWhenNotDirty() {
        CoalescingRefreshCache<Integer> cache =
                new CoalescingRefreshCache<>(0, URGENT_MAX_DELAY_MS);
        AtomicInteger computeCalls = new AtomicInteger();

        cache.refreshIfNeeded(() -> computeCalls.incrementAndGet(), true);

        // compute() must not even be invoked -- there was nothing to refresh.
        assertThat(computeCalls.get()).isZero();
        assertThat(cache.get()).isEqualTo(0);
    }

    @Test
    void testNonUrgentChangeWaitsUntilIdle() {
        CoalescingRefreshCache<Integer> cache =
                new CoalescingRefreshCache<>(0, URGENT_MAX_DELAY_MS);
        AtomicInteger source = new AtomicInteger(1);
        cache.markDirty(false);

        cache.refreshIfNeeded(source::get, false); // not idle -- must wait
        assertThat(cache.get()).isEqualTo(0);
        assertThat(cache.isDirty()).isTrue();

        cache.refreshIfNeeded(source::get, true); // idle -- now it should recompute
        assertThat(cache.get()).isEqualTo(1);
        assertThat(cache.isDirty()).isFalse();
    }

    @Test
    void testUrgentChangeIsBoundedByMaxDelayEvenIfNeverIdle() throws InterruptedException {
        CoalescingRefreshCache<Integer> cache =
                new CoalescingRefreshCache<>(0, URGENT_MAX_DELAY_MS);
        AtomicInteger source = new AtomicInteger(1);
        cache.update(source::get); // establishes a real lastRefreshTimeMs baseline
        source.set(2);

        cache.markDirty(true);
        cache.refreshIfNeeded(source::get, false); // urgent, but delay not yet elapsed
        assertThat(cache.get()).isEqualTo(1);

        Thread.sleep(URGENT_MAX_DELAY_MS + 50);

        cache.refreshIfNeeded(source::get, false); // still not idle, but the bound is up
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
            cache.refreshIfNeeded(
                    () -> {
                        computeCalls.incrementAndGet();
                        return source.incrementAndGet();
                    },
                    false); // never idle -- simulates a burst arriving while the queue is busy
        }
        assertThat(computeCalls.get()).isZero();

        cache.refreshIfNeeded(
                () -> {
                    computeCalls.incrementAndGet();
                    return source.incrementAndGet();
                },
                true); // idle now -- exactly one recompute for the whole burst

        assertThat(computeCalls.get()).isEqualTo(1);
        assertThat(cache.get()).isEqualTo(1);
    }

    @Test
    void testUpdateBypassesPolicyButDoesNotClearDirtyFlags() {
        CoalescingRefreshCache<Integer> cache =
                new CoalescingRefreshCache<>(0, URGENT_MAX_DELAY_MS);
        cache.markDirty(true);

        cache.update(() -> 7);

        assertThat(cache.get()).isEqualTo(7);
        // update() is an unconditional bypass of the policy, not a substitute for it -- it must
        // not silently clear flags that refreshIfNeeded is responsible for managing.
        assertThat(cache.isDirty()).isTrue();
        assertThat(cache.isUrgentlyDirty()).isTrue();
    }
}
