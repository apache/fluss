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

package org.apache.fluss.server.index;

import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class IndexReplicationRetryStateTest {
    @Test
    void concurrentFailuresShareOneBackoffPeriod() {
        ManualClock clock = new ManualClock();
        IndexReplicationRetryState retry =
                new IndexReplicationRetryState(new ExponentialBackoff(100, 2, 10000, 0), clock);
        retry.onFailure();
        clock.advanceTime(50, TimeUnit.MILLISECONDS);
        retry.onFailure();
        assertThat(retry.remainingDelayNanos()).isEqualTo(TimeUnit.MILLISECONDS.toNanos(50));
        clock.advanceTime(50, TimeUnit.MILLISECONDS);
        retry.onFailure();
        assertThat(retry.remainingDelayNanos()).isEqualTo(TimeUnit.MILLISECONDS.toNanos(200));
    }

    @Test
    void yieldingPreservesFailureAgeAndBackoffUntilProgress() {
        ManualClock clock = new ManualClock();
        IndexReplicationRetryState retry =
                new IndexReplicationRetryState(new ExponentialBackoff(1000, 2, 10000, 0), clock);
        retry.onFailure();
        clock.advanceTime(1, TimeUnit.SECONDS);
        retry.onFailure();
        clock.advanceTime(2, TimeUnit.SECONDS);
        retry.onFailure();
        assertThat(retry.shouldReleaseResources(TimeUnit.SECONDS.toNanos(10))).isFalse();
        clock.advanceTime(4, TimeUnit.SECONDS);
        retry.onFailure();
        assertThat(retry.shouldReleaseResources(TimeUnit.SECONDS.toNanos(10))).isTrue();
        retry.onResourcesReleased();
        assertThat(retry.remainingDelayNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(8));
        clock.advanceTime(8, TimeUnit.SECONDS);
        retry.onFailure();
        assertThat(retry.remainingDelayNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(10));
        assertThat(retry.shouldReleaseResources(TimeUnit.SECONDS.toNanos(10))).isTrue();
        retry.reset();
        assertThat(retry.hasFailure()).isFalse();
        assertThat(retry.remainingDelayNanos()).isZero();
        assertThat(retry.shouldReleaseResources(TimeUnit.SECONDS.toNanos(10))).isFalse();
    }
}
