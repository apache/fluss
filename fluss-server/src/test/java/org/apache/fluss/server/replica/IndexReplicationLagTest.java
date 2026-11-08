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

package org.apache.fluss.server.replica;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for independent visibility and recovery progress observations. */
class IndexReplicationLagTest {
    @Test
    void syncProgressDoesNotHideAllIndexStalls() {
        ManualClock clock = new ManualClock();
        IndexReplicationLag lag =
                new IndexReplicationLag(new TableBucket(1L, 0), clock, true, 10, 0, 0);
        clock.advanceTime(100, TimeUnit.MILLISECONDS);
        lag.update(20, 5, 0);
        assertThat(lag.lagOffsets()).isEqualTo(20);
        assertThat(lag.syncLagOffsets()).isEqualTo(15);
        assertThat(lag.noProgressTimeMs()).isEqualTo(100);
        clock.advanceTime(100, TimeUnit.MILLISECONDS);
        lag.update(30, 20, 0);
        assertThat(lag.noProgressTimeMs()).isEqualTo(200);
        lag.update(30, 20, 10);
        assertThat(lag.noProgressTimeMs()).isZero();
        clock.advanceTime(50, TimeUnit.MILLISECONDS);
        lag.update(30, 30, 10);
        assertThat(lag.syncLagOffsets()).isZero();
        assertThat(lag.noProgressTimeMs()).isEqualTo(50);
        lag.update(30, 30, 30);
        clock.advanceTime(100, TimeUnit.MILLISECONDS);
        assertThat(lag.noProgressTimeMs()).isZero();
        lag.update(40, 30, 30);
        assertThat(lag.noProgressTimeMs()).isZero();
    }

    @Test
    void syncStallsSurviveAdvancingAllIndexFrontierAndStaleNotifications() {
        ManualClock clock = new ManualClock();
        IndexReplicationLag lag =
                new IndexReplicationLag(new TableBucket(1L, 0), clock, true, 20, 10, 0);
        clock.advanceTime(100, TimeUnit.MILLISECONDS);
        lag.update(20, 10, 5);
        assertThat(lag.noProgressTimeMs()).isEqualTo(100);
        lag.update(10, 5, 0);
        assertThat(lag.lagOffsets()).isEqualTo(15);
        assertThat(lag.syncLagOffsets()).isEqualTo(10);
        assertThat(lag.noProgressTimeMs()).isEqualTo(100);
        lag.update(20, 20, 10);
        assertThat(lag.noProgressTimeMs()).isZero();
    }

    @Test
    void asyncOnlySourceHasNoSyncLagOrClock() {
        ManualClock clock = new ManualClock();
        IndexReplicationLag lag =
                new IndexReplicationLag(new TableBucket(1L, 0), clock, false, 10, -1, 0);
        clock.advanceTime(100, TimeUnit.MILLISECONDS);
        assertThat(lag.syncLagOffsets()).isZero();
        assertThat(lag.noProgressTimeMs()).isEqualTo(100);
        lag.update(10, -1, 10);
        clock.advanceTime(100, TimeUnit.MILLISECONDS);
        assertThat(lag.noProgressTimeMs()).isZero();
    }
}
