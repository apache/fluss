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

import org.apache.fluss.server.replica.HistoricalLookupCacheBudgetManager.Reservation;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HistoricalLookupCacheBudgetManager}. */
class HistoricalLookupCacheBudgetManagerTest {

    @Test
    void testReserveAndReleaseWithinLimit() {
        HistoricalLookupCacheBudgetManager manager = new HistoricalLookupCacheBudgetManager(10);

        Reservation first = manager.tryReserve(1, 4);
        Reservation second = manager.tryReserve(2, 6);
        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(manager.reservedBytes()).isEqualTo(10);
        assertThat(manager.tryReserve(3, 1)).isNull();
        assertThat(manager.tryReserve(1, 1)).isNull();

        manager.release(first);
        manager.release(first);
        assertThat(manager.reservedBytes()).isEqualTo(6);
        Reservation third = manager.tryReserve(3, 4);
        assertThat(third).isNotNull();
        assertThat(manager.reservedBytes()).isEqualTo(10);
    }

    @Test
    void testReplaceReservationAtomically() {
        HistoricalLookupCacheBudgetManager manager = new HistoricalLookupCacheBudgetManager(12);
        Reservation oldReservation = manager.tryReserve(1, 4);
        Reservation otherReservation = manager.tryReserve(2, 6);
        assertThat(oldReservation).isNotNull();
        assertThat(otherReservation).isNotNull();

        Reservation replacement = manager.tryReplace(oldReservation, 5);
        assertThat(replacement).isNotNull();
        assertThat(replacement.getTableId()).isEqualTo(1);
        assertThat(replacement.getBytes()).isEqualTo(5);
        assertThat(manager.reservedBytes()).isEqualTo(11);

        // Releasing the retired reservation must not affect the replacement.
        manager.release(oldReservation);
        assertThat(manager.reservedBytes()).isEqualTo(11);
        assertThat(manager.tryReplace(oldReservation, 1)).isNull();

        // A failed replacement leaves the current reservation unchanged.
        assertThat(manager.tryReplace(replacement, 7)).isNull();
        assertThat(manager.reservedBytes()).isEqualTo(11);
        manager.release(replacement);
        assertThat(manager.reservedBytes()).isEqualTo(6);
    }

    @Test
    void testReducedLimitAppliesToSubsequentReservations() {
        HistoricalLookupCacheBudgetManager manager = new HistoricalLookupCacheBudgetManager(10);
        Reservation first = manager.tryReserve(1, 6);
        Reservation second = manager.tryReserve(2, 4);
        assertThat(first).isNotNull();
        assertThat(second).isNotNull();

        // Shrinking is lazy: existing reservations remain even though their total exceeds the new
        // limit.
        manager.updateGlobalLimit(7);
        assertThat(manager.maxBytes()).isEqualTo(7);
        assertThat(manager.reservedBytes()).isEqualTo(10);

        // Subsequent reservations use the reduced limit and succeed only after capacity is freed.
        assertThat(manager.tryReserve(3, 1)).isNull();

        manager.release(second);
        assertThat(manager.tryReserve(3, 1)).isNotNull();
    }
}
