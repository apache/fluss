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

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Tracks the configured disk capacity reserved by historical lookupers.
 *
 * <p>This manager accounts for configured cache capacity, not the bytes currently present on disk.
 * A reservation belongs to a table's current or creating lookuper. Once that lookuper is removed
 * from the cache mapping, its reservation is released immediately even if active requests keep the
 * retired lookuper alive for a short time.
 *
 * <p>All mutable state is protected by this instance's monitor. The following invariants therefore
 * hold after every operation:
 *
 * <ul>
 *   <li>Each table ID has at most one current reservation.
 *   <li>{@code reservedBytes} is the sum of the reservations in {@code reservationsByTableId}.
 *   <li>{@code 0 <= reservedBytes <= maxBytes}.
 * </ul>
 */
@ThreadSafe
final class HistoricalLookupCacheBudgetManager {

    // The configured limit is immutable in this version, so readers do not need synchronization.
    private final long maxBytes;

    // Contains only reservations that currently count against the budget. Retired lookupers are
    // deliberately absent even when they are still serving an already acquired lookup.
    @GuardedBy("this")
    private final Map<Long, Reservation> reservationsByTableId = new HashMap<>();

    @GuardedBy("this")
    private long reservedBytes;

    /** Creates a budget manager with the given positive capacity limit. */
    HistoricalLookupCacheBudgetManager(long maxBytes) {
        checkArgument(maxBytes > 0, "maxBytes must be greater than 0.");
        this.maxBytes = maxBytes;
    }

    /**
     * Tries to reserve capacity for a new table lookuper.
     *
     * <p>The check and reservation insertion are one atomic operation. A request fails when the
     * table already owns a reservation or the remaining budget is too small. Subtraction is used
     * for the capacity check to avoid overflowing {@code reservedBytes + bytes}.
     *
     * @return the new reservation, or {@code null} if the table already has a reservation or there
     *     is insufficient remaining capacity
     */
    synchronized @Nullable Reservation tryReserve(long tableId, long bytes) {
        checkArgument(bytes > 0, "bytes must be greater than 0.");
        if (reservationsByTableId.containsKey(tableId) || bytes > maxBytes - reservedBytes) {
            return null;
        }

        Reservation reservation = new Reservation(tableId, bytes);
        reservationsByTableId.put(tableId, reservation);
        reservedBytes = Math.addExact(reservedBytes, bytes);
        return reservation;
    }

    /**
     * Atomically replaces a table's current reservation for a replacement lookuper.
     *
     * <p>The supplied reservation must still be the table's current reservation. A stale object can
     * be observed after expiration, LRU eviction, or another replacement and must not overwrite the
     * newer reservation. If the identity or capacity check fails, the old reservation remains
     * unchanged.
     *
     * @return the replacement reservation, or {@code null} if the supplied reservation is no longer
     *     current or the replacement does not fit within the capacity limit
     */
    synchronized @Nullable Reservation tryReplace(Reservation oldReservation, long newBytes) {
        checkArgument(newBytes > 0, "newBytes must be greater than 0.");
        Reservation currentReservation = reservationsByTableId.get(oldReservation.getTableId());
        if (currentReservation != oldReservation) {
            return null;
        }

        long reservedWithoutOld = Math.subtractExact(reservedBytes, oldReservation.getBytes());
        if (newBytes > maxBytes - reservedWithoutOld) {
            return null;
        }

        Reservation newReservation = new Reservation(oldReservation.getTableId(), newBytes);
        reservationsByTableId.put(oldReservation.getTableId(), newReservation);
        reservedBytes = Math.addExact(reservedWithoutOld, newBytes);
        return newReservation;
    }

    /**
     * Releases a reservation if it is still the table's current reservation.
     *
     * <p>Removal listeners, creation cleanup, and delayed retired-lookuper callbacks can all
     * attempt a release. Comparing the reservation object identity makes those calls idempotent and
     * prevents an old lookuper from releasing its replacement's capacity.
     */
    synchronized void release(Reservation reservation) {
        Reservation currentReservation = reservationsByTableId.get(reservation.getTableId());
        if (currentReservation != reservation) {
            return;
        }

        reservationsByTableId.remove(reservation.getTableId());
        reservedBytes = Math.subtractExact(reservedBytes, reservation.getBytes());
    }

    /** Returns the capacity currently reserved by current and creating lookupers. */
    synchronized long reservedBytes() {
        return reservedBytes;
    }

    /** Returns the configured capacity limit. */
    long maxBytes() {
        return maxBytes;
    }

    /**
     * An immutable capacity reservation for one cached lookuper.
     *
     * <p>Each reserve or replace operation creates a new instance. The budget manager compares
     * object identity so delayed callbacks carrying an older instance are harmless.
     */
    static final class Reservation {
        private final long tableId;
        private final long bytes;

        private Reservation(long tableId, long bytes) {
            this.tableId = tableId;
            this.bytes = bytes;
        }

        long getTableId() {
            return tableId;
        }

        long getBytes() {
            return bytes;
        }
    }
}
