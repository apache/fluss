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

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.clock.Clock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Unifies the write-throttling gates that can delay a bucket from being sent: KV backpressure and
 * disk-write backoff. Both gates are per-{@link TableBucket}.
 *
 * <p>The two reasons are stored and installed separately because their semantics genuinely differ:
 *
 * <ul>
 *   <li><b>KV backpressure</b> uses wall-clock deadlines, is <i>latest-wins</i> (a fresher pressure
 *       signal may shorten or clear the window), and derives its delay quadratically from the
 *       pressure value.
 *   <li><b>Disk-write backoff</b> uses monotonic deadlines (immune to wall-clock shifts), is
 *       <i>never-shortened</i> under concurrency, and derives its delay from an exponential backoff
 *       keyed on the batch retry count.
 * </ul>
 *
 * <p>What is unified is the <i>read</i> side: a bucket cannot become ready until every active gate
 * has cleared, so {@link #remainingDelayMs(TableBucket)} returns the maximum remaining delay across
 * reasons and is the single source consulted by both {@code ready()} and {@code drain()}. Adding a
 * future throttling reason should only touch this class, not those two paths.
 */
@Internal
final class WriteThrottleController {

    // KV backpressure: wall-clock expiry, latest-wins. Accessed strictly by key on hot paths
    // (get / put / remove); the container is sized for lock-striped O(1) updates without any
    // whole-map snapshot cost.
    private final ConcurrentMap<TableBucket, Long> kvThrottleExpiryMs = new ConcurrentHashMap<>();
    private final long maxThrottleMs;

    // Disk protection is independent of KV pressure, whose responses may shorten or clear a
    // throttle. Deadlines use monotonic time and are shared by all queues targeting a bucket.
    private final ConcurrentMap<TableBucket, Long> diskBackoffDeadlineNanos =
            new ConcurrentHashMap<>();
    private final ExponentialBackoff diskBackoff;
    // Only the sender thread performs periodic sweeps.
    private long lastDiskSweepNanos;

    // Latest Cluster snapshot fed to the metadata-driven throttle sweep. Identity equality against
    // this reference short-circuits the sweep when metadata hasn't changed.
    private volatile Cluster lastClusterRef = Cluster.empty();

    private final Clock clock;
    // Shared with the owning accumulator: a late RPC callback must not retain state after final
    // resource destruction.
    private final AtomicBoolean resourcesDestroyed;

    WriteThrottleController(
            long maxThrottleMs,
            ExponentialBackoff diskBackoff,
            Clock clock,
            AtomicBoolean resourcesDestroyed) {
        this.maxThrottleMs = maxThrottleMs;
        this.diskBackoff = checkNotNull(diskBackoff);
        this.clock = clock;
        this.resourcesDestroyed = resourcesDestroyed;
        this.lastDiskSweepNanos = clock.nanoseconds();
    }

    // ------------------------------------------------------------------------
    // Unified read side: the single home for "how long until this bucket may send".
    // ------------------------------------------------------------------------

    /**
     * Remaining delay before the bucket may be sent, i.e. the latest deadline across all active
     * gates. Both reasons express their remainder in milliseconds from now, so the maximum is well
     * defined even though they track different clocks internally. Expired entries are evicted
     * lazily as a side effect.
     */
    long remainingDelayMs(TableBucket tableBucket) {
        return Math.max(kvRemainingMs(tableBucket), diskRemainingMs(tableBucket));
    }

    /** Whether any gate currently blocks the bucket from being sent. */
    boolean isGated(TableBucket tableBucket) {
        return remainingDelayMs(tableBucket) > 0;
    }

    // ------------------------------------------------------------------------
    // KV backpressure (wall-clock millis, latest-wins, quadratic delay).
    // ------------------------------------------------------------------------

    /**
     * Update the throttle state for a bucket based on the received pressure signal.
     *
     * <p>The delay grows quadratically with pressure: {@code delay = maxThrottleMs * p^2}, where
     * {@code p ∈ [0, 1)}. This provides meaningful throttling across the full ramp-up window while
     * remaining gentle at low pressure.
     *
     * @param tableBucket the bucket to update
     * @param pressure value in {@code [0, 1)} on the wire; {@code 0} means recovered, positive
     *     values trigger a throttle window. {@code 1.0f} is reserved as the internal hard-rejection
     *     value (never sent by the server): the Sender passes it when the server rejected the write
     *     outright, and it installs the full {@link #maxThrottleMs} window directly.
     */
    void updateKvPressure(TableBucket tableBucket, float pressure) {
        if (pressure >= 1f) {
            // Hard rejection: stall the bucket for the full max throttle window, bypassing the
            // quadratic curve to avoid long-to-float rounding.
            kvThrottleExpiryMs.put(tableBucket, clock.milliseconds() + maxThrottleMs);
            return;
        }
        if (pressure > 0f) {
            long delay = (long) (maxThrottleMs * pressure * pressure);
            if (delay > 0) {
                kvThrottleExpiryMs.put(tableBucket, clock.milliseconds() + delay);
                return;
            }
        }
        // Recovered or below the meaningful resolution: remove throttle.
        // Note: in production, recovery relies on the last throttle window expiring naturally
        // (server stops sending the pressure field once p reaches 0). This branch exists as
        // defensive completeness and is exercised by unit tests.
        kvThrottleExpiryMs.remove(tableBucket);
    }

    boolean isKvThrottled(TableBucket tableBucket) {
        return kvRemainingMs(tableBucket) > 0;
    }

    private long kvRemainingMs(TableBucket tableBucket) {
        Long expiry = kvThrottleExpiryMs.get(tableBucket);
        if (expiry == null) {
            return 0;
        }
        long remainingMs = expiry - clock.milliseconds();
        if (remainingMs > 0) {
            return remainingMs;
        }
        // Expired — evict to prevent map leak.
        kvThrottleExpiryMs.remove(tableBucket, expiry);
        return 0;
    }

    /**
     * Evict throttle entries whose buckets no longer exist in the given cluster (leader unknown,
     * partition dropped, table dropped).
     *
     * <p>Invoked on every Sender loop with the current cluster snapshot. The identity short-circuit
     * makes this an O(1) no-op when metadata hasn't changed, so the actual O(N) walk only runs once
     * per real metadata refresh.
     */
    void maybeEvictStaleThrottles(Cluster cluster) {
        if (cluster == lastClusterRef) {
            return;
        }
        lastClusterRef = cluster;
        if (kvThrottleExpiryMs.isEmpty()) {
            return;
        }
        kvThrottleExpiryMs.keySet().removeIf(tb -> cluster.leaderFor(tb) == null);
    }

    // ------------------------------------------------------------------------
    // Disk protection (monotonic nanos, never-shorten, exponential backoff).
    // ------------------------------------------------------------------------

    /**
     * Installs disk backoff for the bucket before the batch retry count is increased by
     * re-enqueueing. Concurrent installs never shorten an existing deadline.
     *
     * @param tableBucket the bucket that was rejected by disk protection
     * @param attempts the batch retry count used to derive the exponential backoff
     * @return the effective remaining backoff in milliseconds
     */
    long backoffAfterDiskWrite(TableBucket tableBucket, int attempts) {
        if (resourcesDestroyed.get()) {
            return 0;
        }
        long now = clock.nanoseconds();
        long delayNanos =
                TimeUnit.MILLISECONDS.toNanos(Math.max(1L, diskBackoff.backoff(attempts)));
        Long deadline =
                diskBackoffDeadlineNanos.compute(
                        tableBucket,
                        (bucket, previous) ->
                                previous != null && previous - now > delayNanos
                                        ? previous
                                        : now + delayNanos);
        // A late RPC callback must not retain state after final resource destruction.
        if (resourcesDestroyed.get()) {
            diskBackoffDeadlineNanos.remove(tableBucket, deadline);
            return 0;
        }
        return nanosToCeilMillis(deadline - now);
    }

    /** Returns the remaining disk backoff without changing its deadline. */
    long diskRemainingMs(TableBucket tableBucket) {
        Long deadline = diskBackoffDeadlineNanos.get(tableBucket);
        if (deadline == null) {
            return 0;
        }
        long remainingNanos = deadline - clock.nanoseconds();
        if (remainingNanos > 0) {
            return nanosToCeilMillis(remainingNanos);
        }
        diskBackoffDeadlineNanos.remove(tableBucket, deadline);
        return 0;
    }

    /** Reclaims expired entries even when their queues no longer contain any batches. */
    void maybeEvictExpiredDiskBackoffs() {
        if (diskBackoffDeadlineNanos.isEmpty()) {
            return;
        }
        long now = clock.nanoseconds();
        if (now - lastDiskSweepNanos < TimeUnit.SECONDS.toNanos(1)) {
            return;
        }
        lastDiskSweepNanos = now;
        diskBackoffDeadlineNanos.forEach(
                (bucket, deadline) -> {
                    if (deadline - now <= 0) {
                        diskBackoffDeadlineNanos.remove(bucket, deadline);
                    }
                });
    }

    /** Drops all disk-backoff state; mirrors the accumulator's resource destruction. */
    void clearDiskBackoffs() {
        diskBackoffDeadlineNanos.clear();
    }

    @VisibleForTesting
    int diskBackoffCount() {
        return diskBackoffDeadlineNanos.size();
    }

    private static long nanosToCeilMillis(long nanos) {
        return 1 + (nanos - 1) / TimeUnit.MILLISECONDS.toNanos(1);
    }
}
