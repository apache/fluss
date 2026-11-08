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
import org.apache.fluss.utils.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/** Leader-scoped observation of the SYNC visibility and all-index recovery frontiers. */
final class IndexReplicationLag {
    private static final Logger LOG = LoggerFactory.getLogger(IndexReplicationLag.class);
    private static final long STALL_LOG_INTERVAL_NS = TimeUnit.MINUTES.toNanos(1);

    private final TableBucket sourceBucket;
    private final Clock clock;
    private final Frontier all;
    private final Frontier sync;
    private final boolean hasSyncIndexes;
    private long highWatermark;
    private long lastStallLogNs;

    IndexReplicationLag(
            TableBucket sourceBucket,
            Clock clock,
            boolean hasSyncIndexes,
            long highWatermark,
            long syncOffset,
            long allOffset) {
        this.sourceBucket = sourceBucket;
        this.clock = clock;
        this.hasSyncIndexes = hasSyncIndexes;
        this.all = new Frontier(allOffset);
        this.sync = new Frontier(syncOffset);
        this.lastStallLogNs = clock.nanoseconds();
        update(highWatermark, syncOffset, allOffset);
    }

    synchronized void update(long highWatermark, long syncOffset, long allOffset) {
        // Notifications may race; an older sample must not move an observed frontier backwards.
        this.highWatermark = Math.max(this.highWatermark, highWatermark);
        long now = clock.nanoseconds();
        all.update(allOffset, this.highWatermark, now);
        if (hasSyncIndexes) {
            sync.update(syncOffset, this.highWatermark, now);
        }
    }

    synchronized long lagOffsets() {
        return Math.max(0L, highWatermark - all.pushedOffset);
    }

    synchronized long syncLagOffsets() {
        return hasSyncIndexes ? Math.max(0L, highWatermark - sync.pushedOffset) : 0L;
    }

    synchronized long noProgressTimeMs() {
        return TimeUnit.NANOSECONDS.toMillis(maxNoProgressNs(clock.nanoseconds()));
    }

    // Called by the existing replica maintenance scan, never by a gauge supplier.
    void logIfStalled() {
        long hw;
        long syncOffset;
        long allOffset;
        long allAge;
        long syncAge;
        synchronized (this) {
            long now = clock.nanoseconds();
            if (maxNoProgressNs(now) < STALL_LOG_INTERVAL_NS
                    || now - lastStallLogNs < STALL_LOG_INTERVAL_NS) {
                return;
            }
            lastStallLogNs = now;
            hw = highWatermark;
            syncOffset = sync.pushedOffset;
            allOffset = all.pushedOffset;
            allAge = TimeUnit.NANOSECONDS.toMillis(all.noProgressNs(now));
            syncAge = hasSyncIndexes ? TimeUnit.NANOSECONDS.toMillis(sync.noProgressNs(now)) : 0L;
        }
        LOG.warn(
                "Index replication stalled for {}: HW={}, SYNC={}, ALL={}, "
                        + "SYNC no-progress time={} ms, ALL no-progress time={} ms",
                sourceBucket,
                hw,
                syncOffset,
                allOffset,
                syncAge,
                allAge);
    }

    private long maxNoProgressNs(long now) {
        return Math.max(all.noProgressNs(now), hasSyncIndexes ? sync.noProgressNs(now) : 0L);
    }

    private static final class Frontier {
        private long pushedOffset;
        private long noProgressStartNs;
        private boolean behind;

        private Frontier(long pushedOffset) {
            this.pushedOffset = pushedOffset;
        }

        private void update(long offset, long highWatermark, long now) {
            boolean advanced = offset > pushedOffset;
            pushedOffset = Math.max(pushedOffset, offset);
            if (pushedOffset >= highWatermark) {
                behind = false;
            } else {
                if (!behind || advanced) {
                    noProgressStartNs = now;
                }
                behind = true;
            }
        }

        private long noProgressNs(long now) {
            return behind ? Math.max(0L, now - noProgressStartNs) : 0L;
        }
    }
}
