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

import org.apache.fluss.annotation.VisibleForTesting;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/**
 * A copy-on-write cache of a derived, immutable value, recomputed on demand rather than on a fixed
 * schedule, and coalesced so that many changes reported in a short window trigger at most one
 * recompute.
 *
 * <p>Two independent questions decide whether {@link #refresh} is due, and they are answered by two
 * independent mechanisms:
 *
 * <ul>
 *   <li><b>Rate</b> -- how fast a change that <em>was</em> reported (via {@link #markDirty}) gets
 *       reflected. Urgent changes are bounded by {@code urgentMaxDelayMs}; everything else by the
 *       looser {@code normalMaxDelayMs}. This is purely about not recomputing more often than
 *       necessary once something is known to have changed.
 *   <li><b>Coverage</b> -- an absolute ceiling, {@code safetyNetMaxDelayMs}, that fires regardless
 *       of {@code dirty} at all. A caller that forgets to call {@link #markDirty} for some code
 *       path still can't cause unbounded staleness -- worst case, it's caught within {@code
 *       safetyNetMaxDelayMs}. This is what actually bounds correctness independent of how complete
 *       the caller's instrumentation is.
 * </ul>
 *
 * <p>Conflating these into one mechanism is a trap: gating the safety-net ceiling on {@code dirty}
 * would defeat its entire purpose (it exists precisely for the case where {@code dirty} was never
 * set), while making the rate thresholds unconditional would mean recomputing on every call even
 * when nothing changed -- the "quiet caller costs one boolean check" property this class is built
 * around. Every {@code due} check inspects all three conditions independently:
 *
 * <pre>{@code
 * due = (urgentDirty && elapsed >= urgentMaxDelayMs)
 *     || (dirty && elapsed >= normalMaxDelayMs)
 *     || (elapsed >= safetyNetMaxDelayMs);
 * }</pre>
 *
 * <p>Either way, the eventual recompute is always a full recompute from the supplied {@link
 * Supplier}, never incremental -- a wrong or missed {@code markDirty} call costs at worst one extra
 * recompute (or, at the safety-net bound, a bounded wait), it never leaves the published value
 * permanently wrong.
 *
 * <p>{@link #markDirty} and {@link #refresh} must both be called from a single owning thread; this
 * class does no locking on that side, matching the assumption that whoever computes the new value
 * also holds whatever non-thread-safe source that computation reads from. Only {@link #get()} is
 * safe to call from any thread.
 *
 * @param <T> the type of the derived, immutable snapshot value
 */
@ThreadSafe
public final class CoalescingRefreshCache<T> {

    private final long urgentMaxDelayMs;
    private final long normalMaxDelayMs;
    private final long safetyNetMaxDelayMs;

    private final Lock updateLock = new ReentrantLock();

    @GuardedBy("updateLock")
    private volatile T value;

    // only ever touched from the single owning thread -- see class javadoc.
    // starts true: a freshly constructed cache hasn't computed a real value yet.
    private boolean dirty = true;
    private boolean urgentDirty;
    private long lastRefreshTimeMs;

    public CoalescingRefreshCache(
            T initialValue,
            long urgentMaxDelayMs,
            long normalMaxDelayMs,
            long safetyNetMaxDelayMs) {
        this.value = initialValue;
        this.urgentMaxDelayMs = urgentMaxDelayMs;
        this.normalMaxDelayMs = normalMaxDelayMs;
        this.safetyNetMaxDelayMs = safetyNetMaxDelayMs;
        // seeded in the past (not "now") so that the very first refresh() call, whenever the
        // caller happens to make it, is unconditionally due -- otherwise a warm-up call made
        // shortly after construction would see near-zero elapsed time and be a no-op under all
        // three clauses, silently defeating the caller's warm-up.
        this.lastRefreshTimeMs = System.currentTimeMillis() - safetyNetMaxDelayMs;
    }

    /** Records that something changed. {@code urgent} affects only how soon it is acted on. */
    public void markDirty(boolean urgent) {
        dirty = true;
        if (urgent) {
            urgentDirty = true;
        }
    }

    /**
     * Recomputes and republishes the value via {@code compute}, if and only if it is due: either a
     * reported change has waited long enough for its urgency tier, or the unconditional safety-net
     * bound has elapsed regardless of whether anything was ever reported. See the class javadoc for
     * why both mechanisms exist independently.
     *
     * @param compute recomputes the value from scratch; only invoked if a refresh is due.
     */
    public void refresh(Supplier<T> compute) {
        long now = System.currentTimeMillis();
        long elapsed = now - lastRefreshTimeMs;
        boolean due =
                (urgentDirty && elapsed >= urgentMaxDelayMs)
                        || (dirty && elapsed >= normalMaxDelayMs)
                        || (elapsed >= safetyNetMaxDelayMs);
        if (due) {
            inLock(updateLock, () -> this.value = compute.get());
            lastRefreshTimeMs = now;
            dirty = false;
            urgentDirty = false;
        }
    }

    /** Returns the most recently published value. Safe to call from any thread. */
    public T get() {
        return value;
    }

    @VisibleForTesting
    public boolean isDirty() {
        return dirty;
    }

    @VisibleForTesting
    public boolean isUrgentlyDirty() {
        return urgentDirty;
    }
}
