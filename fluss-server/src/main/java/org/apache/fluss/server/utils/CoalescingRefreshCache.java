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
 * <p>Callers report that something changed via {@link #markDirty(boolean)}; whether that change was
 * urgent affects only how soon {@link #refreshIfNeeded} is willing to act on it -- a non-urgent
 * change waits for the caller-supplied {@code idle} signal (e.g. an empty work queue); an urgent
 * one is bounded by {@code urgentMaxDelayMs} regardless of that signal, so a safety- or
 * correctness-relevant change can't be starved by an owner that never goes idle. Either way, the
 * eventual recompute is always a full recompute from the supplied {@link Supplier}, never
 * incremental -- a wrong or missed {@code markDirty} call costs at worst one extra recompute, it
 * never leaves the published value permanently wrong.
 *
 * <p>{@link #markDirty}, {@link #refreshIfNeeded}, and {@link #update} must all be called from a
 * single owning thread; this class does no locking on that side, matching the assumption that
 * whoever computes the new value also holds whatever non-thread-safe source that computation reads
 * from. Only {@link #get()} is safe to call from any thread.
 *
 * @param <T> the type of the derived, immutable snapshot value
 */
@ThreadSafe
public final class CoalescingRefreshCache<T> {

    private final long urgentMaxDelayMs;

    private final Lock updateLock = new ReentrantLock();

    @GuardedBy("updateLock")
    private volatile T value;

    // only ever touched from the single owning thread -- see class javadoc.
    private boolean dirty;
    private boolean urgentDirty;
    private long lastRefreshTimeMs = System.currentTimeMillis();

    public CoalescingRefreshCache(T initialValue, long urgentMaxDelayMs) {
        this.value = initialValue;
        this.urgentMaxDelayMs = urgentMaxDelayMs;
    }

    /** Records that something changed. {@code urgent} affects only how soon it is acted on. */
    public void markDirty(boolean urgent) {
        dirty = true;
        if (urgent) {
            urgentDirty = true;
        }
    }

    /**
     * Recomputes and republishes the value via {@code compute}, if and only if the accumulated
     * changes since the last refresh warrant it right now.
     *
     * @param compute recomputes the value from scratch; only invoked if a refresh is due.
     * @param idle whether the owning thread's other work is currently caught up. A non-urgent
     *     change waits for this to be {@code true}; an urgent one is bounded by {@code
     *     urgentMaxDelayMs} regardless.
     */
    public void refreshIfNeeded(Supplier<T> compute, boolean idle) {
        if (!dirty) {
            return;
        }
        long now = System.currentTimeMillis();
        boolean due = idle || (urgentDirty && (now - lastRefreshTimeMs) >= urgentMaxDelayMs);
        if (due) {
            update(compute);
            dirty = false;
            urgentDirty = false;
        }
    }

    /** Unconditionally recomputes and republishes {@code compute}'s result. */
    public void update(Supplier<T> compute) {
        inLock(updateLock, () -> this.value = compute.get());
        lastRefreshTimeMs = System.currentTimeMillis();
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
