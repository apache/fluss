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
 * urgent affects only how soon {@link #refresh} is willing to act on it. The {@code force} flag on
 * {@link #refresh} lets a caller override the timing question directly -- typically because it
 * knows something this class doesn't, e.g. that its own work queue is currently empty, so a
 * non-urgent change is now welcome to be acted on. An urgent change doesn't need the caller's help:
 * it's bounded by {@code urgentMaxDelayMs} regardless of {@code force}, so a safety- or
 * correctness-relevant change can't be starved by an owner that never passes {@code force=true}.
 *
 * <p>Crucially, {@code force} only ever overrides the <em>timing</em> gate, never the {@code dirty}
 * gate: {@link #refresh} always checks "did anything actually change" first, unconditionally,
 * before considering {@code force} at all. This is what keeps a quiet caller's cost at one boolean
 * check per call -- if {@code force} skipped that check too, a caller that happens to be idle most
 * of the time (the common case) would pay for a full recompute on every single call, whether or not
 * anything had changed. A freshly constructed cache starts {@code dirty}, since it hasn't computed
 * a real value yet -- that's what lets a one-time forced warm-up work without needing {@code force}
 * to bypass the dirty check.
 *
 * <p>Either way, the eventual recompute is always a full recompute from the supplied {@link
 * Supplier}, never incremental -- a wrong or missed {@code markDirty} call costs at worst one extra
 * recompute, it never leaves the published value permanently wrong.
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

    private final Lock updateLock = new ReentrantLock();

    @GuardedBy("updateLock")
    private volatile T value;

    // only ever touched from the single owning thread -- see class javadoc.
    // starts true: a freshly constructed cache hasn't computed a real value yet.
    private boolean dirty = true;
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
     * Recomputes and republishes the value via {@code compute}, if and only if something has
     * actually changed since the last refresh <em>and</em> now is the right time to act on it.
     *
     * @param compute recomputes the value from scratch; only invoked if a refresh is due.
     * @param force overrides the timing question directly -- pass {@code true} when the caller
     *     already knows now is a good time (e.g. its own work queue is empty), or to force an
     *     unconditional warm-up. Never overrides the {@code dirty} check: if nothing changed, this
     *     is a no-op regardless of {@code force}.
     */
    public void refresh(Supplier<T> compute, boolean force) {
        if (!dirty) {
            return;
        }
        long now = System.currentTimeMillis();
        boolean due = force || (urgentDirty && (now - lastRefreshTimeMs) >= urgentMaxDelayMs);
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
