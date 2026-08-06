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

package org.apache.fluss.server.kv.prewrite;

import org.apache.fluss.annotation.Internal;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * TabletServer-wide memory watermarks shared by all KV pre-write buffers.
 *
 * <p>This is a defensive memory guard rather than the primary backpressure mechanism. Every
 * pre-write-buffer entry reserves its estimated retained heap before it is admitted. Once the high
 * watermark is reached, reservations remain blocked until usage falls to the low watermark.
 *
 * <p>The usage and the pressure latch are kept in one {@link AtomicLong}. The low 63 bits store the
 * reserved bytes and the sign bit stores whether the manager is latched under pressure. Keeping
 * both values in one compare-and-set word prevents a concurrent release from being overwritten by a
 * reservation that was calculated from an older state.
 */
@Internal
@ThreadSafe
public final class KvPreWriteBufferMemoryManager {

    private static final float MAX_PRESSURE = 0.999f;
    private static final long UNDER_PRESSURE_FLAG = Long.MIN_VALUE;
    private static final long USED_BYTES_MASK = Long.MAX_VALUE;
    private static final KvPreWriteBufferMemoryManager DISABLED =
            new KvPreWriteBufferMemoryManager();

    private final boolean enabled;
    private final long highWatermarkBytes;
    private final long lowWatermarkBytes;

    private final AtomicLong used = new AtomicLong();

    /** Creates a TabletServer-wide pre-write-buffer memory guard. */
    public KvPreWriteBufferMemoryManager(long highWatermarkBytes, long lowWatermarkBytes) {
        checkArgument(highWatermarkBytes > 0, "High watermark must be greater than 0.");
        checkArgument(lowWatermarkBytes >= 0, "Low watermark must not be negative.");
        checkArgument(
                lowWatermarkBytes < highWatermarkBytes,
                "Low watermark must be less than high watermark.");
        this.enabled = true;
        this.highWatermarkBytes = highWatermarkBytes;
        this.lowWatermarkBytes = lowWatermarkBytes;
    }

    private KvPreWriteBufferMemoryManager() {
        this.enabled = false;
        this.highWatermarkBytes = Long.MAX_VALUE;
        this.lowWatermarkBytes = 0;
    }

    /** Returns a disabled manager which performs no accounting or synchronization. */
    public static KvPreWriteBufferMemoryManager disabled() {
        return DISABLED;
    }

    /** Returns whether the defensive memory guard is enabled. */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Attempts to reserve bytes from the global memory guard.
     *
     * <p>Reservations are rejected while the manager is under pressure or when the reservation
     * would exceed the high watermark.
     */
    public boolean tryReserve(long bytes) {
        checkArgument(bytes >= 0, "The number of bytes to reserve must not be negative.");
        if (!enabled) {
            return true;
        }
        if (bytes == 0) {
            return true;
        }

        long currentState = used.get();
        while (true) {
            long currentUsedBytes = usedBytes(currentState);
            if (isUnderPressure(currentState)) {
                return false;
            }
            if (bytes > highWatermarkBytes - currentUsedBytes) {
                // Do not latch pressure at or below the low watermark. Otherwise, a single large
                // reservation could block all writes even though no release is required to cross
                // the resume threshold.
                long nextState =
                        currentUsedBytes > lowWatermarkBytes
                                ? underPressureState(currentUsedBytes)
                                : currentUsedBytes;
                if (used.compareAndSet(currentState, nextState)) {
                    return false;
                }
                currentState = used.get();
                continue;
            }

            long nextUsedBytes = currentUsedBytes + bytes;
            long nextState =
                    nextUsedBytes >= highWatermarkBytes
                            ? underPressureState(nextUsedBytes)
                            : nextUsedBytes;
            if (used.compareAndSet(currentState, nextState)) {
                return true;
            }
            currentState = used.get();
        }
    }

    /** Releases bytes previously reserved from this manager. */
    public void release(long bytes) {
        checkArgument(bytes >= 0, "The number of bytes to release must not be negative.");
        if (!enabled) {
            return;
        }
        if (bytes == 0) {
            return;
        }

        long currentState = used.get();
        while (true) {
            long currentUsedBytes = usedBytes(currentState);
            checkState(
                    bytes <= currentUsedBytes,
                    "Cannot release %s bytes when only %s bytes are reserved.",
                    bytes,
                    currentUsedBytes);

            long nextUsedBytes = currentUsedBytes - bytes;
            // Preserve the pressure latch above the low watermark and clear it once the resume
            // threshold is reached. This transition is part of the same CAS as the counter update.
            long nextState =
                    nextUsedBytes <= lowWatermarkBytes
                            ? nextUsedBytes
                            : (isUnderPressure(currentState)
                                    ? underPressureState(nextUsedBytes)
                                    : nextUsedBytes);
            if (used.compareAndSet(currentState, nextState)) {
                return;
            }
            currentState = used.get();
        }
    }

    /** Returns the currently reserved bytes across all KV pre-write buffers. */
    public long usedBytes() {
        if (!enabled) {
            return 0;
        }
        return usedBytes(used.get());
    }

    /** Returns the usage at which writes are rejected. */
    public long highWatermarkBytes() {
        return highWatermarkBytes;
    }

    /** Returns the usage at or below which writes resume. */
    public long lowWatermarkBytes() {
        return lowWatermarkBytes;
    }

    /** Returns whether new reservations are currently blocked by the memory guard. */
    public boolean isUnderPressure() {
        if (!enabled) {
            return false;
        }
        return isUnderPressure(used.get());
    }

    /**
     * Returns normalized pre-write-buffer memory pressure in {@code [0, 1)}.
     *
     * <p>Pressure ramps from zero at the low watermark to the maximum wire value at the high
     * watermark. Once the hard guard is latched, maximum pressure is reported until usage falls to
     * the low watermark.
     */
    public float currentPressure() {
        if (!enabled) {
            return 0f;
        }
        long currentState = used.get();
        long currentUsedBytes = usedBytes(currentState);
        if (isUnderPressure(currentState)) {
            return MAX_PRESSURE;
        }
        if (currentUsedBytes <= lowWatermarkBytes) {
            return 0f;
        }
        double pressure =
                (double) (currentUsedBytes - lowWatermarkBytes)
                        / (highWatermarkBytes - lowWatermarkBytes);
        return (float) Math.min(pressure, MAX_PRESSURE);
    }

    private static long usedBytes(long memoryState) {
        return memoryState & USED_BYTES_MASK;
    }

    private static boolean isUnderPressure(long memoryState) {
        return (memoryState & UNDER_PRESSURE_FLAG) != 0;
    }

    private static long underPressureState(long usedBytes) {
        return usedBytes | UNDER_PRESSURE_FLAG;
    }
}
