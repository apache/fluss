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

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Tablet-server-wide memory watermarks shared by all KV pre-write buffers.
 *
 * <p>Writes are admitted until memory usage reaches the high watermark. They remain under pressure
 * until usage falls to the low watermark.
 *
 * <p>The watermarks are intentionally global rather than per bucket. All reservations and releases
 * are synchronized and safe to call concurrently from different KV tablets.
 */
@Internal
@ThreadSafe
public final class KvPreWriteBufferMemoryManager {

    private final long highWatermarkBytes;
    private final long lowWatermarkBytes;

    private long usedBytes;
    private boolean underPressure;

    /**
     * Creates global pre-write buffer memory watermarks.
     *
     * @param highWatermarkBytes usage at which writes are rejected
     * @param lowWatermarkBytes usage at or below which writes resume
     */
    public KvPreWriteBufferMemoryManager(long highWatermarkBytes, long lowWatermarkBytes) {
        checkArgument(highWatermarkBytes > 0, "High watermark must be greater than 0.");
        checkArgument(lowWatermarkBytes >= 0, "Low watermark must not be negative.");
        checkArgument(
                lowWatermarkBytes < highWatermarkBytes,
                "Low watermark must be less than high watermark.");
        this.highWatermarkBytes = highWatermarkBytes;
        this.lowWatermarkBytes = lowWatermarkBytes;
    }

    /**
     * Attempts to reserve bytes from the global memory pool.
     *
     * <p>Reservations are rejected while the manager is under pressure or when the reservation
     * would exceed the high watermark.
     *
     * @param bytes number of bytes to reserve
     * @return whether the reservation succeeded
     */
    public synchronized boolean tryReserve(long bytes) {
        checkArgument(bytes >= 0, "The number of bytes to reserve must not be negative.");
        if (bytes == 0) {
            return true;
        }

        if (underPressure) {
            return false;
        }

        if (bytes > highWatermarkBytes - usedBytes) {
            underPressure = usedBytes > lowWatermarkBytes;
            return false;
        }

        usedBytes += bytes;
        underPressure = usedBytes >= highWatermarkBytes;
        return true;
    }

    /**
     * Releases bytes previously reserved from this manager.
     *
     * @param bytes number of reserved bytes to release
     */
    public synchronized void release(long bytes) {
        checkArgument(bytes >= 0, "The number of bytes to release must not be negative.");
        if (bytes == 0) {
            return;
        }

        checkState(
                bytes <= usedBytes,
                "Cannot release %s bytes when only %s bytes are reserved.",
                bytes,
                usedBytes);
        usedBytes -= bytes;
        if (usedBytes <= lowWatermarkBytes) {
            underPressure = false;
        }
    }

    /** Returns the currently reserved bytes across all KV pre-write buffers. */
    public synchronized long usedBytes() {
        return usedBytes;
    }

    /** Returns the usage at which writes are rejected. */
    public long highWatermarkBytes() {
        return highWatermarkBytes;
    }

    /** Returns the usage at or below which writes resume. */
    public long lowWatermarkBytes() {
        return lowWatermarkBytes;
    }

    /** Returns whether new reservations are currently under pressure. */
    public synchronized boolean isUnderPressure() {
        return underPressure;
    }
}
