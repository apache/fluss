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

package org.apache.fluss.cluster;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.Objects;

/** A snapshot of the machine resources available to a Fluss server node. */
@PublicEvolving
public final class NodeResourceInfo {

    private final double cpuCores;
    private final long memoryTotalBytes;
    private final double cpuUsageRatio;
    private final long memoryUsedBytes;
    private final @Nullable Long dataDiskTotalBytes;
    private final @Nullable Long dataDiskUsedBytes;
    private final long collectedAtMs;

    /** Creates a node resource information snapshot. */
    public NodeResourceInfo(
            double cpuCores,
            long memoryTotalBytes,
            double cpuUsageRatio,
            long memoryUsedBytes,
            @Nullable Long dataDiskTotalBytes,
            @Nullable Long dataDiskUsedBytes,
            long collectedAtMs) {
        this.cpuCores = cpuCores;
        this.memoryTotalBytes = memoryTotalBytes;
        this.cpuUsageRatio = cpuUsageRatio;
        this.memoryUsedBytes = memoryUsedBytes;
        this.dataDiskTotalBytes = dataDiskTotalBytes;
        this.dataDiskUsedBytes = dataDiskUsedBytes;
        this.collectedAtMs = collectedAtMs;
    }

    /** Returns the effective CPU capacity visible to the Fluss process. */
    public double cpuCores() {
        return cpuCores;
    }

    /** Returns the effective memory capacity visible to the Fluss process, in bytes. */
    public long memoryTotalBytes() {
        return memoryTotalBytes;
    }

    /** Returns the current machine CPU usage ratio in the range [0, 1]. */
    public double cpuUsageRatio() {
        return cpuUsageRatio;
    }

    /** Returns the current machine memory usage, in bytes. */
    public long memoryUsedBytes() {
        return memoryUsedBytes;
    }

    /** Returns the total capacity of the Fluss data disks, or null when unavailable. */
    public @Nullable Long dataDiskTotalBytes() {
        return dataDiskTotalBytes;
    }

    /** Returns the used capacity of the Fluss data disks, or null when unavailable. */
    public @Nullable Long dataDiskUsedBytes() {
        return dataDiskUsedBytes;
    }

    /** Returns the collection timestamp of this snapshot in epoch milliseconds. */
    public long collectedAtMs() {
        return collectedAtMs;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        NodeResourceInfo that = (NodeResourceInfo) other;
        return Double.compare(that.cpuCores, cpuCores) == 0
                && memoryTotalBytes == that.memoryTotalBytes
                && Double.compare(that.cpuUsageRatio, cpuUsageRatio) == 0
                && memoryUsedBytes == that.memoryUsedBytes
                && collectedAtMs == that.collectedAtMs
                && Objects.equals(dataDiskTotalBytes, that.dataDiskTotalBytes)
                && Objects.equals(dataDiskUsedBytes, that.dataDiskUsedBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                cpuCores,
                memoryTotalBytes,
                cpuUsageRatio,
                memoryUsedBytes,
                dataDiskTotalBytes,
                dataDiskUsedBytes,
                collectedAtMs);
    }

    @Override
    public String toString() {
        return "NodeResourceInfo{"
                + "cpuCores="
                + cpuCores
                + ", memoryTotalBytes="
                + memoryTotalBytes
                + ", cpuUsageRatio="
                + cpuUsageRatio
                + ", memoryUsedBytes="
                + memoryUsedBytes
                + ", dataDiskTotalBytes="
                + dataDiskTotalBytes
                + ", dataDiskUsedBytes="
                + dataDiskUsedBytes
                + ", collectedAtMs="
                + collectedAtMs
                + '}';
    }
}
