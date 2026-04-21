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

package org.apache.fluss.microbench.sampling;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

/** Collects JVM MXBean metrics for the current (client) process. */
public class JvmSampler {

    private static final Logger LOG = LoggerFactory.getLogger(JvmSampler.class);

    /** Fills client JVM fields in the given {@link ResourceSnapshot}. */
    public void sample(ResourceSnapshot snap) {
        try {
            MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
            MemoryUsage heap = mem.getHeapMemoryUsage();
            snap.clientHeapUsed = heap.getUsed();
            snap.clientHeapMax = heap.getMax();

            MemoryUsage nonHeap = mem.getNonHeapMemoryUsage();
            snap.clientNonHeapUsed = nonHeap.getUsed();

            // Direct buffer pool
            for (BufferPoolMXBean pool :
                    ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
                if ("direct".equals(pool.getName())) {
                    snap.clientDirectMemoryUsed = pool.getMemoryUsed();
                    break;
                }
            }

            // JVM CPU load via com.sun OperatingSystemMXBean
            java.lang.management.OperatingSystemMXBean osBean =
                    ManagementFactory.getOperatingSystemMXBean();
            if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
                snap.clientJvmCpuLoad =
                        ((com.sun.management.OperatingSystemMXBean) osBean).getProcessCpuLoad()
                                * 100.0;
            }

            // GC stats
            long gcCount = 0;
            long gcTime = 0;
            for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                long c = gc.getCollectionCount();
                long t = gc.getCollectionTime();
                if (c >= 0) {
                    gcCount += c;
                }
                if (t >= 0) {
                    gcTime += t;
                }
            }
            snap.clientGcCount = gcCount;
            snap.clientGcTimeMs = gcTime;
        } catch (Exception e) {
            LOG.warn("Failed to sample JVM metrics", e);
        }
    }
}
