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
import oshi.SystemInfo;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

/** Collects OS-level process metrics via OSHI for a given PID. */
public class OshiSampler {

    private static final Logger LOG = LoggerFactory.getLogger(OshiSampler.class);

    private final long pid;
    private final OperatingSystem os;
    private OSProcess previousSnapshot;

    private OshiSampler(long pid) {
        this.pid = pid;
        this.os = new SystemInfo().getOperatingSystem();
    }

    /** Creates a sampler for the current JVM process. */
    public static OshiSampler forSelf() {
        return new OshiSampler(ProcessHandle.current().pid());
    }

    /** Creates a sampler for the given PID. */
    public static OshiSampler forPid(long pid) {
        return new OshiSampler(pid);
    }

    /** Samples OS-level metrics and returns a {@link ProcessSnapshot}. */
    public ProcessSnapshot sample() {
        ProcessSnapshot snap = new ProcessSnapshot();
        try {
            OSProcess proc = os.getProcess((int) pid);
            if (proc == null) {
                LOG.warn("Process {} not found", pid);
                return snap;
            }

            snap.rssBytes = proc.getResidentSetSize();
            snap.vszBytes = proc.getVirtualSize();
            snap.threadCount = proc.getThreadCount();
            snap.diskReadBytes = proc.getBytesRead();
            snap.diskWriteBytes = proc.getBytesWritten();
            snap.openFileCount = proc.getOpenFiles();
            snap.contextSwitches = proc.getContextSwitches();
            snap.minorFaults = proc.getMinorFaults();

            if (previousSnapshot != null) {
                snap.cpuPercent = proc.getProcessCpuLoadBetweenTicks(previousSnapshot) * 100.0;
            }
            previousSnapshot = proc;
        } catch (Exception e) {
            LOG.warn("Failed to sample OSHI metrics for pid {}", pid, e);
        }
        return snap;
    }
}
