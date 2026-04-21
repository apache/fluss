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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Samples JVM Native Memory Tracking (NMT) data from a remote server process via jcmd. */
public class NmtSampler {

    private static final Logger LOG = LoggerFactory.getLogger(NmtSampler.class);

    // Pattern: -  Category (reserved=NNNkb, committed=NNNkb)
    private static final Pattern CATEGORY_PATTERN =
            Pattern.compile("-\\s+(\\S[^(]*)\\(reserved=(\\d+)KB, committed=(\\d+)KB\\)");

    private final long serverPid;

    public NmtSampler(long serverPid) {
        this.serverPid = serverPid;
    }

    /** Runs jcmd NMT summary and fills server NMT fields in the snapshot. */
    public void sample(ResourceSnapshot snap) {
        try {
            ProcessBuilder pb =
                    new ProcessBuilder(
                            "jcmd", String.valueOf(serverPid), "VM.native_memory", "summary");
            pb.redirectErrorStream(true);
            Process process = pb.start();

            try {
                try (BufferedReader reader =
                        new BufferedReader(
                                new InputStreamReader(
                                        process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        Matcher m = CATEGORY_PATTERN.matcher(line);
                        if (m.find()) {
                            String category = m.group(1).trim();
                            long committedKb = Long.parseLong(m.group(3));
                            long committedBytes = committedKb * 1024L;
                            switch (category) {
                                case "Java Heap":
                                    snap.serverNmtHeap = committedBytes;
                                    break;
                                case "Class":
                                    snap.serverNmtClass = committedBytes;
                                    break;
                                case "Thread":
                                    snap.serverNmtThreadStacks = committedBytes;
                                    break;
                                case "Code":
                                    snap.serverNmtCodeCache = committedBytes;
                                    break;
                                case "GC":
                                    snap.serverNmtGc = committedBytes;
                                    break;
                                case "Internal":
                                    snap.serverNmtInternal = committedBytes;
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                }

                boolean exited = process.waitFor(10, TimeUnit.SECONDS);
                if (!exited) {
                    LOG.debug("jcmd NMT timed out for pid {}, killing process", serverPid);
                    process.destroyForcibly();
                    process.waitFor(5, TimeUnit.SECONDS);
                    return;
                }
                int exitCode = process.exitValue();
                if (exitCode != 0) {
                    LOG.debug("jcmd NMT exited with code {} for pid {}", exitCode, serverPid);
                }
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly();
                }
            }
        } catch (Exception e) {
            LOG.debug("Failed to run jcmd NMT for pid {}", serverPid, e);
            // Leave fields at -1 (default)
        }
    }
}
