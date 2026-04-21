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

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/** Manages JDK Flight Recorder (JFR) recording on a remote server process via jcmd. */
public class JfrManager {

    private static final Logger LOG = LoggerFactory.getLogger(JfrManager.class);

    private final long serverPid;

    public JfrManager(long serverPid) {
        this.serverPid = serverPid;
    }

    /** Starts a JFR recording with the given configuration. */
    public void start(String jfrConfig) {
        try {
            String settings = (jfrConfig != null && !jfrConfig.isEmpty()) ? jfrConfig : "default";
            ProcessBuilder pb =
                    new ProcessBuilder(
                            "jcmd",
                            String.valueOf(serverPid),
                            "JFR.start",
                            "name=perf",
                            "settings=" + settings);
            pb.redirectErrorStream(true);
            Process process = pb.start();
            try {
                String output = new String(process.getInputStream().readAllBytes());
                boolean exited = process.waitFor(30, TimeUnit.SECONDS);
                if (!exited) {
                    LOG.warn("JFR.start timed out for pid {}, killing", serverPid);
                    process.destroyForcibly();
                    process.waitFor(5, TimeUnit.SECONDS);
                    return;
                }
                int exitCode = process.exitValue();
                if (exitCode != 0) {
                    LOG.warn(
                            "JFR.start exited with code {} for pid {}: {}",
                            exitCode,
                            serverPid,
                            output.trim());
                } else {
                    LOG.info(
                            "JFR recording started on pid {} with settings={}",
                            serverPid,
                            settings);
                }
            } finally {
                process.destroyForcibly();
            }
        } catch (Exception e) {
            LOG.warn("Failed to start JFR on pid {}", serverPid, e);
        }
    }

    /** Stops the JFR recording and writes the output to the given path. */
    public void stop(Path outputPath) {
        try {
            ProcessBuilder pb =
                    new ProcessBuilder(
                            "jcmd",
                            String.valueOf(serverPid),
                            "JFR.stop",
                            "name=perf",
                            "filename=" + outputPath.toAbsolutePath());
            pb.redirectErrorStream(true);
            Process process = pb.start();
            try {
                String output = new String(process.getInputStream().readAllBytes());
                boolean exited = process.waitFor(30, TimeUnit.SECONDS);
                if (!exited) {
                    LOG.warn("JFR.stop timed out for pid {}, killing", serverPid);
                    process.destroyForcibly();
                    process.waitFor(5, TimeUnit.SECONDS);
                    return;
                }
                int exitCode = process.exitValue();
                if (exitCode != 0) {
                    LOG.warn(
                            "JFR.stop exited with code {} for pid {}: {}",
                            exitCode,
                            serverPid,
                            output.trim());
                } else {
                    LOG.info("JFR recording saved to {}", outputPath);
                }
            } finally {
                process.destroyForcibly();
            }
        } catch (Exception e) {
            LOG.warn("Failed to stop JFR on pid {}", serverPid, e);
        }
    }
}
