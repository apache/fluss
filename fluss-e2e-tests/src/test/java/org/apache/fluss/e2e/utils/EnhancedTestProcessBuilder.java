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

package org.apache.fluss.e2e.utils;

import org.apache.fluss.testutils.TestProcessBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Enhanced test process builder with monitoring and fault injection capabilities.
 *
 * <p>This class extends {@link TestProcessBuilder} to provide additional features for end-to-end
 * testing:
 *
 * <ul>
 *   <li>Output monitoring with pattern matching
 *   <li>Process lifecycle hooks
 *   <li>Resource usage tracking
 *   <li>Graceful shutdown support
 *   <li>Process state inspection
 * </ul>
 */
public class EnhancedTestProcessBuilder extends TestProcessBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(EnhancedTestProcessBuilder.class);

    private final List<OutputMonitor> outputMonitors = new CopyOnWriteArrayList<>();
    private final List<Consumer<Process>> startHooks = new ArrayList<>();
    private final List<Consumer<Integer>> exitHooks = new ArrayList<>();
    private final ConcurrentHashMap<String, Long> metrics = new ConcurrentHashMap<>();

    private volatile Process process;
    private volatile boolean captureOutput = true;
    private volatile long startTime;
    private volatile long endTime;

    public EnhancedTestProcessBuilder(String entryPointClassName) {
        super(entryPointClassName);
    }

    /**
     * Adds an output monitor that watches for specific patterns in the process output.
     *
     * @param pattern the pattern to match
     * @param callback the callback to invoke when pattern matches
     * @return this builder
     */
    public EnhancedTestProcessBuilder addOutputMonitor(Pattern pattern, Runnable callback) {
        outputMonitors.add(new OutputMonitor(pattern, callback));
        return this;
    }

    /**
     * Adds a hook that will be called when the process starts.
     *
     * @param hook the hook to call with the started process
     * @return this builder
     */
    public EnhancedTestProcessBuilder addStartHook(Consumer<Process> hook) {
        startHooks.add(hook);
        return this;
    }

    /**
     * Adds a hook that will be called when the process exits.
     *
     * @param hook the hook to call with the exit code
     * @return this builder
     */
    public EnhancedTestProcessBuilder addExitHook(Consumer<Integer> hook) {
        exitHooks.add(hook);
        return this;
    }

    /**
     * Sets whether to capture and monitor process output.
     *
     * @param capture true to capture output, false otherwise
     * @return this builder
     */
    public EnhancedTestProcessBuilder setCaptureOutput(boolean capture) {
        this.captureOutput = capture;
        return this;
    }

    /**
     * Waits for a specific pattern to appear in the process output.
     *
     * @param pattern the pattern to wait for
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout
     * @return true if the pattern was found, false if timeout
     * @throws InterruptedException if interrupted while waiting
     */
    public boolean waitForPattern(Pattern pattern, long timeout, TimeUnit unit)
            throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        addOutputMonitor(pattern, latch::countDown);
        return latch.await(timeout, unit);
    }

    /**
     * Starts the process and begins monitoring.
     *
     * @return the started process
     * @throws IOException if an I/O error occurs
     */
    @Override
    public Process start() throws IOException {
        startTime = System.currentTimeMillis();
        process = super.start();

        // Execute start hooks
        for (Consumer<Process> hook : startHooks) {
            try {
                hook.accept(process);
            } catch (Exception e) {
                LOG.warn("Error executing start hook", e);
            }
        }

        // Start output monitoring if enabled
        if (captureOutput && !outputMonitors.isEmpty()) {
            startOutputMonitoring(process);
        }

        // Start exit monitoring
        startExitMonitoring(process);

        return process;
    }

    /**
     * Gets the current process if started.
     *
     * @return the process or null if not started
     */
    public Process getProcess() {
        return process;
    }

    /**
     * Gets the process uptime in milliseconds.
     *
     * @return the uptime, or -1 if not started
     */
    public long getUptimeMillis() {
        if (startTime == 0) {
            return -1;
        }
        long end = endTime > 0 ? endTime : System.currentTimeMillis();
        return end - startTime;
    }

    /**
     * Gets a metric value.
     *
     * @param key the metric key
     * @return the metric value, or null if not set
     */
    public Long getMetric(String key) {
        return metrics.get(key);
    }

    /**
     * Sets a metric value.
     *
     * @param key the metric key
     * @param value the metric value
     */
    public void setMetric(String key, long value) {
        metrics.put(key, value);
    }

    /**
     * Attempts to gracefully shutdown the process.
     *
     * @param timeout the maximum time to wait for shutdown
     * @param unit the time unit of the timeout
     * @return true if the process exited within the timeout, false otherwise
     */
    public boolean shutdown(long timeout, TimeUnit unit) {
        if (process == null || !process.isAlive()) {
            return true;
        }

        // Try graceful shutdown first
        process.destroy();

        try {
            if (process.waitFor(timeout, unit)) {
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

        // Force shutdown if graceful failed
        process.destroyForcibly();
        return false;
    }

    private void startOutputMonitoring(Process process) {
        Thread outputThread =
                new Thread(
                        () -> {
                            try (BufferedReader reader =
                                    new BufferedReader(
                                            new InputStreamReader(
                                                    process.getInputStream(),
                                                    StandardCharsets.UTF_8))) {
                                String line;
                                while ((line = reader.readLine()) != null) {
                                    String finalLine = line;
                                    for (OutputMonitor monitor : outputMonitors) {
                                        if (monitor.pattern.matcher(finalLine).find()) {
                                            try {
                                                monitor.callback.run();
                                            } catch (Exception e) {
                                                LOG.warn(
                                                        "Error executing output monitor callback",
                                                        e);
                                            }
                                        }
                                    }
                                }
                            } catch (IOException e) {
                                LOG.debug("Error reading process output", e);
                            }
                        },
                        "EnhancedTestProcessBuilder-Output-Monitor");
        outputThread.setDaemon(true);
        outputThread.start();
    }

    private void startExitMonitoring(Process process) {
        Thread exitThread =
                new Thread(
                        () -> {
                            try {
                                int exitCode = process.waitFor();
                                endTime = System.currentTimeMillis();

                                for (Consumer<Integer> hook : exitHooks) {
                                    try {
                                        hook.accept(exitCode);
                                    } catch (Exception e) {
                                        LOG.warn("Error executing exit hook", e);
                                    }
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        },
                        "EnhancedTestProcessBuilder-Exit-Monitor");
        exitThread.setDaemon(true);
        exitThread.start();
    }

    private static class OutputMonitor {
        private final Pattern pattern;
        private final Runnable callback;

        OutputMonitor(Pattern pattern, Runnable callback) {
            this.pattern = pattern;
            this.callback = callback;
        }
    }
}
