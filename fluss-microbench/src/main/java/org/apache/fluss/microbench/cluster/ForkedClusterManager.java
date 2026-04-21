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

package org.apache.fluss.microbench.cluster;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.microbench.config.ClusterConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Forks a child JVM that runs {@link ClusterBootstrap} to start a MiniFluss cluster in process
 * isolation.
 *
 * <p>Lifecycle: {@link #start(Path)} forks the child and waits for the {@code READY:address}
 * signal. {@link #stop()} sends {@code STOP} via stdin and waits for graceful shutdown.
 */
public class ForkedClusterManager {

    private static final Logger LOG = LoggerFactory.getLogger(ForkedClusterManager.class);

    private static final long START_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(2);
    private static final long STOP_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);

    private final ClusterConfig clusterConfig;
    private final boolean nmtEnabled;

    @Nullable private Process process;
    @Nullable private String bootstrapServers;
    @Nullable private File metricsFile;
    @Nullable private Thread stdoutDrainer;
    @Nullable private Writer serverLogWriter;

    public ForkedClusterManager(ClusterConfig clusterConfig) {
        this(clusterConfig, false);
    }

    /**
     * Creates a new ForkedClusterManager.
     *
     * @param clusterConfig cluster configuration
     * @param nmtEnabled if true, injects -XX:NativeMemoryTracking=summary into the forked JVM
     */
    public ForkedClusterManager(ClusterConfig clusterConfig, boolean nmtEnabled) {
        this.clusterConfig = clusterConfig;
        this.nmtEnabled = nmtEnabled;
    }

    /** Forks the child JVM and blocks until the cluster is ready. */
    public void start(@Nullable Path logDir) throws Exception {
        metricsFile = Files.createTempFile("fluss-microbench-metrics-", ".json").toFile();
        metricsFile.deleteOnExit();

        List<String> command = buildCommand();
        LOG.info("Starting forked cluster: {}", String.join(" ", command));

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        process = pb.start();

        BufferedReader reader;
        try {
            // Read stdout until READY line or timeout. Returns the reader for reuse.
            reader =
                    new BufferedReader(
                            new InputStreamReader(
                                    process.getInputStream(), StandardCharsets.UTF_8));
            bootstrapServers = waitForReady(process, reader);
        } catch (Exception e) {
            process.destroyForcibly();
            process = null;
            if (metricsFile != null) {
                metricsFile.delete();
                metricsFile = null;
            }
            throw e;
        }

        LOG.info(
                "Forked cluster ready (PID={}), bootstrap.servers={}",
                process.pid(),
                bootstrapServers);

        if (logDir != null) {
            Files.createDirectories(logDir);
            serverLogWriter =
                    Files.newBufferedWriter(
                            logDir.resolve("server-0.log"),
                            StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING);
        }

        // Start background thread to drain child stdout to prevent pipe buffer deadlock.
        // Reuse the same BufferedReader from waitForReady to avoid losing buffered data.
        final BufferedReader drainReader = reader;
        stdoutDrainer =
                new Thread(
                        () -> {
                            try {
                                String line;
                                while ((line = drainReader.readLine()) != null) {
                                    System.err.println("[server] " + line);
                                    if (serverLogWriter != null) {
                                        serverLogWriter.write(line);
                                        serverLogWriter.write('\n');
                                        serverLogWriter.flush();
                                    }
                                }
                            } catch (IOException e) {
                                // Expected when process exits
                            }
                        },
                        "server-stdout-drainer");
        stdoutDrainer.setDaemon(true);
        stdoutDrainer.start();
    }

    /** Returns a client {@link Configuration} pointing to the forked cluster. */
    public Configuration getClientConfig() {
        if (bootstrapServers == null) {
            throw new IllegalStateException("Cluster not started");
        }
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.BOOTSTRAP_SERVERS, Collections.singletonList(bootstrapServers));
        // Set small writer buffer defaults for the forked cluster mode.
        // These are overridden by user's YAML client.config if specified.
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("2mb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("256kb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, MemorySize.parse("1kb"));
        return conf;
    }

    /** Returns the PID of the child JVM process. */
    public long getServerPid() {
        if (process == null) {
            throw new IllegalStateException("Cluster not started");
        }
        return process.pid();
    }

    /** Returns the metrics file path. */
    public File getMetricsFile() {
        return metricsFile;
    }

    /** Sends STOP to the child process and waits for graceful shutdown. */
    public void stop() {
        if (process == null) {
            return;
        }
        try {
            OutputStream os = process.getOutputStream();
            os.write("STOP\n".getBytes(StandardCharsets.UTF_8));
            os.flush();
        } catch (IOException e) {
            LOG.warn("Failed to send STOP to child process", e);
        }
        try {
            process.getOutputStream().close();
        } catch (IOException e) {
            LOG.warn("Failed to close child process stdin", e);
        }

        try {
            boolean exited = process.waitFor(STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (!exited) {
                LOG.warn("Child process did not exit in {}ms, force-killing", STOP_TIMEOUT_MS);
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }

        // Final safety check — ensure process is truly dead
        if (process.isAlive()) {
            LOG.warn("Child process still alive after stop sequence, force-killing");
            process.destroyForcibly();
        }

        // Wait for stdout drainer to finish before nulling process
        if (stdoutDrainer != null) {
            try {
                stdoutDrainer.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (serverLogWriter != null) {
                try {
                    serverLogWriter.close();
                } catch (IOException e) {
                    LOG.debug("Failed to close server log", e);
                }
                serverLogWriter = null;
            }
            stdoutDrainer = null;
        }

        if (metricsFile != null && metricsFile.exists()) {
            if (!metricsFile.delete()) {
                LOG.debug("Could not delete metrics file: {}", metricsFile);
            }
        }
        process = null;
        bootstrapServers = null;
    }

    // -----------------------------------------------------------------------------------

    private List<String> buildCommand() {
        String javaCmd = ProcessHandle.current().info().command().orElse("java");
        String classpath = System.getProperty("java.class.path");

        List<String> cmd = new ArrayList<>();
        cmd.add(javaCmd);

        // Forward JVM args from config
        List<String> jvmArgs = clusterConfig.jvmArgs();
        if (jvmArgs != null) {
            cmd.addAll(jvmArgs);
        }

        // Inject NMT tracking if enabled in sampling config
        if (nmtEnabled) {
            cmd.add("-XX:NativeMemoryTracking=summary");
        }

        cmd.add("-cp");
        cmd.add(classpath);
        cmd.add(ClusterBootstrap.class.getName());

        int tabletServers =
                clusterConfig.tabletServers() != null ? clusterConfig.tabletServers() : 1;
        cmd.add("--tablet-servers=" + tabletServers);

        cmd.add("--metrics-file=" + metricsFile.getAbsolutePath());

        // Build --config arg from config overrides (use semicolon separator
        // because config values may contain commas, e.g. bootstrap.servers=h1:9092,h2:9092)
        Map<String, String> configOverrides = clusterConfig.configOverrides();
        if (configOverrides != null && !configOverrides.isEmpty()) {
            String configStr =
                    configOverrides.entrySet().stream()
                            .map(e -> e.getKey() + "=" + e.getValue())
                            .collect(java.util.stream.Collectors.joining(";"));
            cmd.add("--config=" + configStr);
        }

        return cmd;
    }

    private String waitForReady(Process proc, BufferedReader reader) throws Exception {
        long deadline = System.currentTimeMillis() + START_TIMEOUT_MS;
        StringBuilder childOutput = new StringBuilder();

        while (System.currentTimeMillis() < deadline) {
            if (!proc.isAlive()) {
                throw new IOException(
                        "Child process exited prematurely with code "
                                + proc.exitValue()
                                + ". Output:\n"
                                + childOutput);
            }
            if (reader.ready()) {
                String line = reader.readLine();
                if (line == null) {
                    throw new IOException(
                            "Child process closed stdout without READY signal. Output:\n"
                                    + childOutput);
                }
                if (line.startsWith("READY:")) {
                    return line.substring("READY:".length());
                }
                // Forward non-READY lines as info
                LOG.info("[child] {}", line);
                childOutput.append(line).append('\n');
            } else {
                Thread.sleep(100);
            }
        }
        proc.destroyForcibly();
        throw new IOException(
                "Timed out waiting for cluster READY signal after " + START_TIMEOUT_MS + "ms");
    }
}
