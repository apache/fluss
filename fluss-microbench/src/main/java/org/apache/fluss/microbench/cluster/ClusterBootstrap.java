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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * Child-process main class that boots a Fluss cluster via {@link FlussClusterExtension}. Uses the
 * test infrastructure which handles all internal server registration and coordination.
 *
 * <p>Protocol:
 *
 * <ul>
 *   <li>Parses args: {@code --tablet-servers=N}, {@code --config=key=value,key=value}, {@code
 *       --metrics-file=path}
 *   <li>Starts the cluster
 *   <li>Prints {@code READY:<bootstrap-address>} to stdout
 *   <li>Blocks reading stdin until {@code STOP} or EOF
 *   <li>Gracefully shuts down the cluster
 * </ul>
 */
public class ClusterBootstrap {

    public static void main(String[] args) {
        int tabletServers = 1;
        String configPairs = null;
        String metricsFile = null;

        for (String arg : args) {
            if (arg.startsWith("--tablet-servers=")) {
                tabletServers = Integer.parseInt(arg.substring("--tablet-servers=".length()));
            } else if (arg.startsWith("--config=")) {
                configPairs = arg.substring("--config=".length());
            } else if (arg.startsWith("--metrics-file=")) {
                metricsFile = arg.substring("--metrics-file=".length());
            }
        }

        Configuration clusterConf = new Configuration();
        if (configPairs != null && !configPairs.isEmpty()) {
            for (String pair : configPairs.split(";")) {
                int eq = pair.indexOf('=');
                if (eq > 0) {
                    clusterConf.setString(pair.substring(0, eq), pair.substring(eq + 1));
                }
            }
        }

        // Register FlussMetricSampler via SPI
        if (metricsFile != null && !metricsFile.isEmpty()) {
            clusterConf.setString("metrics.reporters", "perf");
            clusterConf.setString("metrics.reporter.perf.file", metricsFile);
        }

        FlussClusterExtension cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(tabletServers)
                        .setClusterConf(clusterConf)
                        .build();

        Thread shutdownHook =
                new Thread(
                        () -> {
                            try {
                                cluster.close();
                            } catch (Exception e) {
                                // best-effort cleanup
                            }
                        },
                        "cluster-shutdown-hook");

        try {
            cluster.start();
            String bootstrapServers = cluster.getBootstrapServers();

            // Register shutdown hook to clean up if process is killed
            Runtime.getRuntime().addShutdownHook(shutdownHook);

            // Signal readiness to parent process
            System.out.println("READY:" + bootstrapServers);
            System.out.flush();

            // Block until parent sends STOP or closes stdin
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if ("STOP".equals(line.trim())) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("ClusterBootstrap failed: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        } finally {
            try {
                cluster.close();
            } catch (Exception e) {
                System.err.println("Cluster close failed: " + e.getMessage());
            }
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException e) {
                // JVM is already shutting down, hook removal not possible
            }
        }
    }
}
