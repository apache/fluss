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

package org.apache.fluss.dist;

import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ClusterHealth;
import org.apache.fluss.client.admin.ClusterHealthStatus;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.GlobalConfiguration;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.utils.ExceptionUtils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Readiness check CLI tool for Fluss rolling upgrades.
 *
 * <p>Queries the Coordinator's Cluster Health API to determine whether the cluster is healthy
 * enough for the next pod to restart.
 *
 * <p>Exit codes:
 *
 * <ul>
 *   <li>0 = Cluster is GREEN (readiness probe should pass)
 *   <li>1 = Cluster is YELLOW, RED, or UNKNOWN (readiness probe should fail)
 *   <li>2 = Coordinator does not support the API (UnsupportedVersion)
 *   <li>3 = Invalid arguments or configuration error
 * </ul>
 */
public class ClusterHealthReadinessCheck {

    public static final int EXIT_READY = 0;
    public static final int EXIT_NOT_READY = 1;
    public static final int EXIT_API_UNSUPPORTED = 2;
    public static final int EXIT_ERROR = 3;

    private static final long DEFAULT_TIMEOUT_MS = 5000;

    public static void main(String[] args) {
        int exitCode = run(args);
        System.exit(exitCode);
    }

    static int run(String[] args) {
        String configDir = null;
        String bootstrapServers = null;
        long timeoutMs = DEFAULT_TIMEOUT_MS;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--configDir":
                    if (i + 1 < args.length) {
                        configDir = args[++i];
                    }
                    break;
                case "--timeoutMs":
                    if (i + 1 < args.length) {
                        timeoutMs = Long.parseLong(args[++i]);
                    }
                    break;
                case "--bootstrapServers":
                    if (i + 1 < args.length) {
                        bootstrapServers = args[++i];
                    }
                    break;
                default:
                    break;
            }
        }

        if (configDir == null) {
            System.err.println(
                    "Usage: ClusterHealthReadinessCheck --configDir <dir> [--timeoutMs <ms>] [--bootstrapServers <host:port>]");
            return EXIT_ERROR;
        }

        Configuration conf;
        try {
            conf = GlobalConfiguration.loadConfiguration(configDir, null);
        } catch (Exception e) {
            System.err.println("[readiness-check] Failed to load configuration: " + e.getMessage());
            return EXIT_ERROR;
        }

        if (bootstrapServers != null) {
            conf.setString("bootstrap.servers", bootstrapServers);
        }

        try (org.apache.fluss.client.Connection connection =
                ConnectionFactory.createConnection(conf)) {
            Admin admin = connection.getAdmin();

            ClusterHealth health = queryCoordinator(admin, timeoutMs);
            return evaluateHealth(health);

        } catch (ApiUnsupportedException e) {
            System.err.println("[readiness-check] API unsupported: " + e.getMessage());
            return EXIT_API_UNSUPPORTED;

        } catch (TimeoutException e) {
            System.err.println(
                    "[readiness-check] Timeout connecting to Coordinator, treating as not ready: "
                            + e.getMessage());
            return EXIT_NOT_READY;

        } catch (Exception e) {
            System.err.println(
                    "[readiness-check] Cannot reach Coordinator, treating as not ready: "
                            + e.getMessage());
            return EXIT_NOT_READY;
        }
    }

    private static ClusterHealth queryCoordinator(Admin admin, long timeoutMs) throws Exception {
        try {
            return admin.getClusterHealth().get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (ExceptionUtils.findThrowable(cause, UnsupportedVersionException.class)
                    .isPresent()) {
                throw new ApiUnsupportedException(cause.getMessage());
            }
            throw e;
        }
    }

    private static int evaluateHealth(ClusterHealth health) {
        ClusterHealthStatus status = health.getStatus();
        System.out.println(
                "[readiness-check] status="
                        + status
                        + " numReplicas="
                        + health.getNumReplicas()
                        + " inSyncReplicas="
                        + health.getInSyncReplicas()
                        + " numLeaderReplicas="
                        + health.getNumLeaderReplicas()
                        + " activeLeaderReplicas="
                        + health.getActiveLeaderReplicas());
        return status == ClusterHealthStatus.GREEN ? EXIT_READY : EXIT_NOT_READY;
    }

    private static final class ApiUnsupportedException extends Exception {
        ApiUnsupportedException(String message) {
            super(message);
        }
    }
}
