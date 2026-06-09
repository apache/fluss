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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ClusterHealth;
import org.apache.fluss.client.admin.ClusterHealthStatus;
import org.apache.fluss.config.Configuration;
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
 *
 * <h3>Authentication via environment variable</h3>
 *
 * <p>Instead of inheriting the full server-side configuration (which may contain server-only
 * authorizer settings), authentication properties are injected through the environment variable
 * {@code READINESS_HEALTH_CHECK_AUTH}. If the variable is absent or empty, no authentication is
 * applied.
 *
 * <p>Format: {@code 'key1':'value1';'key2':'value2'}
 *
 * <p>Example:
 *
 * <pre>
 * READINESS_HEALTH_CHECK_AUTH='security.protocol':'SASL_PLAINTEXT';'sasl.kerberos.service.name':'fluss'
 * </pre>
 */
public class ClusterHealthReadinessCheck {

    public static final int EXIT_READY = 0;
    public static final int EXIT_NOT_READY = 1;
    public static final int EXIT_API_UNSUPPORTED = 2;
    public static final int EXIT_ERROR = 3;

    private static final long DEFAULT_TIMEOUT_MS = 5000;

    /**
     * Environment variable name for authentication properties used by the health check. Format:
     * {@code 'key1':'value1';'key2':'value2'}. If absent or empty, no authentication is applied.
     */
    private static final String ENV_CLIENT_PROPERTIES = "READINESS_HEALTH_CHECK_AUTH";

    /** Environment variable fallback for bootstrap servers if --bootstrapServers is not given. */
    private static final String ENV_BOOTSTRAP_SERVERS = "READINESS_BOOTSTRAP_SERVERS";

    public static void main(String[] args) {
        int exitCode = run(args);
        System.exit(exitCode);
    }

    static int run(String[] args) {
        String bootstrapServers = null;
        String authProperties = null;
        long timeoutMs = DEFAULT_TIMEOUT_MS;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
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
                case "--auth":
                    if (i + 1 < args.length) {
                        authProperties = args[++i];
                    }
                    break;
                default:
                    break;
            }
        }

        Configuration conf = new Configuration();

        // Bootstrap servers: CLI arg > env var
        if (bootstrapServers == null) {
            bootstrapServers = System.getenv(ENV_BOOTSTRAP_SERVERS);
        }
        if (bootstrapServers != null && !bootstrapServers.trim().isEmpty()) {
            conf.setString("bootstrap.servers", bootstrapServers);
        }
        if (!conf.containsKey("bootstrap.servers")) {
            System.err.println(
                    "[readiness-check] ERROR: bootstrap.servers not set. "
                            + "Provide via --bootstrapServers or env READINESS_BOOTSTRAP_SERVERS");
            return EXIT_ERROR;
        }

        // Auth properties: CLI arg > env var
        if (authProperties == null) {
            authProperties = System.getenv(ENV_CLIENT_PROPERTIES);
        }
        if (authProperties != null && !authProperties.trim().isEmpty()) {
            applyClientProperties(conf, authProperties);
        }

        try (Connection connection = ConnectionFactory.createConnection(conf)) {
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

    // ---- Environment-based client properties injection ----

    /**
     * Parses a properties string and applies each key-value pair to the given configuration.
     *
     * <p>Format: {@code 'key1':'value1';'key2':'value2'}
     *
     * <p>Surrounding single quotes on keys and values are stripped. Pairs are separated by
     * semicolons ({@code ;}).
     */
    static void applyClientProperties(Configuration conf, String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return;
        }

        String[] pairs = raw.split(";");
        for (String pair : pairs) {
            pair = pair.trim();
            if (pair.isEmpty()) {
                continue;
            }
            // Split on the first ':' only to allow ':' inside values
            int colonIdx = findSplitColon(pair);
            if (colonIdx < 0) {
                System.err.println(
                        "[readiness-check] WARN: skipping malformed property (no colon separator): "
                                + pair);
                continue;
            }
            String key = stripQuotes(pair.substring(0, colonIdx).trim());
            String value = stripQuotes(pair.substring(colonIdx + 1).trim());
            if (!key.isEmpty()) {
                conf.setString(key, value);
            }
        }
    }

    /**
     * Find the colon that separates key from value. The colon must be between two quoted tokens,
     * i.e. after the closing quote of the key and before the opening quote of the value. We look
     * for the pattern {@code ':'} as separator.
     */
    private static int findSplitColon(String pair) {
        // The canonical separator between key and value is ':'
        // (after the closing quote of the key, before the opening quote of the value).
        int idx = pair.indexOf("':'");
        if (idx >= 0) {
            return idx + 1; // point at the ':' between the two quotes
        }
        // Fallback: first colon
        return pair.indexOf(':');
    }

    /** Strip leading and trailing single quotes from a token. */
    private static String stripQuotes(String s) {
        if (s.length() >= 2 && s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'') {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }
}
