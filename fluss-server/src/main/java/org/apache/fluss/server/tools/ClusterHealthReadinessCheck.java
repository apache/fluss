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

package org.apache.fluss.server.tools;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metrics.registry.MetricRegistryImpl;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.GetClusterHealthRequest;
import org.apache.fluss.rpc.messages.GetClusterHealthResponse;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.utils.ExceptionUtils;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Lightweight readiness-check CLI for Fluss tablet-server pods.
 *
 * <p>The probe connects to a tablet server (typically the local one on {@code 127.0.0.1}) and
 * issues a single {@code getClusterHealth} call. The tablet server forwards the request to the
 * coordinator over its internal listener and returns the cluster-wide health snapshot. This means
 * the readiness probe never has to know the coordinator's address and survives coordinator pod
 * restarts cleanly.
 *
 * <h3>Inputs</h3>
 *
 * <p>For every input below, a CLI argument (when supplied) takes precedence over the corresponding
 * environment variable. The env vars exist so the Kubernetes manifest can stay declarative; the CLI
 * flags exist so an operator can override them ad-hoc when running the probe by hand.
 *
 * <ul>
 *   <li>{@code --timeoutMs <ms>}: optional, defaults to {@value #DEFAULT_TIMEOUT_MS}.
 *   <li>{@code --host <host>} / env {@value #ENV_TCP_HOST}: tablet server host. Defaults to {@code
 *       127.0.0.1}.
 *   <li>{@code --port <port>} / env {@value #ENV_TCP_PORT}: tablet server port. Defaults to {@value
 *       #DEFAULT_TCP_PORT}.
 *   <li>{@code --healthCheckAuth <props>} / env {@value #ENV_HEALTH_CHECK_AUTH}: optional client
 *       auth configuration as semicolon-separated {@code key:value} pairs (e.g. {@code
 *       client.security.protocol:SASL;client.sasl.mechanism:PLAIN;
 *       client.security.sasl.username:admin;client.security.sasl.password:admin-pass}). When
 *       absent, the probe uses unauthenticated PLAINTEXT.
 * </ul>
 *
 * <h3>Exit codes</h3>
 *
 * <ul>
 *   <li>{@value #EXIT_READY} — cluster status is GREEN (Ready).
 *   <li>{@value #EXIT_NOT_READY} — cluster status is YELLOW/RED/UNKNOWN, the tablet server is
 *       unreachable, or the RPC timed out.
 *   <li>{@value #EXIT_API_UNSUPPORTED} — tablet server is reachable but does not implement the
 *       cluster-health API ({@link UnsupportedVersionException}).
 *   <li>{@value #EXIT_ERROR} — invalid arguments or environment.
 * </ul>
 */
public final class ClusterHealthReadinessCheck {

    public static final int EXIT_READY = 0;
    public static final int EXIT_NOT_READY = 1;
    public static final int EXIT_API_UNSUPPORTED = 2;
    public static final int EXIT_ERROR = 3;

    private static final long DEFAULT_TIMEOUT_MS = 5000;
    private static final String DEFAULT_TCP_HOST = "127.0.0.1";
    private static final int DEFAULT_TCP_PORT = 9124;

    /** Environment variable carrying the tablet server host. */
    static final String ENV_TCP_HOST = "READINESS_TCP_HOST";

    /** Environment variable carrying the tablet server port. */
    static final String ENV_TCP_PORT = "READINESS_TCP_PORT";

    /**
     * Environment variable carrying client auth configuration as semicolon-separated {@code
     * key:value} pairs. Values are forwarded verbatim to {@link Configuration} before {@link
     * RpcClient#create} is called.
     */
    static final String ENV_HEALTH_CHECK_AUTH = "READINESS_HEALTH_CHECK_AUTH";

    /**
     * GREEN status code as produced by {@code CoordinatorService#computeClusterHealth} (mirrors
     * {@code PbClusterHealthStatus.GREEN}).
     */
    private static final int STATUS_GREEN = 0;

    private ClusterHealthReadinessCheck() {}

    public static void main(String[] args) {
        System.exit(run(args));
    }

    static int run(String[] args) {
        long timeoutMs = DEFAULT_TIMEOUT_MS;
        String hostArg = null;
        String portArg = null;
        String authArg = null;
        for (int i = 0; i < args.length; i++) {
            if ("--timeoutMs".equals(args[i]) && i + 1 < args.length) {
                String raw = args[++i];
                try {
                    timeoutMs = Long.parseLong(raw);
                } catch (NumberFormatException e) {
                    System.err.println(
                            "[readiness-check] ERROR: invalid --timeoutMs value: " + raw);
                    return EXIT_ERROR;
                }
            } else if ("--host".equals(args[i]) && i + 1 < args.length) {
                hostArg = args[++i];
            } else if ("--port".equals(args[i]) && i + 1 < args.length) {
                portArg = args[++i];
            } else if ("--healthCheckAuth".equals(args[i]) && i + 1 < args.length) {
                authArg = args[++i];
            }
        }

        // CLI flag takes precedence over env so operators can override ad-hoc.
        String host = firstNonBlank(hostArg, System.getenv(ENV_TCP_HOST), DEFAULT_TCP_HOST);
        String portStr = firstNonBlank(portArg, System.getenv(ENV_TCP_PORT), null);
        int port = DEFAULT_TCP_PORT;
        if (portStr != null) {
            try {
                port = Integer.parseInt(portStr.trim());
            } catch (NumberFormatException e) {
                System.err.println("[readiness-check] ERROR: invalid port \"" + portStr + "\"");
                return EXIT_ERROR;
            }
        }
        // The server id is irrelevant for a one-shot probe; pick a sentinel value.
        ServerNode tabletServer = new ServerNode(0, host, port, ServerType.TABLET_SERVER);
        String address = host + ":" + port;

        String authPropsString = authArg != null ? authArg : System.getenv(ENV_HEALTH_CHECK_AUTH);
        Configuration conf;
        try {
            conf = buildConfiguration(authPropsString);
        } catch (IllegalArgumentException e) {
            System.err.println(
                    "[readiness-check] ERROR: cannot parse auth properties ("
                            + (authArg != null ? "--healthCheckAuth" : ENV_HEALTH_CHECK_AUTH)
                            + "): "
                            + e.getMessage());
            return EXIT_ERROR;
        }

        MetricRegistryImpl registry = new MetricRegistryImpl(Collections.emptyList());
        ClientMetricGroup metricGroup = new ClientMetricGroup(registry, "readiness-probe");
        try (RpcClient rpcClient = RpcClient.create(conf, metricGroup)) {
            TabletServerGateway gateway =
                    GatewayClientProxy.createGatewayProxy(
                            () -> tabletServer, rpcClient, TabletServerGateway.class);
            GetClusterHealthResponse resp =
                    gateway.getClusterHealth(new GetClusterHealthRequest())
                            .get(timeoutMs, TimeUnit.MILLISECONDS);
            return evaluate(resp);
        } catch (TimeoutException e) {
            System.err.println(
                    "[readiness-check] Timeout calling getClusterHealth on "
                            + address
                            + ", treating as not ready");
            return EXIT_NOT_READY;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (ExceptionUtils.findThrowable(cause, UnsupportedVersionException.class)
                    .isPresent()) {
                System.err.println("[readiness-check] API unsupported: " + cause.getMessage());
                return EXIT_API_UNSUPPORTED;
            }
            System.err.println(
                    "[readiness-check] Cannot reach tablet server at "
                            + address
                            + ", treating as not ready: "
                            + cause.getMessage());
            return EXIT_NOT_READY;
        } catch (Exception e) {
            System.err.println(
                    "[readiness-check] Cannot reach tablet server at "
                            + address
                            + ", treating as not ready: "
                            + e.getMessage());
            return EXIT_NOT_READY;
        } finally {
            // Best-effort: shut down the metric registry so the probe JVM can exit cleanly.
            try {
                registry.closeAsync();
            } catch (Exception ignored) {
                // ignore — probe process is about to exit anyway
            }
        }
    }

    /** Returns the first argument that is non-null and non-blank; otherwise {@code fallback}. */
    private static String firstNonBlank(String... candidates) {
        for (int i = 0; i < candidates.length - 1; i++) {
            String c = candidates[i];
            if (c != null && !c.trim().isEmpty()) {
                return c;
            }
        }
        return candidates[candidates.length - 1];
    }

    /**
     * Build the {@link Configuration} fed to {@link RpcClient#create} from the optional auth
     * configuration string. Format: semicolon-separated {@code key:value} pairs, e.g. {@code
     * client.security.protocol:SASL;client.sasl.mechanism:PLAIN}.
     */
    private static Configuration buildConfiguration(String authString) {
        Configuration conf = new Configuration();
        if (authString == null || authString.trim().isEmpty()) {
            return conf;
        }
        // Enable plugin discovery so the client authenticator can discover authentication
        // plugins shipped in the server's plugins/ directory.
        conf.setBoolean(ConfigOptions.CLIENT_SECURITY_ENABLE_PLUGIN_DISCOVERY, true);
        for (String pair : authString.split(";")) {
            String trimmed = pair.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int idx = trimmed.indexOf(':');
            if (idx <= 0 || idx == trimmed.length() - 1) {
                throw new IllegalArgumentException(
                        "expected 'key:value' pairs separated by ';', got: \"" + pair + "\"");
            }
            String key = trimmed.substring(0, idx).trim();
            String value = trimmed.substring(idx + 1).trim();
            conf.setString(key, value);
        }
        return conf;
    }

    private static int evaluate(GetClusterHealthResponse resp) {
        int status = resp.getStatus();
        System.out.println(
                "[readiness-check] status="
                        + status
                        + " numReplicas="
                        + resp.getNumReplicas()
                        + " inSyncReplicas="
                        + resp.getInSyncReplicas()
                        + " numLeaderReplicas="
                        + resp.getNumLeaderReplicas()
                        + " activeLeaderReplicas="
                        + resp.getActiveLeaderReplicas());
        return status == STATUS_GREEN ? EXIT_READY : EXIT_NOT_READY;
    }
}
