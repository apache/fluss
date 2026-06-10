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

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
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
 *   <li>{@code --address <host:port>} / env {@value #ENV_ADDRESS}: the tablet server endpoint to
 *       probe. Required.
 *   <li>{@code --healthCheckAuth <props>} / env {@value #ENV_HEALTH_CHECK_AUTH}: optional, a Java
 *       properties string (newline-separated {@code key=value} pairs) that supplies client auth
 *       configuration such as {@code client.security.protocol}, {@code
 *       client.security.sasl.mechanism}, etc. When absent, the probe uses unauthenticated
 *       PLAINTEXT.
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

    /** Environment variable carrying the tablet server endpoint as {@code host:port}. */
    static final String ENV_ADDRESS = "READINESS_ADDRESS";

    /**
     * Environment variable carrying client auth configuration as a Java properties string
     * (newline-separated {@code key=value} pairs). Values are forwarded verbatim to {@link
     * Configuration} before {@link RpcClient#create} is called.
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
        String addressArg = null;
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
            } else if ("--address".equals(args[i]) && i + 1 < args.length) {
                addressArg = args[++i];
            } else if ("--healthCheckAuth".equals(args[i]) && i + 1 < args.length) {
                authArg = args[++i];
            }
        }

        // CLI flag takes precedence over env so operators can override ad-hoc.
        String address = addressArg != null ? addressArg : System.getenv(ENV_ADDRESS);
        if (address == null || address.trim().isEmpty()) {
            System.err.println(
                    "[readiness-check] ERROR: address not set (pass --address or env "
                            + ENV_ADDRESS
                            + ", expected host:port)");
            return EXIT_ERROR;
        }

        ServerNode tabletServer;
        try {
            tabletServer = parseAddress(address.trim());
        } catch (IllegalArgumentException e) {
            System.err.println(
                    "[readiness-check] ERROR: invalid address \""
                            + address
                            + "\": "
                            + e.getMessage());
            return EXIT_ERROR;
        }

        String authPropsString = authArg != null ? authArg : System.getenv(ENV_HEALTH_CHECK_AUTH);
        Configuration conf;
        try {
            conf = buildConfiguration(authPropsString);
        } catch (IOException e) {
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

    /** Parse a {@code host:port} string into a {@link ServerNode} pointing at the tablet server. */
    private static ServerNode parseAddress(String addr) {
        int idx = addr.lastIndexOf(':');
        if (idx <= 0 || idx >= addr.length() - 1) {
            throw new IllegalArgumentException("expected host:port");
        }
        String host = addr.substring(0, idx);
        int port;
        try {
            port = Integer.parseInt(addr.substring(idx + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid port: " + addr.substring(idx + 1));
        }
        // The server id is irrelevant for a one-shot probe; pick a sentinel value.
        return new ServerNode(0, host, port, ServerType.TABLET_SERVER);
    }

    /**
     * Build the {@link Configuration} fed to {@link RpcClient#create} from the optional auth
     * properties string.
     */
    private static Configuration buildConfiguration(String authPropertiesString)
            throws IOException {
        Configuration conf = new Configuration();
        if (authPropertiesString == null || authPropertiesString.trim().isEmpty()) {
            return conf;
        }
        Properties props = new Properties();
        props.load(new StringReader(authPropertiesString));
        for (Map.Entry<Object, Object> e : props.entrySet()) {
            conf.setString(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
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
