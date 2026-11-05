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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metrics.registry.MetricRegistryImpl;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.DescribeClusterRequest;
import org.apache.fluss.rpc.messages.DescribeClusterResponse;
import org.apache.fluss.rpc.messages.GetClusterHealthRequest;
import org.apache.fluss.rpc.messages.GetClusterHealthResponse;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.utils.ExceptionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Lightweight readiness-check CLI for Fluss server pods.
 *
 * <p>In the default {@code tablet} role, the probe connects to a tablet server (typically the local
 * one on {@code 127.0.0.1}) and issues a single {@code getClusterHealth} call. The tablet server
 * forwards the request to the coordinator over its internal listener and returns the cluster-wide
 * health snapshot. This means the readiness probe never has to know the coordinator's address and
 * survives coordinator pod restarts cleanly. Readiness requires status GREEN.
 *
 * <p>In the {@code coordinator} role, the probe first calls {@code getClusterHealth} on the local
 * coordinator server to learn its role:
 *
 * <ul>
 *   <li>The leader answers. It is Ready with any cluster health status, so a coordinator rollout
 *       does not depend on tablet health; the tablet probes already gate on cluster recovery.
 *   <li>A standby rejects the call with {@link NotCoordinatorLeaderException}. The rejection shows
 *       that its RPC server handles requests. The probe then calls {@code describeCluster} on all
 *       other coordinators in parallel, and the standby is Ready when the leader lists its
 *       advertised address among the standby coordinators.
 * </ul>
 *
 * <h3>Inputs</h3>
 *
 * <p>For every input below, a CLI argument (when supplied) takes precedence over the corresponding
 * environment variable. The env vars exist so the Kubernetes manifest can stay declarative; the CLI
 * flags exist so an operator can override them ad-hoc when running the probe by hand.
 *
 * <ul>
 *   <li>{@code --timeoutMs <ms>}: optional, defaults to {@value #DEFAULT_TIMEOUT_MS}. In the
 *       coordinator role this bounds all RPCs of one probe run together.
 *   <li>{@code --role <role>} / env {@value #ENV_ROLE}: {@code tablet} (default) or {@code
 *       coordinator} - the type of the local server the probe talks to.
 *   <li>{@code --host <host>} / env {@value #ENV_TCP_HOST}: server host. Defaults to {@code
 *       127.0.0.1}.
 *   <li>{@code --port <port>} / env {@value #ENV_TCP_PORT}: server port. Defaults to {@value
 *       #DEFAULT_TCP_PORT}.
 *   <li>{@code --coordinators <host:port,...>} / env {@value #ENV_COORDINATORS}: coordinator role
 *       only, required for a standby. Client listener addresses of all coordinators; the probe asks
 *       them in order for the leader's view of the cluster.
 *   <li>{@code --advertisedAddress <host:port>} / env {@value #ENV_ADVERTISED_ADDRESS}: coordinator
 *       role only, required for a standby. The client listener address the local coordinator
 *       advertises, as the leader reports it in {@code describeCluster}.
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
 *   <li>{@value #EXIT_READY}: Ready. Tablet role: cluster status is GREEN. Coordinator role: the
 *       server is the leader, or a standby that the leader lists.
 *   <li>{@value #EXIT_NOT_READY}: not Ready. Tablet role: cluster status is YELLOW/RED/UNKNOWN.
 *       Coordinator role: a standby that no reachable leader lists. Both roles: the server is
 *       unreachable or the RPC timed out.
 *   <li>{@value #EXIT_API_UNSUPPORTED}: the server is reachable but does not implement the
 *       cluster-health API, or in the coordinator role a standby finds no leader that lists it and
 *       at least one other coordinator does not implement {@code describeCluster} ({@link
 *       UnsupportedVersionException}).
 *   <li>{@value #EXIT_ERROR}: invalid arguments or environment.
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
     * Environment variable carrying the probed server's role: {@code tablet} (default) or {@code
     * coordinator}.
     */
    static final String ENV_ROLE = "READINESS_ROLE";

    /** Environment variable carrying the comma-separated client addresses of all coordinators. */
    static final String ENV_COORDINATORS = "READINESS_COORDINATORS";

    /** Environment variable carrying the client address the local coordinator advertises. */
    static final String ENV_ADVERTISED_ADDRESS = "READINESS_ADVERTISED_ADDRESS";

    static final String ROLE_TABLET = "tablet";
    static final String ROLE_COORDINATOR = "coordinator";

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

    @VisibleForTesting
    public static int run(String[] args) {
        long timeoutMs = DEFAULT_TIMEOUT_MS;
        String hostArg = null;
        String portArg = null;
        String authArg = null;
        String roleArg = null;
        String coordinatorsArg = null;
        String advertisedArg = null;
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
            } else if ("--role".equals(args[i]) && i + 1 < args.length) {
                roleArg = args[++i];
            } else if ("--coordinators".equals(args[i]) && i + 1 < args.length) {
                coordinatorsArg = args[++i];
            } else if ("--advertisedAddress".equals(args[i]) && i + 1 < args.length) {
                advertisedArg = args[++i];
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
        String role =
                firstNonBlank(roleArg, System.getenv(ENV_ROLE), ROLE_TABLET)
                        .trim()
                        .toLowerCase(Locale.ROOT);
        if (!ROLE_TABLET.equals(role) && !ROLE_COORDINATOR.equals(role)) {
            System.err.println("[readiness-check] ERROR: invalid role \"" + role + "\"");
            return EXIT_ERROR;
        }
        boolean coordinatorRole = ROLE_COORDINATOR.equals(role);
        // The server id is irrelevant for a one-shot probe; pick a sentinel value.
        ServerNode serverNode =
                new ServerNode(
                        0,
                        host,
                        port,
                        coordinatorRole ? ServerType.COORDINATOR : ServerType.TABLET_SERVER);
        String address = host + ":" + port;

        // Parsed eagerly so a malformed value fails fast, but only required once the local
        // coordinator turns out to be a standby.
        List<ServerNode> coordinators;
        ServerNode advertised;
        try {
            coordinators =
                    parseCoordinators(
                            firstNonBlank(coordinatorsArg, System.getenv(ENV_COORDINATORS), null));
            String advertisedStr =
                    firstNonBlank(advertisedArg, System.getenv(ENV_ADVERTISED_ADDRESS), null);
            advertised = advertisedStr == null ? null : parseAddress(advertisedStr);
        } catch (IllegalArgumentException e) {
            System.err.println("[readiness-check] ERROR: " + e.getMessage());
            return EXIT_ERROR;
        }

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
            if (coordinatorRole) {
                return checkCoordinator(rpcClient, serverNode, coordinators, advertised, timeoutMs);
            }
            TabletServerGateway gateway =
                    GatewayClientProxy.createGatewayProxy(
                            () -> serverNode, rpcClient, TabletServerGateway.class);
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
            return mapExecutionFailure(cause, address);
        } catch (Exception e) {
            System.err.println(
                    "[readiness-check] Cannot reach server at "
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

    /**
     * Coordinator-role readiness. A leader is Ready regardless of cluster color, because
     * coordinator readiness must not depend on tablet health. A standby is Ready once the leader
     * lists it among the standby coordinators.
     */
    private static int checkCoordinator(
            RpcClient rpcClient,
            ServerNode local,
            List<ServerNode> coordinators,
            ServerNode advertised,
            long timeoutMs)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        try {
            GetClusterHealthResponse resp =
                    coordinatorGateway(rpcClient, local)
                            .getClusterHealth(new GetClusterHealthRequest())
                            .get(timeoutMs, TimeUnit.MILLISECONDS);
            System.out.println("[readiness-check] role=leader status=" + resp.getStatus());
            return EXIT_READY;
        } catch (ExecutionException e) {
            if (!ExceptionUtils.findThrowable(e, NotCoordinatorLeaderException.class).isPresent()) {
                throw e;
            }
        }

        // The local coordinator is a standby. Ask the leader whether it sees this standby.
        if (coordinators.isEmpty() || advertised == null) {
            System.err.println(
                    "[readiness-check] ERROR: the local coordinator is a standby, which needs "
                            + ENV_COORDINATORS
                            + " and "
                            + ENV_ADVERTISED_ADDRESS
                            + " (or --coordinators and --advertisedAddress)");
            return EXIT_ERROR;
        }
        // Ask all other coordinators at once, so a hung or down coordinator cannot use up the
        // budget before the leader is asked.
        List<String> addresses = new ArrayList<>();
        List<CompletableFuture<DescribeClusterResponse>> answers = new ArrayList<>();
        for (ServerNode coordinator : coordinators) {
            if (isSameAddress(coordinator.host(), coordinator.port(), advertised)) {
                // this standby, which already rejected the cluster-health call
                continue;
            }
            addresses.add(coordinator.host() + ":" + coordinator.port());
            answers.add(
                    coordinatorGateway(rpcClient, coordinator)
                            .describeCluster(new DescribeClusterRequest()));
        }
        // ponytail: waits in list order; a hung coordinator listed before the leader delays the
        // answer up to the timeout, which stays within the probe budget.
        boolean unsupported = false;
        for (int i = 0; i < answers.size(); i++) {
            String coordinatorAddress = addresses.get(i);
            long remainingMs = Math.max(0, deadline - System.currentTimeMillis());
            try {
                return evaluateStandby(
                        answers.get(i).get(remainingMs, TimeUnit.MILLISECONDS),
                        advertised,
                        coordinatorAddress);
            } catch (TimeoutException e) {
                System.err.println(
                        "[readiness-check] Timeout calling describeCluster on "
                                + coordinatorAddress
                                + ", skipping it");
            } catch (ExecutionException e) {
                // A standby rejects the call; a pod that is down fails to connect. A coordinator
                // from a version without describeCluster, leader or standby, rejects it as
                // unsupported. Either way, ask the next coordinator.
                if (ExceptionUtils.findThrowable(e, UnsupportedVersionException.class)
                        .isPresent()) {
                    unsupported = true;
                }
                System.err.println(
                        "[readiness-check] Skipping coordinator at "
                                + coordinatorAddress
                                + ": "
                                + e.getCause().getMessage());
            }
        }
        if (unsupported) {
            // No leader listed this standby, and at least one coordinator predates
            // describeCluster: it may be the leader, so the probe cannot decide.
            System.err.println(
                    "[readiness-check] API unsupported: a coordinator does not support"
                            + " describeCluster");
            return EXIT_API_UNSUPPORTED;
        }
        System.err.println(
                "[readiness-check] Standby coordinator found no reachable leader, treating as"
                        + " not ready");
        return EXIT_NOT_READY;
    }

    private static CoordinatorGateway coordinatorGateway(RpcClient rpcClient, ServerNode node) {
        return GatewayClientProxy.createGatewayProxy(
                () -> node, rpcClient, CoordinatorGateway.class);
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

    /** Parses a comma-separated {@code host:port} list; a missing value yields an empty list. */
    @VisibleForTesting
    static List<ServerNode> parseCoordinators(String addresses) {
        List<ServerNode> nodes = new ArrayList<>();
        if (addresses == null) {
            return nodes;
        }
        for (String address : addresses.split(",")) {
            if (!address.trim().isEmpty()) {
                nodes.add(parseAddress(address));
            }
        }
        return nodes;
    }

    private static ServerNode parseAddress(String address) {
        String trimmed = address.trim();
        int idx = trimmed.lastIndexOf(':');
        if (idx <= 0 || idx == trimmed.length() - 1) {
            throw new IllegalArgumentException(
                    "expected a 'host:port' address, got: \"" + address + "\"");
        }
        try {
            return new ServerNode(
                    0,
                    trimmed.substring(0, idx),
                    Integer.parseInt(trimmed.substring(idx + 1)),
                    ServerType.COORDINATOR);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid port in address \"" + address + "\"", e);
        }
    }

    private static boolean isSameAddress(String host, int port, ServerNode node) {
        // DNS host names are case-insensitive
        return port == node.port() && host.equalsIgnoreCase(node.host());
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

    /** Maps a failed RPC's cause to an exit code. */
    static int mapExecutionFailure(Throwable cause, String address) {
        if (ExceptionUtils.findThrowable(cause, UnsupportedVersionException.class).isPresent()) {
            System.err.println("[readiness-check] API unsupported: " + cause.getMessage());
            return EXIT_API_UNSUPPORTED;
        }
        System.err.println(
                "[readiness-check] Cannot reach server at "
                        + address
                        + ", treating as not ready: "
                        + cause.getMessage());
        return EXIT_NOT_READY;
    }

    static int evaluate(GetClusterHealthResponse resp) {
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

    /**
     * A standby is Ready when the leader's {@code describeCluster} answer lists its advertised
     * address. The leader lists a coordinator while its registration in ZooKeeper exists, so an
     * unlisted standby has not registered or has lost its ZooKeeper session.
     */
    static int evaluateStandby(
            DescribeClusterResponse resp, ServerNode advertised, String leaderAddress) {
        for (PbServerNode standby : resp.getCoordinatorStandbysList()) {
            if (isSameAddress(standby.getHost(), standby.getPort(), advertised)) {
                System.out.println(
                        "[readiness-check] role=standby, listed by leader at " + leaderAddress);
                return EXIT_READY;
            }
        }
        System.err.println(
                "[readiness-check] Standby coordinator "
                        + advertised.host()
                        + ":"
                        + advertised.port()
                        + " is not listed by leader at "
                        + leaderAddress
                        + ", treating as not ready");
        return EXIT_NOT_READY;
    }
}
