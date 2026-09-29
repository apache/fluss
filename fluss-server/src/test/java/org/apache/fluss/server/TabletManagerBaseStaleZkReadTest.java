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

package org.apache.fluss.server;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperTestUtils;
import org.apache.fluss.server.zk.data.TableRegistration;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces a tablet server loading table metadata from a ZooKeeper follower that has not yet
 * applied the coordinator's writes.
 *
 * <p>Three-node ensemble. Server 1 reaches the other servers' quorum ports through {@link
 * PausableProxy}, so pausing the proxy stops the leader's proposals and commits from reaching
 * server 1 while it keeps answering client reads. Servers 2 and 3 still form a quorum, so the
 * coordinator's writes commit. This is the state a tablet server connected to a lagging follower
 * sees when it handles {@code NotifyLeaderAndIsr} for a table created a few milliseconds earlier.
 */
class TabletManagerBaseStaleZkReadTest {

    private static final TablePath TABLE_PATH = TablePath.of("stale_read_db", "stale_read_table");
    private static final TablePath WARMUP_PATH = TablePath.of("stale_read_db", "warmup_table");

    private final List<PausableProxy> proxies = new ArrayList<>();
    private TestingCluster cluster;
    private ZooKeeperClient coordinatorZk;
    private ZooKeeperClient tabletServerZk;
    private PausableProxy laggingFollowerLink;

    @BeforeEach
    void setUp() throws Exception {
        // Equal zxids at startup elect the highest server id, so server 1 starts as a follower.
        InstanceSpec s1 = spec(1);
        InstanceSpec s2 = spec(2);
        InstanceSpec s3 = spec(3);
        PausableProxy toS2 = new PausableProxy(s2.getQuorumPort());
        PausableProxy toS3 = new PausableProxy(s3.getQuorumPort());
        proxies.add(toS2);
        proxies.add(toS3);

        Map<InstanceSpec, Collection<InstanceSpec>> views = new LinkedHashMap<>();
        views.put(s1, Arrays.asList(s1, viaProxy(s2, toS2), viaProxy(s3, toS3)));
        views.put(s2, Arrays.asList(s1, s2, s3));
        views.put(s3, Arrays.asList(s1, s2, s3));
        cluster = new TestingCluster(views);
        cluster.start();

        retry(Duration.ofMinutes(1), () -> assertThat(mode(s1)).isEqualTo("follower"));
        InstanceSpec leader = mode(s3).equals("leader") ? s3 : s2;
        assertThat(mode(leader)).isEqualTo("leader");
        laggingFollowerLink = leader == s3 ? toS3 : toS2;

        coordinatorZk = client(leader);
        tabletServerZk = client(s1);

        // Make sure server 1 is connected and caught up before the link is paused.
        coordinatorZk.registerFirstSchema(WARMUP_PATH, DATA1_TABLE_DESCRIPTOR.getSchema());
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(tabletServerZk.getSchemaById(WARMUP_PATH, 1)).isPresent());
    }

    @AfterEach
    void tearDown() throws Exception {
        for (PausableProxy proxy : proxies) {
            proxy.resume();
        }
        if (tabletServerZk != null) {
            tabletServerZk.close();
        }
        if (coordinatorZk != null) {
            coordinatorZk.close();
        }
        if (cluster != null) {
            cluster.close();
        }
        for (PausableProxy proxy : proxies) {
            proxy.close();
        }
    }

    @Test
    void testGetTableInfoSeesTableCommittedBeforeTheRead() throws Exception {
        laggingFollowerLink.pause();

        // What MetadataManager#createTable writes, in the same order, before the coordinator
        // sends NotifyLeaderAndIsr.
        coordinatorZk.registerFirstSchema(TABLE_PATH, DATA1_TABLE_DESCRIPTOR.getSchema());
        registerTable();

        // Precondition: the follower serves a view that predates the committed writes.
        assertThat(tabletServerZk.getSchemaById(TABLE_PATH, 1)).isEmpty();

        assertGetTableInfoWaitsForFollower();
    }

    @Test
    void testGetTableInfoSeesTableRegistrationCommittedAfterTheSchema() throws Exception {
        // The follower applies the schema but not yet the table registration written after it.
        coordinatorZk.registerFirstSchema(TABLE_PATH, DATA1_TABLE_DESCRIPTOR.getSchema());
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(tabletServerZk.getSchemaById(TABLE_PATH, 1)).isPresent());
        laggingFollowerLink.pause();
        registerTable();

        assertThat(tabletServerZk.getTable(TABLE_PATH)).isEmpty();

        assertGetTableInfoWaitsForFollower();
    }

    private void registerTable() throws Exception {
        coordinatorZk.registerTable(
                TABLE_PATH,
                TableRegistration.newTable(1L, "/remote-data", DATA1_TABLE_DESCRIPTOR),
                false);
    }

    private void assertGetTableInfoWaitsForFollower() throws Exception {
        CompletableFuture<TableInfo> tableInfo =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return TabletManagerBase.getTableInfo(tabletServerZk, TABLE_PATH);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        // A read that waits for the follower to catch up completes once the link resumes. A plain
        // read has already failed by now.
        Thread.sleep(1_000);
        laggingFollowerLink.resume();

        assertThat(tableInfo.get(30, TimeUnit.SECONDS).getSchemaId()).isEqualTo(1);
    }

    private static InstanceSpec spec(int serverId) {
        return new InstanceSpec(
                null,
                -1,
                -1,
                -1,
                true,
                serverId,
                -1,
                -1,
                null,
                InetAddress.getLoopbackAddress().getHostAddress());
    }

    /** The same server as {@code target}, with its quorum port replaced by the proxy's port. */
    private static InstanceSpec viaProxy(InstanceSpec target, PausableProxy proxy) {
        return new InstanceSpec(
                target.getDataDirectory(),
                target.getPort(),
                target.getElectionPort(),
                proxy.port(),
                true,
                target.getServerId(),
                target.getTickTime(),
                target.getMaxClientCnxns(),
                target.getCustomProperties(),
                target.getHostname());
    }

    /** A client pinned to one server: ensemble tracking would move it to the full server list. */
    private static ZooKeeperClient client(InstanceSpec server) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.REMOTE_DATA_DIR, "/remote-data");
        conf.set(ConfigOptions.ZOOKEEPER_ENSEMBLE_TRACKING, false);
        return ZooKeeperTestUtils.createZooKeeperClient(
                conf, server.getConnectString(), NOPErrorHandler.INSTANCE);
    }

    /** The server's role from the {@code srvr} four-letter command, e.g. "leader". */
    private static String mode(InstanceSpec server) throws IOException {
        try (Socket socket = new Socket(server.getHostname(), server.getPort())) {
            socket.getOutputStream().write("srvr".getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();
            String response = new String(readAll(socket.getInputStream()), StandardCharsets.UTF_8);
            for (String line : response.split("\n")) {
                if (line.startsWith("Mode: ")) {
                    return line.substring("Mode: ".length()).trim();
                }
            }
            return "unknown";
        }
    }

    private static byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int n;
        while ((n = in.read(buffer)) != -1) {
            out.write(buffer, 0, n);
        }
        return out.toByteArray();
    }

    /**
     * TCP forwarder to a quorum port. While paused, bytes from the target back to the connecting
     * server are held, so a follower behind it stops receiving proposals and commits from the
     * leader.
     */
    private static final class PausableProxy implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final int targetPort;
        private final List<Socket> sockets = new ArrayList<>();
        private volatile boolean paused;

        PausableProxy(int targetPort) throws IOException {
            this.targetPort = targetPort;
            this.serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Thread acceptor = new Thread(this::acceptLoop, "proxy-to-" + targetPort);
            acceptor.setDaemon(true);
            acceptor.start();
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        void pause() {
            paused = true;
        }

        void resume() {
            paused = false;
        }

        private void acceptLoop() {
            while (!serverSocket.isClosed()) {
                try {
                    Socket source = serverSocket.accept();
                    Socket target = new Socket(InetAddress.getLoopbackAddress(), targetPort);
                    synchronized (sockets) {
                        sockets.add(source);
                        sockets.add(target);
                    }
                    pump(source, target, false);
                    pump(target, source, true);
                } catch (IOException e) {
                    // closed
                }
            }
        }

        private void pump(Socket from, Socket to, boolean pausable) {
            Thread thread =
                    new Thread(
                            () -> {
                                byte[] buffer = new byte[8192];
                                try (InputStream in = from.getInputStream();
                                        OutputStream out = to.getOutputStream()) {
                                    int n;
                                    while ((n = in.read(buffer)) != -1) {
                                        while (pausable && paused) {
                                            Thread.sleep(5);
                                        }
                                        out.write(buffer, 0, n);
                                        out.flush();
                                    }
                                } catch (IOException | InterruptedException e) {
                                    // connection closed
                                }
                            });
            thread.setDaemon(true);
            thread.start();
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
            synchronized (sockets) {
                for (Socket socket : sockets) {
                    socket.close();
                }
            }
        }
    }
}
