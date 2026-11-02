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

package org.apache.fluss.compatibilitytest;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ListImagesCmd;
import com.github.dockerjava.api.command.RemoveImageCmd;
import com.github.dockerjava.core.DockerClientBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.UUID;

import static org.apache.fluss.client.utils.MetadataUtils.sendMetadataRequestAndRebuildCluster;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.CLIENT_SALS_PROPERTIES;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_06_IMAGE_TAG;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_06_VERSION_MAGIC;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_07_IMAGE_TAG;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_07_VERSION_MAGIC;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_LATEST_VERSION_MAGIC;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_NETWORK;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.ZK_IMAGE_TAG;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.build06CoordinatorProperties;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.build06TabletServerProperties;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.build07CoordinatorProperties;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.build07TabletServerProperties;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.buildLatestCoordinatorProperties;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.buildLatestTabletServerProperties;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.initCoordinatorServer;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.initTabletServer;
import static org.apache.fluss.compatibilitytest.CompatTestUtils.checkImageExists;
import static org.apache.fluss.compatibilitytest.CompatTestUtils.getAvailablePort;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;

/** Basic class for compatibility test. */
@Testcontainers
public abstract class CompatTest {
    private static final Logger LOG = LoggerFactory.getLogger(CompatTest.class);

    private static final Path PROJECT_ROOT = Paths.get("").toAbsolutePath().getParent().getParent();
    private static final Path DOCKER_DIR = PROJECT_ROOT.resolve("docker/fluss");
    private static final Path BUILD_TARGET_DIR = PROJECT_ROOT.resolve("build-target");

    protected static final GenericContainer<?> ZOOKEEPER =
            new GenericContainer<>(DockerImageName.parse(ZK_IMAGE_TAG))
                    .withNetwork(FLUSS_NETWORK)
                    .withNetworkAliases("zookeeper")
                    .withExposedPorts(2181)
                    .withStartupTimeout(Duration.ofSeconds(60));

    private static String latestImageName;

    protected @Nullable GenericContainer<?> flussCoordinator;
    protected @Nullable GenericContainer<?> flussTabletServer;
    protected int coordinatorServerPort;
    protected int tabletServerPort;
    private DockerClient dockerClient;
    private File localDataDirInHostServer;
    private File remoteDataDirInHostServer;

    @BeforeAll
    static void setupBeforeAll() {
        String latestImageTag = "latest-" + UUID.randomUUID();
        latestImageName = "fluss:" + latestImageTag;
    }

    @AfterAll
    static void tearDownAfterAll() {
        try (DockerClient dockerClient = DockerClientBuilder.getInstance().build()) {
            ListImagesCmd listImagesCmd = dockerClient.listImagesCmd();
            listImagesCmd.exec().stream()
                    .filter(
                            image ->
                                    image.getRepoTags() != null
                                            && image.getRepoTags().length > 0
                                            && image.getRepoTags()[0].contains(latestImageName))
                    .forEach(
                            image -> {
                                try {
                                    RemoveImageCmd removeImageCmd =
                                            dockerClient.removeImageCmd(image.getId());
                                    removeImageCmd.exec();
                                    throw new RuntimeException(
                                            "Removed image: " + image.getRepoTags()[0]);
                                } catch (Exception e) {
                                    throw new RuntimeException(
                                            "Failed to remove image: " + e.getMessage());
                                }
                            });
        } catch (Exception e) {
            LOG.error("Error listing images: " + e.getMessage());
        }
    }

    @BeforeEach
    public void setup() throws Exception {
        ZOOKEEPER.start();
        dockerClient = DockerClientBuilder.getInstance().build();
        localDataDirInHostServer =
                Files.createTempDirectory("fluss-compat-test-local-dir").toFile();
        remoteDataDirInHostServer =
                Files.createTempDirectory("fluss-compat-test-remote-dir").toFile();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (dockerClient != null) {
            dockerClient.close();
        }

        if (flussCoordinator != null) {
            flussCoordinator.stop();
        }

        if (flussTabletServer != null) {
            flussTabletServer.stop();
        }

        ZOOKEEPER.stop();
    }

    boolean verifyServerReady(int serverVersion, int port, int serverId, boolean isTabletServer)
            throws Exception {
        boolean serverSupportAuth = serverVersion >= FLUSS_07_VERSION_MAGIC;
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", "localhost:" + port);
        if (serverSupportAuth) {
            CLIENT_SALS_PROPERTIES.forEach(conf::setString);
        }
        Connection connection = ConnectionFactory.createConnection(conf);
        connection.close();

        RpcClient rpcClient =
                RpcClient.create(
                        conf, new ClientMetricGroup(MetricRegistry.create(conf, null), "1"), false);
        MetadataUpdater metadataUpdater = new MetadataUpdater(conf, rpcClient);
        Cluster cluster =
                sendMetadataRequestAndRebuildCluster(
                        GatewayClientProxy.createGatewayProxy(
                                () ->
                                        new ServerNode(
                                                serverId,
                                                "localhost",
                                                port,
                                                isTabletServer
                                                        ? ServerType.TABLET_SERVER
                                                        : ServerType.COORDINATOR),
                                rpcClient,
                                AdminGateway.class),
                        false,
                        metadataUpdater.getCluster(),
                        null,
                        null,
                        null);
        boolean isReady =
                cluster.getCoordinatorServer() != null
                        && cluster.getAliveTabletServers().size() == 1;
        rpcClient.close();
        return isReady;
    }

    protected void initAndStartFlussServer(int serverVersion) throws Exception {
        if (serverVersion == FLUSS_06_VERSION_MAGIC) {
            initFluss06Server();
        } else if (serverVersion == FLUSS_07_VERSION_MAGIC) {
            initFluss07Server();
        } else if (serverVersion == FLUSS_LATEST_VERSION_MAGIC) {
            initFlussLatestServer();
        } else {
            throw new IllegalArgumentException("Unsupported server version: " + serverVersion);
        }

        // Start the server.
        if (flussCoordinator != null) {
            flussCoordinator.start();
        }

        if (flussTabletServer != null) {
            flussTabletServer.start();
        }

        waitUntilServerReady(serverVersion);
    }

    /**
     * After the container starts, there is a delay before the TabletServer and CoordinatorServer
     * complete their registration. For example, when registering the CoordinatorServer with
     * ZooKeeper, if the node already exists, it will retry. Therefore, we've added a waiting period
     * here. Currently, the readiness of a server is determined by whether a client can successfully
     * establish a connection to it.
     */
    private void waitUntilServerReady(int serverVersion) {
        waitUntil(
                () -> {
                    try {
                        return verifyServerReady(serverVersion, coordinatorServerPort, 0, false)
                                && verifyServerReady(serverVersion, tabletServerPort, 0, true);
                    } catch (Exception e) {
                        Thread.sleep(5000);
                        return false;
                    }
                },
                Duration.ofMinutes(2),
                "Fail to wait for the server to be ready.");
    }

    void initFluss06Server() {
        coordinatorServerPort = getAvailablePort().getPort();
        tabletServerPort = getAvailablePort().getPort();
        flussCoordinator =
                initCoordinatorServer(
                        FLUSS_06_IMAGE_TAG,
                        build06CoordinatorProperties(coordinatorServerPort),
                        ZOOKEEPER,
                        coordinatorServerPort,
                        remoteDataDirInHostServer.getAbsolutePath());
        flussTabletServer =
                initTabletServer(
                        FLUSS_06_IMAGE_TAG,
                        build06TabletServerProperties(tabletServerPort),
                        ZOOKEEPER,
                        flussCoordinator,
                        tabletServerPort,
                        localDataDirInHostServer.getAbsolutePath(),
                        remoteDataDirInHostServer.getAbsolutePath());
    }

    void initFluss07Server() {
        coordinatorServerPort = getAvailablePort().getPort();
        tabletServerPort = getAvailablePort().getPort();
        flussCoordinator =
                initCoordinatorServer(
                        FLUSS_07_IMAGE_TAG,
                        build07CoordinatorProperties(coordinatorServerPort),
                        ZOOKEEPER,
                        coordinatorServerPort,
                        remoteDataDirInHostServer.getAbsolutePath());
        flussTabletServer =
                initTabletServer(
                        FLUSS_07_IMAGE_TAG,
                        build07TabletServerProperties(tabletServerPort),
                        ZOOKEEPER,
                        flussCoordinator,
                        tabletServerPort,
                        localDataDirInHostServer.getAbsolutePath(),
                        remoteDataDirInHostServer.getAbsolutePath());
    }

    void initFlussLatestServer() throws Exception {
        // Build the latest target if it does not exist.
        if (!checkImageExists(dockerClient, latestImageName)) {
            buildDockerImageInDockerDir(latestImageName);
        }

        coordinatorServerPort = getAvailablePort().getPort();
        tabletServerPort = getAvailablePort().getPort();
        flussCoordinator =
                initCoordinatorServer(
                        latestImageName,
                        buildLatestCoordinatorProperties(coordinatorServerPort),
                        ZOOKEEPER,
                        coordinatorServerPort,
                        remoteDataDirInHostServer.getAbsolutePath());
        flussTabletServer =
                initTabletServer(
                        latestImageName,
                        buildLatestTabletServerProperties(tabletServerPort),
                        ZOOKEEPER,
                        flussCoordinator,
                        tabletServerPort,
                        localDataDirInHostServer.getAbsolutePath(),
                        remoteDataDirInHostServer.getAbsolutePath());
    }

    private static void buildDockerImageInDockerDir(String imageName) throws Exception {
        // first copy build target to docker directory.
        copyBuildTargetToDockerDir();

        File oldDir = new File(".").getAbsoluteFile();
        File dockerDir = DOCKER_DIR.toFile();
        if (!dockerDir.exists()) {
            throw new IOException(
                    "Docker directory does not exist: " + dockerDir.getAbsolutePath());
        }

        if (!DOCKER_DIR.resolve("build-target").toFile().exists()) {
            throw new IOException("Build target directory does not exist in : " + DOCKER_DIR);
        }

        ProcessBuilder processBuilder = new ProcessBuilder("docker", "build", "-t", imageName, ".");
        processBuilder.directory(dockerDir);
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();

        // read the output of the process.
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append(System.lineSeparator());
            }
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException(
                    "Docker build failed with exit code" + exitCode + ". Output:\n" + output);
        }

        // change back to the old directory.
        new ProcessBuilder("cd", oldDir.getAbsolutePath()).start();
    }

    private static void copyBuildTargetToDockerDir() throws Exception {
        Path source = BUILD_TARGET_DIR;
        Path target = DOCKER_DIR.resolve("build-target");
        if (!Files.exists(source)) {
            throw new IOException("Build target directory does not exist: " + source);
        }
        if (!Files.exists(DOCKER_DIR)) {
            throw new IOException("Docker directory does not exist: " + target);
        }

        if (Files.exists(target)) {
            FileUtils.deleteDirectory(target.toFile());
        }

        File oldDir = new File(".").getAbsoluteFile();
        File dockerDir = DOCKER_DIR.toFile();

        // Add '-rL' to copy the file itself, not the symbolic link. Otherwise, file not found error
        // will be thrown when building docker image in Github CI.
        ProcessBuilder processBuilder =
                new ProcessBuilder("cp", "-rL", source.toString(), target.toString());
        processBuilder.directory(dockerDir);
        Process process = processBuilder.start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException("File copy failed");
        }

        // change back to the old directory.
        new ProcessBuilder("cd", oldDir.getAbsolutePath()).start();
    }

    void stopServer() throws Exception {
        if (flussCoordinator == null || flussTabletServer == null) {
            return;
        }

        flussCoordinator.stop();
        flussCoordinator = null;

        // Gracefully stop the tabletServer to flush data from the pageCache to disk. Waiting 2s
        flussTabletServer.execInContainer("./bin/tablet-server.sh", "stop");
        Thread.sleep(2_000);
        flussTabletServer.stop();
        flussTabletServer = null;
    }

    protected void createDatabase(Admin admin, String dbName) throws Exception {
        admin.createDatabase(dbName, DatabaseDescriptor.EMPTY, false).get();
    }

    protected void createTable(Admin admin, TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, false).get();
    }
}
