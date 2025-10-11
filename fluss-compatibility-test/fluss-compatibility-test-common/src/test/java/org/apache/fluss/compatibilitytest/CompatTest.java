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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

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
    private static final Path DOCKER_DIR = PROJECT_ROOT.resolve("docker");
    private static final Path BUILD_TARGET_DIR = PROJECT_ROOT.resolve("build-target");

    protected static final GenericContainer<?> ZOOKEEPER =
            new GenericContainer<>(DockerImageName.parse(ZK_IMAGE_TAG))
                    .withNetwork(FLUSS_NETWORK)
                    .withNetworkAliases("zookeeper")
                    .withExposedPorts(2181)
                    .withStartupTimeout(Duration.ofSeconds(60));

    private static final GenericContainer<?> SHARED_TMPFS =
            new GenericContainer<>(DockerImageName.parse("alpine:latest"))
                    .withNetwork(FLUSS_NETWORK)
                    .withCommand("sh", "-c", "while true; do sleep 3600; done")
                    .withEnv("SHARED_DIR", "/tmp/fluss");

    private static String latestImageName;

    protected @Nullable GenericContainer<?> flussCoordinator;
    protected @Nullable GenericContainer<?> flussTabletServer;
    protected int coordinatorServerPort;
    protected int tabletServerPort;
    private DockerClient dockerClient;

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
    public void setup() {
        ZOOKEEPER.start();
        SHARED_TMPFS.start();
        dockerClient = DockerClientBuilder.getInstance().build();
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
        SHARED_TMPFS.stop();
    }

    abstract boolean verifyServerReady(int serverVersion) throws Exception;

    abstract void initFlussConnection(int serverVersion);

    abstract void initFlussAdmin();

    abstract void createDatabase(String dbName) throws Exception;

    abstract boolean tableExists(String dbName, String tableName) throws Exception;

    abstract void createTable(TestingTableDescriptor tableDescriptor) throws Exception;

    abstract void produceLog(String dbName, String tableName, List<Object[]> records)
            throws Exception;

    abstract void subscribe(
            String dbName,
            String tableName,
            @Nullable Integer partitionId,
            int bucketId,
            long offset);

    abstract List<Object[]> poll(String dbName, String tableName, Duration timeout)
            throws Exception;

    abstract void putKv(String dbName, String tableName, List<Object[]> records) throws Exception;

    abstract @Nullable Object[] lookup(String dbName, String tableName, Object[] key)
            throws Exception;

    protected void initAndStartFlussServer(int clientVersionMagic, int serverVersion)
            throws Exception {
        // For Fluss-0.6 client, we need tod disable server authentication as the client is not
        // authenticated.
        boolean enableAuthentication = clientVersionMagic >= FLUSS_07_VERSION_MAGIC;

        if (serverVersion == FLUSS_06_VERSION_MAGIC) {
            initFluss06Server();
        } else if (serverVersion == FLUSS_07_VERSION_MAGIC) {
            initFluss07Server(enableAuthentication);
        } else if (serverVersion == FLUSS_LATEST_VERSION_MAGIC) {
            initFlussLatestServer(enableAuthentication);
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
                        return verifyServerReady(serverVersion);
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
                        coordinatorServerPort);
        flussTabletServer =
                initTabletServer(
                        FLUSS_06_IMAGE_TAG,
                        build06TabletServerProperties(tabletServerPort),
                        ZOOKEEPER,
                        flussCoordinator,
                        tabletServerPort);
    }

    void initFluss07Server(boolean enableAuthentication) {
        coordinatorServerPort = getAvailablePort().getPort();
        tabletServerPort = getAvailablePort().getPort();
        flussCoordinator =
                initCoordinatorServer(
                        FLUSS_07_IMAGE_TAG,
                        build07CoordinatorProperties(coordinatorServerPort, enableAuthentication),
                        ZOOKEEPER,
                        coordinatorServerPort);
        flussTabletServer =
                initTabletServer(
                        FLUSS_07_IMAGE_TAG,
                        build07TabletServerProperties(tabletServerPort, enableAuthentication),
                        ZOOKEEPER,
                        flussCoordinator,
                        tabletServerPort);
    }

    void initFlussLatestServer(boolean enableAuthentication) throws Exception {
        // Build the latest target if it does not exist.
        if (!checkImageExists(dockerClient, latestImageName)) {
            buildDockerImageInDockerDir(latestImageName);
        }

        coordinatorServerPort = getAvailablePort().getPort();
        tabletServerPort = getAvailablePort().getPort();
        flussCoordinator =
                initCoordinatorServer(
                        latestImageName,
                        buildLatestCoordinatorProperties(
                                coordinatorServerPort, enableAuthentication),
                        ZOOKEEPER,
                        coordinatorServerPort);
        flussTabletServer =
                initTabletServer(
                        latestImageName,
                        buildLatestTabletServerProperties(tabletServerPort, enableAuthentication),
                        ZOOKEEPER,
                        flussCoordinator,
                        tabletServerPort);
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
        ProcessBuilder processBuilder = new ProcessBuilder("docker", "build", "-t", imageName, ".");
        processBuilder.directory(dockerDir);
        Process process = processBuilder.start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException("Docker build failed");
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

        ProcessBuilder processBuilder =
                new ProcessBuilder("cp", "-rf", source.toString(), target.toString());
        processBuilder.directory(dockerDir);
        Process process = processBuilder.start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException("Docker build failed");
        }

        // change back to the old directory.
        new ProcessBuilder("cd", oldDir.getAbsolutePath()).start();
    }

    void stopServer() {
        if (flussCoordinator == null || flussTabletServer == null) {
            return;
        }

        flussCoordinator.stop();
        flussCoordinator = null;
        flussTabletServer.stop();
        flussTabletServer = null;
    }
}
