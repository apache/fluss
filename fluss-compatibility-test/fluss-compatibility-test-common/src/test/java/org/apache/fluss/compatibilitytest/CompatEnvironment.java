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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Some basic param for compatibility test. */
public class CompatEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(CompatEnvironment.class);

    public static final String ZK_IMAGE_TAG = "zookeeper:3.9.2";
    public static final Network FLUSS_NETWORK = Network.newNetwork();
    public static final String TABLET_SERVER_LOCAL_DATA_DIR = "/tmp/fluss/data";
    public static final String REMOTE_DATA_DIR = "/tmp/fluss/remote-data";

    public static final Map<String, String> CLIENT_SALS_PROPERTIES = new HashMap<>();

    static {
        CLIENT_SALS_PROPERTIES.put("client.security.protocol", "SASL");
        CLIENT_SALS_PROPERTIES.put("client.security.sasl.mechanism", "PLAIN");
        CLIENT_SALS_PROPERTIES.put("client.security.sasl.username", "admin");
        CLIENT_SALS_PROPERTIES.put("client.security.sasl.password", "admin-pass");
    }

    // ------------------------------------------------------------------------------------------
    // Fluss-0.6 Variables/Utils
    // ------------------------------------------------------------------------------------------

    /**
     * Fluss 0.6 version magic for compatibility test. Fluss-0.6 is the first version to support
     * compatibility test.
     */
    public static final int FLUSS_06_VERSION_MAGIC = 0;

    public static final String FLUSS_06_IMAGE_TAG = "fluss/fluss:0.6.0";

    public static String build06CoordinatorProperties(int exposedFixedPort) {
        List<String> basicProperties =
                Arrays.asList(
                        "zookeeper.address: zookeeper:2181",
                        "coordinator.host: coordinator-server",
                        "coordinator.port: " + exposedFixedPort);
        return String.join("\n", basicProperties);
    }

    public static String build06TabletServerProperties(int exposedFixedPort) {
        List<String> basicProperties =
                Arrays.asList(
                        "zookeeper.address: zookeeper:2181",
                        "tablet-server.host: tablet-server",
                        "tablet-server.id: 0",
                        "tablet-server.port: " + exposedFixedPort,
                        "data.dir: " + TABLET_SERVER_LOCAL_DATA_DIR,
                        "remote.data.dir: " + REMOTE_DATA_DIR);
        return String.join("\n", basicProperties);
    }

    // ------------------------------------------------------------------------------------------
    // Fluss-0.7 Variables/Utils
    // ------------------------------------------------------------------------------------------
    public static final int FLUSS_07_VERSION_MAGIC = 1;
    public static final String FLUSS_07_IMAGE_TAG = "fluss/fluss:0.7.0";

    public static String build07CoordinatorProperties(int exposedFixedPort) {
        List<String> basicProperties =
                new ArrayList<>(
                        Arrays.asList(
                                "zookeeper.address: zookeeper:2181",
                                "bind.listeners: INTERNAL://coordinator-server:0, CLIENT://coordinator-server:"
                                        + exposedFixedPort,
                                "advertised.listeners: CLIENT://localhost:" + exposedFixedPort,
                                "internal.listener.name: INTERNAL",
                                "datalake.format: paimon",
                                "datalake.paimon.metastore: filesystem",
                                "datalake.paimon.warehouse: /tmp/paimon"));
        basicProperties.addAll(
                Arrays.asList(
                        "security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT",
                        "security.sasl.enabled.mechanisms: PLAIN",
                        "security.sasl.plain.jaas.config: com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule "
                                + "required user_admin=\"admin-pass\" "
                                + "user_developer=\"developer-pass\" "
                                + "user_consumer=\"consumer-pass\";",
                        "authorizer.enabled: true",
                        "super.users: User:admin"));

        return String.join("\n", basicProperties);
    }

    public static String build07TabletServerProperties(int exposedFixedPort) {
        List<String> basicProperties =
                Arrays.asList(
                        "zookeeper.address: zookeeper:2181",
                        "bind.listeners: INTERNAL://tablet-server:0, CLIENT://tablet-server:"
                                + exposedFixedPort,
                        "advertised.listeners: CLIENT://localhost:" + exposedFixedPort,
                        "internal.listener.name: INTERNAL",
                        "tablet-server.id: 0",
                        "data.dir: " + TABLET_SERVER_LOCAL_DATA_DIR,
                        "remote.data.dir: " + REMOTE_DATA_DIR,
                        "datalake.format: paimon",
                        "datalake.paimon.metastore: filesystem",
                        "datalake.paimon.warehouse: /tmp/paimon",
                        "security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT",
                        "security.sasl.enabled.mechanisms: PLAIN",
                        "security.sasl.plain.jaas.config: com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule "
                                + "required user_admin=\"admin-pass\" "
                                + "user_developer=\"developer-pass\" "
                                + "user_consumer=\"consumer-pass\";",
                        "authorizer.enabled: true",
                        "super.users: User:admin");
        return String.join("\n", basicProperties);
    }

    // ------------------------------------------------------------------------------------------
    // Fluss latest version Variables/Utils
    // ------------------------------------------------------------------------------------------

    public static final int FLUSS_LATEST_VERSION_MAGIC = 2;

    public static String buildLatestCoordinatorProperties(int exposedFixedPort) {
        List<String> basicProperties =
                Arrays.asList(
                        "zookeeper.address: zookeeper:2181",
                        "bind.listeners: INTERNAL://coordinator-server:0, CLIENT://coordinator-server:"
                                + exposedFixedPort,
                        "advertised.listeners: CLIENT://localhost:" + exposedFixedPort,
                        "internal.listener.name: INTERNAL",
                        "datalake.format: paimon",
                        "datalake.paimon.metastore: filesystem",
                        "datalake.paimon.warehouse: /tmp/paimon",
                        "security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT",
                        "security.sasl.enabled.mechanisms: PLAIN",
                        "security.sasl.plain.jaas.config: org.apache.fluss.security.auth.sasl.plain.PlainLoginModule "
                                + "required user_admin=\"admin-pass\" "
                                + "user_developer=\"developer-pass\" "
                                + "user_consumer=\"consumer-pass\";",
                        "authorizer.enabled: true",
                        "super.users: User:admin");
        return String.join("\n", basicProperties);
    }

    public static String buildLatestTabletServerProperties(int exposedFixedPort) {
        List<String> basicProperties =
                new ArrayList<>(
                        Arrays.asList(
                                "zookeeper.address: zookeeper:2181",
                                "bind.listeners: INTERNAL://tablet-server:0, CLIENT://tablet-server:"
                                        + exposedFixedPort,
                                "advertised.listeners: CLIENT://localhost:" + exposedFixedPort,
                                "internal.listener.name: INTERNAL",
                                "tablet-server.id: 0",
                                "data.dir: " + TABLET_SERVER_LOCAL_DATA_DIR,
                                "remote.data.dir: " + REMOTE_DATA_DIR,
                                "datalake.format: paimon",
                                "datalake.paimon.metastore: filesystem",
                                "datalake.paimon.warehouse: /tmp/paimon"));

        basicProperties.addAll(
                Arrays.asList(
                        "security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT",
                        "security.sasl.enabled.mechanisms: PLAIN",
                        "security.sasl.plain.jaas.config: org.apache.fluss.security.auth.sasl.plain.PlainLoginModule "
                                + "required user_admin=\"admin-pass\" "
                                + "user_developer=\"developer-pass\" "
                                + "user_consumer=\"consumer-pass\";",
                        "authorizer.enabled: true",
                        "super.users: User:admin"));

        return String.join("\n", basicProperties);
    }

    // ------------------------------------------------------------------------------------------
    // Common utils
    // ------------------------------------------------------------------------------------------

    public static GenericContainer<?> initCoordinatorServer(
            String flussImage,
            String coordinatorProperties,
            GenericContainer<?> zkContainer,
            int fixedExposedPort,
            String remoteDataDirInHost) {
        return new FixedHostPortGenericContainer<>(flussImage)
                .withNetwork(FLUSS_NETWORK)
                .withEnv("FLUSS_PROPERTIES", coordinatorProperties)
                .withCommand("coordinatorServer")
                .withNetworkAliases("coordinator-server")
                .withExposedPorts(fixedExposedPort)
                .withFixedExposedPort(fixedExposedPort, fixedExposedPort)
                .withFileSystemBind(remoteDataDirInHost, REMOTE_DATA_DIR, BindMode.READ_WRITE)
                .dependsOn(zkContainer)
                .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    public static GenericContainer<?> initTabletServer(
            String flussImage,
            String tabletServerProperties,
            GenericContainer<?> zkContainer,
            GenericContainer<?> coordinatorContainer,
            int fixedExposedPort,
            String localDataDirInHost,
            String remoteDataDirInHost) {
        return new FixedHostPortGenericContainer<>(flussImage)
                .withNetwork(FLUSS_NETWORK)
                .withEnv("FLUSS_PROPERTIES", tabletServerProperties)
                .withCommand("tabletServer")
                .withNetworkAliases("tablet-server")
                .withExposedPorts(fixedExposedPort)
                .withFixedExposedPort(fixedExposedPort, fixedExposedPort)
                .withFileSystemBind(
                        localDataDirInHost, TABLET_SERVER_LOCAL_DATA_DIR, BindMode.READ_WRITE)
                .withFileSystemBind(remoteDataDirInHost, REMOTE_DATA_DIR, BindMode.READ_WRITE)
                .dependsOn(zkContainer, coordinatorContainer)
                .withLogConsumer(new Slf4jLogConsumer(LOG));
    }
}
