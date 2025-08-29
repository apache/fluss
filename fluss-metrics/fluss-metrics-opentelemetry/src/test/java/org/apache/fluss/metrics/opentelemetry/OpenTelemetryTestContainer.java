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

package org.apache.fluss.metrics.opentelemetry;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.dockerfile.DockerfileBuilder;
import org.testcontainers.images.builder.dockerfile.statement.MultiArgsStatement;
import org.testcontainers.utility.Base58;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** {@link OpenTelemetryTestContainer} provides an OpenTelemetry test instance. */
public class OpenTelemetryTestContainer extends GenericContainer<OpenTelemetryTestContainer> {
    private static final String ALPINE_DOCKER_IMAGE_TAG = "3.22.0";
    private static final String OPENTELEMETRY_COLLECTOR_DOCKER_IMAGE_TAG = "0.128.0";

    private static final int DEFAULT_GRPC_PORT = 4317;

    // must be kept in sync with opentelemetry-config.yaml
    private static final String DATA_DIR = "/data";
    // must be kept in sync with opentelemetry-config.yaml
    private static final String LOG_FILE = "logs.json";

    private static final Path CONFIG_PATH =
            Paths.get("src/test/resources/").resolve("opentelemetry-config.yaml");

    public OpenTelemetryTestContainer() {
        super(
                new ImageFromDockerfile("fluss/opentelemetry-collector-test-container")
                        .withDockerfileFromBuilder(
                                OpenTelemetryTestContainer::buildOpenTelemetryCollectorImage));
        withNetworkAliases(randomString("opentelemetry-collector", 6));
        addExposedPort(DEFAULT_GRPC_PORT);
        withCopyFileToContainer(
                MountableFile.forHostPath(CONFIG_PATH.toString()), "opentelemetry-config.yaml");
        withCommand("--config", "opentelemetry-config.yaml");
    }

    private static void buildOpenTelemetryCollectorImage(DockerfileBuilder builder) {
        builder
                // OpenTelemetry image doesn't have mkdir - use alpine instead.
                .from(constructFullDockerImageName("alpine", ALPINE_DOCKER_IMAGE_TAG))
                // Create the output data directory - OpenTelemetry image doesn't have any directory
                // to write to on its own.
                .run("mkdir -p " + DATA_DIR)
                .from(
                        constructFullDockerImageName(
                                "otel/opentelemetry-collector",
                                OPENTELEMETRY_COLLECTOR_DOCKER_IMAGE_TAG))
                // Copy the output data directory from alpine. It has to be owned by the
                // OpenTelemetry user.
                .withStatement(
                        new MultiArgsStatement("COPY --from=0 --chown=10001", DATA_DIR, DATA_DIR))
                .build();
    }

    public Path getOutputLogPath() {
        return Paths.get(DATA_DIR, LOG_FILE);
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        super.containerIsStarted(containerInfo);
    }

    private static String randomString(String prefix, int length) {
        return String.format("%s-%s", prefix, Base58.randomString(length).toLowerCase(Locale.ROOT));
    }

    public String getGrpcEndpoint() {
        return String.format("http://%s:%s", getHost(), getMappedPort(DEFAULT_GRPC_PORT));
    }

    private static String constructFullDockerImageName(String name, String tag) {
        return name + ":" + tag;
    }
}
