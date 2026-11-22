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

package org.apache.fluss.fs.s3;

import org.apache.fluss.config.Configuration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/** A SeaweedFS S3-compatible test backend with a fixed image and bounded startup wait. */
class SeaweedFsTestContainer extends GenericContainer<SeaweedFsTestContainer> {

    private static final DockerImageName SEAWEEDFS_IMAGE =
            DockerImageName.parse("chrislusf/seaweedfs:4.38");
    private static final int S3_PORT = 8333;
    private static final String ACCESS_KEY = "fluss-test-access";
    private static final String SECRET_KEY = "fluss-test-secret";
    private static final String BUCKET = "fluss-s3-test";

    SeaweedFsTestContainer() {
        super(SEAWEEDFS_IMAGE);

        withExposedPorts(S3_PORT);
        withEnv("AWS_ACCESS_KEY_ID", ACCESS_KEY);
        withEnv("AWS_SECRET_ACCESS_KEY", SECRET_KEY);
        withCommand("mini", "-s3.port=" + S3_PORT, "-dir=/data", "-bucket=" + BUCKET);

        // "weed mini" emits this only after every component and the bucket are ready.
        waitingFor(
                new LogMessageWaitStrategy()
                        .withRegEx("(?s).*All enabled components are running and ready to use.*")
                        .withStartupTimeout(Duration.ofMinutes(2)));
    }

    Configuration createS3Configuration() {
        Configuration configuration = new Configuration();
        configuration.setString("s3.endpoint", getEndpoint());
        configuration.setString("s3.access-key", ACCESS_KEY);
        configuration.setString("s3.secret-key", SECRET_KEY);
        configuration.setString("s3.region", "us-east-1");
        configuration.setString("s3.path-style-access", "true");
        configuration.setString("s3.connection.ssl.enabled", "false");
        return configuration;
    }

    String getBucketUri() {
        return "s3://" + BUCKET + "/";
    }

    private String getEndpoint() {
        return String.format("http://%s:%d", getHost(), getMappedPort(S3_PORT));
    }
}
