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

package org.apache.fluss.fs.s3.minio;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FsPath;

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A shared base for MinIO based S3 integration tests.
 */
@Testcontainers
abstract class MinioITBase {

    @Container
    protected static final MinioTestContainer MINIO = new MinioTestContainer();

    protected static String bucket;

    @BeforeAll
    static void assumeDockerAndProvisionBucket() {
        assumeTrue(
                DockerClientFactory.instance().isDockerAvailable(),
                "Docker is not available; skipping MinIO integration tests.");

        bucket = "fluss-it-" + UUID.randomUUID().toString().substring(0, 8);
        MINIO.createBucket(bucket);
    }

    /**
     * Populates a {@link Configuration} with basic settings.
     *
     * <p>It adds endpoint, region, and path-style settings needed for the Fluss S3 filesystem to talk to the MinIO
     * container.
     */
    protected static Configuration getMinioFlussConfig() {
        Configuration conf = new Configuration();
        conf.setString("fs.s3a.endpoint", MINIO.getEndpoint());
        conf.setString("fs.s3a.region", MinioTestContainer.DEFAULT_REGION);
        conf.setString("fs.s3a.path.style.access", "true");
        return conf;
    }

    /** Returns an {@link FsPath} within the MinIO test bucket. */
    protected static FsPath getBasePath(String suffix) {
        return new FsPath("s3://" + bucket + "/" + suffix);
    }
}
