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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Testcontainers wrapper for a MinIO server.
 *
 * <p>Exposes a single HTTP endpoint that multiplexes both the S3 and the STS APIs. This matches how MinIO
 * deployments are typically run in production.
 */
public class MinioTestContainer extends GenericContainer<MinioTestContainer> {

    public static final String DEFAULT_IMAGE = "minio/minio:RELEASE.2024-10-13T13-34-11Z";
    public static final int PORT = 9000;

    public static final String ROOT_USER = "minioadmin";
    public static final String ROOT_PASSWORD = "minioadmin";
    public static final String DEFAULT_REGION = "us-east-1";

    /**
     * Dummy ARN role.
     *
     * <p>MinIO STS only supports {@code AssumeRole}, and accepts any ARN.
     */
    public static final String DUMMY_ROLE_ARN = "arn:aws:iam::000000000000:role/fluss-it";

    public MinioTestContainer() {
        this(DEFAULT_IMAGE);
    }

    public MinioTestContainer(String image) {
        super(DockerImageName.parse(image));
        withEnv("MINIO_ROOT_USER", ROOT_USER);
        withEnv("MINIO_ROOT_PASSWORD", ROOT_PASSWORD);
        // MINIO_REGION makes STS return credentials scoped to the same region as the client.
        withEnv("MINIO_REGION", DEFAULT_REGION);
        withCommand("server", "/data");
        withExposedPorts(PORT);
        waitingFor(Wait.forHttp("/minio/health/ready").forPort(PORT).forStatusCode(200));
    }

    /** Returns the HTTP endpoint of the MinIO server (e.g. {@code http://host:12345}). */
    public String getEndpoint() {
        return "http://" + getHost() + ":" + getMappedPort(PORT);
    }

    /**
     * Returns the STS endpoint.
     *
     * <p>MinIO serves STS on the same listener as S3, so this is identical to {@link #getEndpoint()}.
     */
    public String getStsEndpoint() {
        return getEndpoint();
    }

    /** Creates the given bucket using the MinIO root credentials. */
    public void createBucket(String bucket) {
        AmazonS3 s3 = buildS3Client(ROOT_USER, ROOT_PASSWORD);
        try {
            if (!s3.doesBucketExistV2(bucket)) {
                s3.createBucket(bucket);
            }
        } finally {
            s3.shutdown();
        }
    }

    /** Builds an AWS SDK client targeting this MinIO instance. */
    public AmazonS3 buildS3Client(final String accessKey, final String secretKey) {
        return AmazonS3ClientBuilder.standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(getEndpoint(), DEFAULT_REGION))
                .withCredentials(
                        new AWSStaticCredentialsProvider(
                                new BasicAWSCredentials(accessKey, secretKey)))
                .build();
    }
}
