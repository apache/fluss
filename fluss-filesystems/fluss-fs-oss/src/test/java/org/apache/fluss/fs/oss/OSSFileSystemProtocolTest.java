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

package org.apache.fluss.fs.oss;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystemFailure;
import org.apache.fluss.fs.FileSystemOperationException;
import org.apache.fluss.fs.FileSystemPathNotFoundException;
import org.apache.fluss.fs.FsPath;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Exercises the OSS adapter through Hadoop and the OSS SDK using a local HTTP server. */
class OSSFileSystemProtocolTest {

    @Test
    void missingBucketIsNotMistakenForADeletedDirectory() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server =
                startServer(
                        exchange -> {
                            requests.incrementAndGet();
                            respondError(exchange, 404, "NoSuchBucket");
                        });
        try {
            OSSFileSystem fs = createFileSystem(server);
            Throwable thrown =
                    catchThrowable(() -> fs.listStatus(new FsPath("oss://missing-bucket/table")));
            assertThat(requests.get()).isGreaterThan(0);
            assertThat(thrown)
                    .isInstanceOf(FileSystemOperationException.class)
                    .satisfies(
                            failure -> {
                                FileSystemFailure normalized = (FileSystemFailure) failure;
                                assertThat(normalized.kind())
                                        .isEqualTo(FileSystemFailure.Kind.NOT_FOUND);
                                assertThat(normalized.resource())
                                        .isEqualTo(FileSystemFailure.Resource.ROOT);
                                assertThat(normalized.requestId()).isEqualTo("local-request-1");
                            });
        } finally {
            server.stop(0);
        }
    }

    @Test
    void deniedMetadataRequestFollowedByEmptyListingDoesNotConfirmPathAbsence() throws Exception {
        AtomicInteger metadataRequests = new AtomicInteger();
        AtomicInteger listRequests = new AtomicInteger();
        HttpServer server =
                startServer(
                        exchange -> {
                            if ("HEAD".equals(exchange.getRequestMethod())) {
                                metadataRequests.incrementAndGet();
                                respondError(exchange, 403, "AccessDenied");
                            } else {
                                listRequests.incrementAndGet();
                                byte[] body =
                                        ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                                                        + "<ListBucketResult><Name>missing-bucket</Name>"
                                                        + "<Prefix>table/</Prefix><KeyCount>0</KeyCount>"
                                                        + "<MaxKeys>1000</MaxKeys>"
                                                        + "<IsTruncated>false</IsTruncated>"
                                                        + "</ListBucketResult>")
                                                .getBytes(StandardCharsets.UTF_8);
                                respond(exchange, 200, body);
                            }
                        });
        try {
            OSSFileSystem fs = createFileSystem(server);
            assertThatThrownBy(() -> fs.listStatus(new FsPath("oss://missing-bucket/table")))
                    .isInstanceOf(FileSystemFailure.class)
                    .isNotInstanceOf(FileSystemPathNotFoundException.class)
                    .satisfies(
                            failure -> {
                                FileSystemFailure normalized = (FileSystemFailure) failure;
                                assertThat(normalized.resource())
                                        .isNotEqualTo(FileSystemFailure.Resource.PATH);
                            });
            assertThat(metadataRequests.get()).isGreaterThan(0);
            assertThat(listRequests.get()).isGreaterThan(0);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void deniedListingRetainsTheServiceError() throws Exception {
        HttpServer server =
                startServer(
                        exchange ->
                                respondError(
                                        exchange,
                                        "HEAD".equals(exchange.getRequestMethod()) ? 404 : 403,
                                        "HEAD".equals(exchange.getRequestMethod())
                                                ? "NoSuchKey"
                                                : "AccessDenied"));
        try {
            OSSFileSystem fs = createFileSystem(server);
            assertThatThrownBy(() -> fs.listStatus(new FsPath("oss://missing-bucket/table")))
                    .isInstanceOf(FileSystemOperationException.class)
                    .satisfies(
                            failure -> {
                                FileSystemFailure normalized = (FileSystemFailure) failure;
                                assertThat(normalized.kind())
                                        .isEqualTo(FileSystemFailure.Kind.PERMISSION_DENIED);
                                assertThat(normalized.resource())
                                        .isEqualTo(FileSystemFailure.Resource.UNKNOWN);
                                assertThat(normalized.serviceCode()).isEqualTo("AccessDenied");
                                assertThat(normalized.requestId()).isEqualTo("local-request-1");
                            });
        } finally {
            server.stop(0);
        }
    }

    @Test
    void throttledListingIsReportedAsTemporary() throws Exception {
        HttpServer server =
                startServer(
                        exchange ->
                                respondError(
                                        exchange,
                                        "HEAD".equals(exchange.getRequestMethod()) ? 404 : 503,
                                        "HEAD".equals(exchange.getRequestMethod())
                                                ? "NoSuchKey"
                                                : "TotalQpsLimitExceeded"));
        try {
            OSSFileSystem fs = createFileSystem(server);
            assertThatThrownBy(() -> fs.listStatus(new FsPath("oss://missing-bucket/table")))
                    .isInstanceOf(FileSystemOperationException.class)
                    .satisfies(
                            failure -> {
                                FileSystemFailure normalized = (FileSystemFailure) failure;
                                assertThat(normalized.kind())
                                        .isEqualTo(FileSystemFailure.Kind.RATE_LIMITED);
                                assertThat(normalized.isTemporary()).isTrue();
                                assertThat(normalized.serviceCode())
                                        .isEqualTo("TotalQpsLimitExceeded");
                            });
        } finally {
            server.stop(0);
        }
    }

    private static OSSFileSystem createFileSystem(HttpServer server) throws IOException {
        Configuration config = new Configuration();
        config.setString("fs.oss.endpoint", "127.0.0.1:" + server.getAddress().getPort());
        config.setString("fs.oss.region", "cn-hangzhou");
        config.setString("fs.oss.accessKeyId", "test-key");
        config.setString("fs.oss.accessKeySecret", "test-secret");
        config.setString("fs.oss.connection.secure.enabled", "false");
        config.setString("fs.oss.attempts.maximum", "0");
        return (OSSFileSystem)
                new OSSFileSystemPlugin()
                        .create(new FsPath("oss://missing-bucket/").toUri(), config);
    }

    private static HttpServer startServer(HttpHandler handler) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", handler);
        server.start();
        return server;
    }

    private static void respondError(HttpExchange exchange, int status, String code)
            throws IOException {
        byte[] body =
                ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                                + "<Error><Code>"
                                + code
                                + "</Code><Message>Test error</Message>"
                                + "<RequestId>local-request-1</RequestId></Error>")
                        .getBytes(StandardCharsets.UTF_8);
        respond(exchange, status, body);
    }

    private static void respond(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/xml");
        exchange.getResponseHeaders().set("x-oss-request-id", "local-request-1");
        // Keep the SDK from reusing a connection after the HEAD response has no body.
        exchange.getResponseHeaders().set("Connection", "close");
        if ("HEAD".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(status, -1);
        } else {
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
        }
        exchange.close();
    }
}
