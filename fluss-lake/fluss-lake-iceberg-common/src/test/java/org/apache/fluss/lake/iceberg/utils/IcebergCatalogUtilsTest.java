/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.utils;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.IcebergLakeCatalog;
import org.apache.fluss.lake.iceberg.source.IcebergSplitPlanner;
import org.apache.fluss.lake.iceberg.tiering.IcebergCatalogProvider;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.IOUtils;

import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ConfigResponseParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IcebergCatalogUtils}. */
class IcebergCatalogUtilsTest {

    private HttpServer mockRestServer;
    private String restCatalogUri;
    private AtomicInteger configRequestCount;
    private AtomicReference<ConfigResponse> configResponseRef;

    @BeforeEach
    void startMockRestServer() throws IOException {
        configRequestCount = new AtomicInteger(0);
        configResponseRef = new AtomicReference<>(ConfigResponse.builder().build());

        mockRestServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        mockRestServer.createContext(
                "/v1/config",
                exchange -> {
                    if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                        exchange.sendResponseHeaders(405, -1);
                        exchange.close();
                        return;
                    }
                    configRequestCount.incrementAndGet();
                    byte[] body =
                            ConfigResponseParser.toJson(configResponseRef.get())
                                    .getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, body.length);
                    try (OutputStream out = exchange.getResponseBody()) {
                        out.write(body);
                    }
                });
        mockRestServer.start();
        restCatalogUri = "http://127.0.0.1:" + mockRestServer.getAddress().getPort();
    }

    @AfterEach
    void stopMockRestServer() {
        if (mockRestServer != null) {
            mockRestServer.stop(0);
        }
    }

    @Test
    void testRestCatalogWithoutScanPlanningMode() throws Exception {
        Configuration conf = baseRestConfig();
        try (RESTCatalog catalog = (RESTCatalog) IcebergCatalogUtils.createIcebergCatalog(conf)) {
            assertThat(catalog).isInstanceOf(RESTCatalog.class);
            assertThat(catalog.properties()).doesNotContainKey("scan-planning-mode");
        }
        assertThat(configRequestCount.get()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"client", "CLIENT", "Client"})
    void testRestCatalogWithClientScanPlanningMode(String mode) throws Exception {
        Configuration conf = baseRestConfig();
        conf.setString("scan-planning-mode", mode);
        try (RESTCatalog catalog = (RESTCatalog) IcebergCatalogUtils.createIcebergCatalog(conf)) {
            assertThat(catalog.properties()).containsEntry("scan-planning-mode", mode);
        }
        assertThat(configRequestCount.get()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"server", "SERVER", "Server"})
    void testRestCatalogRejectsExplicitServerScanPlanningMode(String mode) {
        Configuration conf = baseRestConfig();
        conf.setString("name", "test_rest_catalog");
        conf.setString("token", "super-secret-token");
        conf.setString("scan-planning-mode", mode);

        assertThatThrownBy(() -> IcebergCatalogUtils.createIcebergCatalog(conf))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Server-side scan planning is not supported by Fluss")
                .hasMessageContaining("test_rest_catalog")
                .hasMessageContaining("scan-planning-mode=" + mode)
                .hasMessageContaining("datalake.iceberg.scan-planning-mode: client")
                .hasMessageContaining("server.yaml")
                .satisfies(
                        throwable ->
                                assertThat(throwable.getMessage())
                                        .doesNotContain("super-secret-token"));
        assertThat(configRequestCount.get()).isEqualTo(1);
    }

    @Test
    void testRestCatalogRejectsServerDefaultWithoutClientOverride() {
        configResponseRef.set(
                ConfigResponse.builder().withDefault("scan-planning-mode", "server").build());
        Configuration conf = baseRestConfig();

        assertThatThrownBy(() -> IcebergCatalogUtils.createIcebergCatalog(conf))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("scan-planning-mode=server");

        assertThatThrownBy(() -> new IcebergLakeCatalog(conf))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("scan-planning-mode=server");

        assertThatThrownBy(() -> new IcebergCatalogProvider(conf).get())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("scan-planning-mode=server");

        assertThatThrownBy(
                        () ->
                                new IcebergSplitPlanner(conf, TablePath.of("db", "tbl"), 1L, null)
                                        .plan())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("scan-planning-mode=server");
    }

    @Test
    void testRestCatalogAllowsClientOverrideOfServerDefault() throws Exception {
        configResponseRef.set(
                ConfigResponse.builder().withDefault("scan-planning-mode", "server").build());
        Configuration conf = baseRestConfig();
        conf.setString("scan-planning-mode", "client");

        try (RESTCatalog catalog = (RESTCatalog) IcebergCatalogUtils.createIcebergCatalog(conf)) {
            assertThat(catalog.properties()).containsEntry("scan-planning-mode", "client");
        }
        assertThat(configRequestCount.get()).isEqualTo(1);
    }

    @Test
    void testRestCatalogRejectsServerOverrideOfClientMode() {
        configResponseRef.set(
                ConfigResponse.builder().withOverride("scan-planning-mode", "server").build());
        Configuration conf = baseRestConfig();
        conf.setString("scan-planning-mode", "client");

        assertThatThrownBy(() -> IcebergCatalogUtils.createIcebergCatalog(conf))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("scan-planning-mode=server");
        assertThat(configRequestCount.get()).isEqualTo(1);
    }

    @Test
    void testRestCatalogAllowsServerOverridePermittingClientMode() throws Exception {
        configResponseRef.set(
                ConfigResponse.builder().withOverride("scan-planning-mode", "client").build());
        Configuration conf = baseRestConfig();
        conf.setString("scan-planning-mode", "server");

        try (RESTCatalog catalog = (RESTCatalog) IcebergCatalogUtils.createIcebergCatalog(conf)) {
            assertThat(catalog.properties()).containsEntry("scan-planning-mode", "client");
        }
        assertThat(configRequestCount.get()).isEqualTo(1);
    }

    @Test
    void testRestCatalogRejectsUnknownScanPlanningMode() {
        configResponseRef.set(
                ConfigResponse.builder()
                        .withOverride("scan-planning-mode", "unknown_mode")
                        .build());
        Configuration overrideConf = baseRestConfig();
        assertThatThrownBy(() -> IcebergCatalogUtils.createIcebergCatalog(overrideConf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Invalid scan planning mode: unknown_mode. Valid values are: client, server");

        configResponseRef.set(ConfigResponse.builder().build());
        Configuration clientConf = baseRestConfig();
        clientConf.setString("scan-planning-mode", "unknown_mode");
        assertThatThrownBy(() -> IcebergCatalogUtils.createIcebergCatalog(clientConf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Invalid scan planning mode: unknown_mode. Valid values are: client, server");
    }

    @Test
    void testNonRestCatalogPreservesExistingBehavior(@TempDir File tempWarehouseDir) {
        Configuration inMemoryConf = new Configuration();
        inMemoryConf.setString("warehouse", tempWarehouseDir.toURI().toString());
        inMemoryConf.setString("catalog-impl", InMemoryCatalog.class.getName());
        inMemoryConf.setString("name", "in_memory_catalog");
        inMemoryConf.setString("scan-planning-mode", "server");

        Catalog inMemoryCatalog = IcebergCatalogUtils.createIcebergCatalog(inMemoryConf);
        try {
            assertThat(inMemoryCatalog).isInstanceOf(InMemoryCatalog.class);
        } finally {
            closeIfAutoCloseable(inMemoryCatalog);
        }

        Configuration hadoopConf = new Configuration();
        hadoopConf.setString("type", "hadoop");
        hadoopConf.setString("warehouse", new File(tempWarehouseDir, "hadoop").toURI().toString());
        hadoopConf.setString("scan-planning-mode", "server");

        Catalog hadoopCatalog = IcebergCatalogUtils.createIcebergCatalog(hadoopConf);
        try {
            assertThat(hadoopCatalog.name())
                    .isEqualTo(IcebergCatalogUtils.ICEBERG_CATALOG_DEFAULT_NAME);
        } finally {
            closeIfAutoCloseable(hadoopCatalog);
        }
        assertThat(configRequestCount.get()).isZero();
    }

    @Test
    void testClosesRestCatalogWhenValidationFails() {
        TrackingRestCatalog.CLOSE_COUNT.set(0);
        configResponseRef.set(
                ConfigResponse.builder().withOverride("scan-planning-mode", "server").build());
        Configuration conf = new Configuration();
        conf.setString("catalog-impl", TrackingRestCatalog.class.getName());
        conf.setString("uri", restCatalogUri);

        assertThatThrownBy(() -> IcebergCatalogUtils.createIcebergCatalog(conf))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("scan-planning-mode=server");
        assertThat(TrackingRestCatalog.CLOSE_COUNT.get()).isEqualTo(1);
    }

    @Test
    void testDoesNotMaskValidationErrorWhenCloseFails() {
        configResponseRef.set(
                ConfigResponse.builder().withOverride("scan-planning-mode", "server").build());
        Configuration conf = new Configuration();
        conf.setString("catalog-impl", FailingCloseRestCatalog.class.getName());
        conf.setString("uri", restCatalogUri);

        assertThatThrownBy(() -> IcebergCatalogUtils.createIcebergCatalog(conf))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("scan-planning-mode=server");
    }

    private Configuration baseRestConfig() {
        Configuration conf = new Configuration();
        conf.setString("type", "rest");
        conf.setString("uri", restCatalogUri);
        return conf;
    }

    private static void closeIfAutoCloseable(Catalog catalog) {
        if (catalog instanceof AutoCloseable) {
            IOUtils.closeQuietly((AutoCloseable) catalog, "test-iceberg-catalog");
        }
    }

    /** Test {@link RESTCatalog} subclass that tracks {@link #close()} invocations. */
    public static class TrackingRestCatalog extends RESTCatalog {

        static final AtomicInteger CLOSE_COUNT = new AtomicInteger(0);

        /** Creates a tracking REST catalog instance for reflection-based catalog loading. */
        public TrackingRestCatalog() {
            super();
        }

        @Override
        public void close() throws IOException {
            CLOSE_COUNT.incrementAndGet();
            super.close();
        }
    }

    /** Test {@link RESTCatalog} subclass whose {@link #close()} throws an {@link IOException}. */
    public static class FailingCloseRestCatalog extends RESTCatalog {

        /** Creates a failing-close REST catalog instance for reflection-based catalog loading. */
        public FailingCloseRestCatalog() {
            super();
        }

        @Override
        public void close() throws IOException {
            super.close();
            throw new IOException("Simulated close failure");
        }
    }
}
