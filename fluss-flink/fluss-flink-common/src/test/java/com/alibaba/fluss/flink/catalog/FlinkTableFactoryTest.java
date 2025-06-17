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

package com.alibaba.fluss.flink.catalog;

import com.alibaba.fluss.flink.FlinkConnectorOptions;
import com.alibaba.fluss.flink.sink.FlinkTableSink;
import com.alibaba.fluss.flink.source.FlinkTableSource;
import com.alibaba.fluss.flink.source.lookup.FlinkAsyncLookupFunction;
import com.alibaba.fluss.flink.source.lookup.FlinkLookupFunction;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.runtime.connector.source.LookupRuntimeProviderContext;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static com.alibaba.fluss.flink.FlinkConnectorOptions.BUCKET_KEY;
import static com.alibaba.fluss.flink.FlinkConnectorOptions.BUCKET_NUMBER;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_MAX_ROWS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkTableFactory}. */
abstract class FlinkTableFactoryTest {

    public static final ObjectIdentifier OBJECT_IDENTIFIER =
            ObjectIdentifier.of("default", "default", "t1");

    @Test
    void testTableSourceOptions() {
        ResolvedSchema schema = createBasicSchema();
        Map<String, String> validProperties = getBasicOptions();
        validProperties.put("k1", "v1");

        // test invalid options
        assertThatThrownBy(() -> createTableSource(schema, validProperties))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options:\n" + "\n" + "k1");

        // test scan startup mode options
        Map<String, String> scanModeProperties = getBasicOptions();
        scanModeProperties.put(FlinkConnectorOptions.SCAN_STARTUP_MODE.key(), "timestamp");
        assertThatThrownBy(() -> createTableSource(schema, scanModeProperties))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "'scan.startup.timestamp' is required int 'timestamp' startup mode but missing.");
        scanModeProperties.put(FlinkConnectorOptions.SCAN_STARTUP_TIMESTAMP.key(), "1678883047356");
        createTableSource(schema, scanModeProperties);
        scanModeProperties.put(
                FlinkConnectorOptions.SCAN_STARTUP_TIMESTAMP.key(), "2023-12-09 23:09:12");
        createTableSource(schema, scanModeProperties);

        // test datalake options
        Map<String, String> datalakeProperties = getBasicOptions();
        datalakeProperties.put("table.datalake.format", "paimon");
        datalakeProperties.put("paimon.file.format", "parquet");
        createTableSource(schema, datalakeProperties);
    }

    @Test
    void testScanSource() {
        ResolvedSchema schema = createBasicSchema();
        Map<String, String> properties = getBasicOptionsWithBucketKey();

        FlinkTableSource tableSource = (FlinkTableSource) createTableSource(schema, properties);

        // test bucket key
        assertThat(tableSource.getBucketKeyIndexes()).isEqualTo(new int[] {0});
        // test primary key
        assertThat(tableSource.getPrimaryKeyIndexes()).isEqualTo(new int[] {0, 2});

        // test partition key
        assertThat(tableSource.getPrimaryKeyIndexes()).isEqualTo(new int[] {0, 2});
    }

    @Test
    void testLookupSource() {
        ResolvedSchema schema = createBasicSchema();
        Map<String, String> properties = getBasicOptionsWithBucketKey();
        properties.put("lookup.cache", "partial");
        properties.put(PARTIAL_CACHE_EXPIRE_AFTER_ACCESS.key(), "18000");
        properties.put(PARTIAL_CACHE_EXPIRE_AFTER_WRITE.key(), "36000");
        properties.put(PARTIAL_CACHE_MAX_ROWS.key(), "100000");
        properties.put(PARTIAL_CACHE_CACHE_MISSING_KEY.key(), "false");

        // test cache
        FlinkTableSource tableSource = (FlinkTableSource) createTableSource(schema, properties);
        DefaultLookupCache cache = (DefaultLookupCache) tableSource.getCache();
        DefaultLookupCache expectedCache =
                DefaultLookupCache.newBuilder()
                        .expireAfterAccess(Duration.ofMillis(18000))
                        .expireAfterWrite(Duration.ofMillis(36000))
                        .maximumSize(100000)
                        .cacheMissingKey(false)
                        .build();
        assertThat(cache).isEqualTo(expectedCache);

        // test async
        properties.put(FlinkConnectorOptions.LOOKUP_ASYNC.key(), "true");
        tableSource = (FlinkTableSource) createTableSource(schema, properties);
        int[][] lookupKey = {{0}, {2}};
        LookupTableSource.LookupRuntimeProvider lookupProvider =
                tableSource.getLookupRuntimeProvider(new LookupRuntimeProviderContext(lookupKey));
        assertThat(lookupProvider instanceof AsyncLookupFunctionProvider).isTrue();
        AsyncLookupFunction asyncLookupFunction =
                ((AsyncLookupFunctionProvider) lookupProvider).createAsyncLookupFunction();
        assertThat(asyncLookupFunction instanceof FlinkAsyncLookupFunction).isTrue();

        // test sync
        properties.put(FlinkConnectorOptions.LOOKUP_ASYNC.key(), "false");
        tableSource = (FlinkTableSource) createTableSource(schema, properties);
        lookupProvider =
                tableSource.getLookupRuntimeProvider(new LookupRuntimeProviderContext(lookupKey));
        assertThat(lookupProvider instanceof LookupFunctionProvider).isTrue();
        LookupFunction lookupFunction =
                ((LookupFunctionProvider) lookupProvider).createLookupFunction();
        assertThat(lookupFunction instanceof FlinkLookupFunction).isTrue();

        // test lookup full cache
        Map<String, String> fullCacheProperties = getBasicOptions();
        fullCacheProperties.put("lookup.cache", "full");
        assertThatThrownBy(() -> createTableSource(schema, fullCacheProperties))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Full lookup caching is not supported yet.");
    }

    @Test
    void testSink() {
        ResolvedSchema schema = createBasicSchema();
        Map<String, String> properties = getBasicOptionsWithBucketKey();
        properties.put(BUCKET_NUMBER.key(), "100");
        FlinkTableSink tableSink = (FlinkTableSink) createTableSink(schema, properties);
        List<String> bucketKeys = tableSink.getBucketKeys();
        assertThat(bucketKeys)
                .isEqualTo(Arrays.asList(properties.get(BUCKET_KEY.key()).split(",")));
    }

    private ResolvedSchema createBasicSchema() {
        return new ResolvedSchema(
                Arrays.asList(
                        Column.physical("first", DataTypes.STRING().notNull()),
                        Column.physical("second", DataTypes.INT()),
                        Column.physical("third", DataTypes.STRING().notNull())),
                Collections.emptyList(),
                UniqueConstraint.primaryKey("PK_first_third", Arrays.asList("first", "third")));
    }

    private static Map<String, String> getBasicOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "fluss");
        options.put(BOOTSTRAP_SERVERS.key(), "0.0.0.1:9092");
        return options;
    }

    private static Map<String, String> getBasicOptionsWithBucketKey() {
        Map<String, String> basicOptions = getBasicOptions();
        basicOptions.put(BUCKET_KEY.key(), "first");
        return basicOptions;
    }

    private static DynamicTableSource createTableSource(
            ResolvedSchema schema, Map<String, String> options) {
        return createTableSource(schema, options, Collections.emptyMap());
    }

    private static DynamicTableSource createTableSource(
            ResolvedSchema schema,
            Map<String, String> options,
            Map<String, String> enrichmentOptions) {
        FlinkTableFactory tableFactory = createFlinkTableFactory();
        FactoryUtil.DefaultDynamicTableContext context =
                new FactoryUtil.DefaultDynamicTableContext(
                        OBJECT_IDENTIFIER,
                        new ResolvedCatalogTable(
                                CatalogTable.of(
                                        Schema.newBuilder().fromResolvedSchema(schema).build(),
                                        "mock source",
                                        schema.getPrimaryKey()
                                                .map(UniqueConstraint::getColumns)
                                                .orElse(Collections.emptyList()),
                                        options),
                                schema),
                        enrichmentOptions,
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader(),
                        false);
        return tableFactory.createDynamicTableSource(context);
    }

    private static DynamicTableSink createTableSink(
            ResolvedSchema schema, Map<String, String> options) {

        FlinkTableFactory tableFactory = createFlinkTableFactory();
        FactoryUtil.DefaultDynamicTableContext context =
                new FactoryUtil.DefaultDynamicTableContext(
                        OBJECT_IDENTIFIER,
                        new ResolvedCatalogTable(
                                CatalogTable.of(
                                        Schema.newBuilder().fromResolvedSchema(schema).build(),
                                        "mock sink",
                                        Collections.emptyList(),
                                        options),
                                schema),
                        Collections.emptyMap(),
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader(),
                        false);
        return tableFactory.createDynamicTableSink(context);
    }

    public static FlinkTableFactory createFlinkTableFactory() {
        Optional<Factory> factory = createDefaultFlinkCatalog().getFactory();
        FlinkTableFactory tableFactory = (FlinkTableFactory) factory.get();
        return tableFactory;
    }

    public static FlinkCatalog createDefaultFlinkCatalog() {
        String catalogName = "my_catalog";
        String bootstrapServers = "localhost:9092";
        String dbName = "my_db";

        Map<String, String> options = new HashMap<>();
        options.put(FlinkConnectorOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);
        options.put(FlinkCatalogOptions.DEFAULT_DATABASE.key(), dbName);
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), FlinkCatalogFactory.IDENTIFIER);

        // test create catalog
        FlinkCatalog actualCatalog =
                (FlinkCatalog)
                        FactoryUtil.createCatalog(
                                catalogName,
                                options,
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader());

        return actualCatalog;
    }
}
