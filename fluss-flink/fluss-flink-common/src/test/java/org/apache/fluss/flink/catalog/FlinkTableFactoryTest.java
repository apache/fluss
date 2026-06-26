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

package org.apache.fluss.flink.catalog;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.sink.FlinkTableSink;
import org.apache.fluss.flink.source.FlinkTableSource;
import org.apache.fluss.flink.source.lookup.FlinkAsyncLookupFunction;
import org.apache.fluss.flink.source.lookup.FlinkLookupFunction;
import org.apache.fluss.flink.source.lookup.HybridLakeAsyncLookupFunction;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
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

import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_MAX_ROWS;
import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.FlinkConnectorOptions.BUCKET_KEY;
import static org.apache.fluss.flink.FlinkConnectorOptions.BUCKET_NUMBER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkTableFactory}. */
abstract class FlinkTableFactoryTest {

    public static final ObjectIdentifier OBJECT_IDENTIFIER =
            ObjectIdentifier.of("default", "default", "t1");

    public static final ObjectIdentifier CHANGELOG_TABLE_IDENTIFIER =
            ObjectIdentifier.of("default", "default", "t1" + FlinkCatalog.CHANGELOG_TABLE_SUFFIX);

    public static final ObjectIdentifier BINLOG_TABLE_IDENTIFIER =
            ObjectIdentifier.of("default", "default", "t1" + FlinkCatalog.BINLOG_TABLE_SUFFIX);

    @Test
    void testTableSourceOptions() {
        ResolvedSchema schema = createBasicSchema();
        Map<String, String> validProperties = getBasicOptions();
        validProperties.put("k1", "v1");

        // test create table source with custom properties is ok
        createTableSource(schema, validProperties);

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

        // test split assignment batch size
        Map<String, String> splitAssignmentBatchProperties = getBasicOptions();
        splitAssignmentBatchProperties.put(
                FlinkConnectorOptions.SCAN_SPLIT_ASSIGNMENT_BATCH_SIZE.key(), "0");
        assertThatThrownBy(() -> createTableSource(schema, splitAssignmentBatchProperties))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "'scan.split.assignment.batch-size' must be positive, but was 0.");
        splitAssignmentBatchProperties.put(
                FlinkConnectorOptions.SCAN_SPLIT_ASSIGNMENT_BATCH_SIZE.key(), "1");
        createTableSource(schema, splitAssignmentBatchProperties);

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
    void testLakeFallbackLookupSource() {
        ResolvedSchema schema = createHourlyPartitionedPkSchema();
        Map<String, String> properties = getLakeFallbackOptions();
        FlinkTableSource tableSource =
                (FlinkTableSource)
                        createTableSource(schema, properties, Collections.singletonList("pt"));

        int[][] lookupKey = {{0}, {1}, {2}};
        LookupTableSource.LookupRuntimeProvider lookupProvider =
                tableSource.getLookupRuntimeProvider(new LookupRuntimeProviderContext(lookupKey));
        assertThat(lookupProvider).isInstanceOf(AsyncLookupFunctionProvider.class);
        AsyncLookupFunction asyncLookupFunction =
                ((AsyncLookupFunctionProvider) lookupProvider).createAsyncLookupFunction();
        assertThat(asyncLookupFunction).isInstanceOf(HybridLakeAsyncLookupFunction.class);
    }

    @Test
    void testLakeFallbackLookupSourceValidation() {
        ResolvedSchema schema = createHourlyPartitionedPkSchema();

        Map<String, String> syncLookupProperties = getLakeFallbackOptions();
        syncLookupProperties.put(FlinkConnectorOptions.LOOKUP_ASYNC.key(), "false");
        assertThatThrownBy(
                        () ->
                                ((FlinkTableSource)
                                                createTableSource(
                                                        schema,
                                                        syncLookupProperties,
                                                        Collections.singletonList("pt")))
                                        .getLookupRuntimeProvider(
                                                new LookupRuntimeProviderContext(
                                                        new int[][] {{0}, {1}, {2}})))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "Option 'lookup.lake-fallback.enabled' requires 'lookup.async' to be true.");

        Map<String, String> missingHotWindowProperties = getLakeFallbackOptions();
        missingHotWindowProperties.remove(FlinkConnectorOptions.LOOKUP_HOT_WINDOW.key());
        assertThatThrownBy(
                        () ->
                                ((FlinkTableSource)
                                                createTableSource(
                                                        schema,
                                                        missingHotWindowProperties,
                                                        Collections.singletonList("pt")))
                                        .getLookupRuntimeProvider(
                                                new LookupRuntimeProviderContext(
                                                        new int[][] {{0}, {1}, {2}})))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "Option 'lookup.hot-window' must be configured when 'lookup.lake-fallback.enabled' is true.");

        Map<String, String> cacheProperties = getLakeFallbackOptions();
        cacheProperties.put("lookup.cache", "partial");
        cacheProperties.put(PARTIAL_CACHE_EXPIRE_AFTER_ACCESS.key(), "18000");
        cacheProperties.put(PARTIAL_CACHE_EXPIRE_AFTER_WRITE.key(), "36000");
        cacheProperties.put(PARTIAL_CACHE_MAX_ROWS.key(), "100000");
        assertThatThrownBy(
                        () ->
                                ((FlinkTableSource)
                                                createTableSource(
                                                        schema,
                                                        cacheProperties,
                                                        Collections.singletonList("pt")))
                                        .getLookupRuntimeProvider(
                                                new LookupRuntimeProviderContext(
                                                        new int[][] {{0}, {1}, {2}})))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "Option 'lookup.lake-fallback.enabled' cannot be used with lookup cache.");

        Map<String, String> nonLakeProperties = getLakeFallbackOptions();
        nonLakeProperties.put("table.datalake.enabled", "false");
        assertThatThrownBy(
                        () ->
                                ((FlinkTableSource)
                                                createTableSource(
                                                        schema,
                                                        nonLakeProperties,
                                                        Collections.singletonList("pt")))
                                        .getLookupRuntimeProvider(
                                                new LookupRuntimeProviderContext(
                                                        new int[][] {{0}, {1}, {2}})))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "Option 'lookup.lake-fallback.enabled' requires a datalake-enabled Fluss table.");

        Map<String, String> nonAutoPartitionProperties = getLakeFallbackOptions();
        nonAutoPartitionProperties.put(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), "false");
        assertThatThrownBy(
                        () ->
                                ((FlinkTableSource)
                                                createTableSource(
                                                        schema,
                                                        nonAutoPartitionProperties,
                                                        Collections.singletonList("pt")))
                                        .getLookupRuntimeProvider(
                                                new LookupRuntimeProviderContext(
                                                        new int[][] {{0}, {1}, {2}})))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "Option 'lookup.lake-fallback.enabled' requires auto partition to be enabled.");

        assertThatThrownBy(
                        () ->
                                ((FlinkTableSource)
                                                createTableSource(
                                                        schema,
                                                        getLakeFallbackOptions(),
                                                        Collections.singletonList("pt")))
                                        .getLookupRuntimeProvider(
                                                new LookupRuntimeProviderContext(
                                                        new int[][] {{0}, {2}})))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "Option 'lookup.lake-fallback.enabled' only supports full primary-key lookup.");
    }

    @Test
    void testVirtualLogTableSourceDoesNotSupportBatchMode() {
        ResolvedSchema schema = createBasicSchema();
        Map<String, String> properties = getBasicOptions();
        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        assertThatThrownBy(
                        () ->
                                createTableSource(
                                        CHANGELOG_TABLE_IDENTIFIER,
                                        schema,
                                        properties,
                                        Collections.emptyMap(),
                                        configuration))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("$changelog virtual tables only support streaming mode.");

        assertThatThrownBy(
                        () ->
                                createTableSource(
                                        BINLOG_TABLE_IDENTIFIER,
                                        createBinlogSchema(),
                                        properties,
                                        Collections.emptyMap(),
                                        configuration))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("$binlog virtual tables only support streaming mode.");
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

        // test create table sink with custom properties is ok
        properties.put("k1", "v1");
        createTableSink(schema, properties);
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

    private ResolvedSchema createHourlyPartitionedPkSchema() {
        return new ResolvedSchema(
                Arrays.asList(
                        Column.physical("id", DataTypes.INT().notNull()),
                        Column.physical("sub_id", DataTypes.INT().notNull()),
                        Column.physical("pt", DataTypes.STRING().notNull()),
                        Column.physical("value", DataTypes.STRING())),
                Collections.emptyList(),
                UniqueConstraint.primaryKey(
                        "PK_id_sub_id_pt", Arrays.asList("id", "sub_id", "pt")));
    }

    private ResolvedSchema createBinlogSchema() {
        return new ResolvedSchema(
                Arrays.asList(
                        Column.physical("_change_type", DataTypes.STRING().notNull()),
                        Column.physical("_log_offset", DataTypes.BIGINT().notNull()),
                        Column.physical("_commit_timestamp", DataTypes.TIMESTAMP_LTZ(3).notNull()),
                        Column.physical(
                                "before",
                                DataTypes.ROW(
                                        DataTypes.FIELD("first", DataTypes.STRING().notNull()),
                                        DataTypes.FIELD("second", DataTypes.INT()),
                                        DataTypes.FIELD("third", DataTypes.STRING().notNull()))),
                        Column.physical(
                                "after",
                                DataTypes.ROW(
                                        DataTypes.FIELD("first", DataTypes.STRING().notNull()),
                                        DataTypes.FIELD("second", DataTypes.INT()),
                                        DataTypes.FIELD("third", DataTypes.STRING().notNull())))),
                Collections.emptyList(),
                null);
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

    private static Map<String, String> getLakeFallbackOptions() {
        Map<String, String> options = getBasicOptions();
        options.put(BUCKET_KEY.key(), "id");
        options.put("table.datalake.enabled", "true");
        options.put("table.datalake.format", "paimon");
        options.put(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), "true");
        options.put(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(), "HOUR");
        options.put(FlinkConnectorOptions.LOOKUP_ASYNC.key(), "true");
        options.put(FlinkConnectorOptions.LOOKUP_LAKE_FALLBACK_ENABLED.key(), "true");
        options.put(FlinkConnectorOptions.LOOKUP_HOT_WINDOW.key(), "12 h");
        return options;
    }

    private static DynamicTableSource createTableSource(
            ResolvedSchema schema, Map<String, String> options) {
        return createTableSource(schema, options, Collections.emptyMap());
    }

    private static DynamicTableSource createTableSource(
            ResolvedSchema schema, Map<String, String> options, List<String> partitionKeys) {
        return createTableSource(
                OBJECT_IDENTIFIER,
                schema,
                options,
                Collections.emptyMap(),
                new Configuration(),
                partitionKeys);
    }

    private static DynamicTableSource createTableSource(
            ResolvedSchema schema,
            Map<String, String> options,
            Map<String, String> enrichmentOptions) {
        return createTableSource(
                OBJECT_IDENTIFIER, schema, options, enrichmentOptions, new Configuration());
    }

    private static DynamicTableSource createTableSource(
            ObjectIdentifier objectIdentifier,
            ResolvedSchema schema,
            Map<String, String> options,
            Map<String, String> enrichmentOptions,
            Configuration configuration) {
        return createTableSource(
                objectIdentifier,
                schema,
                options,
                enrichmentOptions,
                configuration,
                schema.getPrimaryKey()
                        .map(UniqueConstraint::getColumns)
                        .orElse(Collections.emptyList()));
    }

    private static DynamicTableSource createTableSource(
            ObjectIdentifier objectIdentifier,
            ResolvedSchema schema,
            Map<String, String> options,
            Map<String, String> enrichmentOptions,
            Configuration configuration,
            List<String> partitionKeys) {
        FlinkTableFactory tableFactory = createFlinkTableFactory();
        FactoryUtil.DefaultDynamicTableContext context =
                new FactoryUtil.DefaultDynamicTableContext(
                        objectIdentifier,
                        new ResolvedCatalogTable(
                                CatalogTable.of(
                                        Schema.newBuilder().fromResolvedSchema(schema).build(),
                                        "mock source",
                                        partitionKeys,
                                        options),
                                schema),
                        enrichmentOptions,
                        configuration,
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
