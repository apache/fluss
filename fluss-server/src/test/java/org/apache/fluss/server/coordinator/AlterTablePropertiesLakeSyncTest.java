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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeStoragePlugin;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.ZkVersion;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the boundary between Fluss property changes and lake metadata changes. */
class AlterTablePropertiesLakeSyncTest {

    private static final String RETENTION = "table.auto-partition.num-retention";
    private static final String ENABLED = "table.datalake.enabled";
    private static final String AUTO_COMPACTION = "table.datalake.auto-compaction";
    private static final AtomicInteger TABLE_IDS = new AtomicInteger();

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOOKEEPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zkClient;

    private RecordingLakeCatalog catalog;
    private MetadataManager metadata;
    private DataLakeFormat format;

    @BeforeAll
    static void initializeZookeeper() throws Exception {
        zkClient = ZOOKEEPER.getCustomExtension().getZooKeeperClient(NOPErrorHandler.INSTANCE);
        zkClient.registerCoordinatorLeader(
                new CoordinatorAddress(
                        "repro", Endpoint.fromListenersString("CLIENT://localhost:10012")));
        zkClient.fenceBecomeCoordinatorLeader("repro");
    }

    @BeforeEach
    void initializeCatalog() {
        catalog = new RecordingLakeCatalog();
        format = DataLakeFormat.ICEBERG;
    }

    private static Stream<Arguments> lakeStates() {
        return Stream.of(DataLakeFormat.values())
                .flatMap(
                        format ->
                                Stream.of(null, Boolean.FALSE, Boolean.TRUE)
                                        .flatMap(
                                                enabled ->
                                                        Stream.of(
                                                                Arguments.of(
                                                                        format, enabled, false),
                                                                Arguments.of(
                                                                        format, enabled, true))));
    }

    @ParameterizedTest
    @MethodSource("lakeStates")
    void testFlussPropertiesDoNotDependOnLake(
            DataLakeFormat lakeFormat, @Nullable Boolean enabled, boolean missing)
            throws Exception {
        format = lakeFormat;
        TablePath path = createTable(enabled, Collections.singletonMap("k1", "v1"));
        catalog.failure =
                missing
                        ? new TableNotExistException("lake table was deleted")
                        : new IllegalStateException("lake catalog unavailable");

        alter(
                path,
                TableChange.set(RETENTION, "3"),
                TableChange.set("table.datalake.freshness", "1min"),
                TableChange.reset("k1"),
                TableChange.set("k2", "v2"));

        assertThat(retention(path)).isEqualTo(3);
        assertThat(metadata.getTable(path).getCustomProperties().toMap())
                .containsEntry("k2", "v2")
                .doesNotContainKey("k1");
        alter(path, TableChange.reset(RETENTION), TableChange.reset("k2"));
        assertThat(retention(path))
                .isEqualTo(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION.defaultValue());
        assertThat(metadata.getTable(path).getCustomProperties().toMap()).doesNotContainKey("k2");
        assertThat(catalog.alterCalls).isZero();
        assertThat(catalog.createCalls).isZero();
    }

    @ParameterizedTest
    @EnumSource(
            value = DataLakeFormat.class,
            names = {"ICEBERG", "PAIMON"})
    void testMixedChangesOnlyForwardLakeNativeOptions(DataLakeFormat lakeFormat) throws Exception {
        format = lakeFormat;
        TablePath path = createTable(true, Collections.singletonMap(nativeKey(), "10"));
        TableChange nativeSet = TableChange.set(nativeKey(), "20");

        alter(path, TableChange.set(RETENTION, "3"), nativeSet, TableChange.set("k2", "v2"));

        assertThat(retention(path)).isEqualTo(3);
        assertThat(catalog.lastChanges).containsExactly(nativeSet);
        assertThat(metadata.getTable(path).getCustomProperties().toMap())
                .containsEntry(nativeKey(), "20")
                .containsEntry("k2", "v2");

        TableChange nativeReset = TableChange.reset(nativeKey());
        alter(path, nativeReset, TableChange.reset("k2"));
        assertThat(catalog.lastChanges).containsExactly(nativeReset);
        assertThat(metadata.getTable(path).getCustomProperties().toMap())
                .doesNotContainKeys(nativeKey(), "k2");
        assertThat(catalog.alterCalls).isEqualTo(2);
    }

    @Test
    void testNativeFailureDoesNotCommitMixedChanges() throws Exception {
        TablePath path = createTable(true, Collections.singletonMap(nativeKey(), "10"));
        catalog.failure = new IllegalStateException("lake catalog unavailable");

        assertThatThrownBy(
                        () ->
                                alter(
                                        path,
                                        TableChange.set(RETENTION, "3"),
                                        TableChange.set(nativeKey(), "20")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("lake catalog unavailable");

        assertThat(retention(path)).isEqualTo(1);
        assertThat(metadata.getTable(path).getCustomProperties().toMap())
                .containsEntry(nativeKey(), "10");
    }

    private static Stream<Arguments> hudiAutoCompactionChanges() {
        return Stream.of(
                Arguments.of(null, TableChange.set(AUTO_COMPACTION, "true")),
                Arguments.of(true, TableChange.set(AUTO_COMPACTION, "false")),
                Arguments.of(true, TableChange.reset(AUTO_COMPACTION)));
    }

    @ParameterizedTest
    @EnumSource(
            value = DataLakeFormat.class,
            names = {"ICEBERG", "PAIMON"})
    void testAutoCompactionStaysLocalForOtherFormats(DataLakeFormat lakeFormat) throws Exception {
        format = lakeFormat;
        TablePath path = createTable(true, Collections.emptyMap());
        catalog.failure = new IllegalStateException("lake catalog unavailable");

        alter(path, TableChange.set(AUTO_COMPACTION, "true"));
        assertThat(metadata.getTable(path).getTableConfig().isDataLakeAutoCompaction()).isTrue();
        alter(path, TableChange.reset(AUTO_COMPACTION));
        assertThat(metadata.getTable(path).getTableConfig().isDataLakeAutoCompaction()).isFalse();
        assertThat(catalog.alterCalls).isZero();
    }

    @ParameterizedTest
    @MethodSource("hudiAutoCompactionChanges")
    void testHudiAutoCompactionChangesAreRejected(
            @Nullable Boolean autoCompaction, TableChange change) throws Exception {
        format = DataLakeFormat.HUDI;
        TablePath path = createTable(true, Collections.emptyMap(), autoCompaction);
        catalog.failure =
                new UnsupportedOperationException(
                        "Alter table is not supported for Hudi at the moment");

        assertThatThrownBy(() -> alter(path, change, TableChange.set(RETENTION, "3")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Alter table is not supported for Hudi at the moment");

        assertThat(catalog.lastChanges).containsExactly(change);
        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(metadata.getTable(path).getTableConfig().isDataLakeAutoCompaction())
                .isEqualTo(Boolean.TRUE.equals(autoCompaction));
        assertThat(retention(path)).isEqualTo(1);
    }

    @Test
    void testMissingLakeStillRejectsNativeChangeWhenEnabled() throws Exception {
        TablePath path = createTable(true, Collections.emptyMap());
        catalog.failure = new TableNotExistException("lake table was deleted");

        assertThatThrownBy(() -> alter(path, TableChange.set(nativeKey(), "20")))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("Lake table doesn't exist for lake-enabled table");
        assertThat(metadata.getTable(path).getCustomProperties().toMap())
                .doesNotContainKey(nativeKey());
    }

    @Test
    void testUnchangedNativeOptionDoesNotBlockFlussChange() throws Exception {
        TablePath path = createTable(true, Collections.singletonMap(nativeKey(), "10"));
        catalog.failure = new IllegalStateException("lake catalog unavailable");

        alter(path, TableChange.set(nativeKey(), "10"), TableChange.set(RETENTION, "3"));

        assertThat(retention(path)).isEqualTo(3);
        assertThat(catalog.alterCalls).isZero();
    }

    @Test
    void testSettingFalseWhileAlreadyDisabledDoesNotContactLake() throws Exception {
        TablePath path = createTable(null, Collections.emptyMap());
        catalog.failure = new IllegalStateException("lake catalog unavailable");

        alter(path, TableChange.set(ENABLED, "false"), TableChange.set(RETENTION, "3"));
        alter(path, TableChange.reset(ENABLED), TableChange.set(RETENTION, "4"));

        assertThat(retention(path)).isEqualTo(4);
        assertThat(catalog.alterCalls).isZero();
        assertThat(catalog.createCalls).isZero();
    }

    @Test
    void testOtherLakeFormatOptionsStayLocal() throws Exception {
        TablePath path = createTable(true, Collections.emptyMap());
        catalog.failure = new IllegalStateException("lake catalog unavailable");

        alter(path, TableChange.set("paimon.target-file-size", "20"));

        assertThat(metadata.getTable(path).getCustomProperties().toMap())
                .containsEntry("paimon.target-file-size", "20");
        assertThat(catalog.alterCalls).isZero();
    }

    @Test
    void testNativeChangesStillRequireCatalogWhenClusterLakeIsDisabled() throws Exception {
        TablePath path = createTable(true, Collections.emptyMap());
        Configuration disabledCluster = new Configuration();
        metadata =
                new MetadataManager(
                        zkClient,
                        disabledCluster,
                        new LakeCatalogDynamicLoader(disabledCluster, null, true));

        alter(path, TableChange.set(RETENTION, "3"));
        assertThat(retention(path)).isEqualTo(3);
        assertThatThrownBy(() -> alter(path, TableChange.set(nativeKey(), "20")))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("the Fluss cluster doesn't enable datalake tables");
        assertThat(metadata.getTable(path).getCustomProperties().toMap())
                .doesNotContainKey(nativeKey());
    }

    @Test
    void testDisableResetAndReenablePreserveLakeStateChanges() throws Exception {
        TablePath path = createTable(true, Collections.emptyMap());
        TableChange disable = TableChange.set(ENABLED, "false");
        alter(path, disable, TableChange.set(RETENTION, "3"));
        assertThat(catalog.lastChanges).containsExactly(disable);
        assertThat(catalog.alterCalls).isEqualTo(1);

        catalog.failure = new IllegalStateException("lake catalog unavailable");
        alter(path, TableChange.set(RETENTION, "4"));
        assertThat(catalog.alterCalls).isEqualTo(1);
        catalog.failure = null;

        TableChange enable = TableChange.set(ENABLED, "true");
        alter(path, enable);
        assertThat(catalog.createCalls).isEqualTo(1);
        assertThat(catalog.lastChanges).containsExactly(enable);

        TableChange reset = TableChange.reset(ENABLED);
        alter(path, reset);
        assertThat(catalog.lastChanges).containsExactly(reset);
        int callsBeforeFlussChange = catalog.alterCalls;
        alter(path, TableChange.set(RETENTION, "5"));
        assertThat(catalog.alterCalls).isEqualTo(callsBeforeFlussChange);

        alter(path, enable);
        assertThat(catalog.createCalls).isEqualTo(2);
        assertThat(catalog.lastChanges).containsExactly(enable);
        assertThat(metadata.getTable(path).getTableConfig().isDataLakeEnabled()).isTrue();
    }

    @Test
    void testEnableWithMappingDoesNotReplayPathChanges() throws Exception {
        format = DataLakeFormat.PAIMON;
        TablePath path = createTable(false, Collections.emptyMap());
        TableChange enable = TableChange.set(ENABLED, "true");

        alter(
                path,
                TableChange.set("table.datalake.database-name", "other_lake_db"),
                enable,
                TableChange.set(RETENTION, "3"));

        assertThat(catalog.createdPath)
                .isEqualTo(TablePath.of("other_lake_db", path.getTableName()));
        assertThat(catalog.alteredPath).isEqualTo(catalog.createdPath);
        assertThat(catalog.lastChanges).containsExactly(enable);
    }

    @Test
    void testLakeMappingChangesStillReachValidation() throws Exception {
        format = DataLakeFormat.PAIMON;
        TablePath path = createTable(true, Collections.emptyMap());
        catalog.failure = new InvalidAlterTableException("lake table mapping cannot be changed");
        TableChange change = TableChange.set("table.datalake.database-name", "other_lake_db");

        assertThatThrownBy(() -> alter(path, change, TableChange.set(RETENTION, "3")))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessage("lake table mapping cannot be changed");

        assertThat(catalog.lastChanges).containsExactly(change);
        assertThat(retention(path)).isEqualTo(1);
        assertThat(metadata.getTable(path).getLakeTablePath()).isEqualTo(path);
    }

    private String nativeKey() {
        return format == DataLakeFormat.ICEBERG
                ? "iceberg.write.target-file-size-bytes"
                : "paimon.target-file-size";
    }

    private TablePath createTable(@Nullable Boolean enabled, Map<String, String> customProperties)
            throws Exception {
        return createTable(enabled, customProperties, null);
    }

    private TablePath createTable(
            @Nullable Boolean enabled,
            Map<String, String> customProperties,
            @Nullable Boolean autoCompaction)
            throws Exception {
        initializeMetadataManager();
        TablePath path = TablePath.of("lake_sync", "table_" + TABLE_IDS.incrementAndGet());
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.BIGINT().copy(false))
                                        .column("pt", DataTypes.STRING().copy(false))
                                        .primaryKey("id", "pt")
                                        .build())
                        .partitionedBy("pt")
                        .distributedBy(1, "id")
                        .property("table.datalake.format", format.toString())
                        .property("table.auto-partition.enabled", "true")
                        .property("table.auto-partition.key", "pt")
                        .property("table.auto-partition.time-unit", "HOUR")
                        .property(RETENTION, "1")
                        .customProperties(customProperties);
        if (enabled != null) {
            builder.property(ENABLED, enabled.toString());
        }
        if (autoCompaction != null) {
            builder.property(AUTO_COMPACTION, autoCompaction.toString());
        }
        TableDescriptor descriptor = builder.build().withReplicationFactor(1);
        metadata.validateTableDescriptor(path, descriptor);
        metadata.createTable(path, zkClient.getDefaultRemoteDataDir(), descriptor, null, false);
        return path;
    }

    private void initializeMetadataManager() throws Exception {
        LakeStoragePlugin plugin =
                new LakeStoragePlugin() {
                    @Override
                    public String identifier() {
                        return format.toString();
                    }

                    @Override
                    public LakeStorage createLakeStorage(Configuration configuration) {
                        return new LakeStorage() {
                            @Override
                            public LakeCatalog createLakeCatalog() {
                                return catalog;
                            }

                            @Override
                            public LakeTieringFactory<?, ?> createLakeTieringFactory() {
                                throw new UnsupportedOperationException("Metadata-only test");
                            }

                            @Override
                            public LakeSource<?> createLakeSource(TablePath path) {
                                throw new UnsupportedOperationException("Metadata-only test");
                            }
                        };
                    }
                };
        PluginManager plugins =
                new PluginManager() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public <P> Iterator<P> load(Class<P> service) {
                        if (service == LakeStoragePlugin.class) {
                            return Collections.singletonList((P) plugin).iterator();
                        }
                        return Collections.<P>emptyList().iterator();
                    }
                };
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.DATALAKE_ENABLED, true);
        conf.set(ConfigOptions.DATALAKE_FORMAT, format);
        metadata =
                new MetadataManager(
                        zkClient, conf, new LakeCatalogDynamicLoader(conf, plugins, true));
        metadata.createDatabase("lake_sync", DatabaseDescriptor.builder().build(), true);
    }

    private int retention(TablePath path) {
        return metadata.getTable(path).getTableConfig().getAutoPartitionStrategy().numToRetain();
    }

    private void alter(TablePath path, TableChange... changes) {
        List<TableChange> tableChanges = Arrays.asList(changes);
        metadata.alterTableProperties(
                path,
                tableChanges,
                CoordinatorService.toTablePropertyChanges(tableChanges),
                false,
                null,
                (current, updated) -> {},
                (current, updated) -> {},
                ZkVersion.MATCH_ANY_VERSION.getVersion());
    }

    private static final class RecordingLakeCatalog implements LakeCatalog {
        private int createCalls;
        private int alterCalls;
        private RuntimeException failure;
        private List<TableChange> lastChanges;
        private TablePath createdPath;
        private TablePath alteredPath;

        @Override
        public void createTable(TablePath path, TableDescriptor descriptor, Context context) {
            createCalls++;
            createdPath = path;
        }

        @Override
        public void alterTable(TablePath path, List<TableChange> changes, Context context) {
            alterCalls++;
            alteredPath = path;
            lastChanges = new ArrayList<>(changes);
            if (failure != null) {
                throw failure;
            }
        }
    }
}
