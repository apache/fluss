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

package org.apache.fluss.server;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.config.cluster.ConfigEntry;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.remote.RemoteDirDynamicLoader;
import org.apache.fluss.server.coordinator.remote.RoundRobinRemoteDirSelector;
import org.apache.fluss.server.coordinator.remote.WeightedRoundRobinRemoteDirSelector;
import org.apache.fluss.server.storage.DiskWriteLimitConfigValidator;
import org.apache.fluss.server.storage.LocalDiskManager;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.ZooKeeperUtils;
import org.apache.fluss.server.zk.data.ZkData.ConfigZNode;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.config.ConfigOptions.DATALAKE_ENABLED;
import static org.apache.fluss.config.ConfigOptions.DATALAKE_FORMAT;
import static org.apache.fluss.metadata.DataLakeFormat.PAIMON;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DynamicConfigManager}. */
public class DynamicConfigChangeTest {

    @RegisterExtension
    public static AllCallbackWrapper<ZooKeeperExtension> zooKeeperExtensionWrapper =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    protected static ZooKeeperClient zookeeperClient;

    /**
     * Managers created during a test. Each one starts a ZooKeeper notification watcher on {@link
     * DynamicConfigManager#startup()}, so they must be closed after the test to stop reacting to
     * config changes written by later tests that share the same static {@link #zookeeperClient}.
     */
    private final List<DynamicConfigManager> managers = new ArrayList<>();

    @BeforeAll
    static void beforeAll() {
        final Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.REMOTE_DATA_DIR, DEFAULT_REMOTE_DATA_DIR);
        configuration.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                zooKeeperExtensionWrapper.getCustomExtension().getConnectString());
        zookeeperClient =
                ZooKeeperUtils.startZookeeperClient(configuration, NOPErrorHandler.INSTANCE);
    }

    @AfterAll
    static void afterAll() {
        if (zookeeperClient != null) {
            zookeeperClient.close();
        }
    }

    @AfterEach
    void after() throws Exception {
        for (DynamicConfigManager manager : managers) {
            manager.close();
        }
        managers.clear();
        // Recursively delete /config so both the entity config (/config/server/global) and the
        // change notifications (/config/changes/...) are removed for the next test.
        try {
            zookeeperClient
                    .getCuratorClient()
                    .delete()
                    .deletingChildrenIfNeeded()
                    .forPath(ConfigZNode.path());
        } catch (KeeperException.NoNodeException ignored) {
            // Nothing was written in this test.
        }
    }

    /**
     * Creates a manager that does not consume change notifications, modelling a coordinator leader
     * (the sole writer via {@code alterConfigs}). This is the default because most tests exercise
     * the write path, and a self-consumed notification could roll back a value the test just set.
     * Use {@link #createListeningManager} for tests that verify notification tracking. The manager
     * is tracked so {@link #after()} closes it.
     */
    private DynamicConfigManager createManager(Configuration configuration) {
        DynamicConfigManager manager = createListeningManager(configuration);
        manager.pauseListening();
        return manager;
    }

    /**
     * Creates a manager that consumes change notifications, modelling a standby coordinator or a
     * tablet server. The manager is tracked so {@link #after()} closes it (stopping its watcher).
     */
    private DynamicConfigManager createListeningManager(Configuration configuration) {
        DynamicConfigManager manager = new DynamicConfigManager(zookeeperClient, configuration);
        managers.add(manager);
        return manager;
    }

    @Test
    void testAlterLakehouseConfigs() throws Exception {
        try (LakeCatalogDynamicLoader lakeCatalogDynamicLoader =
                new LakeCatalogDynamicLoader(new Configuration(), null, true)) {
            DynamicConfigManager dynamicConfigManager = createManager(new Configuration());
            dynamicConfigManager.register(lakeCatalogDynamicLoader);
            dynamicConfigManager.startup();
            assertThatThrownBy(
                            () ->
                                    dynamicConfigManager.alterConfigs(
                                            Collections.singletonList(
                                                    new AlterConfig(
                                                            "un_support_key",
                                                            "value",
                                                            AlterConfigOpType.SET))))
                    .isExactlyInstanceOf(ConfigException.class)
                    .hasMessageContaining(
                            "The config key un_support_key is not allowed to be changed dynamically.");

            dynamicConfigManager.alterConfigs(
                    Arrays.asList(
                            new AlterConfig(DATALAKE_FORMAT.key(), "paimon", AlterConfigOpType.SET),
                            new AlterConfig(
                                    "datalake.paimon.metastore",
                                    "filesystem",
                                    AlterConfigOpType.SET)));
            assertThat(lakeCatalogDynamicLoader.getLakeCatalogContainer().getDataLakeFormat())
                    .isEqualTo(PAIMON);
            assertThat(
                            lakeCatalogDynamicLoader
                                    .getLakeCatalogContainer()
                                    .getDefaultTableLakeOptions())
                    .isEqualTo(
                            Collections.singletonMap(
                                    "table.datalake.paimon.metastore", "filesystem"));
            dynamicConfigManager.alterConfigs(
                    Collections.singletonList(
                            new AlterConfig(
                                    DATALAKE_FORMAT.key(), null, AlterConfigOpType.DELETE)));
            assertThat(lakeCatalogDynamicLoader.getLakeCatalogContainer().getDataLakeFormat())
                    .isEqualTo(null);
            assertThat(
                            lakeCatalogDynamicLoader
                                    .getLakeCatalogContainer()
                                    .getDefaultTableLakeOptions())
                    .isNull();
        }
    }

    @Test
    void testOverrideConfigs() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(DATALAKE_FORMAT.key(), "paimon");
        try (LakeCatalogDynamicLoader lakeCatalogDynamicLoader =
                new LakeCatalogDynamicLoader(configuration, null, true)) {
            DynamicConfigManager dynamicConfigManager = createManager(configuration);
            dynamicConfigManager.register(lakeCatalogDynamicLoader);
            dynamicConfigManager.startup();
            assertThat(lakeCatalogDynamicLoader.getLakeCatalogContainer().getDataLakeFormat())
                    .isEqualTo(PAIMON);
            dynamicConfigManager.alterConfigs(
                    Collections.singletonList(
                            new AlterConfig(DATALAKE_FORMAT.key(), null, AlterConfigOpType.SET)));
            assertThat(lakeCatalogDynamicLoader.getLakeCatalogContainer().getDataLakeFormat())
                    .isEqualTo(null);
            dynamicConfigManager.alterConfigs(
                    Collections.singletonList(
                            new AlterConfig(
                                    DATALAKE_FORMAT.key(), null, AlterConfigOpType.DELETE)));
            assertThat(lakeCatalogDynamicLoader.getLakeCatalogContainer().getDataLakeFormat())
                    .isEqualTo(PAIMON);
        }
    }

    @Test
    void testUnknownLakeHouse() throws Exception {
        Configuration configuration = new Configuration();
        try (LakeCatalogDynamicLoader lakeCatalogDynamicLoader =
                new LakeCatalogDynamicLoader(configuration, null, true)) {
            DynamicConfigManager dynamicConfigManager = createManager(configuration);
            dynamicConfigManager.register(lakeCatalogDynamicLoader);
            dynamicConfigManager.startup();
            assertThatThrownBy(
                            () ->
                                    dynamicConfigManager.alterConfigs(
                                            Collections.singletonList(
                                                    new AlterConfig(
                                                            DATALAKE_FORMAT.key(),
                                                            "unknown",
                                                            AlterConfigOpType.SET))))
                    .hasMessageContaining(
                            "Cannot parse 'unknown' as DataLakeFormat for config 'datalake.format'");

            assertThat(lakeCatalogDynamicLoader.getLakeCatalogContainer().getDataLakeFormat())
                    .isNull();
        }
    }

    @Test
    void testDatalakePrefixValidationSkippedWhenFormatIsNull() throws Exception {
        Configuration configuration = new Configuration();
        try (LakeCatalogDynamicLoader lakeCatalogDynamicLoader =
                new LakeCatalogDynamicLoader(configuration, null, true)) {
            DynamicConfigManager dynamicConfigManager = createManager(configuration);
            dynamicConfigManager.register(lakeCatalogDynamicLoader);
            dynamicConfigManager.startup();

            // Setting `datalake.paimon.*` without setting `datalake.format` should pass because
            // prefix validation is skipped.
            dynamicConfigManager.alterConfigs(
                    Collections.singletonList(
                            new AlterConfig(
                                    "datalake.iceberg.type", "rest", AlterConfigOpType.SET)));

            assertThat(lakeCatalogDynamicLoader.getLakeCatalogContainer().getDataLakeFormat())
                    .isNull();
            assertThatThrownBy(
                            () ->
                                    dynamicConfigManager.alterConfigs(
                                            Arrays.asList(
                                                    new AlterConfig(
                                                            "datalake.iceberg.type",
                                                            "rest",
                                                            AlterConfigOpType.SET),
                                                    new AlterConfig(
                                                            "datalake.format",
                                                            "paimon",
                                                            AlterConfigOpType.SET))))
                    .hasMessageContaining(
                            "Invalid configuration 'datalake.iceberg.type' for 'paimon' datalake format");
        }
    }

    @Test
    void testWrongLakeFormatPrefix() throws Exception {
        Configuration configuration = new Configuration();
        try (LakeCatalogDynamicLoader lakeCatalogDynamicLoader =
                new LakeCatalogDynamicLoader(configuration, null, true)) {
            DynamicConfigManager dynamicConfigManager = createManager(configuration);
            dynamicConfigManager.register(lakeCatalogDynamicLoader);
            dynamicConfigManager.startup();
            assertThatThrownBy(
                            () ->
                                    dynamicConfigManager.alterConfigs(
                                            Arrays.asList(
                                                    new AlterConfig(
                                                            DATALAKE_FORMAT.key(),
                                                            "paimon",
                                                            AlterConfigOpType.SET),
                                                    new AlterConfig(
                                                            "datalake.iceberg.metastore",
                                                            "filesystem",
                                                            AlterConfigOpType.SET))))
                    .hasMessage(
                            "Invalid configuration 'datalake.iceberg.metastore' for 'paimon' datalake format");

            assertThat(lakeCatalogDynamicLoader.getLakeCatalogContainer().getDataLakeFormat())
                    .isNull();
        }
    }

    @Test
    void testListenUnMatchedDynamicConfigChanges() throws Exception {
        Configuration configuration = new Configuration();
        try (LakeCatalogDynamicLoader lakeCatalogDynamicLoader =
                new LakeCatalogDynamicLoader(configuration, null, false)) {
            DynamicConfigManager dynamicConfigManager = createListeningManager(configuration);
            dynamicConfigManager.register(lakeCatalogDynamicLoader);
            dynamicConfigManager.startup();
            assertThat(lakeCatalogDynamicLoader.getLakeCatalogContainer().getDataLakeFormat())
                    .isEqualTo(null);
            Map<String, String> config = new HashMap<>();
            config.put(DATALAKE_FORMAT.key(), "paimon");
            config.put("un_support_key", "value");
            zookeeperClient.upsertServerEntityConfig(config);
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            lakeCatalogDynamicLoader
                                                    .getLakeCatalogContainer()
                                                    .getDataLakeFormat())
                                    .isEqualTo(PAIMON));
        }
    }

    /**
     * Regression test for #3625: a {@link DynamicConfigManager} must keep applying config-change
     * notifications pushed to ZooKeeper by another writer. This is what lets a standby
     * CoordinatorServer stay current so that, once promoted, it does not run with stale config.
     */
    @Test
    void testTracksConfigChangeNotificationsFromOtherWriter() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 1);

        DynamicConfigManager dynamicConfigManager = createListeningManager(configuration);
        AtomicInteger reconfiguredValue = new AtomicInteger();
        dynamicConfigManager.register(
                new ServerReconfigurable() {
                    @Override
                    public void validate(Configuration newConfig) throws ConfigException {}

                    @Override
                    public void reconfigure(Configuration newConfig) {
                        reconfiguredValue.set(
                                newConfig.get(
                                        ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER));
                    }
                });
        dynamicConfigManager.startup();

        // Simulate another server (e.g. the active leader) persisting a config change to ZK,
        // which inserts a change notification the manager under test must react to.
        Map<String, String> config = new HashMap<>();
        config.put(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER.key(), "3");
        zookeeperClient.upsertServerEntityConfig(config);

        retry(Duration.ofMinutes(1), () -> assertThat(reconfiguredValue.get()).isEqualTo(3));
    }

    /**
     * Regression test for #3645 review: an active leader ({@link
     * DynamicConfigManager#pauseListening()}) must ignore change notifications so it cannot roll a
     * value it just set back to an older one (A to B to A). On losing leadership ({@link
     * DynamicConfigManager#resumeListening()}) it re-syncs from ZooKeeper.
     */
    @Test
    void testPausedManagerIgnoresNotificationsAndResumeReSyncs() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 1);

        DynamicConfigManager dynamicConfigManager = createListeningManager(configuration);
        // Seed with the initial value; reconfigure() is only invoked on an effective change.
        AtomicInteger reconfiguredValue = new AtomicInteger(1);
        dynamicConfigManager.register(
                new ServerReconfigurable() {
                    @Override
                    public void validate(Configuration newConfig) throws ConfigException {}

                    @Override
                    public void reconfigure(Configuration newConfig) {
                        reconfiguredValue.set(
                                newConfig.get(
                                        ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER));
                    }
                });
        dynamicConfigManager.startup();

        // Become leader: stop consuming notifications.
        dynamicConfigManager.pauseListening();

        // Another writer changes the config in ZooKeeper. A paused manager must not apply it.
        Map<String, String> config = new HashMap<>();
        config.put(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER.key(), "3");
        zookeeperClient.upsertServerEntityConfig(config);

        // Give the notification a chance to be (wrongly) applied, then assert it was ignored.
        Thread.sleep(2000);
        assertThat(reconfiguredValue.get()).isEqualTo(1);

        // Lose leadership: resume and re-sync from ZooKeeper picks up the missed change.
        dynamicConfigManager.resumeListening();
        assertThat(reconfiguredValue.get()).isEqualTo(3);
    }

    @Test
    void testReStartupContainsNoMatchedDynamicConfig() throws Exception {
        Configuration configuration = new Configuration();
        Map<String, String> config = new HashMap<>();
        config.put(DATALAKE_FORMAT.key(), "paimon");
        config.put("un_support_key", "value");

        // This often happens when upgrading with different allowed configs.
        zookeeperClient.upsertServerEntityConfig(config);

        try (LakeCatalogDynamicLoader lakeCatalogDynamicLoader =
                new LakeCatalogDynamicLoader(configuration, null, true)) {
            DynamicConfigManager dynamicConfigManager = createManager(configuration);
            dynamicConfigManager.register(lakeCatalogDynamicLoader);
            // Startup dynamic manager even is not matched now.
            dynamicConfigManager.startup();
            assertThat(lakeCatalogDynamicLoader.getLakeCatalogContainer().getDataLakeFormat())
                    .isEqualTo(PAIMON);
        }
    }

    @Test
    void testPreventInvalidConfig() throws Exception {
        // Test that generic type validation prevents invalid config values
        Configuration configuration = new Configuration();
        configuration.setString(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(), "100MB");

        DynamicConfigManager dynamicConfigManager = createManager(configuration);
        dynamicConfigManager.startup();

        // Try to set rate limiter to an invalid value - should be rejected by generic type
        // validation
        assertThatThrownBy(
                        () ->
                                dynamicConfigManager.alterConfigs(
                                        Collections.singletonList(
                                                new AlterConfig(
                                                        ConfigOptions
                                                                .KV_SHARED_RATE_LIMITER_BYTES_PER_SEC
                                                                .key(),
                                                        "invalid_value",
                                                        AlterConfigOpType.SET))))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Cannot parse 'invalid_value' as MemorySize for config 'kv.rocksdb.shared-rate-limiter.bytes-per-sec'");
    }

    @Test
    void testConfigValidatorAllowsValidChange() throws Exception {
        // Test that generic type validation allows valid config values
        Configuration configuration = new Configuration();
        configuration.setString(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(), "100MB");

        DynamicConfigManager dynamicConfigManager = createManager(configuration);
        dynamicConfigManager.startup();

        // Adjust rate limiter value - should succeed
        dynamicConfigManager.alterConfigs(
                Collections.singletonList(
                        new AlterConfig(
                                ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(),
                                "200MB",
                                AlterConfigOpType.SET)));

        // Verify config was persisted to ZK
        Map<String, String> zkConfig = zookeeperClient.fetchEntityConfig();
        assertThat(zkConfig.get(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key()))
                .isEqualTo("200MB");
    }

    @Test
    void testConfigValidatorWithMultipleValidators() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(), "100MB");

        try (LakeCatalogDynamicLoader lakeCatalogDynamicLoader =
                new LakeCatalogDynamicLoader(configuration, null, true)) {
            DynamicConfigManager dynamicConfigManager = createManager(configuration);

            // Register reconfigurables - generic type validation works automatically
            dynamicConfigManager.register(lakeCatalogDynamicLoader);
            dynamicConfigManager.startup();

            // Change multiple configs - generic validation applies to all
            dynamicConfigManager.alterConfigs(
                    Arrays.asList(
                            new AlterConfig(
                                    ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(),
                                    "200MB",
                                    AlterConfigOpType.SET),
                            new AlterConfig(
                                    DATALAKE_FORMAT.key(), "paimon", AlterConfigOpType.SET)));

            // Verify both configs were applied
            Map<String, String> zkConfig = zookeeperClient.fetchEntityConfig();
            assertThat(zkConfig.get(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key()))
                    .isEqualTo("200MB");
            assertThat(zkConfig.get(DATALAKE_FORMAT.key())).isEqualTo("paimon");
            assertThat(lakeCatalogDynamicLoader.getLakeCatalogContainer().getDataLakeFormat())
                    .isEqualTo(PAIMON);
        }
    }

    @Test
    void testPreventInvalidSnapshotInterval() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofMinutes(10));

        DynamicConfigManager dynamicConfigManager = createManager(configuration);
        dynamicConfigManager.startup();

        // Try to set snapshot interval to an invalid value - should be rejected by type validation
        assertThatThrownBy(
                        () ->
                                dynamicConfigManager.alterConfigs(
                                        Collections.singletonList(
                                                new AlterConfig(
                                                        ConfigOptions.KV_SNAPSHOT_INTERVAL.key(),
                                                        "invalid_value",
                                                        AlterConfigOpType.SET))))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Cannot parse 'invalid_value' as Duration for config 'kv.snapshot.interval'");
    }

    @Test
    void testDynamicSnapshotIntervalChange() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofMinutes(10));

        DynamicConfigManager dynamicConfigManager = createManager(configuration);

        AtomicReference<Duration> reconfiguredInterval = new AtomicReference<>();
        dynamicConfigManager.register(
                new ServerReconfigurable() {
                    @Override
                    public void validate(Configuration newConfig) throws ConfigException {}

                    @Override
                    public void reconfigure(Configuration newConfig) {
                        reconfiguredInterval.set(newConfig.get(ConfigOptions.KV_SNAPSHOT_INTERVAL));
                    }
                });
        dynamicConfigManager.startup();

        // Change snapshot interval to 5 minutes - should succeed
        dynamicConfigManager.alterConfigs(
                Collections.singletonList(
                        new AlterConfig(
                                ConfigOptions.KV_SNAPSHOT_INTERVAL.key(),
                                "5min",
                                AlterConfigOpType.SET)));

        // Verify config was persisted to ZK
        Map<String, String> zkConfig = zookeeperClient.fetchEntityConfig();
        assertThat(zkConfig.get(ConfigOptions.KV_SNAPSHOT_INTERVAL.key())).isEqualTo("5min");

        // Verify the reconfigurable was notified with the new value
        assertThat(reconfiguredInterval.get()).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void testDynamicKvLeaderReplicaMemoryReservedChange() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.KV_LEADER_REPLICA_MEMORY_RESERVED, MemorySize.parse("8mb"));

        DynamicConfigManager dynamicConfigManager =
                new DynamicConfigManager(zookeeperClient, configuration, true);

        AtomicReference<MemorySize> reconfiguredMemoryReserved = new AtomicReference<>();
        dynamicConfigManager.register(
                new ServerReconfigurable() {
                    @Override
                    public void validate(Configuration newConfig) throws ConfigException {}

                    @Override
                    public void reconfigure(Configuration newConfig) {
                        reconfiguredMemoryReserved.set(
                                newConfig.get(ConfigOptions.KV_LEADER_REPLICA_MEMORY_RESERVED));
                    }
                });
        dynamicConfigManager.startup();

        assertThatCode(
                        () ->
                                dynamicConfigManager.alterConfigs(
                                        Collections.singletonList(
                                                new AlterConfig(
                                                        ConfigOptions
                                                                .KV_LEADER_REPLICA_MEMORY_RESERVED
                                                                .key(),
                                                        "16mb",
                                                        AlterConfigOpType.SET))))
                .doesNotThrowAnyException();

        Map<String, String> zkConfig = zookeeperClient.fetchEntityConfig();
        assertThat(zkConfig.get(ConfigOptions.KV_LEADER_REPLICA_MEMORY_RESERVED.key()))
                .isEqualTo("16mb");
        assertThat(reconfiguredMemoryReserved.get()).isEqualTo(MemorySize.parse("16mb"));
    }

    @Test
    void testDynamicReconfigurationOfRemoteDataDirs() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.REMOTE_DATA_DIR, "hdfs://default-dir");
        configuration.set(
                ConfigOptions.REMOTE_DATA_DIRS, Arrays.asList("hdfs://dir1", "hdfs://dir2"));

        try (RemoteDirDynamicLoader remoteDirDynamicLoader =
                new RemoteDirDynamicLoader(configuration)) {
            DynamicConfigManager dynamicConfigManager = createManager(configuration);
            dynamicConfigManager.register(remoteDirDynamicLoader);
            dynamicConfigManager.startup();

            // Verify initial selector is RoundRobin (default strategy)
            assertThat(remoteDirDynamicLoader.getRemoteDirSelector())
                    .isInstanceOf(RoundRobinRemoteDirSelector.class);

            // Change multiple configs - generic validation applies to all
            dynamicConfigManager.alterConfigs(
                    Arrays.asList(
                            new AlterConfig(
                                    ConfigOptions.REMOTE_DATA_DIRS.key(),
                                    "hdfs://dir1,hdfs://dir2,hdfs://dir3",
                                    AlterConfigOpType.SET),
                            new AlterConfig(
                                    ConfigOptions.REMOTE_DATA_DIRS_STRATEGY.key(),
                                    ConfigOptions.RemoteDataDirStrategy.WEIGHTED_ROUND_ROBIN.name(),
                                    AlterConfigOpType.SET),
                            new AlterConfig(
                                    ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS.key(),
                                    "1,2,3",
                                    AlterConfigOpType.SET)));

            // Verify both configs were applied
            Map<String, String> zkConfig = zookeeperClient.fetchEntityConfig();
            assertThat(zkConfig.get(ConfigOptions.REMOTE_DATA_DIRS.key()))
                    .isEqualTo("hdfs://dir1,hdfs://dir2,hdfs://dir3");
            assertThat(zkConfig.get(ConfigOptions.REMOTE_DATA_DIRS_STRATEGY.key()))
                    .isEqualTo(ConfigOptions.RemoteDataDirStrategy.WEIGHTED_ROUND_ROBIN.name());
            assertThat(zkConfig.get(ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS.key()))
                    .isEqualTo("1,2,3");

            // Wait for config change to propagate via ZK watcher
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(remoteDirDynamicLoader.getRemoteDirSelector())
                                    .isInstanceOf(WeightedRoundRobinRemoteDirSelector.class));
        }
    }

    @Test
    void testPreventInvalidMinInSyncReplicas() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInt(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 1);

        DynamicConfigManager dynamicConfigManager = createManager(configuration);
        dynamicConfigManager.startup();

        // Try to set min-in-sync-replicas to an invalid value - should be rejected by type
        // validation
        assertThatThrownBy(
                        () ->
                                dynamicConfigManager.alterConfigs(
                                        Collections.singletonList(
                                                new AlterConfig(
                                                        ConfigOptions
                                                                .LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER
                                                                .key(),
                                                        "invalid_value",
                                                        AlterConfigOpType.SET))))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Cannot parse 'invalid_value' as Integer for config"
                                + " 'log.replica.min-in-sync-replicas-number'");
    }

    @Test
    void testDynamicMinInSyncReplicasChange() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInt(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 1);

        DynamicConfigManager dynamicConfigManager = createManager(configuration);

        AtomicInteger reconfiguredValue = new AtomicInteger();
        dynamicConfigManager.register(
                new ServerReconfigurable() {
                    @Override
                    public void validate(Configuration newConfig) throws ConfigException {}

                    @Override
                    public void reconfigure(Configuration newConfig) {
                        reconfiguredValue.set(
                                newConfig.get(
                                        ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER));
                    }
                });
        dynamicConfigManager.startup();

        // Change min-in-sync-replicas to 2 - should succeed
        dynamicConfigManager.alterConfigs(
                Collections.singletonList(
                        new AlterConfig(
                                ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER.key(),
                                "2",
                                AlterConfigOpType.SET)));

        // Verify config was persisted to ZK
        Map<String, String> zkConfig = zookeeperClient.fetchEntityConfig();
        assertThat(zkConfig.get(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER.key()))
                .isEqualTo("2");

        // Verify the reconfigurable was notified with the new value
        assertThat(reconfiguredValue.get()).isEqualTo(2);
    }

    @Test
    void testExplicitDataLakeEnabledRequiresDataLakeFormat() throws Exception {
        try (LakeCatalogDynamicLoader lakeCatalogDynamicLoader =
                new LakeCatalogDynamicLoader(new Configuration(), null, true)) {
            DynamicConfigManager dynamicConfigManager = createManager(new Configuration());
            dynamicConfigManager.register(lakeCatalogDynamicLoader);
            dynamicConfigManager.startup();

            assertThatThrownBy(
                            () ->
                                    dynamicConfigManager.alterConfigs(
                                            Collections.singletonList(
                                                    new AlterConfig(
                                                            DATALAKE_ENABLED.key(),
                                                            "true",
                                                            AlterConfigOpType.SET))))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining(
                            "'datalake.format' must be configured when 'datalake.enabled' is explicitly set to true.");
        }
    }

    @Test
    void testCoordinatorValidatesDiskWriteLimitConfigAgainstDefaults() throws Exception {
        DynamicConfigManager dynamicConfigManager = createDiskWriteLimitDynamicConfigManager();

        alterDiskWriteRecoverRatio(dynamicConfigManager, "0.70");

        assertThatThrownBy(() -> alterDiskWriteRecoverRatio(dynamicConfigManager, "0.0"))
                .isInstanceOf(ConfigException.class);

        assertThatThrownBy(() -> alterDiskWriteLimitRatio(dynamicConfigManager, "0.70"))
                .isInstanceOf(ConfigException.class);

        alterDiskWriteLimitRatio(dynamicConfigManager, "1.0");

        assertThatThrownBy(() -> alterDiskWriteLimitRatio(dynamicConfigManager, "1.1"))
                .isInstanceOf(ConfigException.class);

        assertThat(zookeeperClient.fetchEntityConfig())
                .containsEntry(ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO.key(), "1.0")
                .containsEntry(ConfigOptions.SERVER_DATA_DISK_WRITE_RECOVER_RATIO.key(), "0.70");
    }

    @Test
    void testCoordinatorValidatesRecoverRatioWhenLoweringDiskWriteLimitRatio() throws Exception {
        DynamicConfigManager dynamicConfigManager = createDiskWriteLimitDynamicConfigManager();

        assertThatThrownBy(() -> alterDiskWriteLimitRatio(dynamicConfigManager, "0.70"))
                .isInstanceOf(ConfigException.class);

        assertThat(zookeeperClient.fetchEntityConfig())
                .doesNotContainKey(ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO.key());

        alterDiskWriteRecoverRatio(dynamicConfigManager, "0.60");
        alterDiskWriteLimitRatio(dynamicConfigManager, "0.70");

        assertThatThrownBy(() -> alterDiskWriteRecoverRatio(dynamicConfigManager, "0.70"))
                .isInstanceOf(ConfigException.class);

        assertThat(zookeeperClient.fetchEntityConfig())
                .containsEntry(ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO.key(), "0.70")
                .containsEntry(ConfigOptions.SERVER_DATA_DISK_WRITE_RECOVER_RATIO.key(), "0.60");
    }

    @Test
    void testDynamicDiskWriteLimitConfigChange(@TempDir File tempDir) throws Exception {
        File dataDir = new File(tempDir, "data-0");
        assertThat(dataDir.mkdirs()).isTrue();

        Configuration configuration = new Configuration();
        configuration.setInt(ConfigOptions.TABLET_SERVER_ID, 0);
        configuration.setString(ConfigOptions.DATA_DIR, dataDir.getAbsolutePath());
        configuration.set(ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO, 0.85);

        DynamicConfigManager dynamicConfigManager = createManager(configuration);

        // Create LocalDiskManager and register it
        try (LocalDiskManager localDiskManager = LocalDiskManager.create(configuration)) {
            dynamicConfigManager.register(localDiskManager);
            dynamicConfigManager.startup();

            // Verify initial state
            assertThat(localDiskManager.getDiskWriteLimitRatio()).isEqualTo(0.85);
            assertThat(localDiskManager.getDiskWriteRecoverRatio()).isEqualTo(0.80);
            assertThat(localDiskManager.getDiskUsageMonitor().getWriteLimitRatio()).isEqualTo(0.85);
            assertThat(localDiskManager.getDiskUsageMonitor().getWriteRecoverRatio())
                    .isEqualTo(0.80);

            // Lower the recover ratio before lowering the limit.
            alterDiskWriteRecoverRatio(dynamicConfigManager, "0.60");
            alterDiskWriteLimitRatio(dynamicConfigManager, "0.70");

            // Verify both ratios took effect immediately (reconfigure triggers runOnce).
            assertThat(localDiskManager.getDiskWriteLimitRatio()).isEqualTo(0.70);
            assertThat(localDiskManager.getDiskWriteRecoverRatio()).isEqualTo(0.60);
            assertThat(localDiskManager.getDiskUsageMonitor().getWriteLimitRatio()).isEqualTo(0.70);
            assertThat(localDiskManager.getDiskUsageMonitor().getWriteRecoverRatio())
                    .isEqualTo(0.60);

            alterDiskWriteRecoverRatio(dynamicConfigManager, "0.65");

            assertThat(localDiskManager.getDiskWriteLimitRatio()).isEqualTo(0.70);
            assertThat(localDiskManager.getDiskWriteRecoverRatio()).isEqualTo(0.65);
            assertThat(localDiskManager.getDiskUsageMonitor().getWriteLimitRatio()).isEqualTo(0.70);
            assertThat(localDiskManager.getDiskUsageMonitor().getWriteRecoverRatio())
                    .isEqualTo(0.65);

            // Verify config was persisted to ZK
            Map<String, String> zkConfig = zookeeperClient.fetchEntityConfig();
            assertThat(zkConfig.get(ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO.key()))
                    .isEqualTo("0.70");
            assertThat(zkConfig.get(ConfigOptions.SERVER_DATA_DISK_WRITE_RECOVER_RATIO.key()))
                    .isEqualTo("0.65");

            assertThatThrownBy(() -> alterDiskWriteLimitRatio(dynamicConfigManager, "0.0"))
                    .isInstanceOf(ConfigException.class);

            assertThatThrownBy(() -> alterDiskWriteRecoverRatio(dynamicConfigManager, "0.0"))
                    .isInstanceOf(ConfigException.class);

            assertThatThrownBy(() -> alterDiskWriteRecoverRatio(dynamicConfigManager, "0.70"))
                    .isInstanceOf(ConfigException.class);

            Configuration invalidReconfigure = new Configuration(configuration);
            invalidReconfigure.set(ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO, 0.65);
            invalidReconfigure.set(ConfigOptions.SERVER_DATA_DISK_WRITE_RECOVER_RATIO, 0.65);
            assertThatThrownBy(() -> localDiskManager.reconfigure(invalidReconfigure))
                    .isInstanceOf(ConfigException.class);

            assertThat(localDiskManager.getDiskWriteLimitRatio()).isEqualTo(0.70);
            assertThat(localDiskManager.getDiskWriteRecoverRatio()).isEqualTo(0.65);
            assertThat(localDiskManager.getDiskUsageMonitor().getWriteLimitRatio()).isEqualTo(0.70);
            assertThat(localDiskManager.getDiskUsageMonitor().getWriteRecoverRatio())
                    .isEqualTo(0.65);
        }
    }

    private DynamicConfigManager createDiskWriteLimitDynamicConfigManager() throws Exception {
        DynamicConfigManager dynamicConfigManager = createManager(new Configuration());
        dynamicConfigManager.register(new DiskWriteLimitConfigValidator());
        dynamicConfigManager.startup();
        return dynamicConfigManager;
    }

    private static void alterDiskWriteLimitRatio(
            DynamicConfigManager dynamicConfigManager, String value) throws Exception {
        alterConfig(
                dynamicConfigManager,
                ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO.key(),
                value);
    }

    private static void alterDiskWriteRecoverRatio(
            DynamicConfigManager dynamicConfigManager, String value) throws Exception {
        alterConfig(
                dynamicConfigManager,
                ConfigOptions.SERVER_DATA_DISK_WRITE_RECOVER_RATIO.key(),
                value);
    }

    private static void alterConfig(
            DynamicConfigManager dynamicConfigManager, String key, String value) throws Exception {
        dynamicConfigManager.alterConfigs(
                Collections.singletonList(new AlterConfig(key, value, AlterConfigOpType.SET)));
    }

    @Test
    void testAppendAndSubtractOnMapConfig() throws Exception {
        Configuration configuration = new Configuration();
        DynamicConfigManager dynamicConfigManager = createManager(configuration);

        AtomicReference<Map<String, String>> reconfiguredUsers = new AtomicReference<>();
        dynamicConfigManager.register(
                new ServerReconfigurable() {
                    @Override
                    public void validate(Configuration newConfig) throws ConfigException {}

                    @Override
                    public void reconfigure(Configuration newConfig) {
                        reconfiguredUsers.set(newConfig.get(ConfigOptions.SERVER_SASL_CREDENTIALS));
                    }
                });
        dynamicConfigManager.startup();

        // APPEND first user
        dynamicConfigManager.alterConfigs(
                Collections.singletonList(
                        new AlterConfig(
                                ConfigOptions.SERVER_SASL_CREDENTIALS.key(),
                                "admin:admin-secret",
                                AlterConfigOpType.APPEND)));

        Map<String, String> zkConfig = zookeeperClient.fetchEntityConfig();
        assertThat(zkConfig.get(ConfigOptions.SERVER_SASL_CREDENTIALS.key()))
                .isEqualTo("admin:admin-secret");
        assertThat(reconfiguredUsers.get()).containsEntry("admin", "admin-secret");

        // APPEND second user
        dynamicConfigManager.alterConfigs(
                Collections.singletonList(
                        new AlterConfig(
                                ConfigOptions.SERVER_SASL_CREDENTIALS.key(),
                                "bob:bob-secret",
                                AlterConfigOpType.APPEND)));

        zkConfig = zookeeperClient.fetchEntityConfig();
        assertThat(zkConfig.get(ConfigOptions.SERVER_SASL_CREDENTIALS.key()))
                .isEqualTo("admin:admin-secret,bob:bob-secret");
        assertThat(reconfiguredUsers.get())
                .containsEntry("admin", "admin-secret")
                .containsEntry("bob", "bob-secret");

        // SUBTRACT with an existing key but a different value should remove by map key.
        dynamicConfigManager.alterConfigs(
                Collections.singletonList(
                        new AlterConfig(
                                ConfigOptions.SERVER_SASL_CREDENTIALS.key(),
                                "admin:wrong-secret",
                                AlterConfigOpType.SUBTRACT)));

        zkConfig = zookeeperClient.fetchEntityConfig();
        assertThat(zkConfig.get(ConfigOptions.SERVER_SASL_CREDENTIALS.key()))
                .isEqualTo("bob:bob-secret");
        assertThat(reconfiguredUsers.get())
                .containsEntry("bob", "bob-secret")
                .doesNotContainKey("admin");

        // SUBTRACT last user - should write null to override static config
        dynamicConfigManager.alterConfigs(
                Collections.singletonList(
                        new AlterConfig(
                                ConfigOptions.SERVER_SASL_CREDENTIALS.key(),
                                "bob:bob-secret",
                                AlterConfigOpType.SUBTRACT)));

        zkConfig = zookeeperClient.fetchEntityConfig();
        assertThat(zkConfig.containsKey(ConfigOptions.SERVER_SASL_CREDENTIALS.key())).isTrue();
        assertThat(zkConfig.get(ConfigOptions.SERVER_SASL_CREDENTIALS.key())).isNull();
        assertThat(reconfiguredUsers.get()).isNull();
    }

    @Test
    void testAppendOnNonListOrMapConfigIsRejected() throws Exception {
        Configuration configuration = new Configuration();
        DynamicConfigManager dynamicConfigManager = createManager(configuration);
        dynamicConfigManager.startup();

        // APPEND on a non-list/non-map config (kv.snapshot.interval is Duration type)
        // should be rejected immediately by the collection-type check.
        assertThatThrownBy(
                        () ->
                                dynamicConfigManager.alterConfigs(
                                        Collections.singletonList(
                                                new AlterConfig(
                                                        ConfigOptions.KV_SNAPSHOT_INTERVAL.key(),
                                                        "5min",
                                                        AlterConfigOpType.APPEND))))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "APPEND/SUBTRACT operations are only supported for list-typed or map-typed config keys");

        // SUBTRACT on a non-list config should also be rejected.
        assertThatThrownBy(
                        () ->
                                dynamicConfigManager.alterConfigs(
                                        Collections.singletonList(
                                                new AlterConfig(
                                                        ConfigOptions.KV_SNAPSHOT_INTERVAL.key(),
                                                        "5min",
                                                        AlterConfigOpType.SUBTRACT))))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "APPEND/SUBTRACT operations are only supported for list-typed or map-typed config keys");
    }

    @Test
    void testDescribeRedactsSensitiveConfigs() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(ConfigOptions.SERVER_SASL_CREDENTIALS.key(), "admin:admin-secret");
        configuration.setString(
                ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG.key(),
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "user_admin=\"admin-secret\";");

        DynamicConfigManager dynamicConfigManager = createManager(configuration);
        dynamicConfigManager.startup();

        assertThat(dynamicConfigManager.describeConfigs())
                .contains(
                        new ConfigEntry(
                                ConfigOptions.SERVER_SASL_CREDENTIALS.key(),
                                "admin:******",
                                ConfigEntry.ConfigSource.INITIAL_SERVER_CONFIG),
                        new ConfigEntry(
                                ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG.key(),
                                "******",
                                ConfigEntry.ConfigSource.INITIAL_SERVER_CONFIG));
    }

    @Test
    void testAppendReadsStaticConfig() throws Exception {
        Configuration configuration = new Configuration();
        // Pre-configure a static user in server.yaml equivalent
        configuration.setString(ConfigOptions.SERVER_SASL_CREDENTIALS.key(), "admin:admin-secret");

        DynamicConfigManager dynamicConfigManager = createManager(configuration);

        AtomicReference<Map<String, String>> reconfiguredUsers = new AtomicReference<>();
        dynamicConfigManager.register(
                new ServerReconfigurable() {
                    @Override
                    public void validate(Configuration newConfig) throws ConfigException {}

                    @Override
                    public void reconfigure(Configuration newConfig) {
                        reconfiguredUsers.set(newConfig.get(ConfigOptions.SERVER_SASL_CREDENTIALS));
                    }
                });
        dynamicConfigManager.startup();

        // APPEND should build on top of static config
        dynamicConfigManager.alterConfigs(
                Collections.singletonList(
                        new AlterConfig(
                                ConfigOptions.SERVER_SASL_CREDENTIALS.key(),
                                "bob:bob-secret",
                                AlterConfigOpType.APPEND)));

        Map<String, String> zkConfig = zookeeperClient.fetchEntityConfig();
        assertThat(zkConfig.get(ConfigOptions.SERVER_SASL_CREDENTIALS.key()))
                .isEqualTo("admin:admin-secret,bob:bob-secret");
        assertThat(reconfiguredUsers.get())
                .containsEntry("admin", "admin-secret")
                .containsEntry("bob", "bob-secret");
    }

    @Test
    void testSubtractFromStaticConfigWritesNull() throws Exception {
        Configuration configuration = new Configuration();
        // Pre-configure a static user in server.yaml equivalent
        configuration.setString(ConfigOptions.SERVER_SASL_CREDENTIALS.key(), "admin:admin-secret");

        DynamicConfigManager dynamicConfigManager = createManager(configuration);

        AtomicReference<Map<String, String>> reconfiguredUsers = new AtomicReference<>();
        dynamicConfigManager.register(
                new ServerReconfigurable() {
                    @Override
                    public void validate(Configuration newConfig) throws ConfigException {}

                    @Override
                    public void reconfigure(Configuration newConfig) {
                        reconfiguredUsers.set(newConfig.get(ConfigOptions.SERVER_SASL_CREDENTIALS));
                    }
                });
        dynamicConfigManager.startup();

        // SUBTRACT the static user - should write null to override static config
        dynamicConfigManager.alterConfigs(
                Collections.singletonList(
                        new AlterConfig(
                                ConfigOptions.SERVER_SASL_CREDENTIALS.key(),
                                "admin:admin-secret",
                                AlterConfigOpType.SUBTRACT)));

        Map<String, String> zkConfig = zookeeperClient.fetchEntityConfig();
        assertThat(zkConfig.containsKey(ConfigOptions.SERVER_SASL_CREDENTIALS.key())).isTrue();
        assertThat(zkConfig.get(ConfigOptions.SERVER_SASL_CREDENTIALS.key())).isNull();
        assertThat(reconfiguredUsers.get()).isNull();
    }

    @Test
    void testSubtractTrimsWhitespaceAndRemovesByKey() throws Exception {
        Configuration configuration = new Configuration();
        DynamicConfigManager dynamicConfigManager = createManager(configuration);
        dynamicConfigManager.startup();

        // Set up a config with whitespace around entries
        dynamicConfigManager.alterConfigs(
                Collections.singletonList(
                        new AlterConfig(
                                ConfigOptions.SERVER_SASL_CREDENTIALS.key(),
                                "admin:admin-secret, bob:bob-secret",
                                AlterConfigOpType.SET)));

        Map<String, String> zkConfig = zookeeperClient.fetchEntityConfig();
        assertThat(zkConfig.get(ConfigOptions.SERVER_SASL_CREDENTIALS.key()))
                .isEqualTo("admin:admin-secret, bob:bob-secret");

        // SUBTRACT should trim entries and remove the matching map key
        dynamicConfigManager.alterConfigs(
                Collections.singletonList(
                        new AlterConfig(
                                ConfigOptions.SERVER_SASL_CREDENTIALS.key(),
                                "admin:admin-secret",
                                AlterConfigOpType.SUBTRACT)));

        zkConfig = zookeeperClient.fetchEntityConfig();
        assertThat(zkConfig.get(ConfigOptions.SERVER_SASL_CREDENTIALS.key()))
                .isEqualTo("bob:bob-secret");
    }
}
