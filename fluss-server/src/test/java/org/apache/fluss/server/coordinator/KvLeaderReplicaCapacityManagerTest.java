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
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InsufficientKvLeaderReplicaCapacityException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.TabletServerResource;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link KvLeaderReplicaCapacityManager}. */
class KvLeaderReplicaCapacityManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;

    @BeforeAll
    static void beforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    @Test
    void testCapacityUsesAverageMemoryForUnknownTabletServers() {
        CoordinatorMetadataCache metadataCache = new CoordinatorMetadataCache();
        metadataCache.updateMetadata(
                null,
                new HashSet<>(
                        Arrays.asList(
                                tabletServer(0, 100L),
                                tabletServer(1, 100L),
                                unknownMemoryTabletServer(2),
                                tabletServer(3, 10000L))),
                Collections.singletonMap(3, ServerTag.PERMANENT_OFFLINE));

        KvLeaderReplicaCapacityManager manager =
                new KvLeaderReplicaCapacityManager(configWithMemoryReserved(10), metadataCache);

        assertThat(manager.getKvLeaderReplicaCapacity()).isEqualTo(30);
    }

    @Test
    void testCapacityDisabledIfKnownMemoryTabletServersAreNotMajority() {
        CoordinatorMetadataCache metadataCache = new CoordinatorMetadataCache();
        metadataCache.updateMetadata(
                null,
                new HashSet<>(
                        Arrays.asList(
                                tabletServer(0, 100L),
                                unknownMemoryTabletServer(1),
                                unknownMemoryTabletServer(2))),
                Collections.emptyMap());

        KvLeaderReplicaCapacityManager manager =
                new KvLeaderReplicaCapacityManager(configWithMemoryReserved(10), metadataCache);

        assertThat(manager.getKvLeaderReplicaCapacity())
                .isEqualTo(KvLeaderReplicaCapacityManager.CAPACITY_LIMIT_DISABLED);
    }

    @Test
    void testCheckAndIncreaseAndDecrease() {
        CoordinatorMetadataCache metadataCache = new CoordinatorMetadataCache();
        metadataCache.updateMetadata(
                null,
                new HashSet<>(
                        Arrays.asList(
                                tabletServer(0, 100L),
                                tabletServer(1, 100L),
                                unknownMemoryTabletServer(2))),
                Collections.emptyMap());

        KvLeaderReplicaCapacityManager manager =
                new KvLeaderReplicaCapacityManager(configWithMemoryReserved(10), metadataCache);
        manager.checkAndIncrease(20);

        assertThat(manager.getKvLeaderReplicaCount()).isEqualTo(20);
        assertThatThrownBy(() -> manager.checkAndIncrease(11))
                .isInstanceOf(InsufficientKvLeaderReplicaCapacityException.class)
                .hasMessageContaining("currentKvLeaderReplicaCount=20")
                .hasMessageContaining("newKvLeaderReplicaCount=11")
                .hasMessageContaining("kvLeaderReplicaCapacity=30");
        assertThat(manager.getKvLeaderReplicaCount()).isEqualTo(20);

        manager.decrease(5);

        assertThat(manager.getKvLeaderReplicaCount()).isEqualTo(15);
    }

    @Test
    void testReconfigureMemoryReserved() {
        CoordinatorMetadataCache metadataCache = new CoordinatorMetadataCache();
        metadataCache.updateMetadata(
                null,
                new HashSet<>(Arrays.asList(tabletServer(0, 100L), tabletServer(1, 100L))),
                Collections.emptyMap());
        KvLeaderReplicaCapacityManager manager =
                new KvLeaderReplicaCapacityManager(configWithMemoryReserved(10), metadataCache);

        manager.reconfigure(configWithMemoryReserved(20));

        assertThat(manager.getLeaderReplicaMemoryReservedBytes()).isEqualTo(20);
        assertThat(manager.getKvLeaderReplicaCapacity()).isEqualTo(10);

        Configuration invalidConfig = configWithMemoryReserved(0);
        assertThatThrownBy(() -> manager.validate(invalidConfig))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ConfigOptions.KV_LEADER_REPLICA_MEMORY_RESERVED.key());
    }

    @Test
    void testRebuildCurrentCountFromMetadata() {
        MetadataManager metadataManager =
                new MetadataManager(
                        zookeeperClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
        metadataManager.createDatabase("db", DatabaseDescriptor.EMPTY, false);
        metadataManager.createTable(
                TablePath.of("db", "log_table"), DEFAULT_REMOTE_DATA_DIR, logTable(7), null, false);
        metadataManager.createTable(
                TablePath.of("db", "kv_table"), DEFAULT_REMOTE_DATA_DIR, kvTable(5), null, false);
        TablePath partitionedTablePath = TablePath.of("db", "partitioned_kv_table");
        long partitionedTableId =
                metadataManager.createTable(
                        partitionedTablePath,
                        DEFAULT_REMOTE_DATA_DIR,
                        partitionedKvTable(3),
                        null,
                        false);
        PartitionAssignment partitionAssignment = partitionAssignment(partitionedTableId, 3);
        metadataManager.createPartition(
                partitionedTablePath,
                partitionedTableId,
                DEFAULT_REMOTE_DATA_DIR,
                partitionAssignment,
                ResolvedPartitionSpec.fromPartitionName(
                        Collections.singletonList("dt"), "20260707"),
                false);
        metadataManager.createPartition(
                partitionedTablePath,
                partitionedTableId,
                DEFAULT_REMOTE_DATA_DIR,
                partitionAssignment,
                ResolvedPartitionSpec.fromPartitionName(
                        Collections.singletonList("dt"), "20260708"),
                false);

        KvLeaderReplicaCapacityManager manager =
                new KvLeaderReplicaCapacityManager(
                        configWithMemoryReserved(1), new CoordinatorMetadataCache());

        manager.rebuildCurrentCount(metadataManager);

        assertThat(manager.getKvLeaderReplicaCount()).isEqualTo(11);
    }

    @Test
    void testRebuildCurrentCountSkipsDatabaseDeletedDuringScan() {
        MetadataManager metadataManager =
                new TestingMetadataManager()
                        .withDatabases("dropped_db")
                        .withListTablesFailure(
                                "dropped_db",
                                new DatabaseNotExistException(
                                        "Database dropped_db does not exist."),
                                false);
        KvLeaderReplicaCapacityManager manager =
                new KvLeaderReplicaCapacityManager(
                        configWithMemoryReserved(1), new CoordinatorMetadataCache());
        manager.checkAndIncrease(5);

        manager.rebuildCurrentCount(metadataManager);

        assertThat(manager.getKvLeaderReplicaCount()).isEqualTo(0);
    }

    @Test
    void testRebuildCurrentCountSkipsDatabaseDeletedDuringRuntimeListFailure() {
        MetadataManager metadataManager =
                new TestingMetadataManager()
                        .withDatabases("dropped_db")
                        .withListTablesFailure(
                                "dropped_db",
                                new FlussRuntimeException(
                                        "Fail to list tables for database:dropped_db"),
                                false);
        KvLeaderReplicaCapacityManager manager =
                new KvLeaderReplicaCapacityManager(
                        configWithMemoryReserved(1), new CoordinatorMetadataCache());
        manager.checkAndIncrease(5);

        manager.rebuildCurrentCount(metadataManager);

        assertThat(manager.getKvLeaderReplicaCount()).isEqualTo(0);
    }

    @Test
    void testRebuildCurrentCountDoesNotSwallowListTablesFailureForExistingDatabase() {
        MetadataManager metadataManager =
                new TestingMetadataManager()
                        .withDatabases("db")
                        .withListTablesFailure(
                                "db",
                                new FlussRuntimeException("Fail to list tables for database:db"),
                                true);
        KvLeaderReplicaCapacityManager manager =
                new KvLeaderReplicaCapacityManager(
                        configWithMemoryReserved(1), new CoordinatorMetadataCache());

        assertThatThrownBy(() -> manager.rebuildCurrentCount(metadataManager))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("Fail to list tables for database:db");
    }

    @Test
    void testRebuildCurrentCountSkipsTableDeletedDuringScan() {
        TablePath droppedTablePath = TablePath.of("db", "dropped_table");
        MetadataManager metadataManager =
                new TestingMetadataManager()
                        .withDatabases("db")
                        .withTables("db", "dropped_table")
                        .withGetTableFailure(
                                droppedTablePath,
                                new TableNotExistException(
                                        "Table 'db.dropped_table' does not exist."),
                                false);
        KvLeaderReplicaCapacityManager manager =
                new KvLeaderReplicaCapacityManager(
                        configWithMemoryReserved(1), new CoordinatorMetadataCache());
        manager.checkAndIncrease(5);

        manager.rebuildCurrentCount(metadataManager);

        assertThat(manager.getKvLeaderReplicaCount()).isEqualTo(0);
    }

    @Test
    void testRebuildCurrentCountSkipsTableDeletedDuringRuntimeGetFailure() {
        TablePath droppedTablePath = TablePath.of("db", "dropped_table");
        MetadataManager metadataManager =
                new TestingMetadataManager()
                        .withDatabases("db")
                        .withTables("db", "dropped_table")
                        .withGetTableFailure(
                                droppedTablePath,
                                new FlussRuntimeException(
                                        "Failed to get table 'db.dropped_table'."),
                                false);
        KvLeaderReplicaCapacityManager manager =
                new KvLeaderReplicaCapacityManager(
                        configWithMemoryReserved(1), new CoordinatorMetadataCache());
        manager.checkAndIncrease(5);

        manager.rebuildCurrentCount(metadataManager);

        assertThat(manager.getKvLeaderReplicaCount()).isEqualTo(0);
    }

    @Test
    void testRebuildCurrentCountDoesNotSwallowGetTableFailureForExistingTable() {
        TablePath tablePath = TablePath.of("db", "table");
        MetadataManager metadataManager =
                new TestingMetadataManager()
                        .withDatabases("db")
                        .withTables("db", "table")
                        .withGetTableFailure(
                                tablePath,
                                new FlussRuntimeException("Failed to get table 'db.table'."),
                                true);
        KvLeaderReplicaCapacityManager manager =
                new KvLeaderReplicaCapacityManager(
                        configWithMemoryReserved(1), new CoordinatorMetadataCache());

        assertThatThrownBy(() -> manager.rebuildCurrentCount(metadataManager))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("Failed to get table 'db.table'.");
    }

    @Test
    void testRebuildCurrentCountSkipsPartitionedTableDeletedDuringPartitionScan() {
        TablePath droppedTablePath = TablePath.of("db", "dropped_partitioned_table");
        MetadataManager metadataManager =
                new TestingMetadataManager()
                        .withDatabases("db")
                        .withTables("db", "dropped_partitioned_table")
                        .withTableInfo(
                                tableInfo(
                                        droppedTablePath,
                                        1,
                                        partitionedKvTable(3),
                                        DEFAULT_REMOTE_DATA_DIR),
                                false)
                        .withGetPartitionsFailure(
                                droppedTablePath,
                                new FlussRuntimeException("Failed to get partitions."));
        KvLeaderReplicaCapacityManager manager =
                new KvLeaderReplicaCapacityManager(
                        configWithMemoryReserved(1), new CoordinatorMetadataCache());
        manager.checkAndIncrease(5);

        manager.rebuildCurrentCount(metadataManager);

        assertThat(manager.getKvLeaderReplicaCount()).isEqualTo(0);
    }

    @Test
    void testRebuildCurrentCountDoesNotSwallowPartitionScanFailureForExistingTable() {
        TablePath tablePath = TablePath.of("db", "partitioned_table");
        MetadataManager metadataManager =
                new TestingMetadataManager()
                        .withDatabases("db")
                        .withTables("db", "partitioned_table")
                        .withTableInfo(
                                tableInfo(
                                        tablePath,
                                        1,
                                        partitionedKvTable(3),
                                        DEFAULT_REMOTE_DATA_DIR),
                                true)
                        .withGetPartitionsFailure(
                                tablePath, new FlussRuntimeException("Failed to get partitions."));
        KvLeaderReplicaCapacityManager manager =
                new KvLeaderReplicaCapacityManager(
                        configWithMemoryReserved(1), new CoordinatorMetadataCache());

        assertThatThrownBy(() -> manager.rebuildCurrentCount(metadataManager))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("Failed to get partitions.");
    }

    private static final class TestingMetadataManager extends MetadataManager {

        private List<String> databases = Collections.emptyList();
        private final Set<String> existingDatabases = new HashSet<>();
        private final Map<String, List<String>> tablesByDatabase = new HashMap<>();
        private final Map<String, RuntimeException> listTablesFailures = new HashMap<>();
        private final Set<TablePath> existingTables = new HashSet<>();
        private final Map<TablePath, TableInfo> tableInfos = new HashMap<>();
        private final Map<TablePath, RuntimeException> getTableFailures = new HashMap<>();
        private final Map<TablePath, Set<String>> partitionsByTable = new HashMap<>();
        private final Map<TablePath, RuntimeException> getPartitionsFailures = new HashMap<>();

        private TestingMetadataManager() {
            super(null, new Configuration(), null);
        }

        private TestingMetadataManager withDatabases(String... databases) {
            this.databases = Arrays.asList(databases);
            return this;
        }

        private TestingMetadataManager withTables(String database, String... tables) {
            existingDatabases.add(database);
            tablesByDatabase.put(database, Arrays.asList(tables));
            return this;
        }

        private TestingMetadataManager withListTablesFailure(
                String database, RuntimeException failure, boolean databaseExists) {
            if (databaseExists) {
                existingDatabases.add(database);
            } else {
                existingDatabases.remove(database);
            }
            listTablesFailures.put(database, failure);
            return this;
        }

        private TestingMetadataManager withTableInfo(TableInfo tableInfo, boolean tableExists) {
            tableInfos.put(tableInfo.getTablePath(), tableInfo);
            if (tableExists) {
                existingTables.add(tableInfo.getTablePath());
            } else {
                existingTables.remove(tableInfo.getTablePath());
            }
            return this;
        }

        private TestingMetadataManager withGetTableFailure(
                TablePath tablePath, RuntimeException failure, boolean tableExists) {
            if (tableExists) {
                existingTables.add(tablePath);
            } else {
                existingTables.remove(tablePath);
            }
            getTableFailures.put(tablePath, failure);
            return this;
        }

        private TestingMetadataManager withGetPartitionsFailure(
                TablePath tablePath, RuntimeException failure) {
            getPartitionsFailures.put(tablePath, failure);
            return this;
        }

        @Override
        public List<String> listDatabases() {
            return databases;
        }

        @Override
        public boolean databaseExists(String databaseName) {
            return existingDatabases.contains(databaseName);
        }

        @Override
        public List<String> listTables(String databaseName) throws DatabaseNotExistException {
            RuntimeException failure = listTablesFailures.get(databaseName);
            if (failure != null) {
                throw failure;
            }
            List<String> tables = tablesByDatabase.get(databaseName);
            return tables == null ? Collections.emptyList() : tables;
        }

        @Override
        public TableInfo getTable(TablePath tablePath) throws TableNotExistException {
            RuntimeException failure = getTableFailures.get(tablePath);
            if (failure != null) {
                throw failure;
            }
            TableInfo tableInfo = tableInfos.get(tablePath);
            if (tableInfo == null) {
                throw new TableNotExistException("Table '" + tablePath + "' does not exist.");
            }
            return tableInfo;
        }

        @Override
        public boolean tableExists(TablePath tablePath) {
            return existingTables.contains(tablePath);
        }

        @Override
        public Set<String> getPartitions(TablePath tablePath) {
            RuntimeException failure = getPartitionsFailures.get(tablePath);
            if (failure != null) {
                throw failure;
            }
            Set<String> partitions = partitionsByTable.get(tablePath);
            return partitions == null ? Collections.emptySet() : partitions;
        }
    }

    private static Configuration configWithMemoryReserved(long bytes) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KV_LEADER_REPLICA_MEMORY_RESERVED, new MemorySize(bytes));
        return conf;
    }

    private static ServerInfo tabletServer(int serverId, long memoryBytes) {
        return new ServerInfo(
                serverId,
                null,
                Endpoint.fromListenersString("INTERNAL://localhost:" + (10000 + serverId)),
                ServerType.TABLET_SERVER,
                new TabletServerResource(null, memoryBytes));
    }

    private static ServerInfo unknownMemoryTabletServer(int serverId) {
        return new ServerInfo(
                serverId,
                null,
                Endpoint.fromListenersString("INTERNAL://localhost:" + (10000 + serverId)),
                ServerType.TABLET_SERVER,
                TabletServerResource.unknown());
    }

    private static TableDescriptor logTable(int bucketCount) {
        return TableDescriptor.builder()
                .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                .distributedBy(bucketCount)
                .build();
    }

    private static TableDescriptor kvTable(int bucketCount) {
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .primaryKey("id")
                                .build())
                .distributedBy(bucketCount)
                .build();
    }

    private static TableDescriptor partitionedKvTable(int bucketCount) {
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("dt", DataTypes.STRING())
                                .primaryKey("id", "dt")
                                .build())
                .distributedBy(bucketCount)
                .partitionedBy("dt")
                .build();
    }

    private static TableInfo tableInfo(
            TablePath tablePath,
            long tableId,
            TableDescriptor tableDescriptor,
            String remoteDataDir) {
        long currentMillis = System.currentTimeMillis();
        return TableInfo.of(
                tablePath,
                tableId,
                1,
                tableDescriptor,
                remoteDataDir,
                currentMillis,
                currentMillis);
    }

    private static PartitionAssignment partitionAssignment(long tableId, int bucketCount) {
        Map<Integer, BucketAssignment> bucketAssignments = new HashMap<>();
        for (int bucketId = 0; bucketId < bucketCount; bucketId++) {
            bucketAssignments.put(bucketId, BucketAssignment.of(0));
        }
        return new PartitionAssignment(tableId, bucketAssignments);
    }
}
