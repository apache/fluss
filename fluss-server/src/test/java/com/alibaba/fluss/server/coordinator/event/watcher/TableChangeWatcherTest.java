/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.coordinator.event.watcher;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.SchemaInfo;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.coordinator.MetadataManager;
import com.alibaba.fluss.server.coordinator.event.CoordinatorEvent;
import com.alibaba.fluss.server.coordinator.event.CreatePartitionEvent;
import com.alibaba.fluss.server.coordinator.event.CreateTableEvent;
import com.alibaba.fluss.server.coordinator.event.DropPartitionEvent;
import com.alibaba.fluss.server.coordinator.event.DropTableEvent;
import com.alibaba.fluss.server.coordinator.event.TestingEventManager;
import com.alibaba.fluss.server.utils.TableAssignmentUtils;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperExtension;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;
import com.alibaba.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TableChangeWatcher}. */
class TableChangeWatcherTest {

    private static final String DEFAULT_DB = "db";

    private static final TableDescriptor TEST_TABLE =
            TableDescriptor.builder()
                    .schema(Schema.newBuilder().column("a", DataTypes.INT()).build())
                    .distributedBy(3, "a")
                    .build()
                    .withReplicationFactor(3);

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;
    private TestingEventManager eventManager;
    private TableChangeWatcher tableChangeWatcher;
    private static MetadataManager metadataManager;

    @BeforeAll
    static void beforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        metadataManager = new MetadataManager(zookeeperClient, new Configuration());
        metadataManager.createDatabase(DEFAULT_DB, DatabaseDescriptor.builder().build(), false);
    }

    @BeforeEach
    void before() {
        eventManager = new TestingEventManager();
        tableChangeWatcher = new TableChangeWatcher(zookeeperClient, eventManager);
        tableChangeWatcher.start();
    }

    @AfterEach
    void after() {
        if (tableChangeWatcher != null) {
            tableChangeWatcher.stop();
        }
    }

    @Test
    void testTableChanges() {
        // create tables, collect create table events
        List<CoordinatorEvent> expectedCreateTableEvents = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TablePath tablePath = TablePath.of(DEFAULT_DB, "table_" + i);
            TableAssignment tableAssignment =
                    TableAssignmentUtils.generateAssignment(3, 3, new int[] {0, 1, 2});
            long tableId =
                    metadataManager.createTable(tablePath, TEST_TABLE, tableAssignment, false);
            SchemaInfo schemaInfo = metadataManager.getLatestSchema(tablePath);
            long currentMillis = System.currentTimeMillis();
            expectedCreateTableEvents.add(
                    new CreateTableEvent(
                            TableInfo.of(
                                    tablePath,
                                    tableId,
                                    schemaInfo.getSchemaId(),
                                    TEST_TABLE,
                                    currentMillis,
                                    currentMillis),
                            tableAssignment));
        }

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventManager.getEvents())
                                .containsExactlyInAnyOrderElementsOf(expectedCreateTableEvents));

        // drop tables, collect drop table events
        List<CoordinatorEvent> expectedTableEvents = new ArrayList<>();
        for (CoordinatorEvent coordinatorEvent : expectedCreateTableEvents) {
            CreateTableEvent createTableEvent = (CreateTableEvent) coordinatorEvent;
            TableInfo tableInfo = createTableEvent.getTableInfo();
            metadataManager.dropTable(tableInfo.getTablePath(), false);
            expectedTableEvents.add(new DropTableEvent(tableInfo.getTableId(), false));
        }

        // collect all events and check the all events
        List<CoordinatorEvent> allEvents = new ArrayList<>(expectedCreateTableEvents);
        allEvents.addAll(expectedTableEvents);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventManager.getEvents())
                                .containsExactlyInAnyOrderElementsOf(allEvents));
    }

    @Test
    void testPartitionedTable() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "partition_table");
        TableDescriptor partitionedTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .build())
                        .distributedBy(3, "a")
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(), "DAY")
                        .build()
                        .withReplicationFactor(3);
        long tableId = metadataManager.createTable(tablePath, partitionedTable, null, false);
        List<CoordinatorEvent> expectedEvents = new ArrayList<>();
        SchemaInfo schemaInfo = metadataManager.getLatestSchema(tablePath);
        // create table event
        long currentMillis = System.currentTimeMillis();
        expectedEvents.add(
                new CreateTableEvent(
                        TableInfo.of(
                                tablePath,
                                tableId,
                                schemaInfo.getSchemaId(),
                                partitionedTable,
                                currentMillis,
                                currentMillis),
                        TableAssignment.builder().build()));

        // register partition
        PartitionAssignment partitionAssignment =
                new PartitionAssignment(
                        tableId,
                        TableAssignmentUtils.generateAssignment(3, 3, new int[] {0, 1, 2})
                                .getBucketAssignments());
        // register assignment
        zookeeperClient.registerPartitionAssignment(1L, partitionAssignment);
        zookeeperClient.registerPartitionAssignment(2L, partitionAssignment);

        // register partitions
        zookeeperClient.registerPartition(tablePath, tableId, "2011", 1L);
        zookeeperClient.registerPartition(tablePath, tableId, "2022", 2L);

        // create partitions events
        expectedEvents.add(
                new CreatePartitionEvent(tablePath, tableId, 1L, "2011", partitionAssignment));
        expectedEvents.add(
                new CreatePartitionEvent(tablePath, tableId, 2L, "2022", partitionAssignment));

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventManager.getEvents())
                                .containsExactlyInAnyOrderElementsOf(expectedEvents));

        metadataManager.dropTable(tablePath, false);

        // drop partitions event
        expectedEvents.add(new DropPartitionEvent(tableId, 1L, "2011"));
        expectedEvents.add(new DropPartitionEvent(tableId, 2L, "2022"));
        // drop table event
        expectedEvents.add(new DropTableEvent(tableId, true));

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventManager.getEvents())
                                .containsExactlyInAnyOrderElementsOf(expectedEvents));
    }
}
