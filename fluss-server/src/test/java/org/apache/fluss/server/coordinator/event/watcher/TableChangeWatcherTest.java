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

package org.apache.fluss.server.coordinator.event.watcher;

import org.apache.fluss.cluster.TabletServerInfo;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.CreatePartitionEvent;
import org.apache.fluss.server.coordinator.event.CreateTableEvent;
import org.apache.fluss.server.coordinator.event.DropPartitionEvent;
import org.apache.fluss.server.coordinator.event.DropTableEvent;
import org.apache.fluss.server.coordinator.event.SchemaChangeEvent;
import org.apache.fluss.server.coordinator.event.TestingEventManager;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.server.utils.TableAssignmentUtils.generateAssignment;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
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
        metadataManager =
                new MetadataManager(
                        zookeeperClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
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
        List<CoordinatorEvent> expectedEvents = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TablePath tablePath = TablePath.of(DEFAULT_DB, "table_" + i);
            TableAssignment tableAssignment =
                    generateAssignment(
                            3,
                            3,
                            new TabletServerInfo[] {
                                new TabletServerInfo(0, "rack0"),
                                new TabletServerInfo(1, "rack1"),
                                new TabletServerInfo(2, "rack2")
                            });
            long tableId =
                    metadataManager.createTable(tablePath, TEST_TABLE, tableAssignment, false);
            SchemaInfo schemaInfo = metadataManager.getLatestSchema(tablePath);
            long currentMillis = System.currentTimeMillis();
            expectedEvents.add(
                    new CreateTableEvent(
                            TableInfo.of(
                                    tablePath,
                                    tableId,
                                    schemaInfo.getSchemaId(),
                                    TEST_TABLE,
                                    currentMillis,
                                    currentMillis),
                            tableAssignment));
            expectedEvents.add(new SchemaChangeEvent(tablePath, schemaInfo));
        }

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventManager.getEvents())
                                .containsExactlyInAnyOrderElementsOf(expectedEvents));

        // drop tables, collect drop table events
        List<CoordinatorEvent> expectedTableEvents = new ArrayList<>();
        for (CoordinatorEvent coordinatorEvent : expectedEvents) {
            if (coordinatorEvent instanceof SchemaChangeEvent) {
                continue;
            }
            CreateTableEvent createTableEvent = (CreateTableEvent) coordinatorEvent;
            TableInfo tableInfo = createTableEvent.getTableInfo();
            metadataManager.dropTable(tableInfo.getTablePath(), false);
            expectedTableEvents.add(new DropTableEvent(tableInfo.getTableId(), false, false));
        }

        // collect all events and check the all events
        List<CoordinatorEvent> allEvents = new ArrayList<>(expectedEvents);
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
        expectedEvents.add(new SchemaChangeEvent(tablePath, schemaInfo));

        // register partition
        PartitionAssignment partitionAssignment =
                new PartitionAssignment(
                        tableId,
                        generateAssignment(
                                        3,
                                        3,
                                        new TabletServerInfo[] {
                                            new TabletServerInfo(0, "rack0"),
                                            new TabletServerInfo(1, "rack1"),
                                            new TabletServerInfo(2, "rack2")
                                        })
                                .getBucketAssignments());
        // register assignment and metadata
        zookeeperClient.registerPartitionAssignmentAndMetadata(
                1L, "2011", partitionAssignment, tablePath, tableId);
        zookeeperClient.registerPartitionAssignmentAndMetadata(
                2L, "2022", partitionAssignment, tablePath, tableId);

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
        expectedEvents.add(new DropTableEvent(tableId, true, false));

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventManager.getEvents())
                                .containsExactlyInAnyOrderElementsOf(expectedEvents));
    }

    @Test
    void testSchemaChanges() {
        // create tables, collect create table events
        List<CoordinatorEvent> expectedEvents = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TablePath tablePath = TablePath.of(DEFAULT_DB, "table_" + i);
            TableAssignment tableAssignment =
                    generateAssignment(
                            3,
                            3,
                            new TabletServerInfo[] {
                                new TabletServerInfo(0, "rack0"),
                                new TabletServerInfo(1, "rack1"),
                                new TabletServerInfo(2, "rack2")
                            });
            long tableId =
                    metadataManager.createTable(tablePath, TEST_TABLE, tableAssignment, false);
            SchemaInfo schemaInfo = metadataManager.getLatestSchema(tablePath);
            long currentMillis = System.currentTimeMillis();
            expectedEvents.add(
                    new CreateTableEvent(
                            TableInfo.of(
                                    tablePath,
                                    tableId,
                                    schemaInfo.getSchemaId(),
                                    TEST_TABLE,
                                    currentMillis,
                                    currentMillis),
                            tableAssignment));
            expectedEvents.add(new SchemaChangeEvent(tablePath, schemaInfo));
        }

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventManager.getEvents())
                                .containsExactlyInAnyOrderElementsOf(expectedEvents));

        // alter schema.
        List<CoordinatorEvent> expectedTableEvents = new ArrayList<>();
        for (CoordinatorEvent coordinatorEvent : expectedEvents) {
            if (coordinatorEvent instanceof SchemaChangeEvent) {
                continue;
            }
            CreateTableEvent createTableEvent = (CreateTableEvent) coordinatorEvent;
            TableInfo tableInfo = createTableEvent.getTableInfo();
            metadataManager.alterTableSchema(
                    tableInfo.getTablePath(),
                    Collections.singletonList(
                            TableChange.addColumn(
                                    "add_column",
                                    DataTypes.INT(),
                                    null,
                                    TableChange.ColumnPosition.last())),
                    false);
            Schema newSchema =
                    Schema.newBuilder()
                            .fromSchema(tableInfo.getSchema())
                            .column("add_column", DataTypes.INT())
                            .build();
            expectedTableEvents.add(
                    new SchemaChangeEvent(tableInfo.getTablePath(), new SchemaInfo(newSchema, 2)));
        }

        // collect all events and check the all events
        List<CoordinatorEvent> allEvents = new ArrayList<>(expectedEvents);
        allEvents.addAll(expectedTableEvents);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(eventManager.getEvents())
                                .containsExactlyInAnyOrderElementsOf(allEvents));
    }
}
