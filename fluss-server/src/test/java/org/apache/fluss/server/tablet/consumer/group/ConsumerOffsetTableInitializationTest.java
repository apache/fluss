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

package org.apache.fluss.server.tablet.consumer.group;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.metadata.SystemTables;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataProvider;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests initialization of complete consumer-offset table metadata. */
class ConsumerOffsetTableInitializationTest extends ReplicaTestBase {

    private static final long TABLE_ID = 100L;

    private final ManuallyTriggeredScheduledExecutorService executor =
            new ManuallyTriggeredScheduledExecutorService();
    private LakeCatalogDynamicLoader lakeCatalogLoader;
    private MetadataManager metadataManager;
    private ConsumerGroupManager tableManager;

    @BeforeEach
    void createTableManager() throws Exception {
        conf.set(ConfigOptions.OFFSETS_TABLE_NUM_BUCKETS, 50);
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(ConsumerOffsetSchema.getTableSchema())
                        .distributedBy(
                                TEST_ROUTING_BUCKET_COUNT,
                                ConsumerOffsetSchema.getBucketKeyColumn())
                        .build();
        // The table exists before its schema is published.
        zkClient.registerTable(
                SystemTables.CONSUMER_OFFSETS,
                TableRegistration.newTable(
                        TABLE_ID, conf.get(ConfigOptions.REMOTE_DATA_DIR), descriptor));
        lakeCatalogLoader = new LakeCatalogDynamicLoader(conf, null, true);
        metadataManager = new MetadataManager(zkClient, conf, lakeCatalogLoader);
        tableManager =
                new ConsumerGroupManager(
                        TABLET_SERVER_ID,
                        replicaManager,
                        ConsumerOffsetTableConfig.from(conf),
                        zkClient,
                        metadataManager,
                        serverMetadataCache,
                        new TabletServerMetadataProvider(
                                zkClient, metadataManager, serverMetadataCache),
                        testCoordinatorGateway,
                        executor);
    }

    @AfterEach
    void closeLakeCatalogLoader() throws Exception {
        if (lakeCatalogLoader != null) {
            lakeCatalogLoader.close();
        }
    }

    @Test
    void testRequestsShareCompleteTableMetadata() throws Exception {
        int schemaId =
                zkClient.registerFirstSchema(
                        SystemTables.CONSUMER_OFFSETS, ConsumerOffsetSchema.getTableSchema());

        CompletableFuture<TableInfo> first = tableManager.ensureTableAsync();
        CompletableFuture<TableInfo> second = tableManager.ensureTableAsync();
        assertThat(first).isNotDone();
        assertThat(second).isSameAs(first);

        executor.triggerAll();

        TableInfo expected = metadataManager.getTable(SystemTables.CONSUMER_OFFSETS);
        assertThat(expected.getTableId()).isEqualTo(TABLE_ID);
        assertThat(expected.getSchemaId()).isEqualTo(schemaId);
        assertThat(expected.getNumBuckets()).isEqualTo(TEST_ROUTING_BUCKET_COUNT);
        assertThat(first).succeedsWithin(Duration.ofSeconds(10)).isEqualTo(expected);
        assertThat(tableManager.ensureTableAsync()).isSameAs(first);
    }

    @Test
    void testFailedFindPreservesMetadataReceivedFromCoordinator() throws Exception {
        CompletableFuture<TableInfo> pending = tableManager.ensureTableAsync();
        TableInfo published =
                zkClient.getTable(SystemTables.CONSUMER_OFFSETS)
                        .get()
                        .toTableInfo(
                                SystemTables.CONSUMER_OFFSETS,
                                new SchemaInfo(ConsumerOffsetSchema.getTableSchema(), 1));
        TableMetadata update = new TableMetadata(published, Collections.emptyList());
        serverMetadataCache.updateTableMetadata(update);
        tableManager.onTableMetadataUpdate(Collections.singletonList(update));

        executor.triggerAll();
        assertThatThrownBy(pending::join)
                .cause()
                .isInstanceOf(ConsumerGroupOwnerNotAvailableException.class);
        assertThat(tableManager.ensureTableAsync()).isCompletedWithValue(published);
        assertThat(executor.numQueuedRunnables()).isZero();
    }

    @Test
    void testIncompleteMetadataCanBeRetried() throws Exception {
        CompletableFuture<TableInfo> failed = tableManager.ensureTableAsync();
        executor.triggerAll();
        assertThatThrownBy(failed::get)
                .cause()
                .isInstanceOf(ConsumerGroupOwnerNotAvailableException.class);

        zkClient.registerFirstSchema(
                SystemTables.CONSUMER_OFFSETS, ConsumerOffsetSchema.getTableSchema());
        CompletableFuture<TableInfo> retried = tableManager.ensureTableAsync();
        assertThat(retried).isNotSameAs(failed).isNotDone();
        executor.triggerAll();

        assertThat(retried)
                .succeedsWithin(Duration.ofSeconds(10))
                .isEqualTo(metadataManager.getTable(SystemTables.CONSUMER_OFFSETS));
    }
}
