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

package org.apache.fluss.server.metadata;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA2_SCHEMA;
import static org.apache.fluss.record.TestData.DATA2_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ServerSchemaCache}. */
public class ServerSchemaCacheTest {

    @Test
    void testPublishAndSubscribeSchemaChange() {
        ServerSchemaCache manager =
                new ServerSchemaCache(new TestingMetadataManager(Collections.emptyList()));

        SchemaGetter subscriber1 =
                manager.subscribeWithInitialSchema(
                        1L, new TablePath("test_tb", "test_table_1"), (short) 1, DATA1_SCHEMA);
        SchemaGetter subscriber2 =
                manager.subscribeWithInitialSchema(
                        2L, new TablePath("test_tb", "test_table_2"), (short) 1, DATA1_SCHEMA);
        assertThat(subscriber1.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA1_SCHEMA, 1));
        assertThat(subscriber1.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA1_SCHEMA, 1));

        manager.updateLatestSchema(2L, (short) 2, DATA2_SCHEMA);
        assertThat(subscriber1.getLatestSchemaInfo()).isNotNull();
        assertThat(subscriber1.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA1_SCHEMA, 1));
        assertThat(subscriber2.getLatestSchemaInfo()).isNotNull();
        assertThat(subscriber2.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA2_SCHEMA, 2));

        manager.updateLatestSchema(1L, (short) 2, DATA2_SCHEMA);
        assertThat(subscriber1.getLatestSchemaInfo()).isNotNull();
        assertThat(subscriber1.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA2_SCHEMA, 2));
        assertThat(subscriber2.getLatestSchemaInfo()).isNotNull();
        assertThat(subscriber2.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA2_SCHEMA, 2));

        // one more redundant schema change.
        manager.updateLatestSchema(1L, (short) 1, DATA2_SCHEMA);
        assertThat(subscriber1.getLatestSchemaInfo()).isNotNull();
        assertThat(subscriber1.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA2_SCHEMA, 2));
        assertThat(subscriber2.getLatestSchemaInfo()).isNotNull();
        assertThat(subscriber2.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA2_SCHEMA, 2));
    }

    @Test
    void testRefreshLatestSchema() {
        TableInfo evolved =
                TableInfo.of(
                        DATA1_TABLE_PATH,
                        DATA1_TABLE_ID,
                        2,
                        DATA2_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        0L,
                        0L);
        TestingMetadataManager metadata =
                new TestingMetadataManager(Arrays.asList(DATA1_TABLE_INFO, evolved));
        ServerSchemaCache cache = new ServerSchemaCache(metadata);
        SchemaGetter getter =
                cache.subscribeWithInitialSchema(DATA1_TABLE_ID, DATA1_TABLE_PATH, 1, DATA1_SCHEMA);

        assertThat(getter.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA1_SCHEMA, 1));
        assertThat(metadata.latestSchemaLookups).isZero();
        assertThat(getter.refreshLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA2_SCHEMA, 2));
        assertThat(getter.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA2_SCHEMA, 2));
        assertThat(metadata.latestSchemaLookups).isEqualTo(1);
        // A direct refresh always consults metadata; a cached read does not.
        assertThat(getter.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA2_SCHEMA, 2));
        assertThat(metadata.latestSchemaLookups).isEqualTo(1);
        assertThat(getter.refreshLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA2_SCHEMA, 2));
        assertThat(metadata.latestSchemaLookups).isEqualTo(2);
        getter.release();
    }

    @Test
    void testRefreshDoesNotOverwriteNewerNotification() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        MetadataManager metadata =
                new MetadataManager(
                        null,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true)) {
                    @Override
                    public SchemaInfo getLatestSchema(TablePath path) {
                        started.countDown();
                        try {
                            if (!proceed.await(10, TimeUnit.SECONDS)) {
                                throw new IllegalStateException(
                                        "Timed out waiting to finish refresh");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(e);
                        }
                        return new SchemaInfo(DATA2_SCHEMA, 2);
                    }
                };
        ServerSchemaCache cache = new ServerSchemaCache(metadata);
        SchemaGetter getter =
                cache.subscribeWithInitialSchema(DATA1_TABLE_ID, DATA1_TABLE_PATH, 1, DATA1_SCHEMA);
        Schema newer =
                Schema.newBuilder().fromSchema(DATA2_SCHEMA).column("d", DataTypes.INT()).build();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<SchemaInfo> result = executor.submit(() -> getter.refreshLatestSchemaInfo());
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            cache.updateLatestSchema(DATA1_TABLE_ID, (short) 3, newer);
            proceed.countDown();
            assertThat(result.get(10, TimeUnit.SECONDS)).isEqualTo(new SchemaInfo(newer, 3));
            assertThat(getter.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(newer, 3));
        } finally {
            proceed.countDown();
            executor.shutdownNow();
            getter.release();
        }
    }

    @Test
    void testRefreshFailureDoesNotReplaceCachedSchema() {
        MetadataManager metadata =
                new MetadataManager(
                        null,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true)) {
                    @Override
                    public SchemaInfo getLatestSchema(TablePath tablePath) {
                        throw new IllegalStateException("metadata unavailable");
                    }
                };
        ServerSchemaCache cache = new ServerSchemaCache(metadata);
        SchemaGetter getter =
                cache.subscribeWithInitialSchema(DATA1_TABLE_ID, DATA1_TABLE_PATH, 1, DATA1_SCHEMA);
        assertThatThrownBy(() -> getter.refreshLatestSchemaInfo())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("metadata unavailable");
        assertThat(getter.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA1_SCHEMA, 1));
        getter.release();
    }

    @Test
    void testGetHistorySchema() {
        TableInfo tableInfo2 =
                TableInfo.of(
                        DATA1_TABLE_PATH,
                        DATA1_TABLE_ID,
                        2,
                        DATA2_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        ServerSchemaCache manager =
                new ServerSchemaCache(
                        new TestingMetadataManager(Arrays.asList(DATA1_TABLE_INFO, tableInfo2)));
        SchemaGetter schemaGetter =
                manager.subscribeWithInitialSchema(
                        DATA1_TABLE_ID, DATA1_TABLE_PATH, (short) 2, DATA2_SCHEMA);
        assertThat(schemaGetter.getSchema(2)).isEqualTo(DATA2_SCHEMA);
        assertThat(schemaGetter.getSchema(1)).isEqualTo(DATA1_SCHEMA);
        assertThatThrownBy(() -> schemaGetter.getSchema(3))
                .isExactlyInstanceOf(SchemaNotExistException.class);
    }

    @Test
    void testLatestSchemaUpdateDoesNotPolluteHistoricalSchemaCache() {
        long tableId = 1L;
        TablePath tablePath = new TablePath("test_db", "test_table");
        UpdatingSchemaInfoMap latestSchemas = new UpdatingSchemaInfoMap();
        ServerSchemaCache manager =
                new ServerSchemaCache(
                        new TestingMetadataManager(Collections.emptyList()), latestSchemas);
        SchemaGetter schemaGetter =
                manager.subscribeWithInitialSchema(tableId, tablePath, 2, DATA2_SCHEMA);

        latestSchemas.updateAfterNextGet(
                tableId, new SchemaInfo(DATA1_SCHEMA, 1), new SchemaInfo(DATA2_SCHEMA, 2));

        assertThat(schemaGetter.getSchema(1)).isEqualTo(DATA1_SCHEMA);
        assertThat(schemaGetter.getSchema(1)).isEqualTo(DATA1_SCHEMA);
        assertThat(schemaGetter.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA2_SCHEMA, 2));
    }

    @Test
    void testUnsubscribeSchemaChange() {
        ServerSchemaCache manager =
                new ServerSchemaCache(new TestingMetadataManager(Collections.emptyList()));
        SchemaGetter subscriber1 =
                manager.subscribeWithInitialSchema(
                        1L, new TablePath("test_tb", "test_table_1"), (short) 1, DATA1_SCHEMA);
        SchemaGetter subscriber2 =
                manager.subscribeWithInitialSchema(
                        1L, new TablePath("test_tb", "test_table_1"), (short) 1, DATA1_SCHEMA);
        SchemaGetter subscriber3 =
                manager.subscribeWithInitialSchema(
                        2L, new TablePath("test_tb", "test_table_2"), (short) 1, DATA1_SCHEMA);
        assertThat(manager.getLatestSchemaByTableId()).hasSize(2);
        assertThat(manager.getSubscriberCounters()).hasSize(2);
        assertThat(manager.getSubscriberCounters().get(1L)).isEqualTo(2);

        subscriber1.release();
        assertThat(manager.getLatestSchemaByTableId()).hasSize(2);
        assertThat(manager.getSubscriberCounters()).hasSize(2);
        assertThat(manager.getSubscriberCounters().get(1L)).isEqualTo(1);

        // one more redundant unsubscribe.
        subscriber1.release();
        assertThat(manager.getLatestSchemaByTableId()).hasSize(2);
        assertThat(manager.getSubscriberCounters()).hasSize(2);
        assertThat(manager.getSubscriberCounters().get(1L)).isEqualTo(1);

        subscriber2.release();
        assertThat(manager.getLatestSchemaByTableId()).hasSize(1);
        assertThat(manager.getSubscriberCounters()).hasSize(1);
        assertThat(manager.getSubscriberCounters().get(1L)).isNull();

        subscriber3.release();
        assertThat(manager.getLatestSchemaByTableId()).isEmpty();
        assertThat(manager.getSubscriberCounters()).isEmpty();
        subscriber3.release();
    }

    private static final class TestingMetadataManager extends MetadataManager {

        private final Map<TablePath, Map<Short, Schema>> schemaInfoMap;
        private int latestSchemaLookups;

        public TestingMetadataManager(List<TableInfo> tableInfos) {
            super(
                    null,
                    new Configuration(),
                    new LakeCatalogDynamicLoader(new Configuration(), null, true));
            schemaInfoMap = new HashMap<>();
            tableInfos.forEach(
                    tableInfo -> {
                        schemaInfoMap
                                .computeIfAbsent(tableInfo.getTablePath(), k -> new HashMap<>())
                                .put((short) tableInfo.getSchemaId(), tableInfo.getSchema());
                    });
        }

        @Override
        public SchemaInfo getLatestSchema(TablePath tablePath) {
            latestSchemaLookups++;
            short latestId = Collections.max(schemaInfoMap.get(tablePath).keySet());
            return getSchemaById(tablePath, latestId);
        }

        @Override
        public SchemaInfo getSchemaById(TablePath tablePath, int schemaId)
                throws SchemaNotExistException {
            if (schemaInfoMap.containsKey(tablePath)
                    && schemaInfoMap.get(tablePath).containsKey((short) schemaId)) {
                return new SchemaInfo(schemaInfoMap.get(tablePath).get((short) schemaId), schemaId);
            }
            throw new SchemaNotExistException("Schema not exist");
        }
    }

    private static final class UpdatingSchemaInfoMap extends ConcurrentHashMap<Long, SchemaInfo> {
        private static final long serialVersionUID = 1L;

        private Long tableIdToUpdate;
        private SchemaInfo schemaInfoToInstall;
        private boolean updateAfterNextGet;

        void updateAfterNextGet(
                long tableId, SchemaInfo schemaInfoToReturn, SchemaInfo schemaInfoToInstall) {
            put(tableId, schemaInfoToReturn);
            this.tableIdToUpdate = tableId;
            this.schemaInfoToInstall = schemaInfoToInstall;
            this.updateAfterNextGet = true;
        }

        @Override
        public SchemaInfo get(Object key) {
            SchemaInfo schemaInfo = super.get(key);
            if (updateAfterNextGet && tableIdToUpdate.equals(key)) {
                updateAfterNextGet = false;
                put(tableIdToUpdate, schemaInfoToInstall);
            }
            return schemaInfo;
        }
    }
}
