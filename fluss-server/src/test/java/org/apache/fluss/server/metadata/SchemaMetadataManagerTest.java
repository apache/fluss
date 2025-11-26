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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA2_SCHEMA;
import static org.apache.fluss.record.TestData.DATA2_TABLE_DESCRIPTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link SchemaMetadataManager}. */
public class SchemaMetadataManagerTest {

    @Test
    void testPublishAndsubscribeSchemaChange() {
        SchemaMetadataManager manager =
                new SchemaMetadataManager(new TestingMetadataManager(Collections.emptyList()));

        SchemaGetter subscriber1 =
                manager.subscribeWithInitialSchema(
                        new TablePath("test_tb", "test_table_1"), (short) 1, DATA1_SCHEMA);
        SchemaGetter subscriber2 =
                manager.subscribeWithInitialSchema(
                        new TablePath("test_tb", "test_table_2"), (short) 1, DATA1_SCHEMA);
        assertThat(subscriber1.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA1_SCHEMA, 1));
        assertThat(subscriber1.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA1_SCHEMA, 1));

        manager.updateLatestSchema(
                new TablePath("test_tb", "test_table_2"), (short) 2, DATA2_SCHEMA);
        assertThat(subscriber1.getLatestSchemaInfo()).isNotNull();
        assertThat(subscriber1.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA1_SCHEMA, 1));
        assertThat(subscriber2.getLatestSchemaInfo()).isNotNull();
        assertThat(subscriber2.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA2_SCHEMA, 2));

        manager.updateLatestSchema(
                new TablePath("test_tb", "test_table_1"), (short) 2, DATA2_SCHEMA);
        assertThat(subscriber1.getLatestSchemaInfo()).isNotNull();
        assertThat(subscriber1.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA2_SCHEMA, 2));
        assertThat(subscriber2.getLatestSchemaInfo()).isNotNull();
        assertThat(subscriber2.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA2_SCHEMA, 2));

        // one more redundant schema change.
        manager.updateLatestSchema(
                new TablePath("test_tb", "test_table_1"), (short) 1, DATA2_SCHEMA);
        assertThat(subscriber1.getLatestSchemaInfo()).isNotNull();
        assertThat(subscriber1.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA2_SCHEMA, 2));
        assertThat(subscriber2.getLatestSchemaInfo()).isNotNull();
        assertThat(subscriber2.getLatestSchemaInfo()).isEqualTo(new SchemaInfo(DATA2_SCHEMA, 2));
    }

    @Test
    void testGetHistorySchema() {
        TableInfo tableInfo2 =
                TableInfo.of(
                        DATA1_TABLE_PATH,
                        DATA1_TABLE_ID,
                        2,
                        DATA2_TABLE_DESCRIPTOR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        SchemaMetadataManager manager =
                new SchemaMetadataManager(
                        new TestingMetadataManager(Arrays.asList(DATA1_TABLE_INFO, tableInfo2)));
        SchemaGetter schemaGetter =
                manager.subscribeWithInitialSchema(DATA1_TABLE_PATH, (short) 2, DATA2_SCHEMA);
        assertThat(schemaGetter.getSchema(2)).isEqualTo(DATA2_SCHEMA);
        assertThat(schemaGetter.getSchema(1)).isEqualTo(DATA1_SCHEMA);
        assertThatThrownBy(() -> schemaGetter.getSchema(3))
                .isExactlyInstanceOf(SchemaNotExistException.class);
    }

    @Test
    void testUnsubscribeSchemaChange() {
        SchemaMetadataManager manager =
                new SchemaMetadataManager(new TestingMetadataManager(Collections.emptyList()));
        SchemaGetter subscriber1 =
                manager.subscribeWithInitialSchema(
                        new TablePath("test_tb", "test_table_1"), (short) 1, DATA1_SCHEMA);
        SchemaGetter subscriber2 =
                manager.subscribeWithInitialSchema(
                        new TablePath("test_tb", "test_table_1"), (short) 1, DATA1_SCHEMA);
        SchemaGetter subscriber3 =
                manager.subscribeWithInitialSchema(
                        new TablePath("test_tb", "test_table_2"), (short) 1, DATA1_SCHEMA);
        assertThat(manager.getLatestSchemaByTablePath()).hasSize(2);
        assertThat(manager.getSubscriberCounters()).hasSize(2);
        assertThat(manager.getSubscriberCounters().get(new TablePath("test_tb", "test_table_1")))
                .isEqualTo(2);

        subscriber1.release();
        assertThat(manager.getLatestSchemaByTablePath()).hasSize(2);
        assertThat(manager.getSubscriberCounters()).hasSize(2);
        assertThat(manager.getSubscriberCounters().get(new TablePath("test_tb", "test_table_1")))
                .isEqualTo(1);

        // one more redundant unsubscribe.
        subscriber1.release();
        assertThat(manager.getLatestSchemaByTablePath()).hasSize(2);
        assertThat(manager.getSubscriberCounters()).hasSize(2);
        assertThat(manager.getSubscriberCounters().get(new TablePath("test_tb", "test_table_1")))
                .isEqualTo(1);

        subscriber2.release();
        assertThat(manager.getLatestSchemaByTablePath()).hasSize(1);
        assertThat(manager.getSubscriberCounters()).hasSize(1);
        assertThat(manager.getSubscriberCounters().get(new TablePath("test_tb", "test_table_1")))
                .isNull();

        subscriber3.release();
        assertThat(manager.getLatestSchemaByTablePath()).isEmpty();
        assertThat(manager.getSubscriberCounters()).isEmpty();
        subscriber3.release();
    }

    private static final class TestingMetadataManager extends MetadataManager {

        private final Map<TablePath, Map<Short, Schema>> schemaInfoMap;

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
        public SchemaInfo getSchemaById(TablePath tablePath, int schemaId)
                throws SchemaNotExistException {
            if (schemaInfoMap.containsKey(tablePath)
                    && schemaInfoMap.get(tablePath).containsKey((short) schemaId)) {
                return new SchemaInfo(schemaInfoMap.get(tablePath).get((short) schemaId), schemaId);
            }
            throw new SchemaNotExistException("Schema not exist");
        }
    }
}
