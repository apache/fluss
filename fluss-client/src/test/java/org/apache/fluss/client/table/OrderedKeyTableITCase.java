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

package org.apache.fluss.client.table;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.Range;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Ordered-key table writes, lookups, deletes and recovery. */
class OrderedKeyTableITCase extends ClientToServerITCaseBase {
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("g", DataTypes.INT())
                    .column("t", DataTypes.BIGINT())
                    .column("id", DataTypes.INT())
                    .column("v", DataTypes.STRING())
                    .primaryKey("g", "t", "id")
                    .build();

    @Test
    void testInsertIfNotExistsWithOrderedKeys() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TablePath path = TablePath.of("ordered_key", "insert_if_missing");
        long tableId =
                createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(3)
                                .property(ConfigOptions.TABLE_KV_ORDERED_KEY_ENABLED, true)
                                .build(),
                        true);
        waitAllReplicasReady(tableId, 3);
        try (Table table = conn.getTable(path)) {
            Lookuper lookuper = table.newLookup().enableInsertIfNotExists().createLookuper();
            InternalRow inserted = lookuper.lookup(GenericRow.of(7)).get().getSingletonRow();
            assertThat(inserted).isNotNull();
            assertThat(inserted.getInt(0)).isEqualTo(7);
            assertThat(inserted.isNullAt(1)).isTrue();
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(7, "existing"));
            writer.flush();
            InternalRow existing = lookuper.lookup(GenericRow.of(7)).get().getSingletonRow();
            assertThat(existing).isNotNull();
            assertThat(existing.getString(1).toString()).isEqualTo("existing");
        }
    }

    @Test
    void testPointPrefixDeleteAndRestart() throws Exception {
        try (Table table = create("recovery", SCHEMA)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, 255L, 0, "before"));
            writer.upsert(row(1, 256L, 0, "removed"));
            writer.flush();
            assertThat(
                            table.newLookup()
                                    .createLookuper()
                                    .lookup(row(1, 255L, 0))
                                    .get()
                                    .getSingletonRow()
                                    .getString(3)
                                    .toString())
                    .isEqualTo("before");
            assertThat(
                            table.newLookup()
                                    .lookupBy("g", "t")
                                    .createLookuper()
                                    .lookup(row(1, 255L))
                                    .get()
                                    .getRowList())
                    .hasSize(1);
            writer.upsert(row(1, 255L, 0, "after"));
            writer.delete(row(1, 256L, 0, "removed"));
            writer.flush();
            for (int server = 0; server < 3; server++) {
                FLUSS_CLUSTER_EXTENSION.stopTabletServer(server);
                FLUSS_CLUSTER_EXTENSION.startTabletServer(server);
            }
            waitAllReplicasReady(table.getTableInfo().getTableId(), 3);
            try (org.apache.fluss.client.Connection recovered =
                            org.apache.fluss.client.ConnectionFactory.createConnection(
                                    FLUSS_CLUSTER_EXTENSION.getClientConfig());
                    Table restored = recovered.getTable(table.getTableInfo().getTablePath())) {
                assertThat(
                                restored.newLookup()
                                        .lookupBy("g")
                                        .rangeBy("t")
                                        .createLookuper()
                                        .lookup(row(1), Range.closedOpen(row(255L), row(300L)))
                                        .get()
                                        .getRowList())
                        .hasSize(1);
                assertThat(
                                restored.newLookup()
                                        .createLookuper()
                                        .lookup(row(1, 255L, 0))
                                        .get()
                                        .getSingletonRow()
                                        .getString(3)
                                        .toString())
                        .isEqualTo("after");
                assertThat(
                                restored.newLookup()
                                        .createLookuper()
                                        .lookup(row(1, 256L, 0))
                                        .get()
                                        .getRowList())
                        .isEmpty();
            }
        }
    }

    @Test
    void testOrderedKeysWithDefaultBucketRouting() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TablePath path = TablePath.of("ordered_key", "default_bucket");
        long id =
                createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(3)
                                .property(ConfigOptions.TABLE_KV_ORDERED_KEY_ENABLED, true)
                                .build(),
                        true);
        waitAllReplicasReady(id, 3);
        try (Table table = conn.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int key : new int[] {-1, 0, 255, 256}) {
                writer.upsert(row(key, "v"));
            }
            writer.flush();
            for (int key : new int[] {-1, 0, 255, 256}) {
                assertThat(table.newLookup().createLookuper().lookup(row(key)).get().getRowList())
                        .hasSize(1);
            }
            writer.delete(row(256, "v"));
            writer.flush();
            assertThat(table.newLookup().createLookuper().lookup(row(256)).get().getRowList())
                    .isEmpty();
        }
    }

    private Table create(String name, Schema schema) throws Exception {
        TablePath path = TablePath.of("ordered_key", name);
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "g")
                        .property(ConfigOptions.TABLE_KV_ORDERED_KEY_ENABLED, true)
                        .build();
        long id = createTable(path, descriptor, true);
        waitAllReplicasReady(id, 3);
        return conn.getTable(path);
    }
}
