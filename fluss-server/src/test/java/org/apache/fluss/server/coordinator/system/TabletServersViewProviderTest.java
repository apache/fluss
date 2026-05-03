/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator.system;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.record.ValueRecordBatch;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataType;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TabletServersViewProvider}. */
class TabletServersViewProviderTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;
    private static TabletServersViewProvider provider;

    @BeforeAll
    static void beforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        provider = new TabletServersViewProvider(zookeeperClient);
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (zookeeperClient != null) {
            zookeeperClient.close();
        }
    }

    @Test
    void testViewPath() {
        TablePath path = provider.viewPath();
        assertThat(path.getDatabaseName()).isEqualTo("sys");
        assertThat(path.getTableName()).isEqualTo("tablet_servers");
    }

    @Test
    void testSchemaColumns() {
        Schema schema = provider.schema();
        List<Schema.Column> columns = schema.getColumns();
        assertThat(columns).hasSize(4);
        assertThat(columns.get(0).getName()).isEqualTo("server_id");
        assertThat(columns.get(1).getName()).isEqualTo("endpoints");
        assertThat(columns.get(2).getName()).isEqualTo("rack");
        assertThat(columns.get(3).getName()).isEqualTo("register_timestamp");
    }

    @Test
    void testSchemaId() {
        assertThat(provider.schemaId()).isEqualTo(1);
    }

    @Test
    void testScanRowsWithNoServers() throws Exception {
        byte[] result = provider.scanRows(null, null);
        List<InternalRow> rows = deserializeRows(result);
        assertThat(rows).isEmpty();
    }

    @Test
    void testScanRowsWithRegisteredServer() throws Exception {
        long timestamp = System.currentTimeMillis();
        TabletServerRegistration registration =
                new TabletServerRegistration(
                        "rack-a", Endpoint.fromListenersString("CLIENT://myhost:9123"), timestamp);
        zookeeperClient.registerTabletServer(42, registration);

        byte[] result = provider.scanRows(null, null);
        List<InternalRow> rows = deserializeRows(result);
        assertThat(rows).hasSize(1);

        InternalRow row = rows.get(0);
        assertThat(row.getInt(0)).isEqualTo(42);
        assertThat(row.getString(1).toString()).isEqualTo("CLIENT://myhost:9123");
        assertThat(row.getString(2).toString()).isEqualTo("rack-a");
        assertThat(row.getLong(3)).isEqualTo(timestamp);
    }

    /** Deserializes a byte array produced by {@link SystemViewProvider#scanRows} into rows. */
    private List<InternalRow> deserializeRows(byte[] bytes) {
        DataType[] fieldTypes =
                provider.schema().getRowType().getFieldTypes().toArray(new DataType[0]);
        RowDecoder decoder = RowDecoder.create(KvFormat.INDEXED, fieldTypes);
        ValueRecordBatch.ReadContext readContext = schemaId -> decoder;

        DefaultValueRecordBatch batch = DefaultValueRecordBatch.pointToBytes(bytes);
        List<InternalRow> rows = new ArrayList<>();
        for (ValueRecord record : batch.records(readContext)) {
            InternalRow row = record.getRow();
            if (row != null) {
                rows.add(row);
            }
        }
        return rows;
    }
}
