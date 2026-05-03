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

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.SystemTableConstants;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Equal;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.record.ValueRecordBatch;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.ScanSystemViewRequest;
import org.apache.fluss.rpc.messages.ScanSystemViewResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.util.PredicateMessageUtils;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newGetTableInfoRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newListTablesRequest;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for the system view scan API. */
class SystemViewITCase {

    private static final int NUM_TABLET_SERVERS = 3;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER =
            FlussClusterExtension.builder().setNumOfTabletServers(NUM_TABLET_SERVERS).build();

    private static AdminReadOnlyGateway gateway;

    @BeforeAll
    static void setUp() {
        gateway = FLUSS_CLUSTER.newCoordinatorClient();
    }

    @Test
    void testListTablesIncludesServersView() throws Exception {
        List<String> tables =
                gateway.listTables(newListTablesRequest(SystemTableConstants.SYSTEM_DATABASE))
                        .get()
                        .getTableNamesList();
        assertThat(tables).contains(SystemTableConstants.TABLET_SERVERS_VIEW);
    }

    @Test
    void testGetTableInfoForServersView() throws Exception {
        TablePath serversPath =
                TablePath.of(
                        SystemTableConstants.SYSTEM_DATABASE,
                        SystemTableConstants.TABLET_SERVERS_VIEW);
        GetTableInfoResponse response =
                gateway.getTableInfo(newGetTableInfoRequest(serversPath)).get();
        assertThat(response.hasTableKind()).isTrue();
        assertThat(response.getTableKind()).isEqualTo(SystemTableConstants.TABLE_KIND_SYSTEM_VIEW);
        assertThat(response.getSchemaId()).isEqualTo(1);
    }

    @Test
    void testScanSystemViewWithFilterPredicate() throws Exception {
        // Build a filter: server_id = 0
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField("server_id", DataTypes.INT(), 0));
        fields.add(new DataField("endpoints", DataTypes.STRING(), 1));
        fields.add(new DataField("rack", DataTypes.STRING(), 2));
        fields.add(new DataField("register_timestamp", DataTypes.BIGINT(), 3));
        RowType serversRowType = new RowType(fields);

        LeafPredicate filter =
                new LeafPredicate(
                        Equal.INSTANCE,
                        DataTypes.INT(),
                        0,
                        "server_id",
                        Collections.singletonList(0));

        ScanSystemViewRequest request = new ScanSystemViewRequest();
        request.setDatabaseName(SystemTableConstants.SYSTEM_DATABASE);
        request.setViewName(SystemTableConstants.TABLET_SERVERS_VIEW);
        request.setSchemaId("1");
        request.setFilterPredicate(PredicateMessageUtils.toPbPredicate(filter, serversRowType));

        ScanSystemViewResponse response = gateway.scanSystemView(request).get();

        assertThat(response.hasErrorCode()).isFalse();
        assertThat(response.hasRecords()).isTrue();

        byte[] recordBytes = response.getRecords();
        DefaultValueRecordBatch batch = DefaultValueRecordBatch.pointToBytes(recordBytes);

        // Only server_id=0 should be returned
        assertThat(batch.getRecordCount()).isEqualTo(1);

        DataType[] fieldTypes =
                new DataType[] {
                    DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.BIGINT()
                };
        RowDecoder decoder = RowDecoder.create(KvFormat.INDEXED, fieldTypes);
        ValueRecordBatch.ReadContext readContext = schemaId -> decoder;
        for (ValueRecord record : batch.records(readContext)) {
            BinaryRow row = record.getRow();
            assertThat(row).isNotNull();
            assertThat(row.getInt(0)).isEqualTo(0);
            System.out.printf(
                    "Filtered row: server_id=%d, endpoints=%s%n",
                    row.getInt(0), row.getString(1).toString());
        }
    }

    @Test
    void testScanNonExistentViewReturnsError() throws Exception {
        ScanSystemViewRequest request = new ScanSystemViewRequest();
        request.setDatabaseName(SystemTableConstants.SYSTEM_DATABASE);
        request.setViewName("nonexistent_view");
        request.setSchemaId("0");

        ScanSystemViewResponse response = gateway.scanSystemView(request).get();

        assertThat(response.hasErrorCode()).isTrue();
        assertThat(response.getErrorCode()).isEqualTo(Errors.TABLE_NOT_EXIST.code());
        assertThat(response.hasErrorMessage()).isTrue();
        assertThat(response.getErrorMessage()).contains("does not exist");
    }
}
