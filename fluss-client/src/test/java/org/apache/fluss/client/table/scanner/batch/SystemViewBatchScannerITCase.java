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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.metadata.SystemTableConstants;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for reading system views via the client scanner API. */
class SystemViewBatchScannerITCase {

    private static final int NUM_TABLET_SERVERS = 3;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER =
            FlussClusterExtension.builder().setNumOfTabletServers(NUM_TABLET_SERVERS).build();

    @Test
    void testScanSystemViewReturnsServers() throws Exception {
        TablePath serversPath =
                TablePath.of(
                        SystemTableConstants.SYSTEM_DATABASE,
                        SystemTableConstants.TABLET_SERVERS_VIEW);

        try (Connection connection =
                        ConnectionFactory.createConnection(FLUSS_CLUSTER.getClientConfig());
                Table table = connection.getTable(serversPath);
                BatchScanner scanner = table.newScan().createBatchScanner()) {
            TableInfo tableInfo = table.getTableInfo();
            assertThat(tableInfo.isSystemView()).isTrue();

            RowType rowType = tableInfo.getRowType();
            List<DataField> fields = rowType.getFields();
            // Schema should have at least one column
            assertThat(fields).isNotEmpty();

            List<InternalRow> rows = BatchScanUtils.collectRows(scanner);
            assertThat(rows).hasSize(NUM_TABLET_SERVERS);
        }
    }

    @Test
    void testScanSystemViewWithProjection() throws Exception {
        TablePath serversPath =
                TablePath.of(
                        SystemTableConstants.SYSTEM_DATABASE,
                        SystemTableConstants.TABLET_SERVERS_VIEW);

        try (Connection connection =
                        ConnectionFactory.createConnection(FLUSS_CLUSTER.getClientConfig());
                Table table = connection.getTable(serversPath)) {
            RowType rowType = table.getTableInfo().getRowType();
            int fieldCount = rowType.getFieldCount();
            assertThat(fieldCount).isGreaterThanOrEqualTo(2);

            // Project the first and last columns
            int[] projection = new int[] {0, fieldCount - 1};

            try (BatchScanner scanner = table.newScan().project(projection).createBatchScanner()) {
                List<InternalRow> rows = BatchScanUtils.collectRows(scanner);
                assertThat(rows).hasSize(NUM_TABLET_SERVERS);

                // Verify each row has exactly 2 fields (projected)
                // and all rows are distinct (different server data)
                for (InternalRow row : rows) {
                    assertThat(row.isNullAt(0)).isFalse();
                }

                // Verify rows are not all identical (projection bug would cause this)
                assertThat(rows.stream().map(r -> r.toString()).distinct().count())
                        .as("Projected rows should not all be identical")
                        .isEqualTo(NUM_TABLET_SERVERS);
            }
        }
    }
}
