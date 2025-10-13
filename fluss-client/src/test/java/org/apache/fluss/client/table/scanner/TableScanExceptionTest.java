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

package org.apache.fluss.client.table.scanner;

import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.KvSnapshotMetadata;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

/** Test class for improved exception messages in TableScan. */
class TableScanExceptionTest {

    @Mock private FlussConnection mockConnection;
    @Mock private TableInfo mockTableInfo;
    @Mock private Admin mockAdmin;
    @Mock private MetadataUpdater mockMetadataUpdater;
    @Mock private ClientMetricGroup mockClientMetricGroup;

    private TablePath tablePath;
    private RowType rowType;
    private TableScan tableScan;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        tablePath = TablePath.of("test_db", "test_table");
        rowType = RowType.of(
                DataTypes.BIGINT().notNull(),
                DataTypes.STRING(),
                DataTypes.INT())
                .withFieldNames(Arrays.asList("id", "name", "age"));

        when(mockTableInfo.getTablePath()).thenReturn(tablePath);
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockConnection.getConfiguration()).thenReturn(new Configuration());
        when(mockConnection.getAdmin()).thenReturn(mockAdmin);
        when(mockConnection.getMetadataUpdater()).thenReturn(mockMetadataUpdater);
        when(mockConnection.getClientMetricGroup()).thenReturn(mockClientMetricGroup);

        tableScan = new TableScan(mockConnection, mockTableInfo);
    }

    @Test
    void testProjectWithInvalidFieldName_ShouldIncludeContextInMessage() {
        assertThatThrownBy(() -> tableScan.project(Arrays.asList("invalid_field")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Field 'invalid_field' not found in table schema")
                .hasMessageContaining("Available fields: [id, name, age]")
                .hasMessageContaining("Table: test_db.test_table");
    }

    @Test
    void testCreateLogScannerWithLimit_ShouldIncludeContextInMessage() {
        TableScan scanWithLimit = tableScan.limit(100);
        
        assertThatThrownBy(scanWithLimit::createLogScanner)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("LogScanner doesn't support limit pushdown")
                .hasMessageContaining("Table: test_db.test_table")
                .hasMessageContaining("requested limit: 100");
    }

    @Test
    void testCreateBatchScannerWithoutLimit_ShouldIncludeContextInMessage() {
        TableBucket tableBucket = new TableBucket(123L, 5);
        
        assertThatThrownBy(() -> tableScan.createBatchScanner(tableBucket))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Currently, BatchScanner is only available when limit is set")
                .hasMessageContaining("Table: test_db.test_table")
                .hasMessageContaining("bucket: TableBucket{tableId=123, partitionId=null, bucket=5}");
    }

    @Test
    void testCreateSnapshotBatchScannerWithLimit_ShouldIncludeContextInMessage() {
        TableScan scanWithLimit = tableScan.limit(50);
        TableBucket tableBucket = new TableBucket(123L, 5);
        long snapshotId = 789L;
        
        assertThatThrownBy(() -> scanWithLimit.createBatchScanner(tableBucket, snapshotId))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Currently, SnapshotBatchScanner doesn't support limit pushdown")
                .hasMessageContaining("Table: test_db.test_table")
                .hasMessageContaining("bucket: TableBucket{tableId=123, partitionId=null, bucket=5}")
                .hasMessageContaining("snapshot ID: 789")
                .hasMessageContaining("requested limit: 50");
    }

    @Test
    void testCreateSnapshotBatchScannerWithFailedMetadata_ShouldIncludeContextInMessage() {
        TableBucket tableBucket = new TableBucket(123L, 5);
        long snapshotId = 789L;
        
        // Mock admin to throw exception when getting snapshot metadata
        when(mockAdmin.getKvSnapshotMetadata(any(TableBucket.class), anyLong()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")));
        
        assertThatThrownBy(() -> tableScan.createBatchScanner(tableBucket, snapshotId))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("Failed to get snapshot metadata for table bucket")
                .hasMessageContaining("TableBucket{tableId=123, partitionId=null, bucket=5}")
                .hasMessageContaining("snapshot ID: 789")
                .hasMessageContaining("Table: test_db.test_table")
                .hasCauseInstanceOf(RuntimeException.class);
    }
}