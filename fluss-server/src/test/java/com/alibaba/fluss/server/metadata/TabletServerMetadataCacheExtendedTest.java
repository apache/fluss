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

package com.alibaba.fluss.server.metadata;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.TableNotExistException;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.coordinator.MetadataManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.server.zk.data.LeaderAndIsr.NO_LEADER;
import static org.assertj.core.api.Assertions.assertThat;

/** Extended test for {@link TabletServerMetadataCache} new methods. */
public class TabletServerMetadataCacheExtendedTest {

    private TabletServerMetadataCache serverMetadataCache;
    private final TablePath testTablePath = TablePath.of("test_db", "test_table");
    private final long testTableId = 1001L;
    private final TableInfo testTableInfo =
            TableInfo.of(testTablePath, testTableId, 0, DATA1_TABLE_DESCRIPTOR, 100L, 100L);

    private final List<BucketMetadata> testBucketMetadata =
            Arrays.asList(
                    new BucketMetadata(0, 0, 0, Arrays.asList(0, 1, 2)),
                    new BucketMetadata(1, NO_LEADER, 0, Arrays.asList(1, 0, 2)));

    @BeforeEach
    public void setup() {
        serverMetadataCache =
                new TabletServerMetadataCache(
                        new TestingMetadataManager(Arrays.asList(testTableInfo)), null);
    }

    @Test
    void testUpdateTableMetadata() {
        // Given: a table metadata
        TableMetadata tableMetadata = new TableMetadata(testTableInfo, testBucketMetadata);

        // When: update table metadata
        serverMetadataCache.updateTableMetadata(tableMetadata);

        // Then: verify the table metadata is cached correctly
        assertThat(serverMetadataCache.getTablePath(testTableId)).isPresent();
        assertThat(serverMetadataCache.getTablePath(testTableId).get()).isEqualTo(testTablePath);

        TableMetadata cachedMetadata = serverMetadataCache.getTableMetadata(testTablePath);
        assertThat(cachedMetadata).isNotNull();
        assertThat(cachedMetadata.getTableInfo()).isEqualTo(testTableInfo);
        assertThat(cachedMetadata.getBucketMetadataList()).hasSameElementsAs(testBucketMetadata);
    }

    @Test
    void testUpdateTableMetadataMergeWithExisting() {
        // Given: existing table metadata in cache
        TableMetadata existingMetadata = new TableMetadata(testTableInfo, testBucketMetadata);
        serverMetadataCache.updateTableMetadata(existingMetadata);

        // When: update with new bucket metadata
        List<BucketMetadata> newBucketMetadata =
                Arrays.asList(
                        new BucketMetadata(0, 1, 1, Arrays.asList(1, 2, 3)),
                        new BucketMetadata(2, 2, 0, Arrays.asList(2, 3, 4)));
        TableMetadata newMetadata = new TableMetadata(testTableInfo, newBucketMetadata);
        serverMetadataCache.updateTableMetadata(newMetadata);

        // Then: verify the new metadata replaces the old one
        TableMetadata cachedMetadata = serverMetadataCache.getTableMetadata(testTablePath);
        assertThat(cachedMetadata.getBucketMetadataList()).hasSameElementsAs(newBucketMetadata);
        assertThat(cachedMetadata.getBucketMetadataList())
                .doesNotContainAnyElementsOf(testBucketMetadata);
    }

    @Test
    void testUpdatePartitionMetadata() {
        // Given: table metadata exists in cache
        TableMetadata tableMetadata = new TableMetadata(testTableInfo, testBucketMetadata);
        serverMetadataCache.updateTableMetadata(tableMetadata);

        // When: update partition metadata
        String partitionName = "p1";
        long partitionId = 2001L;
        PhysicalTablePath partitionPath = PhysicalTablePath.of(testTablePath, partitionName);
        PartitionMetadata partitionMetadata =
                new PartitionMetadata(testTableId, partitionName, partitionId, testBucketMetadata);

        serverMetadataCache.updatePartitionMetadata(partitionMetadata);

        // Then: verify the partition metadata is cached correctly
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId)).isPresent();
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId).get())
                .isEqualTo(partitionPath);

        PartitionMetadata cachedMetadata = serverMetadataCache.getPartitionMetadata(partitionPath);
        assertThat(cachedMetadata).isNotNull();
        assertThat(cachedMetadata.getTableId()).isEqualTo(testTableId);
        assertThat(cachedMetadata.getPartitionId()).isEqualTo(partitionId);
        assertThat(cachedMetadata.getPartitionName()).isEqualTo(partitionName);
        assertThat(cachedMetadata.getBucketMetadataList()).hasSameElementsAs(testBucketMetadata);
    }

    @Test
    void testUpdatePartitionMetadataWithoutTable() {
        // Given: no table metadata in cache
        String partitionName = "p1";
        long partitionId = 2001L;
        PhysicalTablePath partitionPath = PhysicalTablePath.of(testTablePath, partitionName);
        PartitionMetadata partitionMetadata =
                new PartitionMetadata(testTableId, partitionName, partitionId, testBucketMetadata);

        // When: update partition metadata without table
        serverMetadataCache.updatePartitionMetadata(partitionMetadata);

        // Then: partition metadata should not be cached
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId)).isEmpty();
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath)).isNull();
    }

    @Test
    void testUpdatePartitionMetadataMergeWithExisting() {
        // Given: existing partition metadata in cache
        TableMetadata tableMetadata = new TableMetadata(testTableInfo, testBucketMetadata);
        serverMetadataCache.updateTableMetadata(tableMetadata);

        String partitionName = "p1";
        long partitionId = 2001L;
        PhysicalTablePath partitionPath = PhysicalTablePath.of(testTablePath, partitionName);
        PartitionMetadata existingPartitionMetadata =
                new PartitionMetadata(testTableId, partitionName, partitionId, testBucketMetadata);
        serverMetadataCache.updatePartitionMetadata(existingPartitionMetadata);

        // When: update with new bucket metadata
        List<BucketMetadata> newBucketMetadata =
                Arrays.asList(new BucketMetadata(0, 1, 1, Arrays.asList(1, 2, 3)));
        PartitionMetadata newPartitionMetadata =
                new PartitionMetadata(testTableId, partitionName, partitionId, newBucketMetadata);
        serverMetadataCache.updatePartitionMetadata(newPartitionMetadata);

        // Then: verify the new metadata replaces the old one
        PartitionMetadata cachedMetadata = serverMetadataCache.getPartitionMetadata(partitionPath);
        assertThat(cachedMetadata.getBucketMetadataList()).hasSameElementsAs(newBucketMetadata);
        assertThat(cachedMetadata.getBucketMetadataList())
                .doesNotContainAnyElementsOf(testBucketMetadata);
    }

    @Test
    void testConcurrentUpdateTableMetadata() throws InterruptedException {
        // Given: multiple threads updating table metadata
        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];

        // When: concurrent updates
        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            threads[i] =
                    new Thread(
                            () -> {
                                TablePath tablePath =
                                        TablePath.of("test_db", "test_table_" + index);
                                long tableId = testTableId + index;
                                TableInfo tableInfo =
                                        TableInfo.of(
                                                tablePath,
                                                tableId,
                                                0,
                                                DATA1_TABLE_DESCRIPTOR,
                                                100L,
                                                100L);
                                TableMetadata tableMetadata =
                                        new TableMetadata(tableInfo, testBucketMetadata);
                                serverMetadataCache.updateTableMetadata(tableMetadata);
                            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        // Then: verify all updates are successful
        for (int i = 0; i < threadCount; i++) {
            TablePath tablePath = TablePath.of("test_db", "test_table_" + i);
            long tableId = testTableId + i;
            assertThat(serverMetadataCache.getTablePath(tableId)).isPresent();
            assertThat(serverMetadataCache.getTablePath(tableId).get()).isEqualTo(tablePath);
        }
    }

    @Test
    void testClearTableMetadata() {
        // Given: table and partition metadata in cache
        TableMetadata tableMetadata = new TableMetadata(testTableInfo, testBucketMetadata);
        serverMetadataCache.updateTableMetadata(tableMetadata);

        String partitionName = "p1";
        long partitionId = 2001L;
        PhysicalTablePath partitionPath = PhysicalTablePath.of(testTablePath, partitionName);
        PartitionMetadata partitionMetadata =
                new PartitionMetadata(testTableId, partitionName, partitionId, testBucketMetadata);
        serverMetadataCache.updatePartitionMetadata(partitionMetadata);

        // When: clear table metadata
        serverMetadataCache.clearTableMetadata();

        // Then: verify all table metadata is cleared
        assertThat(serverMetadataCache.getTablePath(testTableId)).isEmpty();
        assertThat(serverMetadataCache.getTableMetadata(testTablePath)).isNull();
        assertThat(serverMetadataCache.getPhysicalTablePath(partitionId)).isEmpty();
        assertThat(serverMetadataCache.getPartitionMetadata(partitionPath)).isNull();
    }

    private static final class TestingMetadataManager extends MetadataManager {
        private final Map<TablePath, TableInfo> tableInfoMap = new HashMap<>();

        public TestingMetadataManager(List<TableInfo> tableInfos) {
            super(null, new Configuration());
            tableInfos.forEach(tableInfo -> tableInfoMap.put(tableInfo.getTablePath(), tableInfo));
        }

        @Override
        public TableInfo getTable(TablePath tablePath) throws TableNotExistException {
            TableInfo tableInfo = tableInfoMap.get(tablePath);
            if (tableInfo == null) {
                throw new TableNotExistException("Table '" + tablePath + "' does not exist.");
            }
            return tableInfo;
        }
    }
}
