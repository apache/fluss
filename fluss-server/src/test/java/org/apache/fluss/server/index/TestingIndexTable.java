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

package org.apache.fluss.server.index;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.utils.IndexTableUtils;

import java.time.Duration;

import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;

/** Main-table and derived-index metadata for server integration tests. */
final class TestingIndexTable {
    final long mainTableId;
    final long indexTableId;
    final TablePath indexPath;
    private final FlussClusterExtension cluster;
    private final String indexName;
    private final int indexBucketCount;

    /** Creates a main table and waits for every derived index bucket leader. */
    TestingIndexTable(
            FlussClusterExtension cluster,
            TablePath mainPath,
            TableDescriptor descriptor,
            String indexName)
            throws Exception {
        this.cluster = cluster;
        this.indexName = indexName;
        this.mainTableId = createTable(cluster, mainPath, descriptor);
        this.indexPath =
                TablePath.of(
                        mainPath.getDatabaseName(),
                        IndexTableUtils.indexTableName(mainPath.getTableName(), indexName));
        TableRegistration index =
                cluster.getZooKeeperClient()
                        .getTable(indexPath)
                        .orElseThrow(() -> new AssertionError("Index Table missing: " + indexPath));
        this.indexTableId = index.tableId;
        this.indexBucketCount = index.bucketCount;
        for (int bucket = 0; bucket < indexBucketCount; bucket++) {
            cluster.waitAndGetLeaderReplica(new TableBucket(indexTableId, bucket));
        }
    }

    /** Resolves the index encoder once the source leader knows every index bucket leader. */
    IndexReplicationEncoder indexEncoder(TableBucket sourceBucket) {
        Replica source = cluster.waitAndGetLeaderReplica(sourceBucket);
        int leader = cluster.waitAndGetLeader(sourceBucket);
        TabletServerMetadataCache cache = cluster.getTabletServerById(leader).getMetadataCache();
        waitUntil(
                () -> {
                    for (int bucket = 0; bucket < indexBucketCount; bucket++) {
                        if (!cache.getBucketLeader(indexTableId, bucket).isPresent()) {
                            return false;
                        }
                    }
                    return true;
                },
                Duration.ofSeconds(30),
                "wait for index leaders in source metadata cache");
        return IndexReplicationEncoder.createAll(source.getTableInfo(), sourceBucket, cache)
                .stream()
                .filter(encoder -> encoder.getIndexName().equals(indexName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Index encoder missing: " + indexName));
    }
}
