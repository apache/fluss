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

import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbPutIndexReqForBucket;
import org.apache.fluss.rpc.messages.PutIndexRequest;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;
import org.apache.fluss.utils.types.Tuple2;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Main-table and derived-index metadata for server integration tests. */
final class TestingIndexTable {
    static final String INDEX_NAME = "idx_b";

    static Schema indexedSchema(IndexVisibility visibility, int buckets) {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .primaryKey("a")
                .index(
                        INDEX_NAME,
                        IndexType.SECONDARY,
                        Collections.singletonList("b"),
                        visibility,
                        buckets)
                .build();
    }

    static PutKvRequest upsertRequest(TableBucket source, int key, @Nullable String value)
            throws Exception {
        return newPutKvRequest(
                source.getTableId(),
                source.getPartitionId(),
                source.getBucket(),
                1,
                genKvRecordBatch(new Object[] {key, value}));
    }

    static PutKvRequest deleteRequest(TableBucket source, int key) throws Exception {
        return newPutKvRequest(
                source.getTableId(),
                source.getPartitionId(),
                source.getBucket(),
                1,
                genKvRecordBatch(Collections.singletonList(Tuple2.of(new Object[] {key}, null))));
    }

    static void writeSource(TabletServerGateway gateway, PutKvRequest request) throws Exception {
        assertThat(gateway.putKv(request).get(60, TimeUnit.SECONDS).getBucketsRespsList())
                .singleElement()
                .satisfies(
                        bucket ->
                                assertThat(bucket.hasErrorCode())
                                        .as("source bucket %s", bucket.getBucketId())
                                        .isFalse());
    }

    static IndexReplicationEncoder.IndexEntry indexEntry(
            IndexReplicationEncoder encoder, int key, String value) {
        return encoder.encodeDataEntry(GenericRow.of(key, fromString(value)));
    }

    static String valueForIndexBucket(
            IndexReplicationEncoder encoder, String prefix, int targetBucket) {
        for (int suffix = 0; suffix < 100_000; suffix++) {
            String value = prefix + '-' + suffix;
            if (indexEntry(encoder, 1, value).targetBucket() == targetBucket) {
                return value;
            }
        }
        throw new AssertionError("No value found for index bucket " + targetBucket);
    }

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
        this.indexPath = IndexTableUtils.indexTablePath(mainPath, indexName);
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
        return createEncoders(source.getTableInfo(), sourceBucket, cache).stream()
                .filter(encoder -> encoder.getIndexName().equals(indexName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Index encoder missing: " + indexName));
    }

    PutIndexRequest mutationRequest(
            IndexReplicationEncoder encoder,
            TableBucket sourceBucket,
            int targetBucket,
            long sourceEndOffset,
            byte[] key,
            @Nullable BinaryRow value)
            throws Exception {
        IndexReplicationEncoder.IndexEntry progress =
                encoder.encodeProgress(targetBucket, sourceEndOffset);
        IndexReplicator.BucketBatchBuilder builder =
                new IndexReplicator.BucketBatchBuilder(
                        (short) encoder.getIndexSchemaId(),
                        encoder.getIndexKvFormat(),
                        new UnmanagedPagedOutputView(4096));
        builder.append(key, value);
        BytesView records = builder.finish(progress.key(), progress.value());
        PutIndexRequest request =
                new PutIndexRequest()
                        .setTableId(indexTableId)
                        .setSourceTableId(sourceBucket.getTableId())
                        .setAcks(-1)
                        .setTimeoutMs(10_000);
        PbPutIndexReqForBucket bucket =
                request.addBucketsReq()
                        .setBucketId(targetBucket)
                        .setSourceBucketId(sourceBucket.getBucket())
                        .setSourceEndOffset(sourceEndOffset)
                        .setProgressKey(progress.key())
                        .setRecordsBytesView(records);
        if (sourceBucket.getPartitionId() != null) {
            bucket.setSourcePartitionId(sourceBucket.getPartitionId());
        }
        return request;
    }

    static List<IndexReplicationEncoder> createEncoders(
            TableInfo main, TableBucket source, TabletServerMetadataCache cache) {
        return IndexReplicationEncoder.createAll(
                main, source, IndexReplicationEncoder.resolveIndexTables(main, cache));
    }
}
