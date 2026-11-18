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

package org.apache.fluss.client.bulkload.protocol;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.messages.AllocateBulkLoadBucketSnapshotRequest;
import org.apache.fluss.rpc.messages.AllocateBulkLoadBucketSnapshotResponse;
import org.apache.fluss.rpc.messages.BeginBulkLoadResponse;
import org.apache.fluss.rpc.messages.CommitBulkLoadBucketSnapshotRequest;
import org.apache.fluss.rpc.messages.CommitBulkLoadRequest;
import org.apache.fluss.rpc.messages.PbBulkLoadTargetInfo;
import org.apache.fluss.rpc.messages.PbTableInfo;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.TestingBulkLoadSnapshot;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.utils.types.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;

/** Produces independent RocksDB inputs using allocated bucket snapshot identities. */
final class TestingBulkLoadInputBuilder {
    void commit(
            FlussClusterExtension cluster,
            AdminGateway gateway,
            TablePath path,
            BeginBulkLoadResponse begun,
            Map<PhysicalTablePath, List<Object[]>> rows)
            throws Exception {
        PbTableInfo frozen = begun.getTableInfo();
        for (PbBulkLoadTargetInfo target : begun.getTargetsList()) {
            TableInfo table =
                    TableInfo.of(
                            path,
                            frozen.getTableId(),
                            frozen.getSchemaId(),
                            TableDescriptor.fromJsonBytes(frozen.getTableJson())
                                    .withBucketCount(target.getBucketCount()),
                            target.getRemoteDataDir(),
                            frozen.getCreatedTime(),
                            frozen.getModifiedTime());
            PhysicalTablePath physical =
                    target.hasPartitionId()
                            ? PhysicalTablePath.of(path, target.getPartitionName())
                            : PhysicalTablePath.of(path);
            List<List<Object[]>> buckets =
                    distribute(table, rows.getOrDefault(physical, Collections.emptyList()));
            for (int id = 0; id < target.getBucketCount(); id++) {
                TableBucket bucket =
                        new TableBucket(
                                table.getTableId(),
                                target.hasPartitionId() ? target.getPartitionId() : null,
                                id);
                cluster.waitUntilAllReplicaReady(bucket);
                AllocateBulkLoadBucketSnapshotRequest allocate =
                        new AllocateBulkLoadBucketSnapshotRequest()
                                .setHandle(begun.getStatus().getHandle())
                                .setBucketId(id);
                CommitBulkLoadBucketSnapshotRequest commit =
                        new CommitBulkLoadBucketSnapshotRequest()
                                .setHandle(begun.getStatus().getHandle())
                                .setBucketId(id);
                if (target.hasPartitionId()) {
                    allocate.setPartitionId(target.getPartitionId());
                    commit.setPartitionId(target.getPartitionId());
                }
                AllocateBulkLoadBucketSnapshotResponse allocation =
                        gateway.allocateBulkLoadBucketSnapshot(allocate).get();
                List<Object[]> records = buckets.get(id);
                TestingBulkLoadSnapshot.writeSnapshot(
                        new FsPath(table.getRemoteDataDir()),
                        physical,
                        bucket,
                        allocation.getSnapshotId(),
                        allocation.getLogOffset(),
                        table.getTableConfig().getChangelogImage() == ChangelogImage.WAL
                                ? null
                                : (long) records.size(),
                        encodeRows(table, records));
                gateway.commitBulkLoadBucketSnapshot(commit).get();
            }
        }
        gateway.commitBulkLoad(new CommitBulkLoadRequest().setHandle(begun.getStatus().getHandle()))
                .get();
    }

    private static List<Tuple2<byte[], byte[]>> encodeRows(TableInfo table, List<Object[]> rows) {
        KeyEncoder keys =
                KeyEncoder.ofPrimaryKeyEncoder(
                        table.getRowType(),
                        table.getPhysicalPrimaryKeys(),
                        table.getTableConfig(),
                        table.isDefaultBucketKey());
        RowEncoder encoder =
                RowEncoder.create(table.getTableConfig().getKvFormat(), table.getRowType());
        List<Tuple2<byte[], byte[]>> encoded = new ArrayList<>(rows.size());
        for (Object[] values : rows) {
            InternalRow row = DataTestUtils.row(table.getRowType(), values);
            encoder.startNewRow();
            for (int field = 0; field < table.getRowType().getFieldCount(); field++) {
                encoder.encodeField(
                        field,
                        InternalRow.createFieldGetter(table.getRowType().getTypeAt(field), field)
                                .getFieldOrNull(row));
            }
            encoded.add(
                    Tuple2.of(
                            keys.encodeKey(row),
                            ValueEncoder.encodeValue(
                                    (short) table.getSchemaId(), encoder.finishRow().copy())));
        }
        return encoded;
    }

    private static List<List<Object[]>> distribute(TableInfo tableInfo, List<Object[]> rows) {
        List<List<Object[]>> rowsByBucket = new ArrayList<>(tableInfo.getNumBuckets());
        for (int bucket = 0; bucket < tableInfo.getNumBuckets(); bucket++) {
            rowsByBucket.add(new ArrayList<>());
        }
        ToIntFunction<Object[]> bucketer = bucketer(tableInfo);
        for (Object[] values : rows) {
            rowsByBucket.get(bucketer.applyAsInt(values)).add(values);
        }
        return rowsByBucket;
    }

    private static ToIntFunction<Object[]> bucketer(TableInfo tableInfo) {
        KeyEncoder primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        KeyEncoder bucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getBucketKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey(),
                        primaryKeyEncoder);
        BucketingFunction bucketingFunction =
                BucketingFunction.of(tableInfo.getTableConfig().getDataLakeFormat().orElse(null));
        return values ->
                bucketingFunction.bucketing(
                        bucketKeyEncoder.encodeKey(
                                DataTestUtils.row(tableInfo.getRowType(), values)),
                        tableInfo.getNumBuckets());
    }
}
