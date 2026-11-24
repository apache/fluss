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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.bucketing.FlussBucketingFunction;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.IndexTableUtils.DATA_RECORD_KIND;
import static org.apache.fluss.utils.IndexTableUtils.PROGRESS_RECORD_KIND;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Encodes DATA and PROGRESS records for one secondary index of one source bucket.
 *
 * <p>Owned by one source replicator. Encoding calls must be serialized because encoders reuse
 * mutable row storage; encoded values are borrowed until the next encoding call.
 */
@Internal
@NotThreadSafe
final class IndexReplicationEncoder {

    /**
     * Encodes one complete physical Index Table entry from a base row. Encoders may reuse storage
     * for entry values between calls, but each returned key and target bucket must remain stable.
     */
    @FunctionalInterface
    interface DataEntryEncoder {
        IndexEntry encode(InternalRow row);
    }

    /** Encodes the bound source bucket's durable progress record for one target bucket. */
    @FunctionalInterface
    interface ProgressEncoder {
        IndexEntry encode(int targetBucket, long sourceEndOffset);
    }

    /**
     * One encoded physical Index Table entry. The key and target bucket remain stable. The value
     * may be backed by a reusable row encoder and is valid only until the next {@link
     * IndexReplicationEncoder#encodeDataEntry(InternalRow)} or {@link
     * IndexReplicationEncoder#encodeProgress(int, long)} call on the same encoder.
     */
    static final class IndexEntry {
        private final byte[] key;
        private final BinaryRow value;
        private final int targetBucket;

        IndexEntry(byte[] key, BinaryRow value, int targetBucket) {
            this.key = checkNotNull(key, "key");
            this.value = checkNotNull(value, "value");
            this.targetBucket = targetBucket;
        }

        byte[] key() {
            return key;
        }

        /** Returns the encoder-backed value, which must be consumed before the next encode call. */
        BinaryRow value() {
            return value;
        }

        int targetBucket() {
            return targetBucket;
        }
    }

    private final long indexTableId;
    private final String indexName;
    private final IndexVisibility visibility;
    private final int indexSchemaId;
    private final KvFormat indexKvFormat;
    private final int[] idxColumnIndices;
    private final DataEntryEncoder dataEncoder;
    private final ProgressEncoder progressEncoder;

    static List<TableInfo> resolveIndexTables(
            TableInfo mainTable, TabletServerMetadataCache cache) {
        List<TableInfo> tables = new ArrayList<>(mainTable.getSchema().getIndexes().size());
        for (Schema.Index index : mainTable.getSchema().getIndexes()) {
            tables.add(resolveIndexTable(mainTable, index, cache));
        }
        return tables;
    }

    static List<IndexReplicationEncoder> createAll(
            TableInfo mainTable, TableBucket bucket, List<TableInfo> indexTables) {
        List<Schema.Index> indexes = mainTable.getSchema().getIndexes();
        List<IndexReplicationEncoder> encoders = new ArrayList<>(indexes.size());
        for (int i = 0; i < indexes.size(); i++) {
            encoders.add(createEncoder(indexes.get(i), mainTable, bucket, indexTables.get(i)));
        }
        return Collections.unmodifiableList(encoders);
    }

    IndexReplicationEncoder(
            String indexName,
            IndexVisibility visibility,
            long indexTableId,
            int indexSchemaId,
            KvFormat indexKvFormat,
            int[] idxColumnIndices,
            DataEntryEncoder dataEncoder,
            ProgressEncoder progressEncoder) {
        this.indexName = checkNotNull(indexName, "indexName");
        this.visibility = checkNotNull(visibility, "visibility");
        this.indexTableId = indexTableId;
        this.indexSchemaId = indexSchemaId;
        this.indexKvFormat = indexKvFormat;
        this.idxColumnIndices = checkNotNull(idxColumnIndices, "idxColumnIndices").clone();
        checkArgument(idxColumnIndices.length > 0, "idxColumnIndices must not be empty.");
        this.dataEncoder = checkNotNull(dataEncoder, "dataEncoder");
        this.progressEncoder = checkNotNull(progressEncoder, "progressEncoder");
    }

    long getIndexTableId() {
        return indexTableId;
    }

    String getIndexName() {
        return indexName;
    }

    boolean isSync() {
        return visibility == IndexVisibility.SYNC;
    }

    int getIndexSchemaId() {
        return indexSchemaId;
    }

    KvFormat getIndexKvFormat() {
        return indexKvFormat;
    }

    /** Returns no DATA entry when any indexed column is null. */
    @Nullable
    IndexEntry encodeDataEntry(InternalRow row) {
        for (int idxColumnIndex : idxColumnIndices) {
            if (row.isNullAt(idxColumnIndex)) {
                return null;
            }
        }
        return dataEncoder.encode(row);
    }

    IndexEntry encodeProgress(int targetBucket, long sourceEndOffset) {
        return progressEncoder.encode(targetBucket, sourceEndOffset);
    }

    private static IndexReplicationEncoder createEncoder(
            Schema.Index index,
            TableInfo mainTableInfo,
            TableBucket sourceBucket,
            TableInfo indexTable) {
        Schema schema = mainTableInfo.getSchema();
        RowType mainRowType = mainTableInfo.getRowType();
        boolean partitioned = mainTableInfo.isPartitioned();
        Long partitionId = sourceBucket.getPartitionId();
        checkArgument(
                partitioned == (partitionId != null),
                "Source bucket partition identity must match the main table");

        int[] idxColumnIndices = schema.getColumnIndexes(index.getColumnNames());

        long indexTableId = indexTable.getTableId();
        int indexBucketCount = indexTable.getNumBuckets();
        int indexSchemaId = indexTable.getSchemaId();

        CompactedKeyEncoder logicalIndexKeyEncoder =
                new CompactedKeyEncoder(mainRowType, idxColumnIndices);
        FlussBucketingFunction bucketingFunction = new FlussBucketingFunction();

        Schema physicalSchema = indexTable.getSchema();
        RowType physicalRowType = physicalSchema.getRowType();
        int recordKindPosition =
                physicalRowType.getFieldIndex(IndexTableUtils.RECORD_KIND_SYSTEM_COLUMN);
        int routingKeyPosition =
                physicalRowType.getFieldIndex(IndexTableUtils.ROUTING_KEY_SYSTEM_COLUMN);
        int rowKeyPosition = physicalRowType.getFieldIndex(IndexTableUtils.ROW_KEY_SYSTEM_COLUMN);
        int sourceProgressPosition =
                physicalRowType.getFieldIndex(IndexTableUtils.SOURCE_PROGRESS_SYSTEM_COLUMN);
        // The derived schema stores source columns first, followed by partition identity and system
        // fields.
        int storedColCount = partitioned ? recordKindPosition - 1 : recordKindPosition;
        List<String> storedColumns = physicalSchema.getColumnNames().subList(0, storedColCount);
        InternalRow.FieldGetter[] valueFieldGetters = new InternalRow.FieldGetter[storedColCount];
        for (int i = 0; i < storedColCount; i++) {
            int pos = mainRowType.getFieldIndex(storedColumns.get(i));
            valueFieldGetters[i] = InternalRow.createFieldGetter(mainRowType.getTypeAt(pos), pos);
        }
        CompactedKeyEncoder primaryKeySuffixEncoder =
                CompactedKeyEncoder.createKeyEncoder(
                        mainRowType,
                        storedColumns.subList(idxColumnIndices.length, storedColCount));
        KvFormat indexKvFormat = indexTable.getTableConfig().getKvFormat();
        RowEncoder physicalRowEncoder = RowEncoder.create(indexKvFormat, physicalRowType);
        KeyEncoder physicalKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        physicalRowType,
                        physicalSchema.getPrimaryKeyColumnNames(),
                        indexTable.getTableConfig(),
                        indexTable.isDefaultBucketKey());
        KeyEncoder physicalBucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        physicalRowType,
                        indexTable.getBucketKeys(),
                        indexTable.getTableConfig(),
                        indexTable.isDefaultBucketKey(),
                        physicalKeyEncoder);
        DataEntryEncoder dataEncoder =
                row -> {
                    byte[] routingKey = logicalIndexKeyEncoder.encodeKey(row);
                    byte[] rowKey = primaryKeySuffixEncoder.encodeKey(row);
                    physicalRowEncoder.startNewRow();
                    for (int i = 0; i < valueFieldGetters.length; i++) {
                        physicalRowEncoder.encodeField(i, valueFieldGetters[i].getFieldOrNull(row));
                    }
                    if (partitioned) {
                        physicalRowEncoder.encodeField(storedColCount, partitionId);
                    }
                    physicalRowEncoder.encodeField(recordKindPosition, DATA_RECORD_KIND);
                    physicalRowEncoder.encodeField(routingKeyPosition, routingKey);
                    physicalRowEncoder.encodeField(rowKeyPosition, rowKey);
                    physicalRowEncoder.encodeField(sourceProgressPosition, null);
                    BinaryRow value = physicalRowEncoder.finishRow();
                    int targetBucket =
                            bucketingFunction.bucketing(
                                    physicalBucketKeyEncoder.encodeKey(value), indexBucketCount);
                    return new IndexReplicationEncoder.IndexEntry(
                            physicalKeyEncoder.encodeKey(value), value, targetBucket);
                };

        byte[] sourceBucketKey =
                ByteBuffer.allocate(Integer.BYTES).putInt(sourceBucket.getBucket()).array();
        byte[] progressRoutingKey = new byte[0];
        ProgressEncoder progressEncoder =
                (targetBucket, sourceEndOffset) -> {
                    checkArgument(
                            targetBucket >= 0 && targetBucket < indexBucketCount,
                            "Invalid target Index Table bucket %s",
                            targetBucket);
                    physicalRowEncoder.startNewRow();
                    for (int i = 0; i < storedColCount; i++) {
                        physicalRowEncoder.encodeField(i, null);
                    }
                    if (partitioned) {
                        physicalRowEncoder.encodeField(storedColCount, partitionId);
                    }
                    physicalRowEncoder.encodeField(recordKindPosition, PROGRESS_RECORD_KIND);
                    // The request selects the target bucket; its local key needs no routing ID.
                    physicalRowEncoder.encodeField(routingKeyPosition, progressRoutingKey);
                    physicalRowEncoder.encodeField(rowKeyPosition, sourceBucketKey);
                    physicalRowEncoder.encodeField(sourceProgressPosition, sourceEndOffset);
                    BinaryRow value = physicalRowEncoder.finishRow();
                    return new IndexReplicationEncoder.IndexEntry(
                            physicalKeyEncoder.encodeKey(value), value, targetBucket);
                };

        return new IndexReplicationEncoder(
                index.getIndexName(),
                index.getVisibility(),
                indexTableId,
                indexSchemaId,
                indexKvFormat,
                idxColumnIndices,
                dataEncoder,
                progressEncoder);
    }

    private static TableInfo resolveIndexTable(
            TableInfo mainTableInfo, Schema.Index index, TabletServerMetadataCache metadataCache) {
        TablePath path =
                IndexTableUtils.indexTablePath(mainTableInfo.getTablePath(), index.getIndexName());
        TableMetadata metadata =
                metadataCache
                        .getTableMetadata(path)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Index Table " + path + " metadata not in cache"));
        TableInfo indexTableInfo = metadata.getTableInfo();
        validateIndexTable(mainTableInfo, index, path, indexTableInfo);
        return indexTableInfo;
    }

    private static void validateIndexTable(
            TableInfo mainTableInfo, Schema.Index index, TablePath path, TableInfo indexTableInfo) {
        if (!indexTableInfo.isIndexTable()) {
            throw invalidMetadata(path, "a table that is not an Index Table");
        }
        if (indexTableInfo.getMainTableId().getAsLong() != mainTableInfo.getTableId()) {
            throw invalidMetadata(
                    path,
                    "an Index Table owned by main table "
                            + mainTableInfo.getTableId()
                            + " but found owner "
                            + indexTableInfo.getMainTableId().getAsLong());
        }

        TableDescriptor expected =
                IndexTableDescriptorFactory.derive(
                        mainTableInfo.toTableDescriptor(), mainTableInfo.getTableId(), index);
        // The main-table bucket count supplies only the creation-time default.
        Integer configuredBucketCount = index.getBucketCount().orElse(null);
        if (configuredBucketCount != null) {
            checkMetadata(
                    path, "bucket count", configuredBucketCount, indexTableInfo.getNumBuckets());
        }
        checkMetadata(
                path, "bucket keys", expected.getBucketKeys(), indexTableInfo.getBucketKeys());
        if (!indexTableInfo.getSchema().equals(expected.getSchema())) {
            throw invalidMetadata(path, "a schema different from the derived Index Table schema");
        }
        TableConfig expectedConfig =
                new TableConfig(Configuration.fromMap(expected.getProperties()));
        TableConfig actual = indexTableInfo.getTableConfig();
        checkMetadata(path, "KV format", expectedConfig.getKvFormat(), actual.getKvFormat());
        checkMetadata(path, "log format", expectedConfig.getLogFormat(), actual.getLogFormat());
        checkMetadata(
                path,
                "changelog image",
                expectedConfig.getChangelogImage(),
                actual.getChangelogImage());
        checkMetadata(
                path,
                "KV format version",
                expectedConfig.getKvFormatVersion().get(),
                actual.getKvFormatVersion().orElse(0));
    }

    private static void checkMetadata(
            TablePath path, String field, Object expected, Object actual) {
        if (!expected.equals(actual)) {
            throw invalidMetadata(path, field + " " + expected + " but found " + actual);
        }
    }

    private static IllegalStateException invalidMetadata(TablePath path, String detail) {
        return new IllegalStateException(
                "Invalid metadata for Index Table " + path + ": expected " + detail);
    }
}
