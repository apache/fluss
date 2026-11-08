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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    /**
     * Builds the list of {@link IndexReplicationEncoder}s for every secondary index declared on the
     * main table.
     *
     * @param mainTableInfo the main (data) table's info
     * @param mainTableBucket the bucket of the main table replica that will drive the replicators
     * @param metadataCache server-wide metadata cache for resolving index-table IDs and bucket
     *     counts
     * @return an unmodifiable list of encoders; empty if the main table has no indexes
     */
    static List<IndexReplicationEncoder> createAll(
            TableInfo mainTableInfo,
            TableBucket mainTableBucket,
            TabletServerMetadataCache metadataCache) {

        Schema schema = mainTableInfo.getSchema();
        List<Schema.Index> indexes = schema.getIndexes();
        if (indexes.isEmpty()) {
            return Collections.emptyList();
        }

        int[] basePkColumnIndices = schema.getPrimaryKeyIndexes();
        List<IndexReplicationEncoder> encoders = new ArrayList<>(indexes.size());
        for (Schema.Index index : indexes) {
            encoders.add(
                    createEncoder(
                            index,
                            mainTableInfo,
                            mainTableBucket,
                            basePkColumnIndices,
                            metadataCache));
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
            int[] basePkColumnIndices,
            TabletServerMetadataCache metadataCache) {
        Schema schema = mainTableInfo.getSchema();
        RowType mainRowType = mainTableInfo.getRowType();
        boolean partitioned = mainTableInfo.isPartitioned();
        Long partitionId = sourceBucket.getPartitionId();
        checkArgument(
                partitioned == (partitionId != null),
                "Source bucket partition identity must match the main table");

        int[] idxColumnIndices = schema.getColumnIndexes(index.getColumnNames());
        int[] indexValueColumnIndices =
                composeIndexValueColumnIndices(idxColumnIndices, basePkColumnIndices);

        TableInfo indexTable = resolveIndexTable(mainTableInfo, index, metadataCache);
        long indexTableId = indexTable.getTableId();
        int indexBucketCount = indexTable.getNumBuckets();
        int indexSchemaId = indexTable.getSchemaId();

        CompactedKeyEncoder logicalIndexKeyEncoder =
                new CompactedKeyEncoder(mainRowType, idxColumnIndices);
        FlussBucketingFunction bucketingFunction = new FlussBucketingFunction();

        int storedColCount = indexValueColumnIndices.length;
        int totalColCount = storedColCount + (partitioned ? 1 : 0);
        DataType[] identityFieldTypes = new DataType[totalColCount];
        InternalRow.FieldGetter[] valueFieldGetters = new InternalRow.FieldGetter[storedColCount];
        for (int i = 0; i < storedColCount; i++) {
            int idxInMain = indexValueColumnIndices[i];
            DataType type = mainRowType.getTypeAt(idxInMain);
            identityFieldTypes[i] = type;
            valueFieldGetters[i] = InternalRow.createFieldGetter(type, idxInMain);
        }
        if (partitioned) {
            identityFieldTypes[storedColCount] = DataTypes.BIGINT().copy(false);
        }

        KvFormat indexKvFormat = KvFormat.COMPACTED;
        CompactedKeyEncoder identityKeyEncoder =
                new CompactedKeyEncoder(RowType.of(identityFieldTypes));
        GenericRow identity = new GenericRow(totalColCount);

        int recordKindPosition = totalColCount;
        int routingKeyPosition = totalColCount + 1;
        int rowKeyPosition = totalColCount + 2;
        int sourceProgressPosition = totalColCount + 3;
        DataType[] physicalFieldTypes = new DataType[totalColCount + 4];
        for (int i = 0; i < storedColCount; i++) {
            physicalFieldTypes[i] = identityFieldTypes[i].copy(true);
        }
        if (partitioned) {
            physicalFieldTypes[storedColCount] = identityFieldTypes[storedColCount];
        }
        physicalFieldTypes[recordKindPosition] = DataTypes.TINYINT().copy(false);
        physicalFieldTypes[routingKeyPosition] = DataTypes.BYTES().copy(false);
        physicalFieldTypes[rowKeyPosition] = DataTypes.BYTES().copy(false);
        physicalFieldTypes[sourceProgressPosition] = DataTypes.BIGINT();
        RowType physicalRowType = RowType.of(physicalFieldTypes);
        RowEncoder physicalRowEncoder = RowEncoder.create(indexKvFormat, physicalFieldTypes);
        int[] physicalKeyIndices =
                partitioned
                        ? new int[] {
                            recordKindPosition, routingKeyPosition, storedColCount, rowKeyPosition
                        }
                        : new int[] {recordKindPosition, routingKeyPosition, rowKeyPosition};
        CompactedKeyEncoder physicalKeyEncoder =
                new CompactedKeyEncoder(physicalRowType, physicalKeyIndices);
        CompactedKeyEncoder physicalBucketKeyEncoder =
                new CompactedKeyEncoder(
                        physicalRowType, new int[] {recordKindPosition, routingKeyPosition});
        DataEntryEncoder dataEncoder =
                row -> {
                    try {
                        for (int i = 0; i < valueFieldGetters.length; i++) {
                            identity.setField(i, valueFieldGetters[i].getFieldOrNull(row));
                        }
                        if (partitioned) {
                            identity.setField(storedColCount, partitionId);
                        }
                        byte[] routingKey = logicalIndexKeyEncoder.encodeKey(row);
                        byte[] rowKey = identityKeyEncoder.encodeKey(identity);
                        physicalRowEncoder.startNewRow();
                        for (int i = 0; i < totalColCount; i++) {
                            physicalRowEncoder.encodeField(i, identity.getField(i));
                        }
                        physicalRowEncoder.encodeField(recordKindPosition, DATA_RECORD_KIND);
                        physicalRowEncoder.encodeField(routingKeyPosition, routingKey);
                        physicalRowEncoder.encodeField(rowKeyPosition, rowKey);
                        physicalRowEncoder.encodeField(sourceProgressPosition, null);
                        BinaryRow value = physicalRowEncoder.finishRow();
                        int targetBucket =
                                bucketingFunction.bucketing(
                                        physicalBucketKeyEncoder.encodeKey(value),
                                        indexBucketCount);
                        return new IndexReplicationEncoder.IndexEntry(
                                physicalKeyEncoder.encodeKey(value), value, targetBucket);
                    } finally {
                        // Source fields may reference WAL pages; do not retain them between calls.
                        for (int i = 0; i < totalColCount; i++) {
                            identity.setField(i, null);
                        }
                    }
                };

        byte[] sourceIdentity = encodeSourceBucket(sourceBucket);
        ByteBuffer progressRoutingKey = ByteBuffer.allocate(Integer.BYTES);
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
                    // The row encoder copies the bytes before this buffer is reused.
                    physicalRowEncoder.encodeField(
                            routingKeyPosition, progressRoutingKey.putInt(0, targetBucket).array());
                    physicalRowEncoder.encodeField(rowKeyPosition, sourceIdentity);
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

    private static byte[] encodeSourceBucket(TableBucket sourceBucket) {
        boolean partitioned = sourceBucket.getPartitionId() != null;
        ByteBuffer buffer =
                ByteBuffer.allocate(
                        Long.BYTES + 1 + (partitioned ? Long.BYTES : 0) + Integer.BYTES);
        buffer.putLong(sourceBucket.getTableId());
        buffer.put((byte) (partitioned ? 1 : 0));
        if (partitioned) {
            buffer.putLong(sourceBucket.getPartitionId());
        }
        buffer.putInt(sourceBucket.getBucket());
        return buffer.array();
    }

    /**
     * Compose index-value column indices: idx columns followed by base-PK columns, deduplicated.
     */
    private static int[] composeIndexValueColumnIndices(
            int[] idxColumnIndices, int[] basePkColumnIndices) {
        Set<Integer> seen = new HashSet<>();
        int[] tmp = new int[idxColumnIndices.length + basePkColumnIndices.length];
        int len = 0;
        for (int i : idxColumnIndices) {
            if (seen.add(i)) {
                tmp[len++] = i;
            }
        }
        for (int i : basePkColumnIndices) {
            if (seen.add(i)) {
                tmp[len++] = i;
            }
        }
        int[] out = new int[len];
        System.arraycopy(tmp, 0, out, 0, len);
        return out;
    }

    private static TablePath indexTablePathFor(TableInfo mainTableInfo, String indexName) {
        return TablePath.of(
                mainTableInfo.getTablePath().getDatabaseName(),
                IndexTableUtils.indexTableName(
                        mainTableInfo.getTablePath().getTableName(), indexName));
    }

    private static TableInfo resolveIndexTable(
            TableInfo mainTableInfo, Schema.Index index, TabletServerMetadataCache metadataCache) {
        TablePath path = indexTablePathFor(mainTableInfo, index.getIndexName());
        long cachedTableId =
                metadataCache
                        .getTableId(path)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Index Table " + path + " not in cache"));
        TableMetadata metadata =
                metadataCache
                        .getTableMetadata(path)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Index Table " + path + " metadata not in cache"));
        TableInfo indexTableInfo = metadata.getTableInfo();
        if (cachedTableId != indexTableInfo.getTableId()) {
            throw invalidMetadata(
                    path,
                    "table id "
                            + cachedTableId
                            + " from the path cache, but metadata contains "
                            + indexTableInfo.getTableId());
        }
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
        if (configuredBucketCount != null
                && indexTableInfo.getNumBuckets() != configuredBucketCount) {
            throw invalidMetadata(
                    path,
                    "bucket count "
                            + configuredBucketCount
                            + " but found "
                            + indexTableInfo.getNumBuckets());
        }
        if (!indexTableInfo.getBucketKeys().equals(expected.getBucketKeys())) {
            throw invalidMetadata(
                    path,
                    "bucket keys "
                            + expected.getBucketKeys()
                            + " but found "
                            + indexTableInfo.getBucketKeys());
        }
        if (!indexTableInfo.getSchema().equals(expected.getSchema())) {
            throw invalidMetadata(path, "a schema different from the derived Index Table schema");
        }
        TableConfig expectedConfig =
                new TableConfig(Configuration.fromMap(expected.getProperties()));
        if (indexTableInfo.getTableConfig().getKvFormat() != expectedConfig.getKvFormat()) {
            throw invalidMetadata(
                    path,
                    "KV format "
                            + expectedConfig.getKvFormat()
                            + " but found "
                            + indexTableInfo.getTableConfig().getKvFormat());
        }
        LogFormat actualLogFormat = indexTableInfo.getTableConfig().getLogFormat();
        if (actualLogFormat != expectedConfig.getLogFormat()) {
            throw invalidMetadata(
                    path,
                    "log format "
                            + expectedConfig.getLogFormat()
                            + " but found "
                            + actualLogFormat);
        }
        ChangelogImage actualChangelogImage = indexTableInfo.getTableConfig().getChangelogImage();
        if (actualChangelogImage != expectedConfig.getChangelogImage()) {
            throw invalidMetadata(
                    path,
                    "changelog image "
                            + expectedConfig.getChangelogImage()
                            + " but found "
                            + actualChangelogImage);
        }
        int expectedKvFormatVersion =
                Integer.parseInt(
                        expected.getProperties().get(ConfigOptions.TABLE_KV_FORMAT_VERSION.key()));
        int actualKvFormatVersion = indexTableInfo.getTableConfig().getKvFormatVersion().orElse(0);
        if (actualKvFormatVersion != expectedKvFormatVersion) {
            throw invalidMetadata(
                    path,
                    "KV format version "
                            + expectedKvFormatVersion
                            + " but found "
                            + actualKvFormatVersion);
        }
    }

    private static IllegalStateException invalidMetadata(TablePath path, String detail) {
        return new IllegalStateException(
                "Invalid metadata for Index Table " + path + ": expected " + detail);
    }
}
