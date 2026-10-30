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
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;

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

/**
 * Factory for building {@link IndexSpec} instances from main-table metadata and the server-side
 * metadata cache.
 *
 * <p>Extracted from {@code Replica} so that the spec-building logic (schema resolution, encoder
 * construction, bucket assignment) lives in one place rather than being scattered across a
 * 2600-line God Class.
 *
 * <p>Caller contract: the {@link IndexSpec.EntryEncoder} instances produced by this factory are
 * {@link NotThreadSafe} because they capture a mutable {@link RowEncoder}. The caller (typically
 * {@link IndexReplicator}) must guarantee single-threaded invocation.
 */
@Internal
public final class IndexSpecFactory {

    private IndexSpecFactory() {}

    /**
     * Builds the list of {@link IndexSpec}s for every secondary index declared on the main table.
     *
     * @param mainTableInfo the main (data) table's info
     * @param mainTableBucket the bucket of the main table replica that will drive the replicators
     * @param metadataCache server-wide metadata cache for resolving index-table IDs and bucket
     *     counts
     * @return an unmodifiable list of specs; empty if the main table has no indexes
     */
    public static List<IndexSpec> buildIndexSpecs(
            TableInfo mainTableInfo,
            TableBucket mainTableBucket,
            TabletServerMetadataCache metadataCache) {

        Schema schema = mainTableInfo.getSchema();
        List<Schema.Index> indexes = schema.getIndexes();
        if (indexes.isEmpty()) {
            return Collections.emptyList();
        }

        int[] basePkColumnIndices = schema.getPrimaryKeyIndexes();
        boolean partitioned = mainTableInfo.isPartitioned();
        RowType mainRowType = mainTableInfo.getRowType();

        Long partitionId = partitioned ? mainTableBucket.getPartitionId() : null;

        List<IndexSpec> specs = new ArrayList<>(indexes.size());
        for (Schema.Index index : indexes) {
            specs.add(
                    buildOneSpec(
                            index,
                            schema,
                            mainTableInfo,
                            mainRowType,
                            basePkColumnIndices,
                            partitioned,
                            partitionId,
                            metadataCache));
        }
        return Collections.unmodifiableList(specs);
    }

    // ---- internal helpers ----

    private static IndexSpec buildOneSpec(
            Schema.Index index,
            Schema schema,
            TableInfo mainTableInfo,
            RowType mainRowType,
            int[] basePkColumnIndices,
            boolean partitioned,
            Long partitionId,
            TabletServerMetadataCache metadataCache) {

        int[] idxColumnIndices = schema.getColumnIndexes(index.getColumnNames());
        int[] indexValueColumnIndices =
                composeIndexValueColumnIndices(idxColumnIndices, basePkColumnIndices);

        String indexName = index.getIndexName();
        ResolvedIndexTable indexTable = resolveIndexTable(mainTableInfo, index, metadataCache);
        long indexTableId = indexTable.tableId;
        int indexBucketCount = indexTable.bucketCount;
        int indexSchemaId = indexTable.schemaId;

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
        RowEncoder identityRowEncoder = RowEncoder.create(indexKvFormat, identityFieldTypes);
        int[] identityIndices = new int[totalColCount];
        for (int i = 0; i < totalColCount; i++) {
            identityIndices[i] = i;
        }
        RowType identityRowType = RowType.of(identityFieldTypes);
        CompactedKeyEncoder identityKeyEncoder =
                new CompactedKeyEncoder(identityRowType, identityIndices);
        InternalRow.FieldGetter[] identityGetters = new InternalRow.FieldGetter[totalColCount];
        for (int i = 0; i < totalColCount; i++) {
            identityGetters[i] = InternalRow.createFieldGetter(identityFieldTypes[i], i);
        }

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
        byte[][] progressRoutingKeys = new byte[indexBucketCount][];
        for (int targetBucket = 0; targetBucket < indexBucketCount; targetBucket++) {
            progressRoutingKeys[targetBucket] =
                    ByteBuffer.allocate(Integer.BYTES).putInt(targetBucket).array();
        }
        IndexSpec.EntryEncoder entryEncoder =
                row -> {
                    identityRowEncoder.startNewRow();
                    for (int i = 0; i < valueFieldGetters.length; i++) {
                        identityRowEncoder.encodeField(i, valueFieldGetters[i].getFieldOrNull(row));
                    }
                    if (partitioned) {
                        identityRowEncoder.encodeField(storedColCount, partitionId);
                    }
                    BinaryRow identity = identityRowEncoder.finishRow();
                    byte[] routingKey = logicalIndexKeyEncoder.encodeKey(row);
                    byte[] rowKey = identityKeyEncoder.encodeKey(identity);

                    physicalRowEncoder.startNewRow();
                    for (int i = 0; i < totalColCount; i++) {
                        physicalRowEncoder.encodeField(
                                i, identityGetters[i].getFieldOrNull(identity));
                    }
                    physicalRowEncoder.encodeField(recordKindPosition, DATA_RECORD_KIND);
                    physicalRowEncoder.encodeField(routingKeyPosition, routingKey);
                    physicalRowEncoder.encodeField(rowKeyPosition, rowKey);
                    physicalRowEncoder.encodeField(sourceProgressPosition, null);
                    BinaryRow value = physicalRowEncoder.finishRow();
                    int targetBucket =
                            bucketingFunction.bucketing(
                                    physicalBucketKeyEncoder.encodeKey(value), indexBucketCount);
                    return new IndexSpec.IndexEntry(
                            physicalKeyEncoder.encodeKey(value), value, targetBucket);
                };

        IndexSpec.ProgressEncoder progressEncoder =
                (sourceBucket, targetBucket, sourceEndOffset) -> {
                    checkArgument(
                            targetBucket >= 0 && targetBucket < indexBucketCount,
                            "Invalid target Index Table bucket %s",
                            targetBucket);
                    physicalRowEncoder.startNewRow();
                    for (int i = 0; i < storedColCount; i++) {
                        physicalRowEncoder.encodeField(i, null);
                    }
                    if (partitioned) {
                        checkArgument(
                                sourceBucket.getPartitionId() != null,
                                "Partitioned source bucket must contain partition ID");
                        physicalRowEncoder.encodeField(
                                storedColCount, sourceBucket.getPartitionId());
                    } else {
                        checkArgument(
                                sourceBucket.getPartitionId() == null,
                                "Unpartitioned source bucket must not contain partition ID");
                    }
                    physicalRowEncoder.encodeField(recordKindPosition, PROGRESS_RECORD_KIND);
                    physicalRowEncoder.encodeField(
                            routingKeyPosition, progressRoutingKeys[targetBucket]);
                    physicalRowEncoder.encodeField(
                            rowKeyPosition, encodeSourceBucket(sourceBucket));
                    physicalRowEncoder.encodeField(sourceProgressPosition, sourceEndOffset);
                    BinaryRow value = physicalRowEncoder.finishRow();
                    return new IndexSpec.IndexEntry(
                            physicalKeyEncoder.encodeKey(value), value, targetBucket);
                };

        return new IndexSpec(
                index.getIndexName(),
                index.getVisibility(),
                indexTableId,
                indexSchemaId,
                indexKvFormat,
                idxColumnIndices,
                entryEncoder,
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
    static int[] composeIndexValueColumnIndices(int[] idxColumnIndices, int[] basePkColumnIndices) {
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

    private static ResolvedIndexTable resolveIndexTable(
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
        return new ResolvedIndexTable(
                indexTableInfo.getTableId(),
                indexTableInfo.getNumBuckets(),
                indexTableInfo.getSchemaId());
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
        int expectedBucketCount =
                expected.getTableDistribution()
                        .flatMap(TableDescriptor.TableDistribution::getBucketCount)
                        .orElseThrow(() -> invalidMetadata(path, "no derived bucket count"));
        if (indexTableInfo.getNumBuckets() != expectedBucketCount) {
            throw invalidMetadata(
                    path,
                    "bucket count "
                            + expectedBucketCount
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

    private static final class ResolvedIndexTable {
        private final long tableId;
        private final int bucketCount;
        private final int schemaId;

        private ResolvedIndexTable(long tableId, int bucketCount, int schemaId) {
            this.tableId = tableId;
            this.bucketCount = bucketCount;
            this.schemaId = schemaId;
        }
    }
}
