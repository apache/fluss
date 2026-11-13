/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.tablet.consumer.group;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.bucketing.FlussBucketingFunction;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.CompactedRowEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Schema and encoding helpers for the {@code sys.consumer_offsets} system table. */
@Internal
public final class ConsumerOffsetSchema {

    private static final String GROUP_ID_COLUMN = "group_id";
    private static final String RECORD_TYPE_COLUMN = "record_type";
    private static final String TABLE_ID_COLUMN = "table_id";
    private static final String PARTITION_ID_COLUMN = "partition_id";
    private static final String BUCKET_ID_COLUMN = "bucket_id";
    private static final String OFFSET_COLUMN = "offset";
    private static final String METADATA_COLUMN = "metadata";

    /** Record type for consumer offset commits. */
    public static final int OFFSET_COMMIT_RECORD_TYPE = 1;

    /** Record type reserved for consumer group metadata. */
    public static final int GROUP_METADATA_RECORD_TYPE = 2;

    /** Sentinel used for non-partitioned tables because primary-key columns cannot be null. */
    public static final long NON_PARTITIONED_PARTITION_ID = -1L;

    private static final List<String> PRIMARY_KEYS =
            Collections.unmodifiableList(
                    Arrays.asList(
                            RECORD_TYPE_COLUMN,
                            GROUP_ID_COLUMN,
                            TABLE_ID_COLUMN,
                            PARTITION_ID_COLUMN,
                            BUCKET_ID_COLUMN));

    private static final List<String> GROUP_LOOKUP_PREFIX_KEYS =
            Collections.unmodifiableList(Arrays.asList(RECORD_TYPE_COLUMN, GROUP_ID_COLUMN));

    private static final Schema TABLE_SCHEMA = createTableSchema();
    private static final RowType ROW_TYPE = TABLE_SCHEMA.getRowType();
    private static final DataType[] DATA_TYPES = ROW_TYPE.getChildren().toArray(new DataType[0]);

    private final ThreadLocal<CompactedRowEncoder> rowEncoder =
            ThreadLocal.withInitial(() -> new CompactedRowEncoder(DATA_TYPES));
    // sys.consumer_offsets has no lake format, so ofPrimaryKeyEncoder uses compacted keys
    // for both KV v1 and v2. Passing the full primary key and a null lake format to
    // ofBucketKeyEncoder selects the same encoding without requiring a TableConfig.
    private final ThreadLocal<KeyEncoder> primaryKeyEncoder =
            ThreadLocal.withInitial(
                    () -> KeyEncoder.ofBucketKeyEncoder(ROW_TYPE, PRIMARY_KEYS, null));
    private final ThreadLocal<KeyEncoder> groupBucketKeyEncoder =
            ThreadLocal.withInitial(
                    () ->
                            KeyEncoder.ofBucketKeyEncoder(
                                    ROW_TYPE, Collections.singletonList(GROUP_ID_COLUMN), null));
    private final ThreadLocal<KeyEncoder> groupLookupPrefixEncoder =
            ThreadLocal.withInitial(
                    () -> KeyEncoder.ofBucketKeyEncoder(ROW_TYPE, GROUP_LOOKUP_PREFIX_KEYS, null));

    /** Returns the schema of the consumer-offset system table. */
    public static Schema getTableSchema() {
        return TABLE_SCHEMA;
    }

    /** Returns the primary-key columns in encoding order. */
    public static List<String> getPrimaryKeys() {
        return PRIMARY_KEYS;
    }

    /** Returns the column used to distribute rows across table buckets. */
    public static String getBucketKeyColumn() {
        return GROUP_ID_COLUMN;
    }

    /** Encodes the complete primary key of an offset row. */
    public byte[] encodeOffsetKey(String groupId, long tableId, Long partitionId, int bucketId) {
        return encodeOffsetKey(
                GenericRow.of(
                        OFFSET_COMMIT_RECORD_TYPE,
                        BinaryString.fromString(groupId),
                        tableId,
                        partitionId == null ? NON_PARTITIONED_PARTITION_ID : partitionId,
                        bucketId));
    }

    /** Creates a decoder for values stored with the consumer-offset schema. */
    public static ValueDecoder createValueDecoder(SchemaGetter schemaGetter) {
        return new ValueDecoder(schemaGetter, KvFormat.COMPACTED, KvValueLayout.PLAIN);
    }

    /** Converts a decoded offset row into its committed offset and metadata. */
    static ConsumerOffsetAndMetadata toOffsetAndMetadata(InternalRow row) {
        return new ConsumerOffsetAndMetadata(
                row.getLong(5), row.isNullAt(6) ? null : row.getString(6).toString());
    }

    /** Converts a decoded offset row into its source table bucket. */
    static TableBucket toTableBucket(InternalRow row) {
        long partitionId = row.getLong(3);
        return new TableBucket(row.getLong(2), partitionId < 0 ? null : partitionId, row.getInt(4));
    }

    /** Encodes the bucket key used to route a group to its offsets-table bucket. */
    public byte[] encodeGroupBucketKey(String groupId) {
        // The bucket-key encoder reads group_id from column 1; record_type is only a placeholder.
        return groupBucketKeyEncoder
                .get()
                .encodeKey(
                        GenericRow.of(OFFSET_COMMIT_RECORD_TYPE, BinaryString.fromString(groupId)));
    }

    /**
     * Encodes the {@code record_type} and {@code group_id} prefix used to enumerate a group's
     * committed offsets.
     */
    public byte[] encodeOffsetCommitPrefix(String groupId) {
        return groupLookupPrefixEncoder
                .get()
                .encodeKey(
                        GenericRow.of(OFFSET_COMMIT_RECORD_TYPE, BinaryString.fromString(groupId)));
    }

    /** Encodes the complete primary key of an already materialized offset row. */
    byte[] encodeOffsetKey(InternalRow row) {
        return primaryKeyEncoder.get().encodeKey(row);
    }

    int bucketFor(String groupId, int bucketCount) {
        return FlussBucketingFunction.bucketForRowKey(encodeGroupBucketKey(groupId), bucketCount);
    }

    /** The returned row uses this thread's buffer until its next call to this method. */
    CompactedRow encodeOffsetRow(
            String groupId,
            int recordType,
            long tableId,
            Long partitionId,
            int bucketId,
            ConsumerOffsetAndMetadata offsetAndMetadata) {
        CompactedRowEncoder encoder = rowEncoder.get();
        encoder.startNewRow();
        encoder.encodeField(0, recordType);
        encoder.encodeField(1, BinaryString.fromString(groupId));
        encoder.encodeField(2, tableId);
        encoder.encodeField(3, partitionId == null ? NON_PARTITIONED_PARTITION_ID : partitionId);
        encoder.encodeField(4, bucketId);
        encoder.encodeField(5, offsetAndMetadata.getOffset());
        encoder.encodeField(6, offsetAndMetadata.getMetadata());
        return encoder.finishRow();
    }

    private static Schema createTableSchema() {
        return Schema.newBuilder()
                .column(RECORD_TYPE_COLUMN, DataTypes.INT())
                .column(GROUP_ID_COLUMN, DataTypes.STRING())
                .column(TABLE_ID_COLUMN, DataTypes.BIGINT())
                .column(PARTITION_ID_COLUMN, DataTypes.BIGINT())
                .column(BUCKET_ID_COLUMN, DataTypes.INT())
                .column(OFFSET_COLUMN, DataTypes.BIGINT())
                .column(METADATA_COLUMN, DataTypes.STRING())
                .primaryKey(PRIMARY_KEYS)
                .build();
    }
}
