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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.CompactedRowEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.row.BinaryString.EMPTY_UTF8;

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

    private static final ConsumerOffsetAndMetadata EMPTY_OFFSET =
            new ConsumerOffsetAndMetadata(-1L, "");

    private static final List<String> PRIMARY_KEYS =
            Collections.unmodifiableList(
                    Arrays.asList(
                            GROUP_ID_COLUMN,
                            RECORD_TYPE_COLUMN,
                            TABLE_ID_COLUMN,
                            PARTITION_ID_COLUMN,
                            BUCKET_ID_COLUMN));

    private static final Schema TABLE_SCHEMA = createTableSchema();
    private static final RowType ROW_TYPE = TABLE_SCHEMA.getRowType();
    private static final DataType[] DATA_TYPES = ROW_TYPE.getChildren().toArray(new DataType[0]);

    private ConsumerOffsetSchema() {}

    /**
     * Creates an encoder for complete consumer-offset primary keys.
     *
     * <p>{@link KeyEncoder} implementations may reuse mutable internal writers and are not thread
     * safe. Each caller should retain and reuse its own encoder rather than sharing one globally.
     */
    public static KeyEncoder createOffsetKeyEncoder() {
        return KeyEncoder.ofBucketKeyEncoder(ROW_TYPE, PRIMARY_KEYS, null);
    }

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
    public static byte[] encodeOffsetKey(
            KeyEncoder keyEncoder, String groupId, long tableId, Long partitionId, int bucketId) {
        return encodeOffsetKey(
                keyEncoder,
                encodeOffsetRow(
                        groupId,
                        OFFSET_COMMIT_RECORD_TYPE,
                        tableId,
                        partitionId,
                        bucketId,
                        EMPTY_OFFSET));
    }

    /** Encodes the {@code group_id} prefix used to enumerate all rows belonging to a group. */
    public static byte[] encodeGroupPrefix(String groupId) {
        return createGroupPrefixEncoder()
                .encodeKey(
                        encodeOffsetRow(
                                groupId,
                                OFFSET_COMMIT_RECORD_TYPE,
                                -1L,
                                NON_PARTITIONED_PARTITION_ID,
                                -1,
                                EMPTY_OFFSET));
    }

    /** Encodes the complete primary key of an already materialized offset row. */
    static byte[] encodeOffsetKey(KeyEncoder keyEncoder, InternalRow row) {
        return keyEncoder.encodeKey(row);
    }

    /**
     * Creates a new group-prefix encoder.
     *
     * <p>{@link KeyEncoder} implementations may reuse mutable internal writers, so a new encoder
     * must be created for each call to keep concurrent key encoding isolated.
     */
    private static KeyEncoder createGroupPrefixEncoder() {
        return KeyEncoder.ofBucketKeyEncoder(
                ROW_TYPE, Collections.singletonList(GROUP_ID_COLUMN), null);
    }

    /** Encodes one consumer-offset table row. */
    public static CompactedRow encodeOffsetRow(
            String groupId,
            int recordType,
            long tableId,
            Long partitionId,
            int bucketId,
            ConsumerOffsetAndMetadata offsetAndMetadata) {
        RowEncoder rowEncoder = new CompactedRowEncoder(DATA_TYPES);
        rowEncoder.startNewRow();
        rowEncoder.encodeField(0, BinaryString.fromString(groupId));
        rowEncoder.encodeField(1, recordType);
        rowEncoder.encodeField(2, tableId);
        rowEncoder.encodeField(3, partitionId == null ? NON_PARTITIONED_PARTITION_ID : partitionId);
        rowEncoder.encodeField(4, bucketId);
        rowEncoder.encodeField(5, offsetAndMetadata.getOffset());
        rowEncoder.encodeField(
                6,
                offsetAndMetadata.getMetadata().isEmpty()
                        ? EMPTY_UTF8
                        : BinaryString.fromString(offsetAndMetadata.getMetadata()));
        return (CompactedRow) rowEncoder.finishRow();
    }

    private static Schema createTableSchema() {
        return Schema.newBuilder()
                .column(GROUP_ID_COLUMN, DataTypes.STRING())
                .column(RECORD_TYPE_COLUMN, DataTypes.INT())
                .column(TABLE_ID_COLUMN, DataTypes.BIGINT())
                .column(PARTITION_ID_COLUMN, DataTypes.BIGINT())
                .column(BUCKET_ID_COLUMN, DataTypes.INT())
                .column(OFFSET_COLUMN, DataTypes.BIGINT())
                .column(METADATA_COLUMN, DataTypes.STRING())
                .primaryKey(PRIMARY_KEYS)
                .build();
    }
}
