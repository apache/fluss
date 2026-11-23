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

package org.apache.fluss.server.metadata;

import org.apache.fluss.bucketing.FlussBucketingFunction;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.record.DefaultKvRecordBatch;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordBatchBuilder;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedRowEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Schema, distribution and encoding of the internal named snapshot binding table. */
public final class MvccSnapshotBindingSchema {
    /** Finite lifetime applied when a writer supplies no deadline. */
    public static final long DEFAULT_LIFETIME_MILLIS = Duration.ofDays(1).toMillis();
    /** Maximum binding lifetime. */
    public static final long MAX_LIFETIME_MILLIS = Duration.ofDays(7).toMillis();
    /** Limit for a complete List response, including the frame. */
    public static final int LIST_RESPONSE_BYTES = 16 * 1024 * 1024;

    private static final String EXPIRY = "expires_at_millis";
    private static final List<String> PREFIX =
            Collections.unmodifiableList(
                    Arrays.asList("table_id", "partition_id", "bucket_id", "namespace"));
    private static final List<String> KEY =
            Arrays.asList("table_id", "partition_id", "bucket_id", "namespace", "name");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("table_id", DataTypes.BIGINT().copy(false))
                    .column("partition_id", DataTypes.BIGINT().copy(false))
                    .column("bucket_id", DataTypes.INT().copy(false))
                    .column("namespace", DataTypes.STRING().copy(false))
                    .column("name", DataTypes.STRING().copy(false))
                    .column("next_offset", DataTypes.BIGINT().copy(false))
                    .column(EXPIRY, DataTypes.BIGINT().copy(false))
                    .primaryKey(KEY)
                    .build();

    private final ThreadLocal<KeyEncoder> keyEncoder =
            ThreadLocal.withInitial(
                    () -> KeyEncoder.ofBucketKeyEncoder(SCHEMA.getRowType(), KEY, null));
    private final ThreadLocal<KeyEncoder> prefixEncoder =
            ThreadLocal.withInitial(
                    () -> KeyEncoder.ofBucketKeyEncoder(SCHEMA.getRowType(), PREFIX, null));
    private final ThreadLocal<CompactedRowEncoder> rowEncoder =
            ThreadLocal.withInitial(
                    () ->
                            new CompactedRowEncoder(
                                    SCHEMA.getRowType().getChildren().toArray(new DataType[0])));

    /** Creates the table descriptor with the initial number of physical buckets. */
    public static TableDescriptor descriptor(int buckets) {
        return TableDescriptor.builder()
                .schema(SCHEMA)
                .distributedBy(buckets, PREFIX.toArray(new String[0]))
                .property(ConfigOptions.TABLE_KV_TTL_TIME_COLUMN, EXPIRY)
                .property(ConfigOptions.TABLE_KV_TTL, Duration.ofHours(1))
                .build();
    }

    /** Encodes the distribution-key prefix of this binding family. */
    public byte[] prefix(TableBucket bucket, String namespace) {
        return prefixEncoder
                .get()
                .encodeKey(
                        GenericRow.of(
                                bucket.getTableId(),
                                partition(bucket),
                                bucket.getBucket(),
                                BinaryString.fromString(namespace)));
    }

    /** Computes the owning bucket using the stored bucket count of the system table. */
    public int shard(TableBucket bucket, String namespace, int numBuckets) {
        return FlussBucketingFunction.bucketForRowKey(prefix(bucket, namespace), numBuckets);
    }

    /** Encodes the complete primary key. */
    public byte[] key(TableBucket bucket, String namespace, String name) {
        return keyEncoder
                .get()
                .encodeKey(
                        GenericRow.of(
                                bucket.getTableId(),
                                partition(bucket),
                                bucket.getBucket(),
                                BinaryString.fromString(namespace),
                                BinaryString.fromString(name)));
    }

    /** Encodes a single ordinary KV upsert into the system table. */
    public KvRecordBatch record(
            Replica replica,
            TableBucket bucket,
            String namespace,
            String name,
            long offset,
            long expiresAt)
            throws IOException {
        CompactedRowEncoder encoder = rowEncoder.get();
        encoder.startNewRow();
        encoder.encodeField(0, bucket.getTableId());
        encoder.encodeField(1, partition(bucket));
        encoder.encodeField(2, bucket.getBucket());
        encoder.encodeField(3, BinaryString.fromString(namespace));
        encoder.encodeField(4, BinaryString.fromString(name));
        encoder.encodeField(5, offset);
        encoder.encodeField(6, expiresAt);
        try (KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        replica.getTableInfo().getSchemaId(),
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(4096),
                        KvFormat.COMPACTED)) {
            builder.append(key(bucket, namespace, name), encoder.finishRow());
            return DefaultKvRecordBatch.pointToBytesView(builder.build());
        }
    }

    /** Creates a value decoder for the current system table schema. */
    public ValueDecoder decoder(Replica replica) {
        return new ValueDecoder(replica.getSchemaGetter(), KvFormat.COMPACTED);
    }

    /** Returns the snapshot name in a stored row. */
    public static String name(InternalRow row) {
        return row.getString(4).toString();
    }

    /** Returns the registered offset in a stored row. */
    public static long offset(InternalRow row) {
        return row.getLong(5);
    }

    /** Returns the absolute expiry in a stored row. */
    public static long expiresAt(InternalRow row) {
        return row.getLong(6);
    }

    private static long partition(TableBucket bucket) {
        return bucket.getPartitionId() == null ? -1L : bucket.getPartitionId();
    }
}
