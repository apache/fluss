/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.table;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Encodes bucket keys and calculates bucket IDs according to Fluss's table routing configuration.
 *
 * <p>The input row layout must match the layout selected by the factory method. Full table rows and
 * projected bucket-key rows use different encoders.
 */
@Internal
@NotThreadSafe
public final class BucketCalculator {

    private final KeyEncoder bucketKeyEncoder;
    private final @Nullable KeyEncoder primaryKeyEncoder;
    private final BucketingFunction bucketingFunction;

    private BucketCalculator(
            KeyEncoder bucketKeyEncoder,
            @Nullable KeyEncoder primaryKeyEncoder,
            BucketingFunction bucketingFunction) {
        this.bucketKeyEncoder = checkNotNull(bucketKeyEncoder, "bucketKeyEncoder is null");
        this.primaryKeyEncoder = primaryKeyEncoder;
        this.bucketingFunction = checkNotNull(bucketingFunction, "bucketingFunction is null");
    }

    /**
     * Creates a calculator for a full log-table row.
     *
     * <p>The table must have an explicit bucket key. Log tables without bucket keys use the
     * existing non-hash bucket assigner.
     */
    public static BucketCalculator forAppendRow(TableInfo tableInfo) {
        checkNotNull(tableInfo, "tableInfo is null");
        checkArgument(
                !tableInfo.hasPrimaryKey(), "Expected a log table: %s", tableInfo.getTablePath());

        return create(tableInfo, tableInfo.getRowType(), null);
    }

    /**
     * Creates a calculator for a full primary-key-table row.
     *
     * <p>The supplied primary key encoder must belong to the same table, use its full row layout,
     * and have been created using the table's primary-key encoding configuration.
     */
    public static BucketCalculator forPrimaryKeyRow(
            TableInfo tableInfo, KeyEncoder primaryKeyEncoder) {
        checkNotNull(tableInfo, "tableInfo is null");
        checkArgument(
                tableInfo.hasPrimaryKey(),
                "Expected a primary key table: %s",
                tableInfo.getTablePath());

        return create(
                tableInfo,
                tableInfo.getRowType(),
                checkNotNull(primaryKeyEncoder, "primaryKeyEncoder is null"));
    }

    /**
     * Creates a calculator for a projected bucket-key row.
     *
     * <p>The input row must contain exactly the fields from TableInfo.getBucketKeys(), in the same
     * order.
     *
     * <p>This is intended for readers and query connectors that already have the bucket-key values
     * but not a full table row.
     */
    public static BucketCalculator forBucketKeyRow(TableInfo tableInfo) {
        checkNotNull(tableInfo, "tableInfo is null");
        RowType rowType = tableInfo.getRowType().project(tableInfo.getBucketKeys());
        KeyEncoder primaryKeyEncoder = null;
        if (tableInfo.hasPrimaryKey() && tableInfo.isDefaultBucketKey()) {
            primaryKeyEncoder =
                    KeyEncoder.ofPrimaryKeyEncoder(
                            rowType,
                            tableInfo.getPhysicalPrimaryKeys(),
                            tableInfo.getTableConfig(),
                            true);
        }
        return create(tableInfo, rowType, primaryKeyEncoder);
    }

    private static BucketCalculator create(
            TableInfo tableInfo, RowType rowType, @Nullable KeyEncoder primaryKeyEncoder) {
        checkArgument(
                tableInfo.hasBucketKey(), "Table %s has no bucket key", tableInfo.getTablePath());
        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);
        KeyEncoder bucketKeyEncoder;
        if (tableInfo.hasPrimaryKey() && tableInfo.isDefaultBucketKey()) {
            bucketKeyEncoder =
                    KeyEncoder.ofBucketKeyEncoder(
                            rowType,
                            tableInfo.getBucketKeys(),
                            tableInfo.getTableConfig(),
                            true,
                            checkNotNull(
                                    primaryKeyEncoder,
                                    "Primary key encoder is required "
                                            + "for the default bucket key"));
        } else {
            bucketKeyEncoder =
                    KeyEncoder.ofBucketKeyEncoder(rowType, tableInfo.getBucketKeys(), lakeFormat);
        }

        return new BucketCalculator(
                bucketKeyEncoder, primaryKeyEncoder, BucketingFunction.of(lakeFormat));
    }

    /** Encodes the bucket key from the configured row layout. */
    public byte[] encodeKey(InternalRow row) {
        return bucketKeyEncoder.encodeKey(checkNotNull(row, "row is null"));
    }

    /**
     * Encodes the bucket key, reusing primary key bytes when both keys use the same encoder.
     *
     * <p>The encodedPrimaryKey must have been produced for the same row by the primary key encoder
     * supplied to forPrimaryKeyRow().
     *
     * <p>The returned array may be the same instance as encodedPrimaryKey. Callers must not modify
     * these bytes while they are used by the write operation.
     */
    public byte[] encodeKey(InternalRow row, byte[] encodedPrimaryKey) {
        checkNotNull(row, "row is null");
        checkNotNull(encodedPrimaryKey, "encodedPrimaryKey is null");
        if (bucketKeyEncoder == primaryKeyEncoder) {
            return encodedPrimaryKey;
        }
        return encodeKey(row);
    }

    /**
     * Calculates a bucket ID from encoded bucket-key bytes.
     *
     * <p>The bucket count must be the physical bucket count of the target partition, not
     * necessarily the table's current default bucket count.
     */
    public int getBucketId(byte[] encodedBucketKey, int bucketCount) {
        checkNotNull(encodedBucketKey, "encodedBucketKey is null");
        checkArgument(bucketCount > 0, "Invalid bucket count: %s", bucketCount);
        int bucketId = bucketingFunction.bucketing(encodedBucketKey, bucketCount);
        checkArgument(
                bucketId >= 0 && bucketId < bucketCount,
                "Invalid bucket ID %s for bucket count %s",
                bucketId,
                bucketCount);
        return bucketId;
    }
}
