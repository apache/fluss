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

package org.apache.fluss.server.kv.rowmerger;

import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.TimestampType;

import javax.annotation.Nullable;

import java.util.Comparator;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * A merger that merges multiple rows and keeps the row with the largest version.
 *
 * @see MergeEngineType#VERSIONED
 */
public class VersionedRowMerger implements RowMerger {

    private static final TimestampNtz MIN_TIMESTAMP_NTZ =
            TimestampNtz.fromMillis(Long.MIN_VALUE, 0);
    private static final TimestampLtz MIN_TIMESTAMP_LTZ =
            TimestampLtz.fromEpochMillis(Long.MIN_VALUE, 0);

    private final Comparator<BinaryRow> versionComparator;

    private final DeleteBehavior deleteBehavior;

    public VersionedRowMerger(
            RowType schema, String versionColumnName, @Nullable DeleteBehavior deleteBehavior) {
        this.versionComparator = createVersionComparator(schema, versionColumnName);
        if (deleteBehavior == DeleteBehavior.ALLOW) {
            throw new IllegalArgumentException(
                    "DELETE is not supported for the versioned merge engine.");
        }
        // for compatibility, default to IGNORE if not specified
        this.deleteBehavior = deleteBehavior != null ? deleteBehavior : DeleteBehavior.IGNORE;
    }

    @Nullable
    @Override
    public BinaryRow merge(BinaryRow oldRow, BinaryRow newRow) {
        // return newRow if newRow's version is larger or equal than oldRow's version
        return versionComparator.compare(oldRow, newRow) <= 0 ? newRow : oldRow;
    }

    @Nullable
    @Override
    public BinaryRow delete(BinaryRow oldRow) {
        throw new UnsupportedOperationException(
                "DELETE is not supported for the versioned merge engine.");
    }

    @Override
    public DeleteBehavior deleteBehavior() {
        return deleteBehavior;
    }

    @Override
    public RowMerger configureTargetColumns(@Nullable int[] targetColumns) {
        if (targetColumns == null) {
            return this;
        } else {
            throw new UnsupportedOperationException(
                    "Partial update is not supported for the versioned merge engine.");
        }
    }

    /** Create a comparator for version column. */
    public static Comparator<BinaryRow> createVersionComparator(
            RowType schema, String versionColumnName) {
        int columnIndex = schema.getFieldIndex(versionColumnName);
        checkArgument(
                columnIndex >= 0,
                "The version column '%s' for versioned merge engine doesn't exist in schema.",
                versionColumnName);
        DataType columnType = schema.getTypeAt(columnIndex);
        int precision;
        switch (columnType.getTypeRoot()) {
            case BIGINT:
                return Comparator.comparing(
                        row ->
                                row.isNullAt(columnIndex)
                                        ? Long.MIN_VALUE
                                        : row.getLong(columnIndex));
            case INTEGER:
                return Comparator.comparing(
                        row ->
                                row.isNullAt(columnIndex)
                                        ? Integer.MIN_VALUE
                                        : row.getInt(columnIndex));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                precision = ((TimestampType) columnType).getPrecision();
                return Comparator.comparing(
                        row ->
                                row.isNullAt(columnIndex)
                                        ? MIN_TIMESTAMP_NTZ
                                        : row.getTimestampNtz(columnIndex, precision));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                precision = ((LocalZonedTimestampType) columnType).getPrecision();
                return Comparator.comparing(
                        row ->
                                row.isNullAt(columnIndex)
                                        ? MIN_TIMESTAMP_LTZ
                                        : row.getTimestampLtz(columnIndex, precision));
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "The version column '%s' for versioned merge engine must be one type of "
                                        + "[INT, BIGINT, TIMESTAMP, TIMESTAMP_LTZ], but is %s.",
                                versionColumnName, columnType));
        }
    }
}
