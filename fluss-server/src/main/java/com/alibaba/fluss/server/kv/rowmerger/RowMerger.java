/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.server.kv.rowmerger;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.TableConfig;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.MergeEngineType;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.row.BinaryRow;

import javax.annotation.Nullable;

import java.util.Optional;

/** A merging interface defines how to merge a new row with existing row. */
public interface RowMerger {

    /**
     * Merge the old row with the new row.
     *
     * @param oldRow the old row
     * @param newRow the new row
     * @return the merged row, if the returned row is the same to the old row, then nothing happens
     *     to the row (no update, no delete).
     */
    BinaryRow merge(BinaryRow oldRow, BinaryRow newRow);

    /**
     * Merge the old row with a delete row.
     *
     * <p>This method will be invoked only when {@link #supportsDelete()} returns true.
     *
     * @param oldRow the old row.
     * @return the merged row, or null if the row is deleted.
     */
    @Nullable
    BinaryRow delete(BinaryRow oldRow);

    /**
     * Whether the merger supports to merge delete rows.
     *
     * @return true if the merger supports delete operation.
     */
    boolean supportsDelete();

    /** Dynamically configure the target columns to merge and return the effective merger. */
    RowMerger configureTargetColumns(@Nullable int[] targetColumns);

    /** Create a row merger based on the given configuration. */
    static RowMerger create(TableConfig tableConf, Schema schema, KvFormat kvFormat) {
        Optional<MergeEngineType> mergeEngineType = tableConf.getMergeEngineType();
        if (mergeEngineType.isPresent()) {
            switch (mergeEngineType.get()) {
                case FIRST_ROW:
                    return new FirstRowRowMerger();
                case VERSIONED:
                    Optional<String> versionColumn = tableConf.getMergeEngineVersionColumn();
                    if (!versionColumn.isPresent()) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "'%s' must be set for versioned merge engine.",
                                        ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key()));
                    }
                    return new VersionedRowMerger(schema.getRowType(), versionColumn.get());
                default:
                    throw new IllegalArgumentException(
                            "Unsupported merge engine type: " + mergeEngineType.get());
            }
        } else {
            return new DefaultRowMerger(schema, kvFormat);
        }
    }
}
