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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;

import javax.annotation.Nullable;

import java.util.Optional;

/** A merging interface defines how to merge a new row with existing row. */
public interface RowMerger {

    /**
     * Merge the old value with the new value.
     *
     * @param oldValue the old value
     * @param newValue the new row
     * @return the merged value, if the returned row is the same to the old row, then nothing
     *     happens to the row (no update, no delete).
     */
    BinaryValue merge(BinaryValue oldValue, BinaryValue newValue);

    /**
     * Merge the old row with a delete row.
     *
     * <p>This method will be invoked only when {@link #deleteBehavior()} returns {@link
     * DeleteBehavior#ALLOW}.
     *
     * @param oldRow the old row.
     * @return the merged row, or null if the row is deleted.
     */
    @Nullable
    BinaryValue delete(BinaryValue oldRow);

    /**
     * The behavior of delete operations on primary key tables.
     *
     * @return {@link DeleteBehavior}
     */
    DeleteBehavior deleteBehavior();

    /**
     * Dynamically configure the target columns to merge and return the effective merger.
     *
     * @param targetColumns the partial update target column positions, null means full update
     * @param schemaId the schema id used to generate new rows
     * @param schema the schema used to generate new rows
     */
    RowMerger configureTargetColumns(@Nullable int[] targetColumns, short schemaId, Schema schema);

    /** Create a row merger based on the given configuration. */
    static RowMerger create(TableConfig tableConf, KvFormat kvFormat) {
        Optional<MergeEngineType> mergeEngineType = tableConf.getMergeEngineType();
        @Nullable DeleteBehavior deleteBehavior = tableConf.getDeleteBehavior().orElse(null);

        if (mergeEngineType.isPresent()) {
            switch (mergeEngineType.get()) {
                case FIRST_ROW:
                    return new FirstRowRowMerger(deleteBehavior);
                case VERSIONED:
                    Optional<String> versionColumn = tableConf.getMergeEngineVersionColumn();
                    if (!versionColumn.isPresent()) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "'%s' must be set for versioned merge engine.",
                                        ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key()));
                    }
                    return new VersionedRowMerger(versionColumn.get(), deleteBehavior);
                default:
                    throw new IllegalArgumentException(
                            "Unsupported merge engine type: " + mergeEngineType.get());
            }
        } else {
            return new DefaultRowMerger(kvFormat, deleteBehavior);
        }
    }
}
