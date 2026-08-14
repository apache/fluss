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

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.exception.InvalidTableException;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/**
 * Utilities describing the two physical layouts a Paimon lake table can have under FIP-27, and the
 * single place that detects which layout a given Paimon table uses.
 *
 * <ul>
 *   <li>{@link LakeLayout#CLEAN} - the table only contains user-defined columns. This is the layout
 *       of every newly created lake table.
 *   <li>{@link LakeLayout#LEGACY} - the table was created before FIP-27 and carries the three
 *       mandatory Fluss system columns {@code __bucket}, {@code __offset}, {@code __timestamp} as
 *       its last three physical columns.
 * </ul>
 *
 * <p>Detection is based purely on the physical Paimon schema, so no extra metadata or table
 * property is needed and existing tables are never migrated. A table that carries only some of the
 * system columns, or carries them with an unexpected type, is neither a clean nor a valid legacy
 * table and is rejected with a clear error.
 */
public class PaimonSystemColumns {

    /**
     * The three mandatory Fluss system columns and their expected Paimon types, in physical order.
     * The {@code __timestamp} type is compared with relaxed precision (see {@link
     * #isSystemTimestampType}) to stay compatible with legacy tables written by older clusters.
     */
    public static final LinkedHashMap<String, DataType> SYSTEM_COLUMNS = new LinkedHashMap<>();

    static {
        // We need __bucket system column to filter out the given bucket
        // for paimon bucket-unaware append only table.
        // It's not required for paimon bucket-aware table like primary key table
        // and bucket-aware append only table, but legacy tables always carry the system column
        // for consistent behavior.
        SYSTEM_COLUMNS.put(BUCKET_COLUMN_NAME, DataTypes.INT());
        SYSTEM_COLUMNS.put(OFFSET_COLUMN_NAME, DataTypes.BIGINT());
        SYSTEM_COLUMNS.put(TIMESTAMP_COLUMN_NAME, DataTypes.TIMESTAMP_LTZ_MILLIS());
    }

    /** The physical layout of a Paimon lake table with respect to Fluss system columns. */
    public enum LakeLayout {
        /** Only user-defined columns are present (FIP-27 default for new tables). */
        CLEAN,
        /** The three Fluss system columns are appended as the last physical columns. */
        LEGACY
    }

    private PaimonSystemColumns() {}

    /** Returns the number of system columns carried by a {@link LakeLayout#LEGACY} table. */
    public static int systemColumnCount() {
        return SYSTEM_COLUMNS.size();
    }

    public static boolean isSystemColumn(String columnName) {
        return SYSTEM_COLUMNS.containsKey(columnName);
    }

    /**
     * Detects whether a Paimon table with the given physical row type uses the clean or the legacy
     * layout.
     *
     * <p>The detection tolerates the {@code __timestamp} precision difference between old
     * (precision 6) and new (precision 3) clusters, mirroring {@link
     * PaimonTableValidation#equalIgnoreSystemColumnTimestampPrecision}.
     *
     * @throws InvalidTableException if the table carries only some of the system columns, carries
     *     them out of order, with an incompatible type, or embeds a system column name among the
     *     business columns. Such a table is neither clean nor a valid legacy table.
     */
    public static LakeLayout detectLayout(RowType paimonRowType) {
        List<DataField> fields = paimonRowType.getFields();

        int firstSystemColumnPos = -1;
        for (int i = 0; i < fields.size(); i++) {
            if (SYSTEM_COLUMNS.containsKey(fields.get(i).name())) {
                firstSystemColumnPos = i;
                break;
            }
        }

        // No system column anywhere -> clean layout.
        if (firstSystemColumnPos < 0) {
            return LakeLayout.CLEAN;
        }

        // A system column exists. For a valid legacy table, all three must appear, in the canonical
        // order, as the very last physical columns, each with a compatible type.
        int businessFieldCount = fields.size() - SYSTEM_COLUMNS.size();
        if (firstSystemColumnPos != businessFieldCount) {
            throw partialLayoutException(paimonRowType);
        }

        int pos = businessFieldCount;
        for (Map.Entry<String, DataType> systemColumn : SYSTEM_COLUMNS.entrySet()) {
            DataField field = fields.get(pos);
            if (!field.name().equals(systemColumn.getKey())
                    || !isSystemColumnTypeCompatible(field.name(), field.type())) {
                throw partialLayoutException(paimonRowType);
            }
            pos++;
        }

        return LakeLayout.LEGACY;
    }

    private static boolean isSystemColumnTypeCompatible(String name, DataType actualType) {
        if (TIMESTAMP_COLUMN_NAME.equals(name)) {
            // Old clusters wrote precision 6, new clusters write precision 3; both are accepted.
            return isSystemTimestampType(actualType);
        }
        // Compare the type family and precision, ignoring nullability: legacy system columns were
        // written as non-null, but we only care that the physical type matches.
        DataType expected = SYSTEM_COLUMNS.get(name);
        return actualType.copy(true).equalsIgnoreFieldId(expected.copy(true));
    }

    private static boolean isSystemTimestampType(DataType actualType) {
        // Legacy tables carry __timestamp with varying timestamp types depending on the cluster
        // that created them: with or without local time zone, and precision 3 (new clusters) or 6
        // (old clusters). Accept the whole timestamp family and let the reader handle the
        // precision, mirroring the relaxed check in
        // PaimonTableValidation#equalIgnoreSystemColumnTimestampPrecision.
        switch (actualType.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return true;
            default:
                return false;
        }
    }

    private static InvalidTableException partialLayoutException(RowType paimonRowType) {
        return new InvalidTableException(
                String.format(
                        "The Paimon table has an incompatible system-column layout. A table must "
                                + "either contain none of the Fluss system columns (clean layout) or "
                                + "contain all of %s as its last columns, in this order, with "
                                + "compatible types (legacy layout). Actual schema: %s.",
                        SYSTEM_COLUMNS.keySet(), paimonRowType));
    }
}
