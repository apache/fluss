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

package org.apache.fluss.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/** Utility constants and helpers shared by the Global Secondary Index Table layer. */
public final class IndexTableUtils {

    /**
     * Single system column appended to partitioned Index Tables. Carries the source main table's
     * {@code partitionId} so that partition tombstone cleanup can filter dead rows without
     * consulting external state.
     */
    public static final String PARTITION_ID_SYSTEM_COLUMN = "__partition_id";

    /** Distinguishes ordinary index entries from source-progress entries. */
    public static final String RECORD_KIND_SYSTEM_COLUMN = "__idx_record_kind";

    /** Routes records to an Index Table bucket and forms the lookup prefix for data rows. */
    public static final String ROUTING_KEY_SYSTEM_COLUMN = "__idx_routing_key";

    /** Uniquely identifies a record within one routing-key prefix. */
    public static final String ROW_KEY_SYSTEM_COLUMN = "__idx_row_key";

    /** Source WAL end offset, populated only for progress records. */
    public static final String SOURCE_PROGRESS_SYSTEM_COLUMN = "__idx_source_progress";

    public static final byte DATA_RECORD_KIND = 0;

    public static final byte PROGRESS_RECORD_KIND = 1;

    /**
     * Reserved system column names that user-defined schemas must not declare on a main table
     * (otherwise Index Table derivation would collide). Index Tables themselves may add these
     * columns at derive time; user-facing schemas must keep their column namespace clean.
     */
    public static final Set<String> RESERVED_INDEX_SYSTEM_COLUMNS =
            Collections.unmodifiableSet(
                    new LinkedHashSet<>(
                            Arrays.asList(
                                    PARTITION_ID_SYSTEM_COLUMN,
                                    RECORD_KIND_SYSTEM_COLUMN,
                                    ROUTING_KEY_SYSTEM_COLUMN,
                                    ROW_KEY_SYSTEM_COLUMN,
                                    SOURCE_PROGRESS_SYSTEM_COLUMN)));

    /** Reserved, operationally distinct prefix for internal Index Tables. */
    public static final String INDEX_TABLE_NAME_PREFIX = "__idx__";

    /** Separator between the owning main-table name and index name. */
    public static final String INDEX_TABLE_NAME_SEPARATOR = "__";

    private IndexTableUtils() {}

    /** Builds the internal Index Table name from its owning main-table name and index name. */
    public static String indexTableName(String mainTableName, String indexName) {
        return INDEX_TABLE_NAME_PREFIX + mainTableName + INDEX_TABLE_NAME_SEPARATOR + indexName;
    }

    /** Returns whether a table name is reserved for a system-managed Index Table. */
    public static boolean isIndexTableName(String tableName) {
        return tableName.startsWith(INDEX_TABLE_NAME_PREFIX);
    }
}
