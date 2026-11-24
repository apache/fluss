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

import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.metadata.TablePath;

import static org.apache.fluss.config.ConfigBuilder.key;

/** Utility constants and helpers shared by the Global Secondary Index Table layer. */
public final class IndexTableUtils {

    /** Persisted ownership link of an internal Index Table. */
    public static final ConfigOption<Long> TABLE_INDEX_META_MAIN_TABLE_ID =
            key("table.index-meta.main-table-id")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "The main table id associated with an internal Index Table. "
                                    + "This option is managed by the system.");

    /**
     * Single system column appended to partitioned Index Tables. Carries the source main table's
     * {@code partitionId} so that filtering and compaction can identify deleted partitions using
     * the cached partition tombstone.
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

    /** Reserved, operationally distinct prefix for internal Index Tables. */
    private static final String INDEX_TABLE_NAME_PREFIX = "__idx__";

    private IndexTableUtils() {}

    /** Returns the internal Index Table path in the owning table's database. */
    public static TablePath indexTablePath(TablePath mainTable, String indexName) {
        return TablePath.of(
                mainTable.getDatabaseName(),
                INDEX_TABLE_NAME_PREFIX + mainTable.getTableName() + "__" + indexName);
    }
}
