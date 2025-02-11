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

package com.alibaba.fluss.metadata;

/**
 * The merge engine for the primary key table.
 *
 * <p>A primary key table with a merge engine is a special kind of table, called "merge table".
 * Fluss provides 3 kinds of table: "primary key table", "log table", and "merge table". Merge table
 * is a primary key table that has a primary key definition but doesn't directly UPDATE and DELETE
 * rows in the table, and instead, it merges the append rows into a new data set according to the
 * defined {@link MergeEngineType}. Therefore, it doesn't support direct UPDATE (also
 * partial-update) and DELETE operations and only supports INSERT or APPEND operations.
 *
 * <p>Note: A primary key table doesn't have a merge engine by default.
 *
 * @since 0.6
 */
public enum MergeEngineType {

    /**
     * A merge engine that only keeps the first appeared row when merging multiple rows on the same
     * primary key.
     */
    FIRST_ROW,

    /**
     * A merge engine that keeps the row with the largest version when merging multiple rows on the
     * same primary key. It requires users to specify a version column (e.g., an event timestamp).
     * When inserting a row, it will compare the version column value with the existing row with the
     * same primary key.
     *
     * <ul>
     *   <li>If the new version is larger to or the same with the old version , then it will replace
     *       the existing row with the new row.
     *   <li>If the new version is smaller to the old version, then it will ignore the new row.
     *   <li>Null version value is treated as the smallest version (i.e., Long.MIN_VALUE)
     * </ul>
     */
    VERSIONED;

    // introduce AGGREGATE merge engine in the future

    /** Creates a {@link MergeEngineType} from the given string. */
    public static MergeEngineType fromString(String type) {
        switch (type.toUpperCase()) {
            case "FIRST_ROW":
                return FIRST_ROW;
            case "VERSIONED":
                return VERSIONED;
            default:
                throw new IllegalArgumentException("Unsupported merge engine type: " + type);
        }
    }
}
