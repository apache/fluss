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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.Internal;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Constants and utility methods for system tables and system views.
 *
 * <p>System tables are managed by the system, reflecting cluster internal state and information.
 * They all reside under the fixed {@code sys} database. Currently only system views are supported:
 *
 * <ul>
 *   <li><b>System views</b>: backed by in-memory or ZK metadata, no independent storage (e.g.,
 *       {@code sys.tablet_servers})
 * </ul>
 *
 * <p>Table names use plain identifiers without any prefix.
 */
@Internal
public final class SystemTableConstants {

    /** The name of the system database that contains all system tables and views. */
    public static final String SYSTEM_DATABASE = "sys";

    /** Name of the tablet servers system view. */
    public static final String TABLET_SERVERS_VIEW = "tablet_servers";

    /** Name of the table buckets system view. */
    public static final String TABLE_BUCKETS_VIEW = "table_buckets";

    /** All known system view names. */
    private static final Set<String> SYSTEM_VIEW_NAMES =
            Collections.unmodifiableSet(
                    new HashSet<>(Arrays.asList(TABLET_SERVERS_VIEW, TABLE_BUCKETS_VIEW)));

    // ---- table_kind values used in GetTableInfoResponse ----

    /** Table kind constant for regular user tables. */
    public static final int TABLE_KIND_TABLE = 0;

    /** Table kind constant for system views (virtual, no independent storage). */
    public static final int TABLE_KIND_SYSTEM_VIEW = 1;

    private SystemTableConstants() {}

    /** Returns {@code true} if the given database name is the system database. */
    public static boolean isSystemDatabase(String databaseName) {
        return SYSTEM_DATABASE.equals(databaseName);
    }

    /**
     * Returns {@code true} if the given table is a known system table or view in the system
     * database.
     *
     * <p>This checks both that the database is {@code sys} and that the table name is a recognized
     * system view.
     */
    public static boolean isSystemTableName(String databaseName, String tableName) {
        if (databaseName == null || tableName == null) {
            return false;
        }
        return isSystemView(databaseName, tableName);
    }

    /**
     * Returns {@code true} if the given table path is a known system table or view in the system
     * database.
     */
    public static boolean isSystemTablePath(TablePath tablePath) {
        if (tablePath == null) {
            return false;
        }
        return isSystemTableName(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    /**
     * Returns {@code true} if the given table in the given database is a known system view. System
     * views are virtual tables that do not have independent storage.
     */
    public static boolean isSystemView(String databaseName, String tableName) {
        if (databaseName == null || tableName == null) {
            return false;
        }
        return isSystemDatabase(databaseName) && SYSTEM_VIEW_NAMES.contains(tableName);
    }
}
