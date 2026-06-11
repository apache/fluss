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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import java.util.Map;
import java.util.Optional;

/** Utility methods for resolving external lake table metadata. */
@Internal
public final class LakeTableUtil {

    private LakeTableUtil() {}

    /** Returns the table path used to access the external datalake table. */
    public static TablePath getLakeTablePath(
            TablePath flussTablePath, Map<String, String> tableProperties) {
        return getLakeTablePath(flussTablePath, Configuration.fromMap(tableProperties));
    }

    /** Returns the table path used to access the external datalake table. */
    public static TablePath getLakeTablePath(TablePath flussTablePath, Configuration tableConf) {
        String lakeDatabaseName =
                getDataLakeDatabaseName(tableConf).orElse(flussTablePath.getDatabaseName());
        String lakeTableName =
                getDataLakeTableName(tableConf).orElse(flussTablePath.getTableName());
        return TablePath.of(lakeDatabaseName, lakeTableName);
    }

    /** Returns whether the table has explicit custom datalake path options. */
    public static boolean hasCustomLakePath(Map<String, String> tableProperties) {
        return hasCustomLakePath(Configuration.fromMap(tableProperties));
    }

    /** Returns whether the table has explicit custom datalake path options. */
    public static boolean hasCustomLakePath(Configuration tableConf) {
        return getDataLakeDatabaseName(tableConf).isPresent()
                || getDataLakeTableName(tableConf).isPresent();
    }

    /** Returns the lake table name with the metadata table suffix from the requested table name. */
    public static String getLakeTableName(String lakeTableName, String requestedTableName) {
        if (lakeTableName == null) {
            return requestedTableName;
        }

        int metadataTableIndex = requestedTableName.indexOf('$');
        if (metadataTableIndex < 0) {
            return lakeTableName;
        }

        String metadataTableSuffix = requestedTableName.substring(metadataTableIndex);
        if (lakeTableName.endsWith(metadataTableSuffix)) {
            return lakeTableName;
        }
        return lakeTableName + metadataTableSuffix;
    }

    /**
     * Returns the lake table name with the metadata table suffix from a table name containing the
     * given lake table splitter.
     */
    public static String getLakeTableName(
            String lakeTableName, String requestedTableName, String lakeTableSplitter) {
        int splitterIndex = requestedTableName.indexOf(lakeTableSplitter);
        if (splitterIndex < 0) {
            return requestedTableName;
        }

        String requestedLakeTableName =
                requestedTableName.substring(0, splitterIndex)
                        + requestedTableName.substring(splitterIndex + lakeTableSplitter.length());
        return getLakeTableName(lakeTableName, requestedLakeTableName);
    }

    private static Optional<String> getDataLakeDatabaseName(Configuration tableConf) {
        return tableConf.getOptional(ConfigOptions.TABLE_DATALAKE_DATABASE_NAME);
    }

    private static Optional<String> getDataLakeTableName(Configuration tableConf) {
        return tableConf.getOptional(ConfigOptions.TABLE_DATALAKE_TABLE_NAME);
    }
}
