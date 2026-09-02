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
import org.apache.fluss.config.ReadableConfig;

/** Utility methods for resolving external lake table metadata. */
@Internal
public final class LakeTableUtil {

    private LakeTableUtil() {}

    /** Resolves the table path used to access the external datalake table. */
    public static TablePath resolveLakeTablePath(
            TablePath flussTablePath, ReadableConfig tableConfig) {
        String lakeDatabaseName =
                tableConfig
                        .getOptional(ConfigOptions.TABLE_DATALAKE_DATABASE_NAME)
                        .orElse(flussTablePath.getDatabaseName());
        String lakeTableName =
                tableConfig
                        .getOptional(ConfigOptions.TABLE_DATALAKE_TABLE_NAME)
                        .orElse(flussTablePath.getTableName());
        return TablePath.of(lakeDatabaseName, lakeTableName);
    }

    /** Returns whether the table change affects the resolved lake table path. */
    public static boolean isLakeTablePathChange(TableChange tableChange) {
        String optionKey;
        if (tableChange instanceof TableChange.SetOption) {
            optionKey = ((TableChange.SetOption) tableChange).getKey();
        } else if (tableChange instanceof TableChange.ResetOption) {
            optionKey = ((TableChange.ResetOption) tableChange).getKey();
        } else {
            return false;
        }
        return ConfigOptions.TABLE_DATALAKE_DATABASE_NAME.key().equals(optionKey)
                || ConfigOptions.TABLE_DATALAKE_TABLE_NAME.key().equals(optionKey);
    }
}
