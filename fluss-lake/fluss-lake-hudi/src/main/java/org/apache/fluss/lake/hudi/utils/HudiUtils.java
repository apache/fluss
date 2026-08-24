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

package org.apache.fluss.lake.hudi.utils;

import org.apache.flink.table.types.logical.RowType;

import static org.apache.fluss.lake.hudi.HudiLakeCatalog.LEGACY_SYSTEM_COLUMNS;

/**
 * Utility methods for Hudi lake tables.
 *
 * <p>FIP-27: Newly created Hudi lake tables ("clean" tables) contain only user columns. Legacy
 * tables created before FIP-27 still carry the three trailing system columns (__bucket, __offset,
 * __timestamp). This class provides detection logic to distinguish between the two layouts.
 */
public final class HudiUtils {

    private HudiUtils() {}

    /**
     * Returns whether the given Hudi table is a legacy table, i.e. one that carries the three Fluss
     * system columns ({@code __bucket}, {@code __offset}, {@code __timestamp}).
     *
     * <p>Detection requires <b>all three</b> system columns to be present. This guards against
     * misdetecting a user table that merely reuses one of the system-column names (for example a
     * table with only a {@code __timestamp} column that is being onboarded to Fluss): such a table
     * has fewer than three system columns and is therefore treated as clean.
     *
     * <p>The {@link RowType} passed here is the physical Hudi table schema (excluding Hudi's own
     * {@code _hoodie_*} metadata columns, which are not part of the logical row type). A projected
     * schema must not be passed to this method.
     */
    public static boolean isLegacyTable(RowType rowType) {
        for (String systemColumn : LEGACY_SYSTEM_COLUMNS.keySet()) {
            if (!rowType.getFieldNames().contains(systemColumn)) {
                return false;
            }
        }
        return true;
    }
}
