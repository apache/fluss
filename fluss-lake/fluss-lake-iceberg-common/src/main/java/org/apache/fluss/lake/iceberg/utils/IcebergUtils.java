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

package org.apache.fluss.lake.iceberg.utils;

import org.apache.fluss.lake.iceberg.IcebergSchemaUtils;

import org.apache.iceberg.Schema;

/**
 * Utility methods for Iceberg lake tables.
 *
 * <p>FIP-27: Newly created Iceberg lake tables ("clean" tables) contain only user columns. Legacy
 * tables created before FIP-27 still carry the three trailing system columns (__bucket, __offset,
 * __timestamp). This class provides detection logic to distinguish between the two layouts.
 */
public final class IcebergUtils {

    private IcebergUtils() {}

    /**
     * Returns whether the given Iceberg table is a legacy table, i.e. one that carries the three
     * system columns ({@code __bucket}, {@code __offset}, {@code __timestamp}).
     *
     * <p>Detection requires <b>all three</b> system columns to be present. This guards against
     * misdetecting a user table that merely reuses one of the system-column names (e.g. a table
     * with only a {@code __timestamp} column that is being onboarded to Fluss): such a table has
     * fewer than three system columns and is therefore treated as clean.
     */
    public static boolean isLegacyTable(Schema icebergSchema) {
        for (String systemColumn : IcebergSchemaUtils.LEGACY_SYSTEM_COLUMNS.keySet()) {
            if (icebergSchema.findField(systemColumn) == null) {
                return false;
            }
        }
        return true;
    }
}
