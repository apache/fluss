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
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
     * system columns ({@code __bucket}, {@code __offset}, {@code __timestamp}) as its trailing
     * columns.
     *
     * <p>Detection requires <b>all three</b> system columns to be present, as the last columns, in
     * canonical order and with matching types. This is deliberately strict: a user table that
     * merely happens to contain a {@code __timestamp} (or one/two of the system names) must be
     * treated as a clean table and never be misinterpreted as legacy — otherwise onboarding such a
     * table to Fluss would corrupt it by enabling system-column enrichment.
     *
     * <ul>
     *   <li>none of the system columns present → clean (returns {@code false});
     *   <li>all three present as the trailing columns in canonical order and type → legacy;
     *   <li>a partial / out-of-order / type-incompatible match → rejected with {@link
     *       IllegalStateException}, since such a physical layout cannot be safely handled either
     *       way.
     * </ul>
     */
    public static boolean isLegacyTable(Schema icebergSchema) {
        Map<String, Type> systemColumns = IcebergSchemaUtils.LEGACY_SYSTEM_COLUMNS;
        List<Types.NestedField> fields = icebergSchema.columns();

        int presentCount = 0;
        List<String> presentNames = new ArrayList<>();
        for (Types.NestedField field : fields) {
            if (systemColumns.containsKey(field.name())) {
                presentCount++;
                presentNames.add(field.name());
            }
        }

        // No system columns → clean table.
        if (presentCount == 0) {
            return false;
        }

        // Some but not all system columns → this is not a Fluss-managed layout. Treat a table that
        // does not carry the complete set as clean rather than legacy, so a user table that happens
        // to reuse a single system-column name is not misdetected.
        if (presentCount < systemColumns.size()) {
            return false;
        }

        // All three names are present: they must be the trailing columns, in canonical order, with
        // matching types. Anything else is an ambiguous layout we cannot safely process.
        List<String> expectedOrder = new ArrayList<>(systemColumns.keySet());
        int systemStart = fields.size() - systemColumns.size();
        for (int i = 0; i < systemColumns.size(); i++) {
            Types.NestedField field = fields.get(systemStart + i);
            String expectedName = expectedOrder.get(i);
            Type expectedType = systemColumns.get(expectedName);
            if (!field.name().equals(expectedName) || !field.type().equals(expectedType)) {
                throw new IllegalStateException(
                        "The Iceberg table carries the reserved system column names "
                                + presentNames
                                + " but not as the trailing "
                                + expectedOrder
                                + " columns in canonical order and type. Such a layout is not a "
                                + "valid Fluss lake table and cannot be tiered.");
            }
        }
        return true;
    }
}
