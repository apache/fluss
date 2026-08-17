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

import org.apache.iceberg.Schema;

import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

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
     * Returns whether the given Iceberg table is a legacy table (has the three trailing system
     * columns).
     *
     * <p>Detection: if the {@code __timestamp} system column exists in the physical schema, this is
     * a legacy table. Clean tables have no system columns.
     */
    public static boolean isLegacyTable(Schema icebergSchema) {
        return icebergSchema.findField(TIMESTAMP_COLUMN_NAME) != null;
    }
}
