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

package org.apache.fluss.lake.iceberg.version;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;

/** Manages Iceberg format version detection and V3 feature gating. */
public final class FormatVersionManager {

    public static final int DEFAULT_FORMAT_VERSION = 2;
    public static final int V3_FORMAT_VERSION = 3;
    public static final int MIN_VERSION_FOR_DELETION_VECTORS = 3;

    private FormatVersionManager() {}

    /** Detects format version from table metadata (not properties, as it's reserved). */
    public static int detectFormatVersion(Table table) {
        if (table instanceof HasTableOperations) {
            return ((HasTableOperations) table).operations().current().formatVersion();
        }
        return DEFAULT_FORMAT_VERSION;
    }

    /** Checks if the table supports deletion vectors (V3+). */
    public static boolean supportsDeletionVectors(Table table) {
        return detectFormatVersion(table) >= MIN_VERSION_FOR_DELETION_VECTORS;
    }

    /** Checks if the given format version is V3 or higher. */
    public static boolean isV3OrHigher(int formatVersion) {
        return formatVersion >= V3_FORMAT_VERSION;
    }
}
