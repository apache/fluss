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

package org.apache.fluss.lake.paimon.utils;

import org.apache.paimon.types.RowType;

import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/** Common utilities for the Paimon lake integration. */
public final class PaimonUtils {

    private PaimonUtils() {}

    /**
     * Returns whether the physical Paimon table uses the legacy layout, i.e. it still carries the
     * three trailing Fluss system columns ({@code __bucket}, {@code __offset}, {@code
     * __timestamp}).
     *
     * <p>A legacy table is identified by the presence of the {@code __timestamp} system column;
     * under FIP-27 newly created tables are clean and carry none of these columns.
     */
    public static boolean isLegacyTable(RowType tableRowType) {
        return tableRowType.getFieldIndex(TIMESTAMP_COLUMN_NAME) >= 0;
    }
}
