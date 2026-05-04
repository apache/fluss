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

package org.apache.fluss.record;

import org.apache.fluss.row.InternalRow;

/** Statistics information of {@link LogRecordBatch LogRecordBatch}. */
public interface LogRecordBatchStatistics {

    /** Sentinel value indicating that null count statistics are not available for a field. */
    int NULL_COUNT_UNAVAILABLE = -1;

    /**
     * Get the minimum values as an InternalRow.
     *
     * @return The minimum values
     */
    InternalRow getMinValues();

    /**
     * Get the maximum values as an InternalRow.
     *
     * @return The maximum values
     */
    InternalRow getMaxValues();

    /**
     * Get the null counts for each field. Uses {@code -1} to indicate that null count statistics
     * are not available for a particular field.
     *
     * @return Array of null counts, where -1 means not available
     */
    int[] getNullCounts();

    /**
     * Whether the statistics information for a specific field is available.
     *
     * @return true if statistics information is available
     */
    boolean hasColumnStatistics(int fieldIndex);

    /**
     * Get the schema id.
     *
     * @return The schema id
     */
    int getSchemaId();
}
