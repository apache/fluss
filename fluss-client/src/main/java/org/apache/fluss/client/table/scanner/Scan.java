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

package org.apache.fluss.client.table.scanner;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Used to configure and create a scanner to scan data for a table.
 *
 * <p>{@link Scan} objects are immutable and can be shared between threads. Refinement methods, like
 * {@link #project} and {@link #limit(int)}, create new Scan instances.
 *
 * @since 0.6
 */
@PublicEvolving
public interface Scan {

    /**
     * Returns a new scan from this that will read the given data columns.
     *
     * @param projectedColumns the selected column indexes
     */
    Scan project(@Nullable int[] projectedColumns);

    /**
     * Returns a new scan from this that will read the given data columns.
     *
     * @param projectedColumnNames the selected column names
     */
    Scan project(List<String> projectedColumnNames);

    /**
     * Returns a new scan from this that will read the given limited row number.
     *
     * @param rowNumber the limited row number to read
     */
    Scan limit(int rowNumber);

    /**
     * Creates a {@link LogScanner} to continuously read log data for this scan.
     *
     * <p>Note: this API doesn't support pre-configured with {@link #limit(int)}.
     */
    LogScanner createLogScanner();

    /**
     * Creates a {@link BatchScanner} to read current data in the given table bucket for this scan.
     *
     * <p>Note: this API doesn't support pre-configured with {@link #project}.
     */
    BatchScanner createBatchScanner(TableBucket tableBucket);

    /**
     * Creates a {@link BatchScanner} to read given snapshot data in the given table bucket for this
     * scan.
     *
     * <p>Note: this API doesn't support pre-configured with {@link #project} and {@link
     * #limit(int)} and only support for Primary Key Tables.
     */
    BatchScanner createBatchScanner(TableBucket tableBucket, long snapshotId);
}
