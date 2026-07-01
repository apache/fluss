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
import org.apache.fluss.client.table.scanner.log.TypedLogScanner;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.predicate.Predicate;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
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
     * Returns a new scan from this that will read the given data columns. Supports Variant
     * sub-field projection using the syntax {@code "variant_col:sub_field"}, for example:
     *
     * <pre>{@code
     * // Project column "id" and sub-fields "name", "age" from Variant column "data"
     * scan.project("id", "data:name", "data:age");
     * }</pre>
     *
     * <p>When a Variant column is referenced with sub-field syntax, the physical Variant column is
     * automatically included in the server-side projection, but the rows returned by {@link
     * LogScanner} are flattened in the user-supplied order. Untyped sub-fields are exposed as raw
     * {@code VARIANT} values; typed sub-fields can use {@code "column:subfield::TYPE"} and are
     * exposed as scalar columns.
     *
     * <p>The first implementation supports only top-level Variant sub-fields on ARROW log scanners.
     * Batch / KV scan paths reject sub-field projections until they have a dedicated flattening
     * path. Server-side pruning removes non-requested {@code typed_value} children for shredded
     * fields, while residual {@code metadata/value} remains available for fallback correctness.
     *
     * @param projectedColumnNames the selected column names, optionally with variant sub-field
     *     syntax {@code "column:subfield"}
     */
    Scan project(List<String> projectedColumnNames);

    /**
     * Returns a new scan from this that will read the given data columns by name. This is a
     * convenience overload of {@link #project(List)}.
     *
     * @param projectedColumnNames the selected column names, optionally with variant sub-field
     *     syntax {@code "column:subfield"}
     */
    default Scan project(String... projectedColumnNames) {
        return project(Arrays.asList(projectedColumnNames));
    }

    /**
     * Returns a new scan from this that will read the given limited row number.
     *
     * @param rowNumber the limited row number to read
     */
    Scan limit(int rowNumber);

    /**
     * Returns a new scan from this that will apply the given predicate filter.
     *
     * <p>Note: the filter currently only supports record batch level filtering for log scanners,
     * not row level filtering. The computing engine still needs to perform secondary filtering on
     * the results. Batch scanners do not support filter pushdown.
     *
     * @param predicate the predicate to apply for record batch level filtering
     */
    Scan filter(@Nullable Predicate predicate);

    /**
     * Creates a {@link LogScanner} to continuously read log data for this scan.
     *
     * <p>Note: this API doesn't support pre-configured with {@link #limit(int)}.
     */
    LogScanner createLogScanner();

    /**
     * Creates a {@link TypedLogScanner} to continuously read log data as POJOs of the given class.
     *
     * <p>Note: this API doesn't support pre-configured with {@link #limit(int)}.
     */
    <T> TypedLogScanner<T> createTypedLogScanner(Class<T> pojoClass);

    /**
     * Creates a {@link BatchScanner} to read current data in the given table bucket.
     *
     * <p>For Primary Key Tables, this performs a full RocksDB-backed KV scan of the bucket and does
     * not require {@link #limit(int)}. For Log Tables, {@link #limit(int)} must be set.
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

    /**
     * Creates a {@link BatchScanner} that scans across all buckets of the table, expanding all
     * partitions for partitioned tables. For Log Tables, {@link #limit(int)} must be set.
     */
    BatchScanner createBatchScanner() throws IOException;
}
