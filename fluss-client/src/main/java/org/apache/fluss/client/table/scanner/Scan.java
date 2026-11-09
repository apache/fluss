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
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
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
     * Returns a new scan with the given equality conditions for an index range scan. Values follow
     * the order of the column names. Repeated calls replace the entire equality condition set. The
     * column names and values are copied before this method returns.
     *
     * <p>Currently, batch range scans require both this configuration and {@link #range}, an
     * order-preserving key format and a routable primary-key index path.
     */
    default Scan equalTo(List<String> columnNames, InternalRow values) {
        throw new UnsupportedOperationException("Equality range configuration is not supported.");
    }

    /**
     * Returns a new scan with a range on the given table column. Repeated calls replace the range
     * column and bounds. Endpoint values are copied before this method returns.
     *
     * <p>Currently, excluding partition columns, equality conditions must identify a physical
     * primary-key prefix containing all bucket fields. All partition fields require equality
     * values, and the range column must immediately follow the physical prefix. Batch range scans
     * require {@code table.kv.ordered-key.enabled=true}. Log and snapshot scanners do not support
     * index range conditions.
     *
     * <p>Each finite endpoint must contain exactly one non-null field of the range column's
     * internal type. Reversed bounds are invalid. The scanner holds a fixed KV view across batches.
     * Results have no ordering guarantee, and a configured limit only caps the returned row count.
     */
    default Scan range(String columnName, Range<? extends InternalRow> bounds) {
        throw new UnsupportedOperationException("Range configuration is not supported.");
    }

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
     * <p>For Primary Key Tables, this reads the RocksDB-backed KV data in the bucket, applying any
     * configured index range conditions, and does not require {@link #limit(int)}. For Log Tables,
     * {@link #limit(int)} must be set.
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
     * partitions for partitioned tables. Configured index range conditions prune the buckets
     * accessed and constrain the returned rows. For Log Tables, {@link #limit(int)} must be set.
     */
    BatchScanner createBatchScanner() throws IOException;
}
