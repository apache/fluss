/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.table;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.lookup.LookupResult;
import com.alibaba.fluss.client.lookup.PrefixLookupResult;
import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.client.scanner.log.LogScan;
import com.alibaba.fluss.client.scanner.log.LogScanner;
import com.alibaba.fluss.client.scanner.snapshot.SnapshotScan;
import com.alibaba.fluss.client.scanner.snapshot.SnapshotScanner;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWrite;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.row.InternalRow;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Used to communicate with a single Fluss table. Obtain an instance from a {@link Connection}.
 *
 * <p>Table can be used to get, put, delete or scan data from a fluss table.
 *
 * @since 0.1
 */
@PublicEvolving
public interface Table extends AutoCloseable {

    /**
     * Get the {@link TableDescriptor} for this table.
     *
     * <p>Note: the table info of this {@link Table} is set during the creation of this {@link
     * Table} and will not be updated after that, even if the table info of the table has been
     * changed. Therefore, if there are any changes to the table info, it may be necessary to
     * reconstruct the {@link Table}.
     */
    TableDescriptor getDescriptor();

    /**
     * Lookups certain row from the given table primary keys.
     *
     * @param key the given table primary keys.
     * @return the result of get.
     */
    CompletableFuture<LookupResult> lookup(InternalRow key);

    /**
     * Prefix lookup certain rows from the given table by prefix key.
     *
     * <p>Only available for Primary Key Table. Will throw exception when the table isn't a Primary
     * Key Table.
     *
     * <p>Note: Currently, if you want to use prefix lookup, the table you created must both define
     * the primary key and the bucket key, in addition, the bucket key needs to be part of the
     * primary key and must be a prefix of the primary key. For example, if a table has fields
     * [a,b,c,d], and the primary key is set to [a, b, c], with the bucket key set to [a, b], then
     * the prefix schema would also be [a, b]. This pattern can use PrefixLookup to lookup by prefix
     * scan.
     *
     * <p>TODO: currently, the interface only support bucket key as the prefix key to lookup.
     * Generalize the prefix lookup to support any prefix key including bucket key.
     *
     * @param bucketKey the given bucket key to do prefix lookup.
     * @return the result of prefix lookup.
     */
    CompletableFuture<PrefixLookupResult> prefixLookup(InternalRow bucketKey);

    /**
     * Extracts limit number of rows from the given table bucket.
     *
     * @param tableBucket the target table bucket to scan.
     * @param limit the given limit number.
     * @param projectedFields the projection fields.
     * @return the result of get.
     */
    CompletableFuture<List<ScanRecord>> limitScan(
            TableBucket tableBucket, int limit, @Nullable int[] projectedFields);

    /**
     * Get a {@link AppendWriter} to write data to the table. Only available for Log Table. Will
     * throw exception when the table is a primary key table.
     *
     * @return the {@link AppendWriter} to write data to the table.
     */
    AppendWriter getAppendWriter();

    /**
     * Get a {@link UpsertWriter} to write data to the table. Only available for Primary Key Table.
     * Will throw exception when the table isn't a Primary Key Table.
     *
     * @return the {@link UpsertWriter} to write data to the table.
     */
    UpsertWriter getUpsertWriter(UpsertWrite upsertWrite);

    /**
     * Get a {@link UpsertWriter} to write data to the table. Only available for Primary Key Table.
     * Will throw exception when the table isn't a Primary Key Table.
     *
     * @return the {@link UpsertWriter} to write data to the table.
     */
    UpsertWriter getUpsertWriter();

    /**
     * Get a {@link LogScanner} to scan log data from this table.
     *
     * @return the {@link LogScanner} to scan log data from this table.
     */
    LogScanner getLogScanner(LogScan logScan);

    /**
     * Get a {@link SnapshotScanner} to scan data from this table according to provided {@link
     * SnapshotScan}.
     *
     * @return the {@link SnapshotScanner} to scan data from this table.
     */
    SnapshotScanner getSnapshotScanner(SnapshotScan snapshotScan);
}
