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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

/**
 * Reads all current rows of a primary-key table directly from the tablet servers' live RocksDB
 * state, with no dependency on remote object storage and no prior snapshot required.
 *
 * <p>Obtain an instance via {@link Table#newKvScan()}.
 *
 * <p>All internal topology is handled transparently: for partitioned tables all partitions are
 * discovered automatically; all buckets within each partition or table are iterated in order. The
 * caller does not need to know about partitions, buckets, or scanner sessions.
 *
 * <p>Memory usage on both client and server is bounded by a fixed fetch batch size (4 MB per RPC),
 * not by table size.
 *
 * <p>Consistency: each bucket is scanned under snapshot isolation — rows reflect the KV state at
 * the moment that bucket's scanner was opened on the server. There is no cross-bucket atomicity:
 * writes that arrive in a later bucket after that bucket's scanner opens will be visible.
 *
 * <p>Example:
 *
 * <pre>{@code
 * try (CloseableIterator<InternalRow> rows = table.newKvScan().execute()) {
 *     while (rows.hasNext()) {
 *         process(rows.next());
 *     }
 * }
 * }</pre>
 *
 * @since 0.9
 */
@PublicEvolving
public interface KvScan {

    /**
     * Executes the KV scan and returns a lazy iterator over all current rows in the table.
     *
     * <p>The iterator is lazy: no scan RPCs are issued until the caller begins consuming rows. Rows
     * are fetched from the server in bounded chunks as the iterator is advanced. Note that for
     * partitioned tables a metadata RPC is issued at call time to discover current partitions.
     *
     * <p>The caller is responsible for closing the returned iterator. Closing it mid-iteration
     * releases all server-side resources immediately without waiting for TTL expiry.
     *
     * @return a closeable, lazy iterator over all rows; never null
     */
    CloseableIterator<InternalRow> execute();
}
