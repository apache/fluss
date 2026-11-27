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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.metadata.BulkLoadBuildContext;
import org.apache.fluss.client.metadata.BulkLoadBuildContext.Target;
import org.apache.fluss.client.metadata.BulkLoadHandle;
import org.apache.fluss.client.metadata.BulkLoadInfo;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Operations for building and publishing bucket snapshots for a single table.
 *
 * <p>Transaction handles and build targets must belong to the physical table for these operations.
 */
@PublicEvolving
public interface BulkLoad {

    /**
     * Begins a transaction for the specified targets and returns its frozen build context.
     *
     * <p>Use an empty partition list for a non-partitioned table. For a partitioned table, provide
     * the partition names explicitly. Persist the caller-supplied ID with the input selection and
     * reuse both when retrying Begin after an uncertain outcome.
     *
     * <p>Replay returns the original transaction while the original physical table exists and the
     * transaction record is retained. Terminal results are subject to the server's retention
     * period. Recreating a table under the same name gives it a new physical identity, so the same
     * caller-supplied ID can begin a new transaction.
     */
    CompletableFuture<BulkLoadBuildContext> begin(String bulkLoadId, List<String> partitionNames);

    /**
     * Opens a BulkLoad bucket writer using frozen target metadata or returns empty once the
     * allocated snapshot is committed. If a previous attempt completed the upload, this method
     * commits that snapshot before returning empty.
     *
     * <p>A publication exception may leave the outcome unknown. Retry with the same target and
     * bucket to confirm the result.
     *
     * <p>The caller owns and must close any returned writer. Invoke this blocking operation from a
     * build worker thread. Build workers need read and write access to the remote snapshot
     * directories and local space for a RocksDB database under the connection's {@code
     * client.io.tmpdir}.
     *
     * <p>Bucket writers support primary-key tables using the plain KV value layout without
     * row-level TTL.
     *
     * @throws IOException if allocation or commit fails, snapshot files cannot be accessed, or the
     *     local database cannot be opened
     */
    Optional<BulkLoadBucketWriter> tryOpenBucketWriter(Target target, int bucketId)
            throws IOException;

    /** Returns transaction status and bucket progress for the original targets. */
    CompletableFuture<BulkLoadInfo> getInfo(BulkLoadHandle handle);

    /**
     * Lists handles of running transactions owning any selected physical target.
     *
     * <p>An empty partition list queries all current targets of the table.
     */
    CompletableFuture<List<BulkLoadHandle>> listInProgress(List<String> partitionNames);

    /**
     * Finalizes the transaction after all buckets of its active targets have been committed.
     *
     * <p>Each bucket snapshot is published when it is registered with the coordinator.
     */
    CompletableFuture<Void> commit(BulkLoadHandle handle);

    /** Stops the transaction while retaining any previously committed bucket snapshots. */
    CompletableFuture<Void> abort(BulkLoadHandle handle, @Nullable String reason);
}
