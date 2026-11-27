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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.metadata.BulkLoadBuildContext;
import org.apache.fluss.client.metadata.BulkLoadBuildContext.Target;
import org.apache.fluss.client.metadata.BulkLoadHandle;
import org.apache.fluss.client.metadata.BulkLoadInfo;
import org.apache.fluss.config.ReadableConfig;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.messages.AbortBulkLoadRequest;
import org.apache.fluss.rpc.messages.BeginBulkLoadRequest;
import org.apache.fluss.rpc.messages.CommitBulkLoadRequest;
import org.apache.fluss.rpc.messages.GetBulkLoadInfoRequest;
import org.apache.fluss.rpc.messages.ListInProgressBulkLoadsRequest;
import org.apache.fluss.rpc.messages.PbBulkLoadStatus;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.utils.ClientRpcMessageUtils.setBulkLoadHandle;
import static org.apache.fluss.client.utils.ClientRpcMessageUtils.toBulkLoadHandle;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Coordinator-backed BulkLoad operations for one table. */
@Internal
public final class TableBulkLoad implements BulkLoad {

    private final TablePath tablePath;
    private final long tableId;
    private final AdminGateway gateway;
    private final ReadableConfig conf;

    /** Creates BulkLoad operations with the table identity and connection resources. */
    public TableBulkLoad(
            TablePath tablePath, long tableId, AdminGateway gateway, ReadableConfig conf) {
        this.tablePath = tablePath;
        this.tableId = tableId;
        this.gateway = gateway;
        this.conf = conf;
    }

    @Override
    public CompletableFuture<BulkLoadBuildContext> begin(
            String bulkLoadId, List<String> partitionNames) {
        checkArgument(
                !checkNotNull(bulkLoadId, "bulkLoadId must not be null").isEmpty(),
                "bulkLoadId must not be empty");
        BeginBulkLoadRequest request = new BeginBulkLoadRequest().setBulkLoadId(bulkLoadId);
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        partitionNames.forEach(request::addPartitionName);
        return gateway.beginBulkLoad(request)
                .thenApply(response -> new BulkLoadBuildContext(tablePath, response));
    }

    @Override
    public Optional<BulkLoadBucketWriter> tryOpenBucketWriter(Target target, int bucketId)
            throws IOException {
        checkHandle(target.getHandle());
        return BulkLoadBucketWriterImpl.tryOpen(target, bucketId, gateway, conf);
    }

    @Override
    public CompletableFuture<BulkLoadInfo> getInfo(BulkLoadHandle handle) {
        checkHandle(handle);
        GetBulkLoadInfoRequest request = new GetBulkLoadInfoRequest();
        setBulkLoadHandle(request.setHandle(), handle);
        return gateway.getBulkLoadInfo(request).thenApply(BulkLoadInfo::new);
    }

    @Override
    public CompletableFuture<List<BulkLoadHandle>> listInProgress(List<String> partitionNames) {
        ListInProgressBulkLoadsRequest request = new ListInProgressBulkLoadsRequest();
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        partitionNames.forEach(request::addPartitionName);
        return gateway.listInProgressBulkLoads(request)
                .thenApply(
                        response -> {
                            List<BulkLoadHandle> handles =
                                    new ArrayList<>(response.getTransactionsCount());
                            for (PbBulkLoadStatus status : response.getTransactionsList()) {
                                handles.add(toBulkLoadHandle(status.getHandle()));
                            }
                            return Collections.unmodifiableList(handles);
                        });
    }

    @Override
    public CompletableFuture<Void> commit(BulkLoadHandle handle) {
        checkHandle(handle);
        CommitBulkLoadRequest request = new CommitBulkLoadRequest();
        setBulkLoadHandle(request.setHandle(), handle);
        return gateway.commitBulkLoad(request).thenApply(ignored -> null);
    }

    @Override
    public CompletableFuture<Void> abort(BulkLoadHandle handle, @Nullable String reason) {
        checkHandle(handle);
        AbortBulkLoadRequest request = new AbortBulkLoadRequest();
        setBulkLoadHandle(request.setHandle(), handle);
        if (reason != null) {
            request.setAbortMessage(reason);
        }
        return gateway.abortBulkLoad(request).thenApply(ignored -> null);
    }

    private void checkHandle(BulkLoadHandle handle) {
        checkArgument(
                handle.getTableId() == tableId,
                "BulkLoad handle %s does not belong to table %s (ID %s)",
                handle,
                tablePath,
                tableId);
    }
}
