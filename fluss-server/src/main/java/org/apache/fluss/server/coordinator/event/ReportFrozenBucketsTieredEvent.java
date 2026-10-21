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

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.rpc.messages.ReportFrozenBucketsTieredRequest;
import org.apache.fluss.rpc.messages.ReportFrozenBucketsTieredResponse;

import java.util.concurrent.CompletableFuture;

/** Reports frozen buckets whose entire log has been committed to the data lake. */
public class ReportFrozenBucketsTieredEvent implements CoordinatorEvent {
    private final ReportFrozenBucketsTieredRequest request;
    private final CompletableFuture<ReportFrozenBucketsTieredResponse> response;

    /** Creates a report event and its RPC completion future. */
    public ReportFrozenBucketsTieredEvent(
            ReportFrozenBucketsTieredRequest request,
            CompletableFuture<ReportFrozenBucketsTieredResponse> response) {
        this.request = request;
        this.response = response;
    }

    /** Returns the reported bucket identities and epochs. */
    public ReportFrozenBucketsTieredRequest getRequest() {
        return request;
    }

    /** Returns the future completed after the coordinator processes the report. */
    public CompletableFuture<ReportFrozenBucketsTieredResponse> getResponse() {
        return response;
    }
}
