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

package org.apache.fluss.flink.action.orphan.build;

import org.apache.fluss.flink.action.orphan.RpcErrorClassifier;

import javax.annotation.Nullable;

/**
 * Per-target status of a list RPC (target = one {@code (tableId, partitionId|null)} pair), shared
 * by {@link LogActiveRefsFetchResult} and {@link KvActiveRefsFetchResult}.
 *
 * <p>Captures the {@code listOk + listFailureReason} pair so both result types can delegate the
 * per-target axis to a single value and surface identical {@code listOk()} / {@code
 * listFailureReason()} APIs to consumers.
 */
final class RpcListStatus {

    private static final RpcListStatus OK = new RpcListStatus(true, null, null, null);

    private final boolean ok;
    @Nullable private final String reason;
    @Nullable private final RpcErrorClassifier.Category category;
    @Nullable private final Throwable cause;

    private RpcListStatus(
            boolean ok,
            @Nullable String reason,
            @Nullable RpcErrorClassifier.Category category,
            @Nullable Throwable cause) {
        this.ok = ok;
        this.reason = reason;
        this.category = category;
        this.cause = cause;
    }

    static RpcListStatus ok() {
        return OK;
    }

    static RpcListStatus listFailed(String reason) {
        return new RpcListStatus(false, reason, null, null);
    }

    static RpcListStatus listFailed(
            String reason, RpcErrorClassifier.Category category, Throwable cause) {
        return new RpcListStatus(false, reason, category, cause);
    }

    boolean isOk() {
        return ok;
    }

    @Nullable
    String reason() {
        return reason;
    }

    @Nullable
    RpcErrorClassifier.Category category() {
        return category;
    }

    @Nullable
    Throwable cause() {
        return cause;
    }
}
