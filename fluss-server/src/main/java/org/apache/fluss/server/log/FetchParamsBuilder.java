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

package org.apache.fluss.server.log;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.record.Filter;

import java.util.Map;

import static org.apache.fluss.server.log.FetchParams.DEFAULT_MAX_WAIT_MS;
import static org.apache.fluss.server.log.FetchParams.DEFAULT_MIN_FETCH_BYTES;

/** Builder of FetchParams. */
public final class FetchParamsBuilder {
    private int replicaId;
    private boolean fetchOnlyLeader;
    private int maxFetchBytes;
    private Map<Long, Filter> tableFilterMap;
    private int minFetchBytes;
    private long maxWaitMs;

    public FetchParamsBuilder(int replicaId, int maxFetchBytes) {
        this.replicaId = replicaId;
        this.maxFetchBytes = maxFetchBytes;
        this.minFetchBytes = DEFAULT_MIN_FETCH_BYTES;
        this.maxWaitMs = DEFAULT_MAX_WAIT_MS;
    }

    @VisibleForTesting
    public FetchParamsBuilder withFetchOnlyLeader(boolean fetchOnlyLeader) {
        this.fetchOnlyLeader = fetchOnlyLeader;
        return this;
    }

    public FetchParamsBuilder withTableFilterMap(Map<Long, Filter> tableFilterMap) {
        this.tableFilterMap = tableFilterMap;
        return this;
    }

    public FetchParamsBuilder withMinFetchBytes(int minFetchBytes) {
        this.minFetchBytes = minFetchBytes;
        return this;
    }

    public FetchParamsBuilder withMaxWaitMs(long maxWaitMs) {
        this.maxWaitMs = maxWaitMs;
        return this;
    }

    public FetchParams build() {
        return new FetchParams(
                replicaId,
                fetchOnlyLeader,
                maxFetchBytes,
                minFetchBytes,
                maxWaitMs,
                tableFilterMap);
    }
}
