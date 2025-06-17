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

package com.alibaba.fluss.server.kv.partialupdate;

import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.Schema;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Arrays;

/** The cache for {@link PartialUpdater}. */
@ThreadSafe
public class PartialUpdaterCache {

    private final Cache<String, PartialUpdater> rowPartialUpdaters;

    public PartialUpdaterCache() {
        // currently, the cache is used per-bucket, so we limit the cache size to 5 to have a
        // maximal 5 parallel partial updaters. This is a temporary solution and should be
        // shared across all buckets in the future.
        this.rowPartialUpdaters = Caffeine.newBuilder().maximumSize(5).build();
    }

    // TODO: extend to tableId and schemaId when the cache is shared across all tables
    public PartialUpdater getOrCreatePartialUpdater(
            KvFormat kvFormat, Schema schema, int[] targetColumns) {
        return rowPartialUpdaters.get(
                getPartialUpdaterKey(targetColumns),
                k -> new PartialUpdater(kvFormat, schema, targetColumns));
    }

    private String getPartialUpdaterKey(int[] targetColumns) {
        return Arrays.toString(targetColumns);
    }
}
