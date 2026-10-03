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

package org.apache.fluss.server.kv.partialupdate;

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.server.kv.rowmerger.SequenceGroups;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.concurrent.ThreadSafe;

import java.time.Duration;
import java.util.Arrays;

/** The cache for {@link PartialUpdater}. */
@ThreadSafe
public class PartialUpdaterCache {

    private final Cache<String, PartialUpdater> rowPartialUpdaters;
    private final boolean arbitrateSequenceGroups;

    /**
     * @param arbitrateSequenceGroups whether the cached updaters arbitrate the sequence groups
     *     declared on the schema; otherwise they replace the target columns blindly, as required
     *     when recovering by overwriting an already decided value
     */
    public PartialUpdaterCache(boolean arbitrateSequenceGroups) {
        this.arbitrateSequenceGroups = arbitrateSequenceGroups;
        // currently, the cache is used per-bucket, so we limit the cache size to 5 to have a
        // maximal 5 parallel partial updaters. This is a temporary solution and should be
        // shared across all buckets in the future.
        this.rowPartialUpdaters =
                Caffeine.newBuilder()
                        .maximumSize(5)
                        .expireAfterAccess(Duration.ofMinutes(5))
                        .build();
    }

    // TODO: extend to tableId and schemaId when the cache is shared across all tables
    public PartialUpdater getOrCreatePartialUpdater(
            KvFormat kvFormat, short schemaId, Schema schema, int[] targetColumns) {
        return rowPartialUpdaters.get(
                getPartialUpdaterKey(targetColumns, schemaId),
                k ->
                        new PartialUpdater(
                                kvFormat,
                                schemaId,
                                schema,
                                targetColumns,
                                arbitrateSequenceGroups ? SequenceGroups.create(schema) : null));
    }

    private String getPartialUpdaterKey(int[] targetColumns, int schemaId) {
        return schemaId + "_" + Arrays.toString(targetColumns);
    }
}
