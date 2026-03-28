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

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.utils.SchemaUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Resolves a filter predicate for a given batch schema ID, handling schema evolution transparently.
 *
 * <p>When the batch schema matches the predicate schema, the original predicate is returned
 * directly. When they differ, the predicate is adapted using field index mapping and the result is
 * cached to avoid redundant adaptation across batches and segments.
 *
 * <p>Adapted predicates are cached in a simple HashMap since the resolver is created per-request
 * and does not require thread-safety or eviction policies.
 */
public final class PredicateSchemaResolver {

    private static final Logger LOG = LoggerFactory.getLogger(PredicateSchemaResolver.class);

    private final Predicate predicate;
    private final int predicateSchemaId;
    @Nullable private final Schema predicateSchema;
    @Nullable private final SchemaGetter schemaGetter;

    private final Map<Integer, Optional<Predicate>> cache = new HashMap<>();

    public PredicateSchemaResolver(
            Predicate predicate, int predicateSchemaId, @Nullable SchemaGetter schemaGetter) {
        this.predicate = predicate;
        this.predicateSchemaId = predicateSchemaId;
        this.schemaGetter = schemaGetter;

        // Pre-resolve predicate schema once
        Schema resolved = null;
        if (schemaGetter != null && predicateSchemaId >= 0) {
            try {
                resolved = schemaGetter.getSchema(predicateSchemaId);
            } catch (Exception e) {
                LOG.warn(
                        "Failed to get predicate schema (schemaId={}), "
                                + "server-side filter will be disabled for cross-schema batches.",
                        predicateSchemaId,
                        e);
            }
        }
        this.predicateSchema = resolved;
    }

    /**
     * Resolve the effective predicate for a batch with the given schema ID.
     *
     * @return the adapted predicate, or {@code null} if adaptation is not possible (safe fallback:
     *     include the batch)
     */
    @Nullable
    public Predicate resolve(int batchSchemaId) {
        // Fast path: same schema
        if (predicateSchemaId == batchSchemaId || predicateSchemaId < 0) {
            return predicate;
        }

        // Check cache
        Optional<Predicate> cached = cache.get(batchSchemaId);
        if (cached != null) {
            return cached.orElse(null);
        }

        // No schema getter or predicate schema, cannot adapt
        if (schemaGetter == null || predicateSchema == null) {
            LOG.warn(
                    "Cannot adapt predicate for batch schemaId={}: "
                            + "schema getter or predicate schema unavailable, "
                            + "skipping filter for this batch.",
                    batchSchemaId);
            cache.put(batchSchemaId, Optional.empty());
            return null;
        }

        try {
            Schema batchSchema = schemaGetter.getSchema(batchSchemaId);
            if (batchSchema == null) {
                LOG.warn(
                        "Batch schema not found (schemaId={}), "
                                + "skipping filter for this batch.",
                        batchSchemaId);
                cache.put(batchSchemaId, Optional.empty());
                return null;
            }

            // indexMapping[predicateIdx] = batchIdx
            int[] indexMapping = SchemaUtil.getIndexMapping(batchSchema, predicateSchema);
            Optional<Predicate> adapted =
                    PredicateBuilder.transformFieldMapping(predicate, indexMapping);
            cache.put(batchSchemaId, adapted);
            return adapted.orElse(null);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to adapt predicate for batch schemaId={} "
                            + "(predicate schemaId={}), skipping filter for this batch.",
                    batchSchemaId,
                    predicateSchemaId,
                    e);
            cache.put(batchSchemaId, Optional.empty());
            return null;
        }
    }
}
