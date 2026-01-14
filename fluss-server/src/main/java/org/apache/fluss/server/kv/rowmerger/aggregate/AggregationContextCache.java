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

package org.apache.fluss.server.kv.rowmerger.aggregate;

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.concurrent.ThreadSafe;

import java.time.Duration;

/** The cache for {@link AggregationContext}. */
@ThreadSafe
public class AggregationContextCache {

    private final Cache<Integer, AggregationContext> contexts;
    private final SchemaGetter schemaGetter;
    private final KvFormat kvFormat;

    public AggregationContextCache(SchemaGetter schemaGetter, KvFormat kvFormat) {
        this.schemaGetter = schemaGetter;
        this.kvFormat = kvFormat;
        // Limit cache size to prevent memory leak, and expire entries after 5 minutes of inactivity
        this.contexts =
                Caffeine.newBuilder()
                        .maximumSize(5)
                        .expireAfterAccess(Duration.ofMinutes(5))
                        .build();
    }

    /**
     * Get aggregation context for a given schema ID with null safety check.
     *
     * <p>This method retrieves the schema from SchemaGetter and creates or retrieves the
     * AggregationContext from cache. Note: Schema is not returned separately as it's already
     * accessible via {@link AggregationContext#getSchema()}.
     *
     * @param schemaId the schema ID
     * @return AggregationContext for the schema
     * @throws IllegalStateException if schema is not found
     */
    public AggregationContext getContext(short schemaId) {
        if (schemaId < 0) {
            throw new IllegalArgumentException(
                    "Schema ID must be non-negative, but got: " + schemaId);
        }
        Schema schema = schemaGetter.getSchema(schemaId);
        if (schema == null) {
            throw new IllegalStateException(String.format("Schema with ID %d not found", schemaId));
        }
        return getOrCreateContext(schemaId, schema);
    }

    /**
     * Get or create aggregation context for a given schema ID when schema is already available.
     *
     * <p>This method is useful when the schema is already retrieved (e.g., in constructors) to
     * avoid redundant schema lookups.
     *
     * @param schemaId the schema ID
     * @param schema the schema (must not be null)
     * @return the aggregation context
     */
    public AggregationContext getOrCreateContext(int schemaId, Schema schema) {
        return contexts.get(schemaId, k -> AggregationContext.create(schema, kvFormat));
    }
}
