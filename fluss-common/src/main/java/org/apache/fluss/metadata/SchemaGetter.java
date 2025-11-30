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

package org.apache.fluss.metadata;

import org.apache.fluss.exception.SchemaNotExistException;

import java.util.concurrent.CompletableFuture;

/**
 * Provides retrieval and lifecycle management for {@link Schema} instances identified by schema
 * ids.
 *
 * <p>Implementations may expose both blocking and non-blocking retrieval paths. Clients can obtain
 * a schema synchronously via {@link #getSchema(int)} or asynchronously via {@link
 * #getSchemaInfoAsync(int)}.
 */
public interface SchemaGetter {

    /**
     * Synchronously obtains the {@link Schema} associated with the given {@code schemaId}.
     *
     * <p>This method may block while fetching or deserializing the schema. The implementation may
     * perform an RPC request to fetching schema metadata from the Cluster.
     *
     * @param schemaId numeric identifier of the requested schema
     * @return the resolved {@code Schema}, or {@code null} if no schema is available for the id
     * @throws SchemaNotExistException if the requested schema id is not exists.
     */
    Schema getSchema(int schemaId);

    /**
     * Asynchronously obtains the {@code Schema} associated with the given {@code schemaId}.
     *
     * <p>The returned {@link CompletableFuture} completes with the resolved {@link SchemaInfo} or
     * completes exceptionally if an error occurs.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link SchemaNotExistException} if the requested schema id is not exists.
     * </ul>
     *
     * @param schemaId numeric identifier of the requested schema
     * @return a {@link CompletableFuture} that will be completed with the {@link SchemaInfo} or
     *     completed exceptionally
     */
    CompletableFuture<SchemaInfo> getSchemaInfoAsync(int schemaId);

    /** Returns metadata describing the latest known schema. */
    SchemaInfo getLatestSchemaInfo();

    /**
     * Releases any resources held by this {@code SchemaGetter}.
     *
     * <p>After calling this method, the instance may no longer be usable. Implementations should
     * free caches, close network connections, cancel outstanding requests, and stop background
     * threads as appropriate.
     */
    void release();
}
