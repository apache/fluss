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

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Visibility semantics for writes to a table that owns global indexes.
 *
 * <p>Stored per index in {@link Schema.Index} metadata.
 *
 * @since 0.10
 */
@PublicEvolving
public enum IndexVisibility {

    /**
     * Synchronous visibility: PutKv ack waits until all generated index mutations have been applied
     * at every target index bucket. The strongest read-your-write guarantee Fluss offers for
     * indexes; this is the default.
     */
    SYNC,

    /**
     * Asynchronous visibility: PutKv ack returns as soon as the data WAL is committed; index
     * mutations land independently afterwards. Lower write latency at the cost of a transient
     * window during which lookups may not yet see the new entries.
     */
    ASYNC
}
