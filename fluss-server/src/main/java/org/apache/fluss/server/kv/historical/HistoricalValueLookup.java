/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.fluss.server.kv.historical;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

/** Resolves a lake value already memoized for the current historical write request. */
@Internal
@FunctionalInterface
public interface HistoricalValueLookup {

    /**
     * Returns the encoded value for the primary key, or null when it does not exist.
     *
     * <p>This method is invoked while the KV write lock is held and must not perform lake or file
     * I/O.
     */
    @Nullable
    byte[] lookup(byte[] primaryKey);
}
