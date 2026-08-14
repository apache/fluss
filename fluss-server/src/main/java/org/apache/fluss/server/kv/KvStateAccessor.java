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

package org.apache.fluss.server.kv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;

import javax.annotation.Nullable;

/** Accessor for the local state used while processing KV records. */
@Internal
public interface KvStateAccessor {

    /**
     * Encodes the logical primary key into the physical key used by this state.
     *
     * <p>Normal KV state keeps the primary key unchanged. Historical KV state also encodes the
     * original partition context because multiple original partitions share one historical KV
     * tablet.
     */
    Key encodeKey(byte[] primaryKey);

    /** Looks up an encoded key from local state and an optional external fallback. */
    KvStateLookupResult lookup(Key key) throws Exception;

    /** Adds an insert mutation to the prewrite buffer. */
    void insert(Key key, byte[] value, long logOffset);

    /** Adds an update mutation to the prewrite buffer. */
    void update(Key key, @Nullable byte[] value, long logOffset);

    /** Adds a delete mutation to the prewrite buffer. */
    void delete(Key key, long logOffset);

    /** Truncates pending mutations to the given log offset. */
    void truncateTo(long logOffset, TruncateReason reason);
}
