/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.shaded.arrow.org.apache.arrow.vector.util;

import java.util.Iterator;
import java.util.Map;

// TODO: This is a temporary workaround to resolve Arrow 15's dependency on eclipse-collections,
//  which carries a Category-B license (see https://github.com/apache/arrow/issues/40896).
//  Without these override classes, ArrowReaderWriterTest fails with a ClassNotFoundException.
//  This workaround should be removed once we upgrade to Arrow 16 or later. Alternatively, these
//  override classes could be moved to `fluss-shaded-arrow` to isolate the dependency.
/**
 * A vendored specialized copy of Netty's IntObjectMap for use within Arrow. Avoids requiring Netty
 * in the Arrow core just for this one class.
 *
 * @param <V> the value type stored in the map.
 */
interface IntObjectMap<V> extends Map<Integer, V> {

    /**
     * A primitive entry in the map, provided by the iterator from {@link #entries()}.
     *
     * @param <V> the value type stored in the map.
     */
    interface PrimitiveEntry<V> {
        /** Gets the key for this entry. */
        int key();

        /** Gets the value for this entry. */
        V value();

        /** Sets the value for this entry. */
        void setValue(V value);
    }

    /**
     * Gets the value in the map with the specified key.
     *
     * @param key the key whose associated value is to be returned.
     * @return the value or {@code null} if the key was not found in the map.
     */
    V get(int key);

    /**
     * Puts the given entry into the map.
     *
     * @param key the key of the entry.
     * @param value the value of the entry.
     * @return the previous value for this key or {@code null} if there was no previous mapping.
     */
    V put(int key, V value);

    /**
     * Removes the entry with the specified key.
     *
     * @param key the key for the entry to be removed from this map.
     * @return the previous value for the key, or {@code null} if there was no mapping.
     */
    V remove(int key);

    /**
     * Gets an iterable to traverse over the primitive entries contained in this map. As an
     * optimization, the {@link PrimitiveEntry}s returned by the {@link Iterator} may change as the
     * {@link Iterator} progresses. The caller should not rely on {@link PrimitiveEntry} key/value
     * stability.
     */
    Iterable<PrimitiveEntry<V>> entries();

    /** Indicates whether or not this map contains a value for the specified key. */
    boolean containsKey(int key);
}
