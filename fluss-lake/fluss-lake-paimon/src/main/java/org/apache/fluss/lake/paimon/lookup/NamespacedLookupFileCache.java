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

package org.apache.fluss.lake.paimon.lookup;

import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Policy;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.stats.CacheStats;

import javax.annotation.Nullable;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Namespace-scoped cache view delegating all storage to a shared lookup-file cache. */
final class NamespacedLookupFileCache implements Cache<String, LookupFile> {

    private final Cache<SharedLookupFileCache.Key, LookupFile> sharedCache;
    private final String namespace;

    NamespacedLookupFileCache(
            Cache<SharedLookupFileCache.Key, LookupFile> sharedCache, String namespace) {
        this.sharedCache = checkNotNull(sharedCache, "sharedCache must not be null.");
        this.namespace = checkNotNull(namespace, "namespace must not be null.");
    }

    @Override
    public @Nullable LookupFile getIfPresent(Object fileName) {
        return fileName instanceof String ? sharedCache.getIfPresent(key((String) fileName)) : null;
    }

    @Override
    public LookupFile get(
            String fileName, Function<? super String, ? extends LookupFile> mappingFunction) {
        return sharedCache.get(key(fileName), ignored -> mappingFunction.apply(fileName));
    }

    @Override
    public Map<String, LookupFile> getAllPresent(Iterable<?> fileNames) {
        Map<String, LookupFile> result = new LinkedHashMap<>();
        for (Object fileName : fileNames) {
            LookupFile lookupFile = getIfPresent(fileName);
            if (lookupFile != null) {
                result.put((String) fileName, lookupFile);
            }
        }
        return result;
    }

    @Override
    public void put(String fileName, LookupFile lookupFile) {
        sharedCache.put(key(fileName), lookupFile);
    }

    @Override
    public void putAll(Map<? extends String, ? extends LookupFile> entries) {
        entries.forEach(this::put);
    }

    @Override
    public void invalidate(Object fileName) {
        if (fileName instanceof String) {
            sharedCache.invalidate(key((String) fileName));
        }
    }

    @Override
    public void invalidateAll(Iterable<?> fileNames) {
        for (Object fileName : fileNames) {
            invalidate(fileName);
        }
    }

    @Override
    public void invalidateAll() {
        // ponytail: O(n) namespace scan; add a namespace index if cache cardinality makes close
        // slow.
        Set<SharedLookupFileCache.Key> keys = new HashSet<>();
        for (SharedLookupFileCache.Key key : sharedCache.asMap().keySet()) {
            if (key.namespace.equals(namespace)) {
                keys.add(key);
            }
        }
        sharedCache.invalidateAll(keys);
    }

    @Override
    public long estimatedSize() {
        return sharedCache.asMap().keySet().stream()
                .filter(key -> key.namespace.equals(namespace))
                .count();
    }

    @Override
    public CacheStats stats() {
        return sharedCache.stats();
    }

    @Override
    public ConcurrentMap<String, LookupFile> asMap() {
        return new NamespacedMap();
    }

    @Override
    public void cleanUp() {
        sharedCache.cleanUp();
    }

    @Override
    public Policy<String, LookupFile> policy() {
        throw new UnsupportedOperationException(
                "Policy access is not supported by the namespaced cache view.");
    }

    /** Concurrent-map view required by Paimon's closed-entry removal path. */
    private final class NamespacedMap extends AbstractMap<String, LookupFile>
            implements ConcurrentMap<String, LookupFile> {

        @Override
        public Set<Entry<String, LookupFile>> entrySet() {
            Set<Entry<String, LookupFile>> entries = new HashSet<>();
            sharedCache
                    .asMap()
                    .forEach(
                            (key, lookupFile) -> {
                                if (key.namespace.equals(namespace)) {
                                    entries.add(
                                            new SimpleImmutableEntry<>(key.fileName, lookupFile));
                                }
                            });
            return entries;
        }

        @Override
        public @Nullable LookupFile get(Object fileName) {
            return fileName instanceof String
                    ? sharedCache.asMap().get(key((String) fileName))
                    : null;
        }

        @Override
        public @Nullable LookupFile put(String fileName, LookupFile lookupFile) {
            return sharedCache.asMap().put(key(fileName), lookupFile);
        }

        @Override
        public @Nullable LookupFile remove(Object fileName) {
            return fileName instanceof String
                    ? sharedCache.asMap().remove(key((String) fileName))
                    : null;
        }

        @Override
        public boolean remove(Object fileName, Object lookupFile) {
            return fileName instanceof String
                    && sharedCache.asMap().remove(key((String) fileName), lookupFile);
        }

        @Override
        public @Nullable LookupFile putIfAbsent(String fileName, LookupFile lookupFile) {
            return sharedCache.asMap().putIfAbsent(key(fileName), lookupFile);
        }

        @Override
        public boolean replace(String fileName, LookupFile oldValue, LookupFile newValue) {
            return sharedCache.asMap().replace(key(fileName), oldValue, newValue);
        }

        @Override
        public @Nullable LookupFile replace(String fileName, LookupFile lookupFile) {
            return sharedCache.asMap().replace(key(fileName), lookupFile);
        }

        @Override
        public void clear() {
            invalidateAll();
        }
    }

    private SharedLookupFileCache.Key key(String fileName) {
        return new SharedLookupFileCache.Key(namespace, fileName);
    }
}
