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

import org.apache.fluss.annotation.Internal;

import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.paimon.mergetree.LookupUtils.fileKibiBytes;

/** A weighted lookup-file cache shared by multiple Paimon table lookupers. */
@Internal
public final class SharedLookupFileCache implements AutoCloseable {

    private final Cache<Key, LookupFile> cache;

    /** Creates a shared lookup-file cache. */
    public SharedLookupFileCache(Duration fileRetention, MemorySize maxDiskSize) {
        checkNotNull(fileRetention, "fileRetention must not be null.");
        checkNotNull(maxDiskSize, "maxDiskSize must not be null.");
        this.cache =
                Caffeine.newBuilder()
                        .expireAfterAccess(fileRetention)
                        .maximumWeight(Math.max(1L, maxDiskSize.getKibiBytes()))
                        .weigher(
                                (Key key, LookupFile lookupFile) ->
                                        Math.max(1, fileKibiBytes(lookupFile.localFile())))
                        .removalListener(SharedLookupFileCache::removeLookupFile)
                        .executor(Runnable::run)
                        .build();
    }

    Cache<String, LookupFile> namespaced(String namespace) {
        return new NamespacedLookupFileCache(cache, namespace);
    }

    /** Updates the maximum cache weight. */
    public void updateMaxDiskSize(MemorySize maxDiskSize) {
        cache.policy().eviction().get().setMaximum(Math.max(1L, maxDiskSize.getKibiBytes()));
    }

    @Override
    public void close() {
        cache.invalidateAll();
        cache.cleanUp();
    }

    private static void removeLookupFile(
            @Nullable Key key, @Nullable LookupFile lookupFile, RemovalCause cause) {
        if (lookupFile != null) {
            try {
                lookupFile.close(cause);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    static final class Key {
        final String namespace;
        final String fileName;

        Key(String namespace, String fileName) {
            this.namespace = checkNotNull(namespace, "namespace must not be null.");
            this.fileName = checkNotNull(fileName, "fileName must not be null.");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Key)) {
                return false;
            }
            Key key = (Key) o;
            return namespace.equals(key.namespace) && fileName.equals(key.fileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(namespace, fileName);
        }
    }
}
