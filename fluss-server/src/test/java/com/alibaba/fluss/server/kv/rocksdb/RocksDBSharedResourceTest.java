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

package com.alibaba.fluss.server.kv.rocksdb;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.Cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RocksDBSharedResource}. */
public class RocksDBSharedResourceTest {

    @AfterEach
    public void tearDown() {
        // Reset instance after each test
        RocksDBSharedResource.resetInstance();
    }

    @Test
    public void testSingletonInstance() {
        Configuration config = new Configuration();

        RocksDBSharedResource instance1 = RocksDBSharedResource.getInstance(config);
        RocksDBSharedResource instance2 = RocksDBSharedResource.getInstance(config);

        assertThat(instance1).isSameAs(instance2);
    }

    @Test
    public void testReferenceCountingBasics() {
        Configuration config = new Configuration();
        RocksDBSharedResource sharedResource = RocksDBSharedResource.getInstance(config);

        assertThat(sharedResource.getReferenceCount()).isEqualTo(0);

        int count1 = sharedResource.acquire();
        assertThat(count1).isEqualTo(1);
        assertThat(sharedResource.getReferenceCount()).isEqualTo(1);

        int count2 = sharedResource.acquire();
        assertThat(count2).isEqualTo(2);
        assertThat(sharedResource.getReferenceCount()).isEqualTo(2);

        int count3 = sharedResource.release();
        assertThat(count3).isEqualTo(1);
        assertThat(sharedResource.getReferenceCount()).isEqualTo(1);

        int count4 = sharedResource.release();
        assertThat(count4).isEqualTo(0);
        assertThat(sharedResource.getReferenceCount()).isEqualTo(0);
    }

    @Test
    public void testSharedBlockCacheNotEnabledByDefault() {
        Configuration config = new Configuration();
        RocksDBSharedResource sharedResource = RocksDBSharedResource.getInstance(config);

        Cache cache = sharedResource.getSharedBlockCache();
        assertThat(cache).isNull();
    }

    @Test
    public void testSharedBlockCacheEnabled() {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.KV_SHARED_BLOCK_CACHE_ENABLED.key(), "true");
        config.setString(ConfigOptions.KV_SHARED_BLOCK_CACHE_SIZE.key(), "100MB");

        RocksDBSharedResource sharedResource = RocksDBSharedResource.getInstance(config);

        Cache cache = sharedResource.getSharedBlockCache();
        assertThat(cache).isNotNull();
        assertThat(cache.isOwningHandle()).isTrue();
    }

    @Test
    public void testSharedBlockCacheLifecycle() {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.KV_SHARED_BLOCK_CACHE_ENABLED.key(), "true");
        config.setString(ConfigOptions.KV_SHARED_BLOCK_CACHE_SIZE.key(), "100MB");

        RocksDBSharedResource sharedResource = RocksDBSharedResource.getInstance(config);

        // Get cache instance
        Cache cache = sharedResource.getSharedBlockCache();
        assertThat(cache).isNotNull();
        assertThat(cache.isOwningHandle()).isTrue();

        // Acquire reference
        sharedResource.acquire();
        assertThat(cache.isOwningHandle()).isTrue();
        assertThat(sharedResource.isCloseable()).isFalse();

        // Release reference, cache should still exist but become closeable
        sharedResource.release();
        assertThat(cache.isOwningHandle()).isTrue(); // Modified: won't auto-release now
        assertThat(sharedResource.isCloseable()).isTrue(); // Modified: check closeable state

        // Get cache again should still return valid cache
        Cache cache2 = sharedResource.getSharedBlockCache();
        assertThat(cache2).isNotNull();
        assertThat(cache2).isSameAs(cache);

        // Resources are released only after actively closing
        sharedResource.close();
        assertThat(sharedResource.isClosed()).isTrue();
        assertThat(cache.isOwningHandle()).isFalse();

        // Getting cache after closing should return null
        Cache cache3 = sharedResource.getSharedBlockCache();
        assertThat(cache3).isNull();
    }

    @Test
    public void testMultipleContainersSharedCache() {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.KV_SHARED_BLOCK_CACHE_ENABLED.key(), "true");
        config.setString(ConfigOptions.KV_SHARED_BLOCK_CACHE_SIZE.key(), "100MB");

        RocksDBSharedResource sharedResource = RocksDBSharedResource.getInstance(config);

        // Simulate multiple RocksDBResourceContainer using same shared resource
        sharedResource.acquire(); // First container
        sharedResource.acquire(); // Second container

        assertThat(sharedResource.getReferenceCount()).isEqualTo(2);
        assertThat(sharedResource.isCloseable()).isFalse();

        Cache cache = sharedResource.getSharedBlockCache();
        assertThat(cache).isNotNull();
        assertThat(cache.isOwningHandle()).isTrue();

        // Release reference from first container
        sharedResource.release();
        assertThat(sharedResource.getReferenceCount()).isEqualTo(1);
        assertThat(sharedResource.isCloseable()).isFalse();
        // cache should still exist
        assertThat(cache.isOwningHandle()).isTrue();

        // Release reference from second container
        sharedResource.release();
        assertThat(sharedResource.getReferenceCount()).isEqualTo(0);
        assertThat(sharedResource.isCloseable()).isTrue();
        // cache should still exist until actively closed
        assertThat(cache.isOwningHandle()).isTrue();

        // Resources are actually released only after actively closing
        sharedResource.close();
        assertThat(sharedResource.isClosed()).isTrue();
        assertThat(cache.isOwningHandle()).isFalse();
    }

    @Test
    public void testAcquireAfterClose() {
        Configuration config = new Configuration();
        RocksDBSharedResource sharedResource = RocksDBSharedResource.getInstance(config);

        sharedResource.close();

        assertThatThrownBy(() -> sharedResource.acquire())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RocksDBSharedResource has been closed");
    }

    @Test
    public void testGetSharedBlockCacheAfterClose() {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.KV_SHARED_BLOCK_CACHE_ENABLED.key(), "true");
        config.setString(ConfigOptions.KV_SHARED_BLOCK_CACHE_SIZE.key(), "100MB");

        RocksDBSharedResource sharedResource = RocksDBSharedResource.getInstance(config);

        // Get cache first
        Cache cache = sharedResource.getSharedBlockCache();
        assertThat(cache).isNotNull();

        // Close resource
        sharedResource.close();

        // Getting cache after closing should return null
        Cache cache2 = sharedResource.getSharedBlockCache();
        assertThat(cache2).isNull();

        // Original cache should be closed
        assertThat(cache.isOwningHandle()).isFalse();
    }

    @Test
    public void testCloseIsIdempotent() {
        Configuration config = new Configuration();
        RocksDBSharedResource sharedResource = RocksDBSharedResource.getInstance(config);

        // Multiple closes should not cause problems
        sharedResource.close();
        sharedResource.close();
        sharedResource.close();

        // Verify state is still closed
        assertThatThrownBy(() -> sharedResource.acquire())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testCloseWaitsForReferenceCountToBeZero() throws Exception {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.KV_SHARED_BLOCK_CACHE_ENABLED.key(), "true");
        config.setString(ConfigOptions.KV_SHARED_BLOCK_CACHE_SIZE.key(), "100MB");

        RocksDBSharedResource sharedResource = RocksDBSharedResource.getInstance(config);

        // Acquire reference
        sharedResource.acquire();
        assertThat(sharedResource.getReferenceCount()).isEqualTo(1);

        Cache cache = sharedResource.getSharedBlockCache();
        assertThat(cache).isNotNull();

        // Release reference with delay in another thread
        Thread releaseThread =
                new Thread(
                        () -> {
                            try {
                                Thread.sleep(100); // Delay 100ms
                                sharedResource.release();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
        releaseThread.start();

        // Record start time
        long startTime = System.currentTimeMillis();

        // Calling close should block until reference is released
        sharedResource.close();

        // Record end time
        long endTime = System.currentTimeMillis();

        // Verify that it actually waited for some time
        assertThat(endTime - startTime).isGreaterThan(50); // At least waited 50ms

        // Verify resource has been closed
        assertThat(sharedResource.isClosed()).isTrue();
        assertThat(cache.isOwningHandle()).isFalse();

        releaseThread.join();
    }

    @Test
    public void testAcquireAfterCloseableStateResetsState() {
        Configuration config = new Configuration();
        RocksDBSharedResource sharedResource = RocksDBSharedResource.getInstance(config);

        // Acquire and release reference, enter closeable state
        sharedResource.acquire();
        assertThat(sharedResource.isCloseable()).isFalse();

        sharedResource.release();
        assertThat(sharedResource.isCloseable()).isTrue();
        assertThat(sharedResource.getReferenceCount()).isEqualTo(0);

        // Acquiring reference again should reset closeable state
        sharedResource.acquire();
        assertThat(sharedResource.isCloseable()).isFalse();
        assertThat(sharedResource.getReferenceCount()).isEqualTo(1);

        // Clean up
        sharedResource.release();
        sharedResource.close();
    }
}
