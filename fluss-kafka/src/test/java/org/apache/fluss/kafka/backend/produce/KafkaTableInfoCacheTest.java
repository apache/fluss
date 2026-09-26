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

package org.apache.fluss.kafka.backend.produce;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaTableInfoCacheTest {

    private static final TablePath TABLE_PATH = TablePath.of("kafka", "orders");

    @Test
    void testReusesUnchangedResponseAndReloadsPropertyOnlyChange() {
        KafkaTableInfoCache cache = new KafkaTableInfoCache(10);
        AtomicInteger loadCount = new AtomicInteger();
        GetTableInfoResponse firstResponse = response(1L, 1, descriptor("v1"));

        TableInfo first =
                cache.getOrLoad(
                        TABLE_PATH,
                        firstResponse,
                        () -> load(TABLE_PATH, firstResponse, loadCount));
        GetTableInfoResponse equivalentResponse = response(1L, 1, descriptor("v1"));
        TableInfo reused =
                cache.getOrLoad(
                        TABLE_PATH,
                        equivalentResponse,
                        () -> load(TABLE_PATH, equivalentResponse, loadCount));

        assertThat(reused).isSameAs(first);
        assertThat(loadCount).hasValue(1);

        // A Kafka contract property may change without a schema ID or timestamp change.
        GetTableInfoResponse changedResponse = response(1L, 1, descriptor("v2"));
        TableInfo changed =
                cache.getOrLoad(
                        TABLE_PATH,
                        changedResponse,
                        () -> load(TABLE_PATH, changedResponse, loadCount));

        assertThat(changed).isNotSameAs(first);
        assertThat(loadCount).hasValue(2);
    }

    @Test
    void testReloadsDeleteAndRecreateAndSupportsExplicitInvalidation() {
        KafkaTableInfoCache cache = new KafkaTableInfoCache(10);
        AtomicInteger loadCount = new AtomicInteger();
        GetTableInfoResponse original = response(1L, 1, descriptor("v1"));
        TableInfo first =
                cache.getOrLoad(TABLE_PATH, original, () -> load(TABLE_PATH, original, loadCount));

        GetTableInfoResponse recreated = response(2L, 1, descriptor("v1"));
        TableInfo second =
                cache.getOrLoad(
                        TABLE_PATH, recreated, () -> load(TABLE_PATH, recreated, loadCount));
        assertThat(second).isNotSameAs(first);

        cache.invalidate(TABLE_PATH);
        TableInfo third =
                cache.getOrLoad(
                        TABLE_PATH, recreated, () -> load(TABLE_PATH, recreated, loadCount));
        assertThat(third).isNotSameAs(second);
        assertThat(loadCount).hasValue(3);
    }

    @Test
    void testReloadsWhenOnlyBucketCountEpochChanges() {
        KafkaTableInfoCache cache = new KafkaTableInfoCache(10);
        AtomicInteger loads = new AtomicInteger();
        GetTableInfoResponse response = response(1L, 1, descriptor("v1"));
        TableInfo first =
                cache.getOrLoad(TABLE_PATH, response, () -> load(TABLE_PATH, response, loads));
        response.setBucketCountEpoch(7L);
        TableInfo changed =
                cache.getOrLoad(TABLE_PATH, response, () -> load(TABLE_PATH, response, loads));
        assertThat(changed).isNotSameAs(first);
        assertThat(changed.getBucketCountEpoch()).isEqualTo(7L);
        assertThat(loads).hasValue(2);
    }

    @Test
    void testLoadFailureIsNotCached() {
        KafkaTableInfoCache cache = new KafkaTableInfoCache(10);
        AtomicInteger attempts = new AtomicInteger();
        GetTableInfoResponse response = response(1L, 1, descriptor("v1"));

        assertThatThrownBy(
                        () ->
                                cache.getOrLoad(
                                        TABLE_PATH,
                                        response,
                                        () -> {
                                            attempts.incrementAndGet();
                                            throw new IllegalStateException("broken descriptor");
                                        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("broken descriptor");

        TableInfo loaded =
                cache.getOrLoad(TABLE_PATH, response, () -> load(TABLE_PATH, response, attempts));
        assertThat(loaded.getTableId()).isEqualTo(1L);
        assertThat(attempts).hasValue(2);
    }

    @Test
    void testConcurrentColdLoadIsSingleFlight() throws Exception {
        KafkaTableInfoCache cache = new KafkaTableInfoCache(10);
        GetTableInfoResponse response = response(1L, 1, descriptor("v1"));
        AtomicInteger loadCount = new AtomicInteger();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<TableInfo>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                futures.add(
                        executor.submit(
                                () ->
                                        cache.getOrLoad(
                                                TABLE_PATH,
                                                response,
                                                () -> {
                                                    loadCount.incrementAndGet();
                                                    loaderStarted.countDown();
                                                    await(releaseLoader);
                                                    return toTableInfo(TABLE_PATH, response);
                                                })));
            }
            loaderStarted.await();
            releaseLoader.countDown();

            TableInfo first = futures.get(0).get();
            for (Future<TableInfo> future : futures) {
                assertThat(future.get()).isSameAs(first);
            }
            assertThat(loadCount).hasValue(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testInvalidationDoesNotAllowInFlightLoadToResurrect() throws Exception {
        KafkaTableInfoCache cache = new KafkaTableInfoCache(10);
        GetTableInfoResponse response = response(1L, 1, descriptor("v1"));
        AtomicInteger loadCount = new AtomicInteger();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<TableInfo> loading =
                    executor.submit(
                            () ->
                                    cache.getOrLoad(
                                            TABLE_PATH,
                                            response,
                                            () -> {
                                                loadCount.incrementAndGet();
                                                loaderStarted.countDown();
                                                await(releaseLoader);
                                                return toTableInfo(TABLE_PATH, response);
                                            }));
            loaderStarted.await();
            Future<?> invalidating = executor.submit(() -> cache.invalidate(TABLE_PATH));
            releaseLoader.countDown();

            TableInfo loadedBeforeInvalidation = loading.get();
            invalidating.get();
            TableInfo loadedAfterInvalidation =
                    cache.getOrLoad(
                            TABLE_PATH, response, () -> load(TABLE_PATH, response, loadCount));

            assertThat(loadedAfterInvalidation).isNotSameAs(loadedBeforeInvalidation);
            assertThat(loadCount).hasValue(2);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testCacheSizeIsBounded() {
        KafkaTableInfoCache cache = new KafkaTableInfoCache(2);
        for (int i = 0; i < 10; i++) {
            TablePath path = TablePath.of("kafka", "topic_" + i);
            GetTableInfoResponse response = response(i + 1L, 1, descriptor("v1"));
            cache.getOrLoad(path, response, () -> toTableInfo(path, response));
        }

        assertThat(cache.estimatedSize()).isLessThanOrEqualTo(2);
    }

    private static TableInfo load(
            TablePath tablePath, GetTableInfoResponse response, AtomicInteger loadCount) {
        loadCount.incrementAndGet();
        return toTableInfo(tablePath, response);
    }

    private static TableInfo toTableInfo(TablePath tablePath, GetTableInfoResponse response) {
        return TableInfo.of(
                tablePath,
                response.getTableId(),
                response.getSchemaId(),
                TableDescriptor.fromJsonBytes(response.getTableJson()),
                null,
                response.getCreatedTime(),
                response.getModifiedTime(),
                response.hasBucketCountEpoch() ? response.getBucketCountEpoch() : 0L);
    }

    private static GetTableInfoResponse response(
            long tableId, int schemaId, TableDescriptor descriptor) {
        return new GetTableInfoResponse()
                .setTableId(tableId)
                .setSchemaId(schemaId)
                .setTableJson(descriptor.toJsonBytes())
                .setCreatedTime(1L)
                .setModifiedTime(1L);
    }

    private static TableDescriptor descriptor(String version) {
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("value", DataTypes.BYTES()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_LOG_FORMAT, LogFormat.ARROW)
                        .customProperty("kafka.value.format", "raw");
        if ("v2".equals(version)) {
            builder.customProperty("application.version", "changed");
        }
        return builder.build();
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for test loader.", e);
        }
    }
}
