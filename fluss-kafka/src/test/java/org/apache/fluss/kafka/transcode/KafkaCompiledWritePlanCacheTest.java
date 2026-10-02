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

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.kafka.schema.KafkaTopicSchema;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaResolver;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaCompiledWritePlanCacheTest {

    @Test
    void testReusesSameIdentityAndSeparatesEquivalentAndChangedInstances() {
        KafkaCompiledWritePlanCache cache = new KafkaCompiledWritePlanCache(10);
        AtomicInteger compileCount = new AtomicInteger();
        TableInfo original = tableInfo("orders", 1L, 1, "v1");

        KafkaTopicWritePlan first =
                cache.getOrCompile(original, () -> compile(original, compileCount));
        KafkaTopicWritePlan reused =
                cache.getOrCompile(original, () -> compile(original, compileCount));
        assertThat(reused).isSameAs(first);
        assertThat(cache.getIfPresent(original)).isSameAs(first);

        // The authoritative TableInfo cache owns canonical instances. A separately constructed,
        // structurally equivalent object is conservatively treated as a new metadata snapshot.
        TableInfo equivalent = tableInfo("orders", 1L, 1, "v1");
        assertThat(cache.getIfPresent(equivalent)).isNull();
        KafkaTopicWritePlan equivalentPlan =
                cache.getOrCompile(equivalent, () -> compile(equivalent, compileCount));
        assertThat(equivalentPlan).isNotSameAs(first);

        TableInfo propertyChange = tableInfo("orders", 1L, 1, "v2");
        KafkaTopicWritePlan propertyPlan =
                cache.getOrCompile(propertyChange, () -> compile(propertyChange, compileCount));
        TableInfo schemaChange = tableInfo("orders", 1L, 2, "v2");
        KafkaTopicWritePlan schemaPlan =
                cache.getOrCompile(schemaChange, () -> compile(schemaChange, compileCount));
        TableInfo recreated = tableInfo("orders", 2L, 2, "v2");
        KafkaTopicWritePlan recreatedPlan =
                cache.getOrCompile(recreated, () -> compile(recreated, compileCount));

        assertThat(propertyPlan).isNotSameAs(first);
        assertThat(schemaPlan).isNotSameAs(propertyPlan);
        assertThat(recreatedPlan).isNotSameAs(schemaPlan);
        assertThat(compileCount).hasValue(5);
    }

    @Test
    void testEquivalentInstanceCannotInvalidateCurrentIdentity() {
        KafkaCompiledWritePlanCache cache = new KafkaCompiledWritePlanCache(10);
        AtomicInteger compileCount = new AtomicInteger();
        TableInfo current = tableInfo("orders", 1L, 1, "v1");
        KafkaTopicWritePlan currentPlan =
                cache.getOrCompile(current, () -> compile(current, compileCount));

        cache.invalidate(tableInfo("orders", 1L, 1, "v1"));

        assertThat(cache.getOrCompile(current, () -> compile(current, compileCount)))
                .isSameAs(currentPlan);
        assertThat(compileCount).hasValue(1);
    }

    @Test
    void testInvalidationAndCompileFailureRecovery() {
        KafkaCompiledWritePlanCache cache = new KafkaCompiledWritePlanCache(10);
        AtomicInteger compileCount = new AtomicInteger();
        TableInfo tableInfo = tableInfo("orders", 1L, 1, "v1");
        KafkaTopicWritePlan first =
                cache.getOrCompile(tableInfo, () -> compile(tableInfo, compileCount));

        cache.invalidate(tableInfo);
        KafkaTopicWritePlan second =
                cache.getOrCompile(tableInfo, () -> compile(tableInfo, compileCount));
        assertThat(second).isNotSameAs(first);

        TableInfo failingTable = tableInfo("payments", 2L, 1, "v1");
        assertThatThrownBy(
                        () ->
                                cache.getOrCompile(
                                        failingTable,
                                        () -> {
                                            compileCount.incrementAndGet();
                                            throw new IllegalStateException("compile failed");
                                        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("compile failed");
        assertThat(cache.getOrCompile(failingTable, () -> compile(failingTable, compileCount)))
                .isNotNull();
        assertThat(compileCount).hasValue(4);
    }

    @Test
    void testConcurrentColdCompileIsSingleFlight() throws Exception {
        KafkaCompiledWritePlanCache cache = new KafkaCompiledWritePlanCache(10);
        TableInfo tableInfo = tableInfo("orders", 1L, 1, "v1");
        AtomicInteger compileCount = new AtomicInteger();
        CountDownLatch compilerStarted = new CountDownLatch(1);
        CountDownLatch releaseCompiler = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<KafkaTopicWritePlan>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                futures.add(
                        executor.submit(
                                () ->
                                        cache.getOrCompile(
                                                tableInfo,
                                                () -> {
                                                    compileCount.incrementAndGet();
                                                    compilerStarted.countDown();
                                                    await(releaseCompiler);
                                                    return compile(tableInfo);
                                                })));
            }
            compilerStarted.await();
            releaseCompiler.countDown();

            KafkaTopicWritePlan first = futures.get(0).get();
            for (Future<KafkaTopicWritePlan> future : futures) {
                assertThat(future.get()).isSameAs(first);
            }
            assertThat(compileCount).hasValue(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testInvalidationDoesNotAllowInFlightCompileToResurrect() throws Exception {
        KafkaCompiledWritePlanCache cache = new KafkaCompiledWritePlanCache(10);
        TableInfo tableInfo = tableInfo("orders", 1L, 1, "v1");
        AtomicInteger compileCount = new AtomicInteger();
        CountDownLatch compilerStarted = new CountDownLatch(1);
        CountDownLatch releaseCompiler = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<KafkaTopicWritePlan> compiling =
                    executor.submit(
                            () ->
                                    cache.getOrCompile(
                                            tableInfo,
                                            () -> {
                                                compileCount.incrementAndGet();
                                                compilerStarted.countDown();
                                                await(releaseCompiler);
                                                return compile(tableInfo);
                                            }));
            compilerStarted.await();
            Future<?> invalidating = executor.submit(() -> cache.invalidate(tableInfo));
            releaseCompiler.countDown();

            KafkaTopicWritePlan planBeforeInvalidation = compiling.get();
            invalidating.get();
            KafkaTopicWritePlan planAfterInvalidation =
                    cache.getOrCompile(tableInfo, () -> compile(tableInfo, compileCount));

            assertThat(planAfterInvalidation).isNotSameAs(planBeforeInvalidation);
            assertThat(compileCount).hasValue(2);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testCompiledAssemblerIsSafeToShare() throws Exception {
        KafkaCompiledWritePlanCache cache = new KafkaCompiledWritePlanCache(10);
        TableInfo tableInfo = tableInfo("orders", 1L, 1, "v1");
        KafkaTopicWritePlan plan = cache.getOrCompile(tableInfo, () -> compile(tableInfo));
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<Object>> futures = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                final int id = i;
                futures.add(
                        executor.submit(
                                () ->
                                        plan.rowAssembler()
                                                .assemble(
                                                        new Object[0],
                                                        new Object[] {
                                                            Integer.toString(id)
                                                                    .getBytes(
                                                                            StandardCharsets.UTF_8)
                                                        },
                                                        id,
                                                        new ArrayList<>())
                                                .getField(0)));
            }
            for (int i = 0; i < futures.size(); i++) {
                assertThat((byte[]) futures.get(i).get())
                        .isEqualTo(Integer.toString(i).getBytes(StandardCharsets.UTF_8));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testCacheSizeIsBounded() {
        KafkaCompiledWritePlanCache cache = new KafkaCompiledWritePlanCache(2);
        for (int i = 0; i < 10; i++) {
            TableInfo tableInfo = tableInfo("topic_" + i, i + 1L, 1, "v1");
            cache.getOrCompile(tableInfo, () -> compile(tableInfo));
        }

        assertThat(cache.estimatedSize()).isLessThanOrEqualTo(2);
    }

    private static KafkaTopicWritePlan compile(TableInfo tableInfo, AtomicInteger compileCount) {
        compileCount.incrementAndGet();
        return compile(tableInfo);
    }

    private static KafkaTopicWritePlan compile(TableInfo tableInfo) {
        KafkaTopicSchema schema =
                new KafkaTopicSchemaResolver().resolve(tableInfo.toTableDescriptor());
        return new KafkaTopicWritePlan(tableInfo, schema);
    }

    private static TableInfo tableInfo(
            String tableName, long tableId, int schemaId, String propertyValue) {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("payload", DataTypes.BYTES()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_LOG_FORMAT, LogFormat.ARROW)
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "raw")
                        .customProperty("application.version", propertyValue)
                        .build();
        return TableInfo.of(
                TablePath.of("kafka", tableName), tableId, schemaId, descriptor, null, 1L, 1L);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for test compiler.", e);
        }
    }
}
