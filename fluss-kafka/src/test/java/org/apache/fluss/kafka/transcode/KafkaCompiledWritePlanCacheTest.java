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
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.kafka.format.KafkaDecodedValue;
import org.apache.fluss.kafka.format.KafkaFormatFactoryRegistry;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaCompiledWritePlanCacheTest {

    @Test
    void testReusesTableAndSchemaKeyAndSeparatesChangedVersions() {
        KafkaCompiledWritePlanCache cache = new KafkaCompiledWritePlanCache(10);
        AtomicInteger compileCount = new AtomicInteger();
        TableInfo original = tableInfo("orders", 1L, 1, "v1");

        KafkaTopicWritePlan first =
                cache.getOrCompile(original, () -> compile(original, compileCount));
        KafkaTopicWritePlan reused =
                cache.getOrCompile(original, () -> compile(original, compileCount));
        assertThat(reused).isSameAs(first);
        assertThat(cache.getIfPresent(original)).isSameAs(first);

        TableInfo equivalent = tableInfo("orders", 1L, 1, "v1");
        assertThat(cache.getIfPresent(equivalent)).isSameAs(first);
        KafkaTopicWritePlan equivalentPlan =
                cache.getOrCompile(equivalent, () -> compile(equivalent, compileCount));
        assertThat(equivalentPlan).isSameAs(first);

        TableInfo propertyChange = tableInfo("orders", 1L, 1, "v2");
        assertThat(cache.getIfPresent(propertyChange)).isSameAs(first);
        KafkaTopicWritePlan propertyPlan =
                cache.getOrCompile(propertyChange, () -> compile(propertyChange, compileCount));
        TableInfo schemaChange = tableInfo("orders", 1L, 2, "v2");
        KafkaTopicWritePlan schemaPlan =
                cache.getOrCompile(schemaChange, () -> compile(schemaChange, compileCount));
        TableInfo recreated = tableInfo("orders", 2L, 2, "v2");
        KafkaTopicWritePlan recreatedPlan =
                cache.getOrCompile(recreated, () -> compile(recreated, compileCount));

        assertThat(propertyPlan).isSameAs(first);
        assertThat(propertyPlan.valueDecoder()).isSameAs(first.valueDecoder());
        assertThat(schemaPlan.valueDecoder()).isNotSameAs(first.valueDecoder());
        assertThat(schemaPlan).isNotSameAs(propertyPlan);
        assertThat(recreatedPlan).isNotSameAs(schemaPlan);
        assertThat(cache.getIfPresent(propertyChange)).isSameAs(propertyPlan);
        assertThat(cache.getIfPresent(schemaChange)).isSameAs(schemaPlan);
        assertThat(cache.getIfPresent(recreated)).isSameAs(recreatedPlan);
        assertThat(compileCount).hasValue(3);
    }

    @Test
    void testInvalidationOnlyRemovesMatchingTableAndSchema() {
        KafkaCompiledWritePlanCache cache = new KafkaCompiledWritePlanCache(10);
        TableInfo original = tableInfo("orders", 1L, 1, "v1");
        TableInfo newSchema = tableInfo("orders", 1L, 2, "v1");
        TableInfo recreated = tableInfo("orders", 2L, 1, "v1");
        KafkaTopicWritePlan first = cache.getOrCompile(original, () -> compile(original));
        KafkaTopicWritePlan newer = cache.getOrCompile(newSchema, () -> compile(newSchema));
        KafkaTopicWritePlan replacement = cache.getOrCompile(recreated, () -> compile(recreated));
        cache.invalidate(tableInfo("orders", 1L, 1, "v2"));
        assertThat(cache.getIfPresent(original)).isNull();
        assertThat(cache.getIfPresent(newSchema)).isSameAs(newer);
        assertThat(cache.getIfPresent(recreated)).isSameAs(replacement);
        assertThat(cache.getOrCompile(original, () -> compile(original))).isNotSameAs(first);
    }

    @Test
    void testFailedCompilationOfNewSchemaKeepsExistingPlan() {
        KafkaCompiledWritePlanCache cache = new KafkaCompiledWritePlanCache(10);
        AtomicInteger compileCount = new AtomicInteger();
        TableInfo original = tableInfo("orders", 1L, 1, "v1");
        KafkaTopicWritePlan originalPlan =
                cache.getOrCompile(original, () -> compile(original, compileCount));
        TableInfo changed = tableInfo("orders", 1L, 2, "v2");
        for (int i = 0; i < 2; i++) {
            assertThatThrownBy(
                            () ->
                                    cache.getOrCompile(
                                            changed,
                                            () -> {
                                                compileCount.incrementAndGet();
                                                throw new IllegalStateException("compile failed");
                                            }))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("compile failed");
            assertThat(cache.getIfPresent(changed)).isNull();
            assertThat(cache.getIfPresent(original)).isSameAs(originalPlan);
        }

        assertThat(cache.getOrCompile(changed, () -> compile(changed, compileCount)))
                .isNotSameAs(originalPlan);
        assertThat(compileCount).hasValue(4);
    }

    @Test
    void testConcurrentColdCompileReusesDecoderWithoutSharingRecordState() throws Exception {
        KafkaCompiledWritePlanCache cache = new KafkaCompiledWritePlanCache(10);
        TableInfo tableInfo = tableInfo("orders", 1L, 1, "v1");
        AtomicInteger compileCount = new AtomicInteger();
        CountDownLatch compilerStarted = new CountDownLatch(1);
        CountDownLatch releaseCompiler = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<KafkaTopicWritePlan>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                int id = i;
                futures.add(
                        executor.submit(
                                () -> {
                                    KafkaTopicWritePlan plan =
                                            cache.getOrCompile(
                                                    tableInfo,
                                                    () -> {
                                                        compileCount.incrementAndGet();
                                                        compilerStarted.countDown();
                                                        await(releaseCompiler);
                                                        return compile(tableInfo);
                                                    });
                                    String json = "{\"id\":" + id + ",\"extra\":" + id + "}";
                                    KafkaDecodedValue decoded =
                                            plan.valueDecoder()
                                                    .decode(
                                                            ByteBuffer.wrap(
                                                                    json.getBytes(
                                                                            StandardCharsets
                                                                                    .UTF_8)));
                                    assertThat(decoded.valueFields()).containsExactly(id);
                                    assertThat(decoded.rescue())
                                            .isEqualTo("{\"extra\":" + id + "}");
                                    return plan;
                                }));
            }
            assertThat(compilerStarted.await(10, TimeUnit.SECONDS)).isTrue();
            releaseCompiler.countDown();

            KafkaTopicWritePlan first = futures.get(0).get(10, TimeUnit.SECONDS);
            for (Future<KafkaTopicWritePlan> future : futures) {
                assertThat(future.get(10, TimeUnit.SECONDS)).isSameAs(first);
            }
            assertThat(compileCount).hasValue(1);
        } finally {
            releaseCompiler.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
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

    @Test
    void testIdleExpirationIsRefreshedByAccess() {
        AtomicLong nanos = new AtomicLong();
        KafkaCompiledWritePlanCache cache = new KafkaCompiledWritePlanCache(10, nanos::get);
        AtomicInteger compileCount = new AtomicInteger();
        TableInfo table = tableInfo("orders", 1L, 1, "v1");
        KafkaTopicWritePlan first = cache.getOrCompile(table, () -> compile(table, compileCount));

        nanos.addAndGet(Duration.ofMinutes(4).toNanos());
        assertThat(cache.getOrCompile(table, () -> compile(table, compileCount))).isSameAs(first);
        nanos.addAndGet(Duration.ofMinutes(4).toNanos());
        assertThat(cache.getIfPresent(table)).isSameAs(first);
        nanos.addAndGet(Duration.ofMinutes(5).toNanos());
        assertThat(cache.getIfPresent(table)).isNull();
        assertThat(cache.getOrCompile(table, () -> compile(table, compileCount)))
                .isNotSameAs(first);
        assertThat(compileCount).hasValue(2);
    }

    private static KafkaTopicWritePlan compile(TableInfo tableInfo, AtomicInteger compileCount) {
        compileCount.incrementAndGet();
        return compile(tableInfo);
    }

    private static KafkaTopicWritePlan compile(TableInfo tableInfo) {
        KafkaTableSchema schema = KafkaTableSchema.resolve(tableInfo.toTableDescriptor());
        return new KafkaTopicWritePlan(schema, new KafkaFormatFactoryRegistry());
    }

    private static TableInfo tableInfo(
            String tableName, long tableId, int schemaId, String propertyValue) {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("_kafka_rescue", DataTypes.STRING())
                                        .column("_kafka_key", DataTypes.BYTES())
                                        .build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_LOG_FORMAT, LogFormat.ARROW)
                        .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE,
                                KafkaValueConversionType.PARSE_JSON)
                        .property(
                                ConfigOptions.TABLE_KAFKA_CONVERT_KEY_TYPE.key(),
                                propertyValue.equals("v1") ? "binary" : "BINARY")
                        .build();
        return TableInfo.of(
                TablePath.of("kafka", tableName), tableId, schemaId, descriptor, null, 1L, 1L);
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for test compiler.", e);
        }
    }
}
