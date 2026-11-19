/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.admin;

import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for consumer group Admin API result models. */
class ConsumerGroupAdminResultTest {

    private static final TableBucket TABLE_BUCKET = new TableBucket(1L, null, 0);

    @Test
    void testAlterConsumerGroupOffsetsAggregatesFutures() {
        CompletableFuture<Void> first = CompletableFuture.completedFuture(null);
        CompletableFuture<Void> second = new CompletableFuture<>();
        Map<TableBucket, CompletableFuture<Void>> futures = new LinkedHashMap<>();
        futures.put(TABLE_BUCKET, first);
        futures.put(new TableBucket(2L, null, 0), second);
        AlterConsumerGroupOffsetsResult result = new AlterConsumerGroupOffsetsResult(futures);

        assertThat(result.bucketResult(TABLE_BUCKET)).isSameAs(first);
        assertThat(result.all()).isNotDone();
        second.complete(null);
        assertThat(result.all()).isCompleted();
    }

    @Test
    void testDeleteConsumerGroupsAggregatesFutures() {
        CompletableFuture<Void> first = CompletableFuture.completedFuture(null);
        CompletableFuture<Void> second = new CompletableFuture<>();
        Map<String, CompletableFuture<Void>> futures = new LinkedHashMap<>();
        futures.put("group-1", first);
        futures.put("group-2", second);
        DeleteConsumerGroupsResult result = new DeleteConsumerGroupsResult(futures);

        assertThat(result.deletedGroup("group-1")).isSameAs(first);
        assertThat(result.all()).isNotDone();
        second.complete(null);
        assertThat(result.all()).isCompleted();
        assertThatThrownBy(() -> result.perGroupFutures().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testDeleteConsumerOffsetsAggregatesFutures() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        DeleteConsumerGroupOffsetsResult result =
                new DeleteConsumerGroupOffsetsResult(
                        Collections.singletonMap(TABLE_BUCKET, future));

        assertThat(result.bucketResult(TABLE_BUCKET)).isSameAs(future);
        assertThat(result.all()).isNotDone();
        future.complete(null);
        assertThat(result.all()).isCompleted();
    }

    @Test
    void testAllCompletesExceptionallyWhenAnyFutureFails() {
        CompletableFuture<Void> failure = new CompletableFuture<>();
        failure.completeExceptionally(new RuntimeException("delete failed"));
        Map<String, CompletableFuture<Void>> futures = new HashMap<>();
        futures.put("group-1", CompletableFuture.completedFuture(null));
        futures.put("group-2", failure);
        DeleteConsumerGroupsResult result = new DeleteConsumerGroupsResult(futures);

        assertThat(result.all()).isCompletedExceptionally();
    }

    @Test
    void testUnknownKeyReturnsNullFuture() {
        DeleteConsumerGroupsResult groupsResult =
                new DeleteConsumerGroupsResult(
                        Collections.singletonMap("group-1", new CompletableFuture<Void>()));
        assertThat(groupsResult.deletedGroup("unknown")).isNull();

        DeleteConsumerGroupOffsetsResult offsetsResult =
                new DeleteConsumerGroupOffsetsResult(
                        Collections.singletonMap(TABLE_BUCKET, new CompletableFuture<Void>()));
        assertThat(offsetsResult.bucketResult(new TableBucket(9L, null, 9))).isNull();

        AlterConsumerGroupOffsetsResult alterResult =
                new AlterConsumerGroupOffsetsResult(
                        Collections.singletonMap(TABLE_BUCKET, new CompletableFuture<Void>()));
        assertThat(alterResult.bucketResult(new TableBucket(9L, null, 9))).isNull();
    }

    @Test
    void testOffsetMetadataNormalizesNullMetadata() {
        assertThat(new OffsetMetadata(42L).getMetadata()).isEmpty();
        assertThat(new OffsetMetadata(42L, null).getMetadata()).isEmpty();
        assertThat(new OffsetMetadata(42L, "checkpoint").getMetadata()).isEqualTo("checkpoint");
        assertThat(new OffsetMetadata(42L, "checkpoint"))
                .isEqualTo(new OffsetMetadata(42L, "checkpoint"));
        assertThat(new OffsetMetadata(42L, "a")).isNotEqualTo(new OffsetMetadata(42L, "b"));
    }

    @Test
    void testOffsetMetadataRejectsNegativeOffsetsInBothConstructors() {
        assertThatThrownBy(() -> new OffsetMetadata(-1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("offset");
        assertThatThrownBy(() -> new OffsetMetadata(-2L, "checkpoint"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("offset");
        assertThat(new OffsetMetadata(0L).getOffset()).isZero();
        assertThat(new OffsetMetadata(0L, "checkpoint").getOffset()).isZero();
    }
}
