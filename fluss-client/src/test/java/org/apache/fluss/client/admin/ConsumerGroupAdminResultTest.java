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
        CompletableFuture<AlterConsumerGroupOffsetsResult.BucketOutcome> first =
                CompletableFuture.completedFuture(
                        new AlterConsumerGroupOffsetsResult.BucketOutcome(12L, null, null));
        CompletableFuture<AlterConsumerGroupOffsetsResult.BucketOutcome> second =
                new CompletableFuture<>();
        Map<TableBucket, CompletableFuture<AlterConsumerGroupOffsetsResult.BucketOutcome>> futures =
                new LinkedHashMap<>();
        futures.put(TABLE_BUCKET, first);
        futures.put(new TableBucket(2L, null, 0), second);
        AlterConsumerGroupOffsetsResult result = new AlterConsumerGroupOffsetsResult(futures);

        assertThat(result.bucketResult(TABLE_BUCKET).isDone()).isTrue();
        assertThat(result.bucketResult(TABLE_BUCKET).join().isSuccess()).isTrue();
        assertThat(result.bucketResult(TABLE_BUCKET).join().newOffset()).isEqualTo(12L);
        assertThat(result.all()).isNotDone();
        second.complete(new AlterConsumerGroupOffsetsResult.BucketOutcome(3L, null, null));
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
    void testBucketOutcomeDefaults() {
        AlterConsumerGroupOffsetsResult.BucketOutcome success =
                new AlterConsumerGroupOffsetsResult.BucketOutcome(5L, null, null);
        assertThat(success.errorCode()).isEqualTo("NONE");
        assertThat(success.errorMessage()).isEmpty();
        assertThat(success.isSuccess()).isTrue();

        AlterConsumerGroupOffsetsResult.BucketOutcome failure =
                new AlterConsumerGroupOffsetsResult.BucketOutcome(
                        -1L, "CONSUMER_GROUP_ID_NOT_FOUND", null);
        assertThat(failure.isSuccess()).isFalse();
        assertThat(failure.errorMessage()).isEmpty();
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
}
