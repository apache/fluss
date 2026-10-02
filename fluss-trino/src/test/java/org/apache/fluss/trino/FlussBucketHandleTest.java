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

package org.apache.fluss.trino;

import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests physical bucket identity independently of a table's schema identity. */
final class FlussBucketHandleTest {
    @Test
    void testPartitionIdentityRoundTripAndConversion() {
        FlussBucketHandle bucket = new FlussBucketHandle(42, Optional.of(7L), 2);
        FlussBucketHandle copy =
                jsonCodec(FlussBucketHandle.class)
                        .fromJson(jsonCodec(FlussBucketHandle.class).toJson(bucket));
        assertThat(copy).isEqualTo(bucket).hasSameHashCodeAs(bucket);
        assertThat(copy.toTableBucket()).isEqualTo(new TableBucket(42, 7L, 2));
        assertThat(copy.getRequiredPartitionId()).isEqualTo(7);
        assertThat(copy).isNotEqualTo(new FlussBucketHandle(42, Optional.of(8L), 2));
        assertThat(copy).isNotEqualTo(new FlussBucketHandle(43, Optional.of(7L), 2));
        assertThat(copy).isNotEqualTo(new FlussBucketHandle(42, Optional.of(7L), 3));
        FlussBucketHandle unpartitioned = new FlussBucketHandle(42, Optional.empty(), 2);
        assertThat(unpartitioned.toTableBucket()).isEqualTo(new TableBucket(42, 2));
        assertThat(unpartitioned).isNotEqualTo(copy);
        assertThatThrownBy(unpartitioned::getRequiredPartitionId)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testInvalidIdentifiers() {
        assertThatThrownBy(() -> new FlussBucketHandle(-1, Optional.empty(), 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FlussBucketHandle(1, Optional.of(-1L), 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FlussBucketHandle(1, Optional.empty(), -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testPartitionNameAndIdMustAgree() {
        FlussBucketHandle bucket = new FlussBucketHandle(42, Optional.of(7L), 0);
        assertThat(
                        new FlussPhysicalBucket(bucket, Optional.of("region=us"))
                                .getRequiredPartitionName())
                .isEqualTo("region=us");
        assertThatThrownBy(() -> new FlussPhysicalBucket(bucket, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new FlussPhysicalBucket(
                                        new FlussBucketHandle(42, Optional.empty(), 0),
                                        Optional.of("region=us")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
