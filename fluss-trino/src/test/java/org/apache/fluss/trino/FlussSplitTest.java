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

import io.airlift.json.JsonCodec;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the complete physical split protocol sent to workers. */
final class FlussSplitTest {
    @Test
    void testJsonRoundTrip() {
        JsonCodec<FlussSplit> codec = jsonCodec(FlussSplit.class);
        for (Optional<Long> partition : Arrays.asList(Optional.<Long>empty(), Optional.of(9L))) {
            FlussBucketHandle bucket = new FlussBucketHandle(42, partition, 2);
            for (FlussSplit split :
                    Arrays.asList(
                            FlussSplit.forLog(bucket, 11, Long.MAX_VALUE),
                            FlussSplit.forKv(bucket))) {
                FlussSplit copy = codec.fromJson(codec.toJson(split));
                assertThat(copy.getScanType()).isEqualTo(split.getScanType());
                assertThat(copy.getBucket()).isEqualTo(bucket);
                assertThat(copy.getLogRange()).isEqualTo(split.getLogRange());
                assertThat(copy.isRemotelyAccessible()).isTrue();
                assertThat(copy.getAddresses()).isEmpty();
                assertThat(copy.getRetainedSizeInBytes()).isPositive();
            }
        }
    }

    @Test
    void testScanTypeRequiresMatchingRange() {
        FlussBucketHandle bucket = new FlussBucketHandle(42, Optional.empty(), 0);
        assertThatThrownBy(() -> new FlussSplit(FlussScanType.LOG, bucket, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new FlussSplit(
                                        FlussScanType.KV,
                                        bucket,
                                        Optional.of(new FlussLogRange(0, 1))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FlussSplit.forKv(bucket).getRequiredLogRange())
                .isInstanceOf(IllegalStateException.class);
        assertThat(FlussSplit.forLog(bucket, 1000, 1000).getRequiredLogRange().isEmpty()).isTrue();
    }

    @Test
    void testInvalidJson() {
        JsonCodec<FlussSplit> codec = jsonCodec(FlussSplit.class);
        String bucket = "\"bucket\":{\"tableId\":42,\"partitionId\":null,\"bucketId\":0}";
        for (String json :
                Arrays.asList(
                        "{" + bucket + "}",
                        "{\"scanType\":\"UNKNOWN\"," + bucket + "}",
                        "{\"scanType\":\"LOG\"," + bucket + "}",
                        "{\"scanType\":\"KV\",\"bucket\":null}",
                        "{\"scanType\":\"KV\","
                                + bucket
                                + ",\"logRange\":{\"startOffset\":0,\"stoppingOffset\":1}}")) {
            assertThatThrownBy(() -> codec.fromJson(json))
                    .as(json)
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(codec.fromJson("{\"scanType\":\"KV\"," + bucket + "}").getLogRange()).isEmpty();
    }

    @Test
    void testLogJsonRequiresBothOffsets() {
        JsonCodec<FlussSplit> codec = jsonCodec(FlussSplit.class);
        assertThatThrownBy(
                        () ->
                                codec.fromJson(
                                        "{\"scanType\":\"LOG\",\"bucket\":{\"tableId\":42,\"bucketId\":0},\"logRange\":{\"stoppingOffset\":1}}"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
