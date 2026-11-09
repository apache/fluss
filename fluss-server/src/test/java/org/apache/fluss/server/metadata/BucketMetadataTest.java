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

package org.apache.fluss.server.metadata;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BucketMetadataTest {

    @Test
    void testDefensivelyCopiesAndExposesImmutableReplicaAndIsrLists() {
        List<Integer> replicas = new ArrayList<>(Arrays.asList(1, 2, 3));
        List<Integer> isr = new ArrayList<>(Arrays.asList(1, 2));
        BucketMetadata metadata = new BucketMetadata(0, 1, 2, replicas, isr, 3);

        replicas.add(4);
        isr.add(3);

        assertThat(metadata.getReplicas()).containsExactly(1, 2, 3);
        assertThat(metadata.getIsr()).containsExactly(1, 2);
        assertThatThrownBy(() -> metadata.getReplicas().add(4))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> metadata.getIsr().add(3))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testEqualityHashCodeAndStringIncludeAuthoritativeState() {
        BucketMetadata metadata =
                new BucketMetadata(0, 1, 2, Arrays.asList(1, 2, 3), Arrays.asList(1, 2), 3);
        BucketMetadata sameMetadata =
                new BucketMetadata(0, 1, 2, Arrays.asList(1, 2, 3), Arrays.asList(1, 2), 3);
        BucketMetadata differentIsr =
                new BucketMetadata(
                        0, 1, 2, Arrays.asList(1, 2, 3), Collections.singletonList(1), 3);
        BucketMetadata differentEpoch =
                new BucketMetadata(0, 1, 2, Arrays.asList(1, 2, 3), Arrays.asList(1, 2), 4);

        assertThat(metadata).isEqualTo(sameMetadata).hasSameHashCodeAs(sameMetadata);
        assertThat(metadata).isNotEqualTo(differentIsr).isNotEqualTo(differentEpoch);
        assertThat(metadata.toString()).contains("isr=[1, 2]", "bucketEpoch=3");
    }

    @Test
    void testAuthoritativeEmptyIsrDiffersFromLegacyUnknownIsr() {
        BucketMetadata authoritative =
                new BucketMetadata(
                        0,
                        null,
                        null,
                        Arrays.asList(1, 2, 3),
                        Collections.emptyList(),
                        BucketMetadata.NO_LEADER_ISR_STATE_EPOCH);
        BucketMetadata legacy = new BucketMetadata(0, null, null, Arrays.asList(1, 2, 3));

        assertThat(authoritative.getBucketEpoch())
                .isEqualTo(BucketMetadata.NO_LEADER_ISR_STATE_EPOCH);
        assertThat(legacy.getBucketEpoch()).isNull();
        assertThat(authoritative).isNotEqualTo(legacy);
    }
}
