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

package org.apache.fluss.metadata;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link BucketInfo}. */
class BucketInfoTest {

    @Test
    void testBucketInfoWithPartitionAndLeader() {
        TablePath tablePath = TablePath.of("db", "table");
        List<Integer> replicas = new ArrayList<>(Arrays.asList(1, 2, 3));
        List<Integer> isr = new ArrayList<>(Arrays.asList(1, 3));

        BucketInfo bucketInfo =
                new BucketInfo(tablePath, 10L, 100L, "p1", 0, 1, 7, 8, replicas, isr);

        assertThat(bucketInfo.getTablePath()).isEqualTo(tablePath);
        assertThat(bucketInfo.getTableId()).isEqualTo(10L);
        assertThat(bucketInfo.getPartitionId()).hasValue(100L);
        assertThat(bucketInfo.getPartitionName()).isEqualTo("p1");
        assertThat(bucketInfo.getBucketId()).isEqualTo(0);
        assertThat(bucketInfo.getLeaderId()).hasValue(1);
        assertThat(bucketInfo.getLeaderEpoch()).hasValue(7);
        assertThat(bucketInfo.getBucketEpoch()).hasValue(8);
        assertThat(bucketInfo.getReplicas()).containsExactly(1, 2, 3);
        assertThat(bucketInfo.getIsr()).containsExactly(1, 3);

        replicas.add(4);
        isr.clear();
        assertThat(bucketInfo.getReplicas()).containsExactly(1, 2, 3);
        assertThat(bucketInfo.getIsr()).containsExactly(1, 3);
        assertThatThrownBy(() -> bucketInfo.getReplicas().add(4))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> bucketInfo.getIsr().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testBucketInfoWithoutPartitionAndLeader() {
        BucketInfo bucketInfo =
                new BucketInfo(
                        TablePath.of("db", "table"),
                        10L,
                        null,
                        null,
                        0,
                        null,
                        null,
                        null,
                        Collections.singletonList(1),
                        Collections.emptyList());

        assertThat(bucketInfo.getPartitionId()).isEmpty();
        assertThat(bucketInfo.getPartitionName()).isNull();
        assertThat(bucketInfo.getLeaderId()).isEmpty();
        assertThat(bucketInfo.getLeaderEpoch()).isEmpty();
        assertThat(bucketInfo.getBucketEpoch()).isEmpty();
        assertThat(bucketInfo.getReplicas()).containsExactly(1);
        assertThat(bucketInfo.getIsr()).isEmpty();
    }

    @Test
    void testBucketInfoRejectsNullRequiredFields() {
        TablePath tablePath = TablePath.of("db", "table");

        assertThatThrownBy(
                        () ->
                                new BucketInfo(
                                        null,
                                        10L,
                                        null,
                                        null,
                                        0,
                                        null,
                                        null,
                                        null,
                                        Collections.singletonList(1),
                                        Collections.emptyList()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("tablePath should not be null");
        assertThatThrownBy(
                        () ->
                                new BucketInfo(
                                        tablePath,
                                        10L,
                                        null,
                                        null,
                                        0,
                                        null,
                                        null,
                                        null,
                                        null,
                                        Collections.emptyList()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("replicas should not be null");
        assertThatThrownBy(
                        () ->
                                new BucketInfo(
                                        tablePath,
                                        10L,
                                        null,
                                        null,
                                        0,
                                        null,
                                        null,
                                        null,
                                        Collections.singletonList(1),
                                        null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("isr should not be null");
    }
}
