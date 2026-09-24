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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.testutils.common.MultiVersionTest;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests full-cache routing and stable replica selection. */
@MultiVersionTest
class FullCacheLookupInputPartitionerTest {

    private static final RowType KEY_TYPE =
            RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});

    @Test
    void testRoutingAlwaysSelectsBucketOwner() {
        for (int buckets : new int[] {1, 3, 8}) {
            FlussLookupInputPartitioner bucketPartitioner = bucketPartitioner(buckets);
            FullCacheLookupInputPartitioner partitioner =
                    new FullCacheLookupInputPartitioner(bucketPartitioner, buckets);
            for (int parallelism : new int[] {1, 2, 3, 4, 11}) {
                for (int id = 0; id < 100; id++) {
                    RowData key = GenericRowData.of(id);
                    int target = partitioner.partition(key, parallelism);
                    assertThat(partitioner.ownedBuckets(target, parallelism))
                            .contains(bucketPartitioner.partition(key, buckets));
                }
            }
        }
    }

    @Test
    void testReplicaIsIndependentOfRowKindRepresentationAndSerialization() throws Exception {
        FullCacheLookupInputPartitioner partitioner =
                new FullCacheLookupInputPartitioner(bucketPartitioner(3), 3);
        RowDataSerializer serializer = new RowDataSerializer(KEY_TYPE);
        // Initialize transient encoders before serializing, as happens during plan reuse.
        partitioner.partition(GenericRowData.of(0), 11);
        FullCacheLookupInputPartitioner restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(partitioner),
                        getClass().getClassLoader());
        for (int id = 0; id < 100; id++) {
            GenericRowData key = GenericRowData.of(id);
            int expected = partitioner.partition(key, 11);
            for (RowKind kind : RowKind.values()) {
                key.setRowKind(kind);
                assertThat(partitioner.partition(key, 11)).isEqualTo(expected);
                assertThat(partitioner.partition(serializer.toBinaryRow(key), 11))
                        .isEqualTo(expected);
                assertThat(restored.partition(key, 11)).isEqualTo(expected);
            }
        }
        assertThat(partitioner.isDeterministic()).isTrue();
        assertThat(partitioner.partition(GenericRowData.of((Object) null), 11)).isZero();
    }

    private static FlussLookupInputPartitioner bucketPartitioner(int buckets) {
        return new FlussLookupInputPartitioner(
                LookupNormalizer.createPrimaryKeyLookupNormalizer(new int[] {0}, KEY_TYPE),
                KEY_TYPE,
                Collections.singletonList("id"),
                null,
                buckets);
    }
}
