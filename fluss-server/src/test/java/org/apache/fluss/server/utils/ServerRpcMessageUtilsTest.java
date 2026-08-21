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

package org.apache.fluss.server.utils;

import org.apache.fluss.rpc.messages.PbBucketMetadata;
import org.apache.fluss.rpc.messages.PbPartitionMetadata;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.metadata.PartitionMetadata;
import org.apache.fluss.server.metadata.TableMetadata;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.server.metadata.BucketMetadata.NO_LEADER_ISR_STATE_EPOCH;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getUpdateMetadataRequestData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeUpdateMetadataRequest;
import static org.assertj.core.api.Assertions.assertThat;

class ServerRpcMessageUtilsTest {

    @Test
    void testAuthoritativeBucketMetadataTableAndPartitionRoundTrip() {
        List<BucketMetadata> bucketMetadata =
                Arrays.asList(
                        new BucketMetadata(0, 1, 2, Arrays.asList(1, 2, 3), Arrays.asList(1, 2), 3),
                        new BucketMetadata(
                                1,
                                null,
                                null,
                                Arrays.asList(1, 2, 3),
                                Collections.emptyList(),
                                NO_LEADER_ISR_STATE_EPOCH),
                        new BucketMetadata(
                                2, 2, 4, Arrays.asList(2, 3), Collections.emptyList(), 5));
        TableMetadata tableMetadata = new TableMetadata(DATA1_TABLE_INFO, bucketMetadata);
        PartitionMetadata partitionMetadata =
                new PartitionMetadata(DATA1_TABLE_INFO.getTableId(), "p1", 10L, bucketMetadata);

        UpdateMetadataRequest request =
                makeUpdateMetadataRequest(
                        null,
                        null,
                        Collections.emptySet(),
                        Collections.singletonList(tableMetadata),
                        Collections.singletonList(partitionMetadata));

        PbTableMetadata pbTableMetadata = request.getTableMetadatasList().get(0);
        assertAuthoritativeWireMetadata(pbTableMetadata.getBucketMetadatasList());
        PbPartitionMetadata pbPartitionMetadata = request.getPartitionMetadatasList().get(0);
        assertAuthoritativeWireMetadata(pbPartitionMetadata.getBucketMetadatasList());

        ClusterMetadata decoded = getUpdateMetadataRequestData(request);
        assertThat(decoded.getTableMetadataList().get(0).getBucketMetadataList())
                .containsExactlyElementsOf(bucketMetadata);
        assertThat(decoded.getPartitionMetadataList().get(0).getBucketMetadataList())
                .containsExactlyElementsOf(bucketMetadata);
    }

    @Test
    void testLegacyBucketMetadataRoundTripOmitsBucketEpoch() {
        BucketMetadata legacy = new BucketMetadata(0, 1, 2, Arrays.asList(1, 2, 3));
        UpdateMetadataRequest request =
                makeUpdateMetadataRequest(
                        null,
                        null,
                        Collections.emptySet(),
                        Collections.singletonList(
                                new TableMetadata(
                                        DATA1_TABLE_INFO, Collections.singletonList(legacy))),
                        Collections.emptyList());

        PbBucketMetadata pbBucketMetadata =
                request.getTableMetadatasList().get(0).getBucketMetadatasList().get(0);
        assertThat(pbBucketMetadata.hasBucketEpoch()).isFalse();
        assertThat(pbBucketMetadata.getIsrIds()).isEmpty();

        BucketMetadata decoded =
                getUpdateMetadataRequestData(request)
                        .getTableMetadataList()
                        .get(0)
                        .getBucketMetadataList()
                        .get(0);
        assertThat(decoded).isEqualTo(legacy);
        assertThat(decoded.getBucketEpoch()).isNull();
    }

    private static void assertAuthoritativeWireMetadata(List<PbBucketMetadata> bucketMetadata) {
        assertThat(bucketMetadata.get(0).getBucketId()).isEqualTo(0);
        assertThat(bucketMetadata.get(0).getLeaderId()).isEqualTo(1);
        assertThat(bucketMetadata.get(0).getLeaderEpoch()).isEqualTo(2);
        assertThat(bucketMetadata.get(0).getReplicaIds()).containsExactly(1, 2, 3);
        assertThat(bucketMetadata.get(0).hasBucketEpoch()).isTrue();
        assertThat(bucketMetadata.get(0).getBucketEpoch()).isEqualTo(3);
        assertThat(bucketMetadata.get(0).getIsrIds()).containsExactly(1, 2);

        assertThat(bucketMetadata.get(1).hasBucketEpoch()).isTrue();
        assertThat(bucketMetadata.get(1).getBucketEpoch()).isEqualTo(NO_LEADER_ISR_STATE_EPOCH);
        assertThat(bucketMetadata.get(1).getIsrIds()).isEmpty();

        assertThat(bucketMetadata.get(2).hasBucketEpoch()).isTrue();
        assertThat(bucketMetadata.get(2).getBucketEpoch()).isEqualTo(5);
        assertThat(bucketMetadata.get(2).getIsrIds()).isEmpty();
    }
}
