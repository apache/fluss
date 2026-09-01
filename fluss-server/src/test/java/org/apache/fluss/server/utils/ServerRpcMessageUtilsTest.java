/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.messages.LookupResponse;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.entity.PutKvDataForBucket;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ServerRpcMessageUtils}. */
class ServerRpcMessageUtilsTest {

    @Test
    void testLookupResponseSupportsMixedValueLayouts() {
        byte[] plainValue = new byte[] {1, 0, 11, 12};
        byte[] taggedValue = new byte[] {0, 0, 0, 0, 0, 0, 0, 7, 1, 0, 21, 22};
        LookupResultForBucket result =
                new LookupResultForBucket(
                        new TableBucket(1L, 0),
                        Arrays.asList(
                                KvValueLayout.PLAIN.toValueBodySlice(plainValue),
                                KvValueLayout.TAGGED.toValueBodySlice(taggedValue)),
                        "dt=2025-01-01");

        LookupResponse response =
                ServerRpcMessageUtils.makeLookupResponse(Collections.singletonList(result));

        assertThat(response.getBucketsRespAt(0).getValuesList())
                .extracting(value -> value.getValues())
                .containsExactly(new byte[] {1, 0, 11, 12}, new byte[] {1, 0, 21, 22});
    }

    @Test
    void testHistoricalPutKvRequestAndResponsePreserveOriginalPartitions() throws Exception {
        long tableId = 1L;
        long partitionId = 2L;
        KvRecordBatch records = genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE);
        PutKvRequest request = newPutKvRequest(tableId, 0, 1, records);
        PbPutKvReqForBucket bucketRequest = request.getBucketsReqsList().get(0);
        bucketRequest.setPartitionId(partitionId).setOriginalPartitionName("dt=2025-01-01");
        request.addAllBucketsReqs(
                Collections.singletonList(
                        new PbPutKvReqForBucket()
                                .copyFrom(bucketRequest)
                                .setOriginalPartitionName("dt=2025-01-02")));

        TableBucket tableBucket = new TableBucket(tableId, partitionId, 0);
        List<PutKvDataForBucket> decoded = ServerRpcMessageUtils.toPutKvDataForBuckets(request);
        assertThat(decoded).extracting(PutKvDataForBucket::tableBucket).containsOnly(tableBucket);
        assertThat(decoded)
                .extracting(PutKvDataForBucket::originalPartitionName)
                .containsExactly("dt=2025-01-01", "dt=2025-01-02");
        assertThat(decoded).extracting(PutKvDataForBucket::records).containsOnly(records);

        PutKvResponse response =
                ServerRpcMessageUtils.makePutKvResponse(
                        Arrays.asList(
                                PutKvResultForBucket.historicalSuccess(
                                        tableBucket, 1L, "dt=2025-01-01"),
                                PutKvResultForBucket.historicalSuccess(
                                        tableBucket, 2L, "dt=2025-01-02")));
        assertThat(response.getBucketsRespsList())
                .extracting(PbPutKvRespForBucket::getOriginalPartitionName)
                .containsExactly("dt=2025-01-01", "dt=2025-01-02");

        request.addAllBucketsReqs(
                Collections.singletonList(new PbPutKvReqForBucket().copyFrom(bucketRequest)));
        assertThatThrownBy(() -> ServerRpcMessageUtils.toPutKvDataForBuckets(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate table bucket");
    }
}
