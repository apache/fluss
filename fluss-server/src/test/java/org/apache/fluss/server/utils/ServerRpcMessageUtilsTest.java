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
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.server.entity.PutKvDataForBucket;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.apache.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ServerRpcMessageUtils}. */
class ServerRpcMessageUtilsTest {

    @Test
    void testDecodeHistoricalPutKvRequest() throws Exception {
        long tableId = 1L;
        long partitionId = 2L;
        KvRecordBatch records = genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE);
        PutKvRequest request = newPutKvRequest(tableId, 0, 1, records);
        PbPutKvReqForBucket bucketRequest = request.getBucketsReqsList().get(0);
        bucketRequest.setPartitionId(partitionId).setOriginalPartitionName("dt=2025-01-01");

        TableBucket tableBucket = new TableBucket(tableId, partitionId, 0);
        Map<TableBucket, PutKvDataForBucket> decoded =
                ServerRpcMessageUtils.toPutKvDataForBuckets(request);
        assertThat(decoded).containsOnlyKeys(tableBucket);
        assertThat(decoded.get(tableBucket).originalPartitionName()).isEqualTo("dt=2025-01-01");
        assertThat(decoded.get(tableBucket).records()).isEqualTo(records);

        request.addAllBucketsReqs(Collections.singletonList(bucketRequest));
        assertThatThrownBy(() -> ServerRpcMessageUtils.toPutKvDataForBuckets(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate table bucket");
    }
}
