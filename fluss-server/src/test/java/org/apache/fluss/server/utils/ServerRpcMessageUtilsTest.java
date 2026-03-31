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

import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.PbFetchLogReqForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogReqForTable;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ServerRpcMessageUtils}. */
class ServerRpcMessageUtilsTest {

    @Test
    void testFetchLogDataRejectsVariantProjectionWithoutColumnIndex() {
        FetchLogRequest request = new FetchLogRequest();
        PbFetchLogReqForTable tableReq =
                new PbFetchLogReqForTable().setTableId(1L).setProjectionPushdownEnabled(false);
        tableReq.addVariantFieldProjection().addFieldName("name");
        tableReq.addAllBucketsReqs(
                Collections.singletonList(
                        new PbFetchLogReqForBucket()
                                .setBucketId(0)
                                .setFetchOffset(0L)
                                .setMaxFetchBytes(1024)));
        request.addAllTablesReqs(Collections.singletonList(tableReq));

        assertThatThrownBy(() -> ServerRpcMessageUtils.getFetchLogData(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column_index");
    }
}
