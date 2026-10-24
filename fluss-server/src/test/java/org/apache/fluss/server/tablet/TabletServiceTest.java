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

package org.apache.fluss.server.tablet;

import org.apache.fluss.exception.InvalidBucketRoutingException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsRequestBucket;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsRequestTable;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsResponseBucket;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsResponseTable;
import org.apache.fluss.rpc.protocol.Errors;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TabletService}. */
class TabletServiceTest {

    @Test
    void testEmptyCommitConsumerOffsetsRequestProducesEmptyResponse() {
        CommitConsumerOffsetsRequest emptyRequest =
                new CommitConsumerOffsetsRequest().setGroupId("group-1");
        assertThat(TabletService.decodeCommitConsumerOffsetsRequest(emptyRequest)).isEmpty();

        PbCommitConsumerOffsetsRequestTable emptyTable =
                new PbCommitConsumerOffsetsRequestTable().setTableId(1L);
        CommitConsumerOffsetsRequest requestWithEmptyTable =
                new CommitConsumerOffsetsRequest()
                        .setGroupId("group-1")
                        .addAllTables(Collections.singletonList(emptyTable));
        assertThat(TabletService.decodeCommitConsumerOffsetsRequest(requestWithEmptyTable))
                .isEmpty();

        CommitConsumerOffsetsResponse response =
                TabletService.buildCommitConsumerOffsetsResponse(Collections.emptyMap());
        assertThat(response.getTablesList()).isEmpty();
    }

    @Test
    void testCommitConsumerOffsetsRequestRejectsDuplicateBucket() {
        PbCommitConsumerOffsetsRequestBucket first =
                new PbCommitConsumerOffsetsRequestBucket().setBucketId(1).setOffset(10L);
        PbCommitConsumerOffsetsRequestBucket duplicate =
                new PbCommitConsumerOffsetsRequestBucket().setBucketId(1).setOffset(20L);
        PbCommitConsumerOffsetsRequestTable table =
                new PbCommitConsumerOffsetsRequestTable()
                        .setTableId(2L)
                        .addAllBuckets(Arrays.asList(first, duplicate));
        CommitConsumerOffsetsRequest request =
                new CommitConsumerOffsetsRequest()
                        .setGroupId("group-1")
                        .addAllTables(Collections.singletonList(table));

        assertThatThrownBy(() -> TabletService.decodeCommitConsumerOffsetsRequest(request))
                .isInstanceOf(InvalidBucketRoutingException.class)
                .hasMessageContaining("Duplicate table bucket")
                .hasMessageContaining("tableId=2")
                .hasMessageContaining("bucket=1");
    }

    @Test
    void testCommitConsumerOffsetsResponsePreservesRequestOrder() {
        Map<TableBucket, Errors> results = new LinkedHashMap<>();
        results.put(new TableBucket(2L, 3), Errors.NONE);
        results.put(new TableBucket(2L, 1), Errors.NOT_CONSUMER_GROUP_OWNER);
        results.put(new TableBucket(1L, 2), Errors.NONE);

        CommitConsumerOffsetsResponse response =
                TabletService.buildCommitConsumerOffsetsResponse(results);

        List<PbCommitConsumerOffsetsResponseTable> tables = response.getTablesList();
        assertThat(tables)
                .extracting(PbCommitConsumerOffsetsResponseTable::getTableId)
                .containsExactly(2L, 1L);

        List<PbCommitConsumerOffsetsResponseBucket> tableTwoBuckets =
                tables.get(0).getBucketsList();
        assertThat(tableTwoBuckets)
                .extracting(PbCommitConsumerOffsetsResponseBucket::getBucketId)
                .containsExactly(3, 1);
        assertThat(tableTwoBuckets.get(0).hasErrorCode()).isFalse();
        assertThat(tableTwoBuckets.get(1).getErrorCode())
                .isEqualTo(Errors.NOT_CONSUMER_GROUP_OWNER.code());

        assertThat(tables.get(1).getBucketsList())
                .extracting(PbCommitConsumerOffsetsResponseBucket::getBucketId)
                .containsExactly(2);
    }
}
