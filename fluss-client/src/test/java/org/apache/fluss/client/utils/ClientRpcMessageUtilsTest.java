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

package org.apache.fluss.client.utils;

import org.apache.fluss.client.write.KvWriteBatch;
import org.apache.fluss.client.write.ReadyWriteBatch;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.PreAllocatedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.protocol.MergeMode;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ClientRpcMessageUtils}.
 *
 * <p>Focuses on MergeMode consistency validation in makePutKvRequest.
 */
class ClientRpcMessageUtilsTest {

    private static final long TABLE_ID = DATA1_TABLE_ID_PK;
    private static final int ACKS = 1;
    private static final int TIMEOUT_MS = 30000;

    @Test
    void testMakePutKvRequestWithConsistentMergeMode() throws Exception {
        // Create two batches with same mergeMode (DEFAULT)
        KvWriteBatch batch1 = createKvWriteBatch(0, MergeMode.DEFAULT);
        KvWriteBatch batch2 = createKvWriteBatch(1, MergeMode.DEFAULT);

        List<ReadyWriteBatch> readyBatches =
                Arrays.asList(
                        new ReadyWriteBatch(new TableBucket(TABLE_ID, 0), batch1),
                        new ReadyWriteBatch(new TableBucket(TABLE_ID, 1), batch2));

        // Should succeed without exception
        PutKvRequest request =
                ClientRpcMessageUtils.makePutKvRequest(TABLE_ID, ACKS, TIMEOUT_MS, readyBatches);

        // Verify mergeMode is set correctly in request
        assertThat(request.hasAggMode()).isTrue();
        assertThat(request.getAggMode()).isEqualTo(MergeMode.DEFAULT.getProtoValue());
    }

    @Test
    void testMakePutKvRequestWithOverwriteMode() throws Exception {
        // Create batches with OVERWRITE mode
        KvWriteBatch batch1 = createKvWriteBatch(0, MergeMode.OVERWRITE);
        KvWriteBatch batch2 = createKvWriteBatch(1, MergeMode.OVERWRITE);

        List<ReadyWriteBatch> readyBatches =
                Arrays.asList(
                        new ReadyWriteBatch(new TableBucket(TABLE_ID, 0), batch1),
                        new ReadyWriteBatch(new TableBucket(TABLE_ID, 1), batch2));

        PutKvRequest request =
                ClientRpcMessageUtils.makePutKvRequest(TABLE_ID, ACKS, TIMEOUT_MS, readyBatches);

        // Verify OVERWRITE mode is set correctly
        assertThat(request.hasAggMode()).isTrue();
        assertThat(request.getAggMode()).isEqualTo(MergeMode.OVERWRITE.getProtoValue());
    }

    @Test
    void testMakePutKvRequestWithInconsistentMergeMode() throws Exception {
        // Create batches with different mergeModes
        KvWriteBatch defaultBatch = createKvWriteBatch(0, MergeMode.DEFAULT);
        KvWriteBatch overwriteBatch = createKvWriteBatch(1, MergeMode.OVERWRITE);

        List<ReadyWriteBatch> readyBatches =
                Arrays.asList(
                        new ReadyWriteBatch(new TableBucket(TABLE_ID, 0), defaultBatch),
                        new ReadyWriteBatch(new TableBucket(TABLE_ID, 1), overwriteBatch));

        // Should throw exception due to inconsistent mergeMode
        assertThatThrownBy(
                        () ->
                                ClientRpcMessageUtils.makePutKvRequest(
                                        TABLE_ID, ACKS, TIMEOUT_MS, readyBatches))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "All the write batches to make put kv request should have the same mergeMode")
                .hasMessageContaining("DEFAULT")
                .hasMessageContaining("OVERWRITE");
    }

    @Test
    void testMakePutKvRequestWithSingleBatch() throws Exception {
        // Single batch should always succeed
        KvWriteBatch batch = createKvWriteBatch(0, MergeMode.OVERWRITE);

        List<ReadyWriteBatch> readyBatches =
                Collections.singletonList(new ReadyWriteBatch(new TableBucket(TABLE_ID, 0), batch));

        PutKvRequest request =
                ClientRpcMessageUtils.makePutKvRequest(TABLE_ID, ACKS, TIMEOUT_MS, readyBatches);

        assertThat(request.getAggMode()).isEqualTo(MergeMode.OVERWRITE.getProtoValue());
    }

    private KvWriteBatch createKvWriteBatch(int bucketId, MergeMode mergeMode) throws Exception {
        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        PreAllocatedPagedOutputView outputView =
                new PreAllocatedPagedOutputView(Collections.singletonList(segment));
        return new KvWriteBatch(
                DATA1_TABLE_ID_PK,
                bucketId,
                PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                DATA1_TABLE_INFO_PK.getSchemaId(),
                KvFormat.COMPACTED,
                Integer.MAX_VALUE,
                outputView,
                null,
                mergeMode,
                System.currentTimeMillis());
    }
}
