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

package org.apache.fluss.server.replica;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.MergeMode;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.assertj.core.api.Assertions.assertThat;

/** Protocol compatibility for tables using ordered keys. */
class OrderedKeyProtocolTest extends ReplicaTestBase {

    @Test
    void testOrderedTableRejectsOldPutVersion() throws Exception {
        TablePath path = TablePath.of("test_db", "ordered_table");
        long tableId =
                registerTableInZkClient(
                        path,
                        DATA1_SCHEMA_PK,
                        2998233L,
                        Collections.singletonList("a"),
                        Collections.singletonMap(
                                ConfigOptions.TABLE_KV_ORDERED_KEY_ENABLED.key(), "true"));
        TableBucket bucket = new TableBucket(tableId, 0);
        makeKvTableAsLeader(tableId, path, 0);
        CompletableFuture<List<PutKvResultForBucket>> response = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                20000,
                1,
                Collections.singletonMap(bucket, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)),
                null,
                MergeMode.DEFAULT,
                ApiKeys.PUT_KV,
                (short) 3,
                response::complete);
        PutKvResultForBucket result = response.get(10, TimeUnit.SECONDS).get(0);
        assertThat(result.getError().error()).isEqualTo(Errors.UNSUPPORTED_VERSION);
        assertThat(replicaManager.getReplicaOrException(bucket).getLocalLogEndOffset()).isZero();
    }
}
