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
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.entity.PrefixLookupResultForBucket;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.messages.PbPrefixLookupKey;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_2;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests protocol compatibility across KV formats. */
class KvFormatCompatibilityTest extends ReplicaTestBase {

    @Test
    void testKvFormatV2TableRejectV0Client() throws Exception {
        // Test that old client version (version 0) is rejected for new format tables
        // with non-default bucket key. This is because new tables use CompactedKeyEncoder
        // which is incompatible with old clients that expect lake's encoder.
        TablePath tablePath = TablePath.of("test_db", "new_format_table");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.STRING())
                        .primaryKey("a", "b")
                        .build();
        RowType rowType = schema.getRowType();
        RowType keyType =
                DataTypes.ROW(
                        new DataField("a", DataTypes.INT()),
                        new DataField("b", DataTypes.STRING()));

        // Create table with:
        // 1. kv format version 2 (new format)
        // 2. non-default bucket key (a is subset of pk (a, b))
        // 3. data lake format (paimon)
        Map<String, String> properties = new HashMap<>();
        properties.put(
                ConfigOptions.TABLE_KV_FORMAT_VERSION.key(), String.valueOf(KV_FORMAT_VERSION_2));
        properties.put(ConfigOptions.TABLE_DATALAKE_FORMAT.key(), DataLakeFormat.PAIMON.toString());
        long tableId =
                registerTableInZkClient(
                        tablePath,
                        schema,
                        2998232L,
                        Collections.singletonList("a"), // bucket key is subset of pk
                        properties);
        TableBucket tb = new TableBucket(tableId, 0);
        makeKvTableAsLeader(tableId, tablePath, tb.getBucket());

        List<Tuple2<Object[], Object[]>> data =
                Collections.singletonList(
                        Tuple2.of(new Object[] {1, "a"}, new Object[] {1, "a", "value1"}));

        // Test putKvRecords with old client version (version 0) - should fail
        short oldClientVersion = 0;
        CompletableFuture<List<PutKvResultForBucket>> putFuture = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                20000,
                1,
                Collections.singletonMap(tb, genKvRecordBatch(keyType, rowType, data)),
                null,
                MergeMode.DEFAULT,
                ApiKeys.PUT_KV,
                oldClientVersion,
                putFuture::complete);
        PutKvResultForBucket putResult = putFuture.get().get(0);
        assertThat(putResult.failed()).isTrue();
        assertThat(putResult.getErrorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
        assertThat(putResult.getErrorMessage()).contains("Client API version 0 is not supported");

        // Test lookups with old client version (version 0) - should fail
        CompactedKeyEncoder keyEncoder = new CompactedKeyEncoder(rowType, new int[] {0, 1});
        byte[] keyBytes = keyEncoder.encodeKey(row(1, "a"));
        CompletableFuture<Map<TableBucket, LookupResultForBucket>> lookupFuture =
                new CompletableFuture<>();
        replicaManager.lookups(
                Collections.singletonMap(tb, Collections.singletonList(keyBytes)),
                oldClientVersion,
                lookupFuture::complete);
        LookupResultForBucket lookupResult = lookupFuture.get().get(tb);
        assertThat(lookupResult.failed()).isTrue();
        assertThat(lookupResult.getErrorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
        assertThat(lookupResult.getErrorMessage())
                .contains("Client API version 0 is not supported");

        // Test prefixLookups with old client version (version 0) - should fail
        CompactedKeyEncoder prefixKeyEncoder = new CompactedKeyEncoder(rowType, new int[] {0});
        byte[] prefixKeyBytes = prefixKeyEncoder.encodeKey(row(1));
        CompletableFuture<Map<TableBucket, PrefixLookupResultForBucket>> prefixFuture =
                new CompletableFuture<>();
        replicaManager.prefixLookups(
                Collections.singletonMap(
                        tb,
                        Collections.singletonList(
                                new PbPrefixLookupKey().setPrefixKey(prefixKeyBytes))),
                oldClientVersion,
                prefixFuture::complete);
        PrefixLookupResultForBucket prefixResult = prefixFuture.get().get(tb);
        assertThat(prefixResult.failed()).isTrue();
        assertThat(prefixResult.getError().error()).isEqualTo(Errors.UNSUPPORTED_VERSION);
        assertThat(prefixResult.getError().message())
                .contains("Client API version 0 is not supported");
    }
}
