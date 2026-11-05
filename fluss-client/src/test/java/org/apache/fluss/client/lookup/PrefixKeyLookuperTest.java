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

package org.apache.fluss.client.lookup;

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.messages.PbPrefixLookupRespForBucket;
import org.apache.fluss.rpc.messages.PbValueList;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.messages.PrefixLookupResponse;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.fluss.client.metadata.TestingMetadataUpdater.COORDINATOR;
import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE1;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PrefixKeyLookuper}. */
class PrefixKeyLookuperTest {

    private static final TablePath TABLE_PATH = TablePath.of("test_db", "legacy_paimon_table");
    private static final long TABLE_ID = 1L;
    private static final byte[] FIRST = "abcdefghij".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SECOND = "klmnopqrst".getBytes(StandardCharsets.UTF_8);

    @Test
    void testClientFilterKeepsStringKeyWhenRowIsReused() throws Exception {
        assertClientFilterKeepsKeyWhenRowIsReused(DataTypes.STRING(), BinaryString::fromBytes);
    }

    @Test
    void testClientFilterKeepsBinaryKeyWhenRowIsReused() throws Exception {
        assertClientFilterKeepsKeyWhenRowIsReused(DataTypes.BYTES(), bytes -> bytes);
    }

    /**
     * Legacy Paimon tables send the bucket key a to the server and filter b on the client. The
     * caller may reuse the lookup row's buffer while the response is pending, which must not change
     * the filter.
     */
    private static void assertClientFilterKeepsKeyWhenRowIsReused(
            DataType keyType, Function<byte[], Object> keyValue) throws Exception {
        TableInfo tableInfo = legacyPaimonTable(keyType);
        ControllablePrefixLookupGateway gateway = new ControllablePrefixLookupGateway();
        TestingMetadataUpdater metadataUpdater =
                TestingMetadataUpdater.builder(Collections.singletonMap(TABLE_PATH, tableInfo))
                        .withTabletServerGateway(NODE1.id(), gateway)
                        .build();
        metadataUpdater.updateCluster(cluster());

        LookupClient lookupClient = new LookupClient(new Configuration(), metadataUpdater);
        try {
            PrefixKeyLookuper lookuper =
                    new PrefixKeyLookuper(
                            tableInfo,
                            new TestingSchemaGetter(tableInfo.getSchemaId(), tableInfo.getSchema()),
                            metadataUpdater,
                            lookupClient,
                            Arrays.asList("a", "b"));
            byte[] buffer = FIRST.clone();
            CompletableFuture<LookupResult> result =
                    lookuper.lookup(GenericRow.of(1L, keyValue.apply(buffer)));

            // the request is sent and its response is held; the caller now reuses the buffer
            CompletableFuture<PrefixLookupResponse> response = gateway.pollResponse();
            assertThat(response).isNotNull();
            System.arraycopy(SECOND, 0, buffer, 0, buffer.length);
            response.complete(
                    response(
                            tableInfo,
                            new Object[] {1L, keyValue.apply(FIRST), 1L, "first"},
                            new Object[] {1L, keyValue.apply(SECOND), 1L, "second"}));

            List<InternalRow> rows = result.get(5, TimeUnit.SECONDS).getRowList();
            assertThat(rows)
                    .extracting(row -> row.getString(3).toString())
                    .containsExactly("first");
        } finally {
            lookupClient.close(Duration.ofSeconds(5));
        }
    }

    private static TableInfo legacyPaimonTable(DataType keyType) {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.BIGINT())
                        .column("b", keyType)
                        .column("c", DataTypes.BIGINT())
                        .column("v", DataTypes.STRING())
                        .primaryKey("a", "b", "c")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1, "a")
                        .property(ConfigOptions.TABLE_KV_FORMAT_VERSION, 1)
                        .property(ConfigOptions.TABLE_DATALAKE_FORMAT, DataLakeFormat.PAIMON)
                        .build();
        return TableInfo.of(TABLE_PATH, TABLE_ID, 1, descriptor, null, 0L, 0L);
    }

    private static Cluster cluster() {
        PhysicalTablePath path = PhysicalTablePath.of(TABLE_PATH);
        BucketLocation bucket =
                new BucketLocation(
                        path, new TableBucket(TABLE_ID, 0), NODE1.id(), new int[] {NODE1.id()});
        return new Cluster(
                Collections.singletonMap(NODE1.id(), NODE1),
                COORDINATOR,
                Collections.singletonMap(path, Collections.singletonList(bucket)),
                Collections.singletonMap(TABLE_PATH, TABLE_ID),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    private static PrefixLookupResponse response(TableInfo tableInfo, Object[]... rows) {
        PbValueList values = new PbValueList();
        for (Object[] row : rows) {
            byte[] value =
                    ValueEncoder.encodeValue(
                            (short) tableInfo.getSchemaId(),
                            compactedRow(tableInfo.getRowType(), row));
            values.addValue(value, 0, value.length);
        }
        PbPrefixLookupRespForBucket bucket = new PbPrefixLookupRespForBucket();
        bucket.setBucketId(0);
        bucket.addAllValueLists(Collections.singletonList(values));
        PrefixLookupResponse response = new PrefixLookupResponse();
        response.addAllBucketsResps(Collections.singletonList(bucket));
        return response;
    }

    /** Holds every prefix lookup response until the test completes it. */
    private static final class ControllablePrefixLookupGateway extends TestTabletServerGateway {
        private final BlockingQueue<CompletableFuture<PrefixLookupResponse>> responses =
                new LinkedBlockingQueue<>();

        private ControllablePrefixLookupGateway() {
            super(false, Collections.emptySet());
        }

        @Override
        public CompletableFuture<PrefixLookupResponse> prefixLookup(PrefixLookupRequest request) {
            CompletableFuture<PrefixLookupResponse> response = new CompletableFuture<>();
            responses.add(response);
            return response;
        }

        private CompletableFuture<PrefixLookupResponse> pollResponse() throws InterruptedException {
            return responses.poll(5, TimeUnit.SECONDS);
        }
    }
}
