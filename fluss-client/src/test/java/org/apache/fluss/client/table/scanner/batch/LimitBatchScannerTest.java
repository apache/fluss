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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.LimitScanRequest;
import org.apache.fluss.rpc.messages.LimitScanResponse;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DEFAULT_MAGIC;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.createRecordsWithoutBaseLogOffset;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link LimitBatchScanner} closes {@link org.apache.fluss.record.LogRecordReadContext}
 * after parsing Arrow log records.
 */
class LimitBatchScannerTest {

    private static final TableBucket BUCKET_0 = new TableBucket(DATA1_TABLE_ID, 0);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);
    private static final SchemaGetter SCHEMA_GETTER =
            new TestingSchemaGetter(DEFAULT_SCHEMA_ID, DATA1_SCHEMA);

    @Test
    void pollBatchClosesArrowReadContextSoScannerCloseSucceeds() throws Exception {
        MemoryLogRecords records =
                createRecordsWithoutBaseLogOffset(
                        DATA1_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        0L,
                        1000L,
                        DEFAULT_MAGIC,
                        Arrays.asList(new Object[] {1, "a"}, new Object[] {2, "b"}),
                        LogFormat.ARROW);
        byte[] recordBytes = new byte[records.sizeInBytes()];
        records.getMemorySegment().get(records.getPosition(), recordBytes);

        LimitScanResponse response =
                new LimitScanResponse().setIsLogTable(true).setRecords(recordBytes);
        LimitGateway gateway = new LimitGateway(response);

        LimitBatchScanner scanner =
                new LimitBatchScanner(
                        DATA1_TABLE_INFO,
                        BUCKET_0,
                        SCHEMA_GETTER,
                        new TestMetadataUpdater(gateway),
                        null,
                        10);
        try {
            CloseableIterator<InternalRow> batch = scanner.pollBatch(POLL_TIMEOUT);
            assertThat(batch).isNotNull();
            List<InternalRow> rows = new ArrayList<>();
            while (batch.hasNext()) {
                rows.add(batch.next());
            }
            batch.close();
            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getInt(0)).isEqualTo(1);
            assertThat(rows.get(1).getInt(0)).isEqualTo(2);
        } finally {
            scanner.close();
        }
    }

    private static final class LimitGateway extends TestingTabletGatewayService {
        private final LimitScanResponse response;

        private LimitGateway(LimitScanResponse response) {
            this.response = response;
        }

        @Override
        public CompletableFuture<LimitScanResponse> limitScan(LimitScanRequest request) {
            return CompletableFuture.completedFuture(response);
        }
    }

    private static final class TestMetadataUpdater extends MetadataUpdater {
        private final TabletServerGateway gateway;

        TestMetadataUpdater(TabletServerGateway gateway) {
            super(null, new Configuration(), Cluster.empty());
            this.gateway = gateway;
        }

        @Override
        public void checkAndUpdateMetadata(TablePath tablePath, TableBucket tableBucket) {}

        @Override
        public int leaderFor(TablePath tablePath, TableBucket tableBucket) {
            return 0;
        }

        @Override
        public @Nullable TabletServerGateway newTabletServerClientForNode(int serverId) {
            return gateway;
        }
    }
}
