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
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.LimitScanRequest;
import org.apache.fluss.rpc.messages.LimitScanResponse;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for the response buffer lifecycle in {@link LimitBatchScanner}. */
class LimitBatchScannerTest {

    private static final TableBucket BUCKET = new TableBucket(DATA1_TABLE_ID_PK, 0);
    private static final SchemaGetter SCHEMA_GETTER =
            new TestingSchemaGetter((short) 1, DATA1_SCHEMA_PK);

    private final List<ByteBuf> parsedBuffers = new ArrayList<>();

    @AfterEach
    void releaseParsedBuffers() {
        for (ByteBuf parsedBuffer : parsedBuffers) {
            while (parsedBuffer.refCnt() > 0) {
                parsedBuffer.release();
            }
        }
    }

    @Test
    void testCloseReleasesCompletedResponseBuffer() throws Exception {
        LimitScanResponse response =
                parseFromWire(new LimitScanResponse().setIsLogTable(false), false);
        ByteBuf buffer = response.getParsedByteBuf();
        LimitBatchScanner scanner = newScanner(CompletableFuture.completedFuture(response));

        scanner.close();

        assertThat(buffer.refCnt()).isZero();
    }

    @Test
    void testCloseReleasesResponseCompletedAfterClose() throws Exception {
        CompletableFuture<LimitScanResponse> responseFuture = new CompletableFuture<>();
        LimitScanResponse response =
                parseFromWire(new LimitScanResponse().setIsLogTable(false), false);
        ByteBuf buffer = response.getParsedByteBuf();
        LimitBatchScanner scanner = newScanner(responseFuture);

        scanner.close();
        assertThat(responseFuture.isCancelled()).isFalse();

        responseFuture.complete(response);
        assertThat(buffer.refCnt()).isZero();
    }

    @Test
    void testCloseDoesNotReleaseConsumedResponseTwice() throws Exception {
        LimitScanResponse response =
                parseFromWire(new LimitScanResponse().setIsLogTable(false), true);
        ByteBuf buffer = response.getParsedByteBuf();
        LimitBatchScanner scanner = newScanner(CompletableFuture.completedFuture(response));

        scanner.pollBatch(Duration.ofSeconds(5));
        scanner.close();

        assertThat(buffer.refCnt()).isZero();
        verify(buffer, times(1)).release();
    }

    private LimitBatchScanner newScanner(CompletableFuture<LimitScanResponse> responseFuture) {
        TabletServerGateway gateway = new LimitScanGateway(responseFuture);
        return new LimitBatchScanner(
                DATA1_TABLE_INFO_PK,
                BUCKET,
                SCHEMA_GETTER,
                new TestMetadataUpdater(gateway),
                null,
                1);
    }

    private LimitScanResponse parseFromWire(LimitScanResponse response, boolean trackRelease) {
        ByteBuf buffer = Unpooled.wrappedBuffer(response.toByteArray());
        if (trackRelease) {
            buffer = spy(buffer);
        }
        LimitScanResponse parsed = new LimitScanResponse();
        parsed.parseFrom(buffer, buffer.readableBytes());
        parsedBuffers.add(buffer);
        return parsed;
    }

    private static final class LimitScanGateway extends TestingTabletGatewayService {
        private final CompletableFuture<LimitScanResponse> responseFuture;

        private LimitScanGateway(CompletableFuture<LimitScanResponse> responseFuture) {
            this.responseFuture = responseFuture;
        }

        @Override
        public CompletableFuture<LimitScanResponse> limitScan(LimitScanRequest request) {
            return responseFuture;
        }
    }

    private static final class TestMetadataUpdater extends MetadataUpdater {
        private final TabletServerGateway gateway;

        private TestMetadataUpdater(TabletServerGateway gateway) {
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
