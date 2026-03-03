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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.client.metadata.ClientSchemaGetter;
import org.apache.fluss.client.metadata.TestingClientSchemaGetter;
import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.metrics.TestingScannerMetricGroup;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FetchException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.remote.RemoteLogFetchInfo;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.tablet.TestTabletServerGateway;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE1;
import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE2;
import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE3;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getFetchLogData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeFetchLogResponse;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** UT Test for {@link LogFetcher}. */
public class LogFetcherTest {
    private final TableBucket tb1 = new TableBucket(DATA1_TABLE_ID, 0);

    private TestingMetadataUpdater metadataUpdater;
    private LogFetcher logFetcher = null;

    // TODO Add more ut tests like kafka.

    @BeforeEach
    public void setup() {
        metadataUpdater = initializeMetadataUpdater();
        ClientSchemaGetter clientSchemaGetter =
                new TestingClientSchemaGetter(
                        DATA1_TABLE_PATH,
                        new SchemaInfo(DATA1_SCHEMA, 0),
                        metadataUpdater,
                        new Configuration());
        LogScannerStatus logScannerStatus = initializeLogScannerStatus();
        logFetcher =
                new LogFetcher(
                        DATA1_TABLE_INFO,
                        null,
                        logScannerStatus,
                        new Configuration(),
                        metadataUpdater,
                        TestingScannerMetricGroup.newInstance(),
                        new RemoteFileDownloader(1),
                        clientSchemaGetter);
    }

    @Test
    void sendFetchRequestWithNotLeaderOrFollowerException() {
        Map<Integer, FetchLogRequest> requestMap = logFetcher.prepareFetchLogRequests();
        Set<Integer> serverSet = requestMap.keySet();
        assertThat(serverSet).containsExactlyInAnyOrder(1);

        assertThat(metadataUpdater.getBucketLocation(tb1))
                .hasValue(
                        new BucketLocation(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb1,
                                1,
                                new int[] {1, 2, 3}));

        // send fetchLogRequest to serverId 1, which will respond with NotLeaderOrFollowerException
        // as responseLogicId=1 do.
        logFetcher.sendFetchRequest(1, requestMap.get(1));

        // When NotLeaderOrFollowerException is received, the bucketLocation will be removed from
        // metadata updater to trigger get the latest bucketLocation in next fetch round.
        assertThat(metadataUpdater.getBucketLocation(tb1)).isNotPresent();
    }

    @Test
    void throwExceptionWhenRemoteDownloadFails() throws Exception {
        RemoteFileDownloader failingDownloader =
                new RemoteFileDownloader(1) {
                    @Override
                    protected long downloadFile(Path targetFilePath, FsPath remoteFilePath)
                            throws IOException {
                        throw new IOException("Simulated remote download failure");
                    }
                };

        TestingMetadataUpdater localMetadataUpdater =
                new TestingMetadataUpdater(
                        TestingMetadataUpdater.COORDINATOR,
                        Arrays.asList(NODE1, NODE2, NODE3),
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO),
                        Collections.singletonMap(1, new RemoteFetchTabletServerGateway()),
                        new Configuration());

        ClientSchemaGetter clientSchemaGetter =
                new TestingClientSchemaGetter(
                        DATA1_TABLE_PATH,
                        new SchemaInfo(DATA1_SCHEMA, 0),
                        localMetadataUpdater,
                        new Configuration());

        try (LogFetcher fetcher =
                new LogFetcher(
                        DATA1_TABLE_INFO,
                        null,
                        initializeLogScannerStatus(),
                        new Configuration(),
                        localMetadataUpdater,
                        TestingScannerMetricGroup.newInstance(),
                        failingDownloader,
                        clientSchemaGetter)) {

            Map<Integer, FetchLogRequest> requestMap = fetcher.prepareFetchLogRequests();
            fetcher.sendFetchRequest(1, requestMap.get(1));
            retry(Duration.ofSeconds(30), () -> assertThat(fetcher.hasAvailableFetches()).isTrue());
            // collectFetch should throw FetchException due to download failure
            assertThatThrownBy(fetcher::collectFetch)
                    .isInstanceOf(FetchException.class)
                    .rootCause()
                    .hasMessageContaining("Simulated remote download failure");
        }
    }

    private TestingMetadataUpdater initializeMetadataUpdater() {

        return new TestingMetadataUpdater(
                TestingMetadataUpdater.COORDINATOR,
                Arrays.asList(NODE1, NODE2, NODE3),
                Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO),
                Collections.singletonMap(1, new NotLeaderOrFollowerExceptionTabletServerGateway()),
                new Configuration());
    }

    private LogScannerStatus initializeLogScannerStatus() {
        Map<TableBucket, Long> scanBucketAndOffsets = new HashMap<>();
        scanBucketAndOffsets.put(tb1, 0L);
        LogScannerStatus status = new LogScannerStatus();
        status.assignScanBuckets(scanBucketAndOffsets);
        return status;
    }

    private static class RemoteFetchTabletServerGateway extends TestTabletServerGateway {

        public RemoteFetchTabletServerGateway() {
            super(false, Collections.emptySet());
        }

        @Override
        public CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request) {
            Map<TableBucket, FetchReqInfo> fetchLogData = getFetchLogData(request);
            Map<TableBucket, FetchLogResultForBucket> resultForBucketMap = new HashMap<>();
            fetchLogData.forEach(
                    (tableBucket, fetchReqInfo) -> {
                        RemoteLogSegment segment =
                                RemoteLogSegment.Builder.builder()
                                        .tableBucket(tableBucket)
                                        .physicalTablePath(PhysicalTablePath.of(DATA1_TABLE_PATH))
                                        .remoteLogSegmentId(UUID.randomUUID())
                                        .remoteLogStartOffset(fetchReqInfo.getFetchOffset())
                                        .remoteLogEndOffset(fetchReqInfo.getFetchOffset() + 100)
                                        .maxTimestamp(1000L)
                                        .segmentSizeInBytes(1024)
                                        .build();
                        RemoteLogFetchInfo remoteLogFetchInfo =
                                new RemoteLogFetchInfo(
                                        "/tmp/test-tablet-dir",
                                        null,
                                        Collections.singletonList(segment),
                                        0);
                        resultForBucketMap.put(
                                tableBucket,
                                new FetchLogResultForBucket(tableBucket, remoteLogFetchInfo, 100L));
                    });
            return CompletableFuture.completedFuture(makeFetchLogResponse(resultForBucketMap));
        }
    }

    private static class NotLeaderOrFollowerExceptionTabletServerGateway
            extends TestTabletServerGateway {

        public NotLeaderOrFollowerExceptionTabletServerGateway() {
            super(false, Collections.emptySet());
        }

        @Override
        public CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request) {
            Map<TableBucket, FetchReqInfo> fetchLogData = getFetchLogData(request);
            Map<TableBucket, FetchLogResultForBucket> resultForBucketMap = new HashMap<>();
            // return with NotLeaderOrFollowerException.
            fetchLogData.forEach(
                    (tableBucket, fetchData) -> {
                        FetchLogResultForBucket fetchLogResultForBucket =
                                new FetchLogResultForBucket(
                                        tableBucket,
                                        ApiError.fromThrowable(
                                                new NotLeaderOrFollowerException(
                                                        "mock fetchLog fail for not leader or follower exception.")));
                        resultForBucketMap.put(tableBucket, fetchLogResultForBucket);
                    });
            return CompletableFuture.completedFuture(makeFetchLogResponse(resultForBucketMap));
        }
    }
}
