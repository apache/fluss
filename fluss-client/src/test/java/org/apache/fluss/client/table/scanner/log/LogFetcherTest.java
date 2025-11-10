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

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.metrics.TestingScannerMetricGroup;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.FetchLogRequest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/** UT Test for {@link LogFetcher}. */
public class LogFetcherTest {
    private final TableBucket tb1 = new TableBucket(DATA1_TABLE_ID, 0);

    private TestingMetadataUpdater metadataUpdater;
    private LogFetcher logFetcher = null;

    // TODO Add more ut tests like kafka.

    @BeforeEach
    public void setup() {
        metadataUpdater = initializeMetadataUpdater();
        LogScannerStatus logScannerStatus = initializeLogScannerStatus();
        logFetcher =
                new LogFetcher(
                        DATA1_TABLE_INFO,
                        null,
                        logScannerStatus,
                        new Configuration(),
                        metadataUpdater,
                        TestingScannerMetricGroup.newInstance(),
                        new RemoteFileDownloader(1));
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
        metadataUpdater.setResponseLogicId(1, 1);
        logFetcher.sendFetchRequest(1, requestMap.get(1));

        // When NotLeaderOrFollowerException is received, the bucketLocation will be removed from
        // metadata updater to trigger get the latest bucketLocation in next fetch round.
        assertThat(metadataUpdater.getBucketLocation(tb1)).isNotPresent();
    }

    private TestingMetadataUpdater initializeMetadataUpdater() {
        return new TestingMetadataUpdater(
                Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));
    }

    private LogScannerStatus initializeLogScannerStatus() {
        Map<TableBucket, Long> scanBucketAndOffsets = new HashMap<>();
        scanBucketAndOffsets.put(tb1, 0L);
        LogScannerStatus status = new LogScannerStatus();
        status.assignScanBuckets(scanBucketAndOffsets);
        return status;
    }
}
