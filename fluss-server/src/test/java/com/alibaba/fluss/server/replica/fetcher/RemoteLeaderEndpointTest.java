/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.replica.fetcher;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.FetchLogResponse;
import com.alibaba.fluss.server.replica.fetcher.LeaderEndpoint.FetchData;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.server.testutils.RpcMessageTestUtils;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.Map;

import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.assertFetchLogResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newFetchLogRequest;
import static com.alibaba.fluss.testutils.DataTestUtils.assertLogRecordsEquals;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RemoteLeaderEndpoint}. */
public class RemoteLeaderEndpointTest {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private ZooKeeperClient zkClient;

    @BeforeEach
    void beforeEach() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
    }

    @Test
    void testFetchLogReturnedOffHeapMemoryData() throws Exception {
        // set bucket count to 1 to easy for debug.
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(1, "a").build();
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, tableDescriptor);
        int bucketId = 0;
        TableBucket tb = new TableBucket(tableId, bucketId);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // send one batch
        assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                RpcMessageTestUtils.newProduceLogRequest(
                                        tableId,
                                        tb.getBucket(),
                                        -1,
                                        genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                bucketId,
                0L);

        // check leader log data.
        assertFetchLogResponse(
                leaderGateWay.fetchLog(newFetchLogRequest(-1, tableId, bucketId, 0L)).get(),
                tableId,
                bucketId,
                10L,
                DATA1);

        // mock follower to fetch data.
        LeaderAndIsr leaderAndIsr = zkClient.getLeaderAndIsr(tb).get();
        Integer follower = leaderAndIsr.isr().stream().filter(id -> id != leader).findFirst().get();

        RemoteLeaderEndpoint remoteLeaderEndpoint =
                FLUSS_CLUSTER_EXTENSION
                        .getTabletServerById(follower)
                        .getReplicaManager()
                        .getReplicaFetcherManager()
                        .buildRemoteLogEndpoint(leader);
        FetchData fetchData =
                remoteLeaderEndpoint
                        .fetchLog(
                                remoteLeaderEndpoint
                                        .buildFetchLogContext(
                                                Collections.singletonMap(
                                                        tb,
                                                        new BucketFetchStatus(
                                                                tableId,
                                                                DATA1_TABLE_PATH,
                                                                0L,
                                                                null)))
                                        .get())
                        .get();

        FetchLogResponse response = fetchData.getFetchLogResponse();
        assertThat(response.isLazilyParsed()).isTrue();
        assertThat(response.getParsedByteBuf().isDirect()).isTrue();

        Map<TableBucket, FetchLogResultForBucket> fetchLogResultMap =
                fetchData.getFetchLogResultMap();
        assertThat(fetchLogResultMap.size()).isEqualTo(1);
        MemoryLogRecords logRecords = (MemoryLogRecords) fetchLogResultMap.get(tb).recordsOrEmpty();
        MemorySegment memorySegment = logRecords.getMemorySegment();
        assertThat(memorySegment.getHeapMemory()).isNull();
        assertThat(memorySegment.getOffHeapBuffer()).isNotNull();
        assertLogRecordsEquals(DATA1_ROW_TYPE, logRecords, DATA1);
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        return conf;
    }
}
