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

package org.apache.fluss.kafka.backend.produce;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.kafka.KafkaServerTestBase;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.PartitionWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TopicWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.PartitionResult;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.authorizer.AuthorizationPlugin;
import org.apache.fluss.server.authorizer.DefaultAuthorizer;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IOUtils;

import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests direct local append, per-partition failures and shared conversion memory. */
class GatewayKafkaProduceBackendTest extends KafkaServerTestBase {
    private final List<GatewayKafkaProduceBackend> backends = new ArrayList<>();

    @Override
    protected Configuration getServerConf() {
        Configuration conf = super.getServerConf();
        conf.set(ConfigOptions.SERVER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.SERVER_BUFFER_PAGE_SIZE, MemorySize.parse("128kb"));
        conf.set(ConfigOptions.SERVER_BUFFER_PER_REQUEST_MEMORY_SIZE, MemorySize.parse("128kb"));
        conf.set(ConfigOptions.SERVER_BUFFER_POOL_WAIT_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 2);
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofMinutes(10));
        return conf;
    }

    @AfterEach
    void closeBackends() throws Exception {
        IOUtils.closeAll(backends);
        assertThat(replicaManager.getMemorySegmentPool().usedPages()).isZero();
    }

    @Test
    void testMultiTableAppendIsolatesUnknownTopicsAndBuckets() throws Exception {
        createTable("topic", 42L, Collections.singletonList(TABLET_SERVER_ID));
        createTable("other", 43L, Collections.singletonList(TABLET_SERVER_ID));
        KafkaProduceResult result =
                backend()
                        .write(
                                command(
                                        (short) 1,
                                        topic("topic", good(0), good(8), good(1)),
                                        topic("missing", good(0)),
                                        topic("other", good(0)),
                                        topic("kafka.topic", good(2))))
                        .get(10, TimeUnit.SECONDS);
        assertErrors(result, Errors.NONE, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.NONE);
        assertThat(result.topics())
                .extracting(t -> t.partitions().get(0).error())
                .containsExactly(
                        Errors.NONE, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.NONE, Errors.NONE);
        assertThat(result.topics())
                .extracting(t -> t.partitions().get(0).baseOffset())
                .containsExactly(0L, -1L, 0L, 0L);
        assertThat(
                        replicaManager
                                .getReplicaOrException(new TableBucket(42L, 0))
                                .getLocalLogEndOffset())
                .isEqualTo(1L);
        assertThat(
                        replicaManager
                                .getReplicaOrException(new TableBucket(43L, 0))
                                .getLocalLogEndOffset())
                .isEqualTo(1L);
    }

    @Test
    void testRevokedWritePermissionIsCheckedForEveryRequest() throws Exception {
        createTable("topic", 42L, Collections.singletonList(TABLET_SERVER_ID));
        boolean[] allowed = {true};
        DefaultAuthorizer authorizer =
                new DefaultAuthorizer(
                        new AuthorizationPlugin.Context() {
                            @Override
                            public Configuration getConfiguration() {
                                return conf;
                            }

                            @Override
                            public Optional<ZooKeeperClient> getZooKeeperClient() {
                                return Optional.of(zkClient);
                            }
                        }) {
                    @Override
                    public void authorize(
                            Session session, OperationType operation, Resource resource) {
                        assertThat(session.getListenerName()).isEqualTo("KAFKA");
                        assertThat(session.getInetAddress())
                                .isEqualTo(InetAddress.getLoopbackAddress());
                        assertThat(operation).isEqualTo(OperationType.WRITE);
                        if (!allowed[0]) {
                            throw new AuthorizationException("Write denied");
                        }
                    }
                };
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(createTabletService(authorizer), mapper());
        backends.add(backend);
        try {
            KafkaProduceCommand command = command((short) 1, topic("topic", good(0)));
            assertErrors(backend.write(command).join(), Errors.NONE);
            allowed[0] = false;
            assertErrors(backend.write(command).join(), Errors.TOPIC_AUTHORIZATION_FAILED);
            assertThat(
                            replicaManager
                                    .getReplicaOrException(new TableBucket(42L, 0))
                                    .getLocalLogEndOffset())
                    .isEqualTo(1L);
        } finally {
            authorizer.close();
        }
    }

    @Test
    void testOneMultiTableAppendReleasesMemoryBeforeAcknowledgement() throws Exception {
        createTable("topic", 42L, Arrays.asList(TABLET_SERVER_ID, 2));
        createTable("other", 43L, Arrays.asList(TABLET_SERVER_ID, 2));
        GatewayKafkaProduceBackend backend = backend();
        CompletableFuture<KafkaProduceResult> result =
                backend.write(
                        command(
                                (short) -1,
                                topic("topic", good(0), good(1)),
                                topic("other", good(0))));
        assertThat(result).isNotDone();
        // A single delayed operation proves all tables were submitted in one append call.
        assertThat(replicaManager.getDelayedWriteManager().numDelayed()).isEqualTo(1);
        assertThat(replicaManager.getMemorySegmentPool().usedPages()).isZero();
        assertThat(
                        replicaManager
                                .getReplicaOrException(new TableBucket(42L, 0))
                                .getLocalLogEndOffset())
                .isEqualTo(1L);
        backend.close();
        acknowledge(new TableBucket(42L, 0));
        acknowledge(new TableBucket(42L, 1));
        acknowledge(new TableBucket(43L, 0));
        assertErrors(result.get(10, TimeUnit.SECONDS), Errors.NONE, Errors.NONE);
        assertThat(result.join().topics().get(1).partitions().get(0).error())
                .isEqualTo(Errors.NONE);
    }

    @Test
    void testInsufficientReplicasReleasesAllConvertedRecords() throws Exception {
        createTable("topic", 42L, Collections.singletonList(TABLET_SERVER_ID));
        createTable("other", 43L, Collections.singletonList(TABLET_SERVER_ID));
        KafkaProduceResult result =
                backend()
                        .write(
                                command(
                                        (short) -1,
                                        topic("topic", good(0), good(1)),
                                        topic("other", good(0))))
                        .join();
        assertErrors(result, Errors.NOT_ENOUGH_REPLICAS, Errors.NOT_ENOUGH_REPLICAS);
        assertThat(result.topics().get(1).partitions().get(0).error())
                .isEqualTo(Errors.NOT_ENOUGH_REPLICAS);
        assertThat(replicaManager.getMemorySegmentPool().usedPages()).isZero();
    }

    private void acknowledge(TableBucket bucket) throws Exception {
        CompletableFuture<Void> fetched = new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                new FetchParams(2, Integer.MAX_VALUE),
                Collections.singletonMap(
                        bucket, new FetchReqInfo(bucket.getTableId(), 1L, Integer.MAX_VALUE)),
                null,
                ignored -> fetched.complete(null));
        fetched.get(10, TimeUnit.SECONDS);
    }

    private void createTable(String name, long tableId, List<Integer> replicas) throws Exception {
        TablePath path = TablePath.of("kafka", name);
        Map<String, String> properties = new HashMap<>();
        properties.put(ConfigOptions.TABLE_KAFKA_ENABLED.key(), "true");
        properties.put(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(), "binary");
        registerTableInZkClient(
                path,
                Schema.newBuilder().column("_kafka_value", DataTypes.BYTES()).build(),
                tableId,
                Collections.emptyList(),
                properties);
        List<BucketMetadata> buckets = new ArrayList<>();
        for (int bucket = 0; bucket < 3; bucket++) {
            makeLeaderAndFollower(
                    Collections.singletonList(
                            new NotifyLeaderAndIsrData(
                                    PhysicalTablePath.of(path),
                                    new TableBucket(tableId, bucket),
                                    replicas,
                                    new LeaderAndIsr(
                                            TABLET_SERVER_ID,
                                            0,
                                            replicas,
                                            Collections.emptyList(),
                                            0,
                                            0),
                                    3,
                                    0L)));
            buckets.add(new BucketMetadata(bucket, TABLET_SERVER_ID, 0, replicas, replicas, 0));
        }
        TableInfo tableInfo =
                replicaManager.getReplicaOrException(new TableBucket(tableId, 0)).getTableInfo();
        serverMetadataCache.updateTableMetadata(new TableMetadata(tableInfo, buckets));
    }

    private GatewayKafkaProduceBackend backend() {
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(createTabletService(), mapper());
        backends.add(backend);
        return backend;
    }

    private static KafkaTopicMapper mapper() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, "kafka");
        return new KafkaTopicMapper(config);
    }

    private static KafkaProduceCommand command(short acks, TopicWrite... topics) {
        return new KafkaProduceCommand(
                acks, 30000, Arrays.asList(topics), "KAFKA", InetAddress.getLoopbackAddress());
    }

    private static TopicWrite topic(String name, PartitionWrite... partitions) {
        return new TopicWrite(name, Arrays.asList(partitions));
    }

    private static PartitionWrite good(int id) {
        return new PartitionWrite(
                id,
                Collections.singletonList(
                        new Record(
                                123L,
                                null,
                                ByteBuffer.wrap(new byte[] {65}),
                                Collections.emptyList())));
    }

    private static void assertErrors(KafkaProduceResult result, Errors... errors) {
        assertThat(result.topics().get(0).partitions())
                .extracting(PartitionResult::error)
                .containsExactly(errors);
        result.topics()
                .forEach(
                        topic ->
                                topic.partitions()
                                        .forEach(
                                                partition -> {
                                                    if (partition.error() != Errors.NONE) {
                                                        assertThat(partition.baseOffset())
                                                                .isEqualTo(-1L);
                                                    }
                                                }));
    }
}
