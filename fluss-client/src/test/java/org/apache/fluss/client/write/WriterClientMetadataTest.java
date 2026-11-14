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

package org.apache.fluss.client.write;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.admin.FlussAdmin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.TestingWriterMetricGroup;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests metadata preparation before a record is admitted to the shared writer. */
class WriterClientMetadataTest extends ClientToServerITCaseBase {

    @ParameterizedTest
    @ValueSource(strings = {"recover", "exhausted", "missing", "create"})
    void testPartitionPreparationPreservesRetriableFailures(String outcome) throws Exception {
        TablePath tablePath = TablePath.of("metadata_retry", outcome);
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("pt", DataTypes.STRING())
                                        .build())
                        .distributedBy(1)
                        .partitionedBy("pt")
                        .build();
        long tableId = createTable(tablePath, descriptor, true);
        PartitionSpec partition = new PartitionSpec(Collections.singletonMap("pt", "p1"));
        if (outcome.equals("recover") || outcome.equals("exhausted")) {
            admin.createPartition(tablePath, partition, false).get();
            long partitionId =
                    FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath, 1).get("p1");
            FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(tableId, partitionId);
        }
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        Configuration conf = new Configuration(clientConf);
        conf.set(ConfigOptions.CLIENT_WRITER_RETRIES, 2);
        conf.set(ConfigOptions.CLIENT_WRITER_RETRY_BACKOFF, Duration.ZERO);
        conf.set(ConfigOptions.CLIENT_WRITER_RETRY_BACKOFF_MAX, Duration.ZERO);
        conf.set(
                ConfigOptions.CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED,
                outcome.equals("create"));
        AtomicInteger failuresRemaining = new AtomicInteger(outcome.equals("exhausted") ? 3 : 2);
        if (outcome.equals("missing")) {
            failuresRemaining.set(0);
        }
        AtomicInteger metadataChecks = new AtomicInteger();
        AtomicInteger creates = new AtomicInteger();
        AtomicInteger callbacks = new AtomicInteger();
        CompletableFuture<Exception> result = new CompletableFuture<>();
        try (RpcClient rpc = RpcClient.create(conf, TestingClientMetricGroup.newInstance())) {
            MetadataUpdater updater =
                    new MetadataUpdater(conf, rpc) {
                        @Override
                        public boolean checkAndUpdatePartitionMetadata(
                                PhysicalTablePath path, long deadlineNanos) {
                            metadataChecks.incrementAndGet();
                            if (failuresRemaining.getAndDecrement() > 0) {
                                throw new StaleMetadataException("metadata temporarily unknown");
                            }
                            return super.checkAndUpdatePartitionMetadata(path, deadlineNanos);
                        }
                    };
            try (FlussAdmin countingAdmin =
                    new FlussAdmin(rpc, updater) {
                        @Override
                        public CompletableFuture<Void> createPartition(
                                TablePath path, PartitionSpec spec, boolean ignoreIfExists) {
                            // Creation must only follow authoritative confirmation of absence.
                            assertThat(failuresRemaining.get()).isNegative();
                            creates.incrementAndGet();
                            return super.createPartition(path, spec, ignoreIfExists);
                        }
                    }) {
                WriterClient writer =
                        new WriterClient(
                                conf,
                                updater,
                                TestingWriterMetricGroup.newInstance(),
                                countingAdmin);
                try {
                    WriteRecord record =
                            WriteRecord.forArrowAppend(
                                    tableInfo,
                                    PhysicalTablePath.of(tablePath, "p1"),
                                    row(1, "p1"),
                                    null);
                    WriteCallback callback =
                            (bucket, offset, error) -> {
                                callbacks.incrementAndGet();
                                result.complete(error);
                            };
                    if (outcome.equals("exhausted")) {
                        assertThatThrownBy(() -> writer.send(record, callback))
                                .isInstanceOf(StaleMetadataException.class);
                        assertThat(metadataChecks.get()).isEqualTo(3);
                        assertThat(callbacks.get()).isZero();
                        assertThat(creates.get()).isZero();
                        // Retry exhaustion must not poison this writer or enqueue the failed call.
                        writer.send(record, callback);
                    } else if (outcome.equals("missing")) {
                        assertThatThrownBy(() -> writer.send(record, callback))
                                .hasCauseInstanceOf(PartitionNotExistException.class);
                        assertThat(metadataChecks.get()).isOne();
                        assertThat(callbacks.get()).isZero();
                        assertThat(creates.get()).isZero();
                        return;
                    } else {
                        writer.send(record, callback);
                        assertThat(metadataChecks.get()).isEqualTo(3);
                    }
                    assertThat(result.get(20, TimeUnit.SECONDS)).isNull();
                    assertThat(callbacks.get()).isOne();
                    assertThat(creates.get()).isEqualTo(outcome.equals("create") ? 1 : 0);
                } finally {
                    writer.close(Duration.ZERO);
                }
            }
        }
    }
}
