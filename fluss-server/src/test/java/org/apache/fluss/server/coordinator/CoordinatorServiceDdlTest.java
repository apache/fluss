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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.CreateTableFromLakeRequest;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.server.coordinator.remote.RemoteDirDynamicLoader;
import org.apache.fluss.server.lakehouse.TestingPaimonStoragePlugin.TestingPaimonCatalog;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.fromTablePath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Concurrency tests for coordinator DDL request scopes. */
class CoordinatorServiceDdlTest extends CoordinatorEventProcessorTestBase {

    @Test
    void testLakeDescriptorReadDoesNotBlockAnotherTableDdl() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        LakeCatalog catalog =
                new TestingPaimonCatalog() {
                    @Override
                    public TableDescriptor getTableDescriptor(TablePath tablePath) {
                        entered.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        throw new TableNotExistException("Lake descriptor unavailable");
                    }
                };
        LakeCatalogDynamicLoader loader =
                new LakeCatalogDynamicLoader(new Configuration(), null, true) {
                    @Override
                    public LakeCatalogContainer getLakeCatalogContainer() {
                        return new LakeCatalogContainer(new Configuration(), null, false) {
                            @Override
                            public boolean isClusterDataLakeTableEnabled() {
                                return true;
                            }

                            @Override
                            public DataLakeFormat getDataLakeFormat() {
                                return DataLakeFormat.PAIMON;
                            }

                            @Override
                            public LakeCatalog getLakeCatalog() {
                                return catalog;
                            }
                        };
                    }
                };
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
        ExecutorService ioExecutor = Executors.newSingleThreadExecutor();
        CoordinatorService service =
                new CoordinatorService(
                        conf,
                        FileSystem.get(URI.create("file:///")),
                        zookeeperClient,
                        () -> eventProcessor,
                        serverMetadataCache,
                        metadataManager,
                        null,
                        loader,
                        lakeTableTieringManager,
                        new RemoteDirDynamicLoader(conf),
                        null,
                        ioExecutor,
                        kvSnapshotLeaseManager,
                        null,
                        replicaCapacityController);
        FutureTask<Object> lakeRequest =
                new FutureTask<>(
                        () -> {
                            service.setCurrentSession(
                                    new Session(
                                            (short) 1,
                                            "FLUSS",
                                            false,
                                            InetAddress.getLoopbackAddress(),
                                            new FlussPrincipal("test", "USER")));
                            return service.createTableFromLake(
                                            new CreateTableFromLakeRequest()
                                                    .setTablePath(
                                                            fromTablePath(
                                                                    TablePath.of(
                                                                            defaultDatabase,
                                                                            "lake"))))
                                    .get();
                        });
        FutureTask<Long> otherRequest =
                new FutureTask<>(
                        () ->
                                eventProcessor
                                        .getMetadataManager()
                                        .createTable(
                                                TablePath.of(defaultDatabase, "other"),
                                                remoteDataDir,
                                                TableDescriptor.builder()
                                                        .schema(
                                                                Schema.newBuilder()
                                                                        .column(
                                                                                "id",
                                                                                DataTypes.INT())
                                                                        .build())
                                                        .distributedBy(1)
                                                        .build(),
                                                null,
                                                false));
        Thread lakeThread = new Thread(lakeRequest);
        Thread otherThread = new Thread(otherRequest);
        try {
            lakeThread.start();
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            otherThread.start();
            assertThat(otherRequest.get(10, TimeUnit.SECONDS)).isNotNegative();
            assertThat(lakeRequest).isNotDone();
        } finally {
            release.countDown();
            lakeThread.join(10000);
            otherThread.join(10000);
            service.shutdown();
            ioExecutor.shutdownNow();
        }
        assertThatThrownBy(lakeRequest::get).hasRootCauseInstanceOf(TableNotExistException.class);
    }
}
