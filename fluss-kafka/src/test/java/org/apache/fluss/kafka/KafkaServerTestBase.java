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

package org.apache.fluss.kafka;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.authorizer.Authorizer;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.tablet.TabletService;
import org.apache.fluss.server.tablet.consumer.group.ConsumerOffsetTableConfig;
import org.apache.fluss.server.tablet.consumer.group.GroupManager;

import org.junit.jupiter.api.AfterEach;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Real local TabletService and replicas for Kafka protocol tests. */
public class KafkaServerTestBase extends ReplicaTestBase {
    private final ExecutorService serviceExecutor = Executors.newSingleThreadExecutor();

    /** Creates a local service sharing the test replica manager and metadata. */
    protected TabletService createTabletService() {
        return createTabletService(null);
    }

    /** Creates a local service with the given authorizer. */
    protected TabletService createTabletService(Authorizer authorizer) {
        MetadataManager metadataManager =
                new MetadataManager(
                        zkClient,
                        conf,
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
        return new TabletService(
                TABLET_SERVER_ID,
                null,
                zkClient,
                replicaManager,
                serverMetadataCache,
                metadataManager,
                authorizer,
                null,
                serviceExecutor,
                serviceExecutor,
                scannerManager,
                testCoordinatorGateway,
                "INTERNAL",
                new GroupManager(ConsumerOffsetTableConfig.from(conf)));
    }

    @AfterEach
    void closeServiceExecutor() {
        serviceExecutor.shutdownNow();
    }
}
