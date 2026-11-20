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

package org.apache.fluss.trino;

import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.TrinoException;
import jakarta.annotation.PreDestroy;
import org.junit.jupiter.api.Test;

import static io.trino.spi.StandardErrorCode.UNSUPPORTED_ISOLATION_LEVEL;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.SERIALIZABLE;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests connector transaction and lifecycle contracts. */
final class FlussConnectorTest extends FlussTrinoTestBase {
    @Test
    void testMetadataForReadCommittedTransaction() throws Exception {
        createDatabase("sales");
        LifeCycleManager lifecycle = createLifecycle();
        FlussMetadata metadata = new FlussMetadata(access);
        FlussConnector connector = createConnector(lifecycle, metadata);
        try {
            assertThat(connector.beginTransaction(READ_COMMITTED, true, true))
                    .isSameAs(FlussTransactionHandle.INSTANCE);
            assertThat(connector.getMetadata(SESSION, FlussTransactionHandle.INSTANCE))
                    .isSameAs(metadata);
            assertThat(
                            connector
                                    .getMetadata(SESSION, FlussTransactionHandle.INSTANCE)
                                    .listSchemaNames(SESSION))
                    .contains("sales");
        } finally {
            connector.shutdown();
        }
    }

    @Test
    void testRejectUnsupportedIsolation() {
        FlussConnector connector = createConnector(createLifecycle(), new FlussMetadata(access));
        try {
            assertThatThrownBy(() -> connector.beginTransaction(SERIALIZABLE, true, true))
                    .isInstanceOfSatisfying(
                            TrinoException.class,
                            failure ->
                                    assertThat(failure.getErrorCode())
                                            .isEqualTo(UNSUPPORTED_ISOLATION_LEVEL.toErrorCode()));
        } finally {
            connector.shutdown();
        }
    }

    @Test
    void testShutdownStopsLifecycle() throws Exception {
        LifeCycleManager lifecycle = createLifecycle();
        CloseTracker tracker = new CloseTracker();
        lifecycle.addInstance(tracker);
        FlussConnector connector = createConnector(lifecycle, new FlussMetadata(access));
        connector.shutdown();
        connector.shutdown();
        assertThat(tracker.closes).isEqualTo(1);
    }

    private FlussConnector createConnector(LifeCycleManager lifecycle, FlussMetadata metadata) {
        return new FlussConnector(
                lifecycle,
                metadata,
                new FlussSplitManager(access, new FlussSplitPlanner(access)),
                new FlussPageSourceProvider(clients));
    }

    private static LifeCycleManager createLifecycle() {
        return new Bootstrap()
                .doNotInitializeLogging()
                .disableSystemProperties()
                .initialize()
                .getInstance(LifeCycleManager.class);
    }

    /** Resource registered with the real Airlift lifecycle. */
    public static final class CloseTracker {
        private int closes;

        /** Records lifecycle disposal. */
        @PreDestroy
        public void close() {
            closes++;
        }
    }
}
