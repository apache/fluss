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

import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import org.junit.jupiter.api.Test;

import static io.trino.spi.StandardErrorCode.UNSUPPORTED_ISOLATION_LEVEL;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.SERIALIZABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Tests connector transaction and lifecycle contracts. */
final class FlussConnectorTest {
    @Test
    void testMetadataForReadCommittedTransaction() {
        ConnectorMetadata metadata = mock(ConnectorMetadata.class);
        FlussConnector connector = new FlussConnector(mock(LifeCycleManager.class), metadata);
        assertThat(connector.beginTransaction(READ_COMMITTED, true, true))
                .isSameAs(FlussTransactionHandle.INSTANCE);
        assertThat(
                        connector.getMetadata(
                                mock(ConnectorSession.class), FlussTransactionHandle.INSTANCE))
                .isSameAs(metadata);
    }

    @Test
    void testRejectUnsupportedIsolation() {
        FlussConnector connector =
                new FlussConnector(mock(LifeCycleManager.class), mock(ConnectorMetadata.class));
        assertThatThrownBy(() -> connector.beginTransaction(SERIALIZABLE, true, true))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        failure ->
                                assertThat(failure.getErrorCode())
                                        .isEqualTo(UNSUPPORTED_ISOLATION_LEVEL.toErrorCode()));
    }

    @Test
    void testShutdownStopsLifecycle() {
        LifeCycleManager lifecycle = mock(LifeCycleManager.class);
        new FlussConnector(lifecycle, mock(ConnectorMetadata.class)).shutdown();
        verify(lifecycle).stop();
    }
}
