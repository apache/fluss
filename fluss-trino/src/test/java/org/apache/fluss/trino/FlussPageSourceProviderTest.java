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

import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/** Verifies provider validation without starting network reads. */
final class FlussPageSourceProviderTest {
    @Test
    void testEmptySplitAndInvalidBucket() throws Exception {
        FlussClientManager clients = mock(FlussClientManager.class);
        FlussTableHandle table =
                new FlussTableHandle("sales", "events", "Sales", "Events", 42, 1, 1, 0);
        FlussPageSourceProvider provider = new FlussPageSourceProvider(clients);
        try (ConnectorPageSource source =
                provider.createPageSource(
                        FlussTransactionHandle.INSTANCE,
                        mock(ConnectorSession.class),
                        new FlussSplit(0, 0, 0),
                        table,
                        Optional.empty(),
                        Collections.emptyList(),
                        DynamicFilter.EMPTY,
                        MemoryContext.NO_LIMIT)) {
            assertThat(source.isFinished()).isTrue();
            assertThat(source.isBlocked().isDone()).isTrue();
        }
        assertThatThrownBy(
                        () ->
                                provider.createPageSource(
                                        FlussTransactionHandle.INSTANCE,
                                        mock(ConnectorSession.class),
                                        new FlussSplit(1, 0, 1),
                                        table,
                                        Optional.empty(),
                                        Collections.emptyList(),
                                        DynamicFilter.EMPTY,
                                        MemoryContext.NO_LIMIT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bucketId");
        verifyNoInteractions(clients);
    }
}
