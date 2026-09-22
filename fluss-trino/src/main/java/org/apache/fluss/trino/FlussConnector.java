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
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;

import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Trino connector for Apache Fluss. */
public class FlussConnector implements Connector {

    private final LifeCycleManager lifeCycleManager;
    private final ConnectorMetadata metadata;

    public FlussConnector(LifeCycleManager lifeCycleManager, ConnectorMetadata metadata) {
        this.lifeCycleManager = checkNotNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(
            IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return FlussTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(
            ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties() {
        return FlussTableProperties.getTableProperties();
    }

    @Override
    public void shutdown() {
        lifeCycleManager.stop();
    }
}
