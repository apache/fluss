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

package org.apache.fluss.connector.trino;

import org.apache.fluss.connector.trino.connection.FlussClientManager;

import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

/**
 * Fluss Trino connector implementation.
 */
public class FlussConnector implements Connector {

    private static final Logger log = Logger.get(FlussConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final FlussClientManager clientManager;
    private final FlussMetadata metadata;
    private final FlussSplitManager splitManager;
    private final FlussPageSourceProvider pageSourceProvider;

    @Inject
    public FlussConnector(
            LifeCycleManager lifeCycleManager,
            FlussClientManager clientManager,
            FlussMetadata metadata,
            FlussSplitManager splitManager,
            FlussPageSourceProvider pageSourceProvider) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.clientManager = requireNonNull(clientManager, "clientManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        // For now, we only support read-only transactions
        if (!readOnly) {
            throw new UnsupportedOperationException("Write transactions are not yet supported");
        }
        return new FlussTransactionHandle();
    }

    @Override
    public ConnectorMetadata getMetadata(
            ConnectorSession session,
            ConnectorTransactionHandle transaction) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return pageSourceProvider;
    }

    @Override
    public void shutdown() {
        try {
            log.info("Shutting down Fluss connector");
            
            // Close page source provider resources
            if (pageSourceProvider instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) pageSourceProvider).close();
                    log.debug("Page source provider closed");
                } catch (Exception e) {
                    log.warn(e, "Error closing page source provider");
                }
            }
            
            // Close split manager resources
            if (splitManager instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) splitManager).close();
                    log.debug("Split manager closed");
                } catch (Exception e) {
                    log.warn(e, "Error closing split manager");
                }
            }
            
            // Close metadata resources
            if (metadata instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) metadata).close();
                    log.debug("Metadata provider closed");
                } catch (Exception e) {
                    log.warn(e, "Error closing metadata provider");
                }
            }
            
            // Close client manager
            if (clientManager instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) clientManager).close();
                    log.debug("Client manager closed");
                } catch (Exception e) {
                    log.warn(e, "Error closing client manager");
                }
            }
            
            // Stop lifecycle manager
            lifeCycleManager.stop();
            log.info("Fluss connector shutdown completed");
        } catch (Exception e) {
            log.error(e, "Error shutting down Fluss connector");
        }
    }
}
