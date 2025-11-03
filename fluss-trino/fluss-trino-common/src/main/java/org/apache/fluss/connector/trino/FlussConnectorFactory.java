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

import org.apache.fluss.connector.trino.module.FlussConnectorModule;
import org.apache.fluss.connector.trino.module.FlussLakehouseModule;
import org.apache.fluss.connector.trino.module.FlussMetadataModule;
import org.apache.fluss.connector.trino.module.FlussOptimizationModule;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Factory for creating Fluss Trino connector instances.
 * 
 * <p>This is the entry point for the Trino plugin system to create Fluss connectors.
 */
public class FlussConnectorFactory implements ConnectorFactory {

    private static final Logger log = Logger.get(FlussConnectorFactory.class);
    private static final String CONNECTOR_NAME = "fluss";

    @Override
    public String getName() {
        return CONNECTOR_NAME;
    }

    @Override
    public Connector create(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context) {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");
        requireNonNull(context, "context is null");

        try {
            log.info("Creating Fluss connector for catalog: %s", catalogName);
            
            // Validate required configuration
            validateConfiguration(config);

            // Create Guice injector with all necessary modules
            Bootstrap app = new Bootstrap(
                    new FlussConnectorModule(catalogName, context.getTypeManager()),
                    new FlussMetadataModule(),
                    new FlussSplitModule(),
                    new FlussPageSourceModule(),
                    new FlussOptimizationModule(),
                    new FlussLakehouseModule()
            );

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            Connector connector = injector.getInstance(FlussConnector.class);
            log.info("Successfully created Fluss connector for catalog: %s", catalogName);
            
            return connector;
        } catch (Exception e) {
            log.error(e, "Failed to create Fluss connector for catalog: %s", catalogName);
            throw new RuntimeException("Failed to create Fluss connector", e);
        }
    }

    /**
     * Validate required configuration properties.
     */
    private void validateConfiguration(Map<String, String> config) {
        String bootstrapServers = config.get("bootstrap.servers");
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Required configuration 'bootstrap.servers' is missing or empty");
        }
    }
}
