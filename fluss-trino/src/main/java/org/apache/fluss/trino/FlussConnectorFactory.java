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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;

import java.util.Map;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Factory for creating Fluss connectors. */
public class FlussConnectorFactory implements ConnectorFactory {

    @Override
    public String getName() {
        return "fluss";
    }

    @Override
    public Connector create(
            String catalogName, Map<String, String> config, ConnectorContext context) {
        checkNotNull(catalogName, "catalogName is null");
        checkNotNull(config, "config is null");
        checkNotNull(context, "context is null");

        checkStrictSpiVersionMatch(context, this);

        Bootstrap app =
                new Bootstrap(
                        "io.trino.bootstrap.catalog." + catalogName,
                        new ConnectorContextModule(catalogName, context),
                        new FlussConnectorModule(),
                        binder ->
                                binder.bind(ClassLoader.class)
                                        .toInstance(FlussConnectorFactory.class.getClassLoader()));

        Injector injector =
                app.doNotInitializeLogging()
                        .disableSystemProperties()
                        .setRequiredConfigurationProperties(config)
                        .initialize();

        LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        ConnectorMetadata metadata = injector.getInstance(ConnectorMetadata.class);

        return new FlussConnector(lifeCycleManager, metadata);
    }
}
