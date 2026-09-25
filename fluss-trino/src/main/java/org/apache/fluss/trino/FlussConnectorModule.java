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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ForClassLoaderSafe;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import static io.airlift.configuration.ConfigBinder.configBinder;

/** Guice bindings for the Fluss connector. */
public class FlussConnectorModule extends AbstractConfigurationAwareModule {

    @Override
    public void setup(Binder binder) {
        configBinder(binder).bindConfig(FlussConfig.class);

        binder.bind(FlussConnector.class).in(Scopes.SINGLETON);
        binder.bind(FlussClientManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class)
                .annotatedWith(ForClassLoaderSafe.class)
                .to(FlussPageSourceProvider.class)
                .in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class)
                .to(ClassLoaderSafeConnectorPageSourceProvider.class)
                .in(Scopes.SINGLETON);

        binder.bind(ConnectorMetadata.class)
                .annotatedWith(ForClassLoaderSafe.class)
                .to(FlussMetadata.class)
                .in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class)
                .annotatedWith(ForClassLoaderSafe.class)
                .to(FlussSplitManager.class)
                .in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class)
                .to(ClassLoaderSafeConnectorSplitManager.class)
                .in(Scopes.SINGLETON);

        binder.bind(FlussMetadataAccess.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorMetadata.class)
                .to(ClassLoaderSafeConnectorMetadata.class)
                .in(Scopes.SINGLETON);
    }
}
