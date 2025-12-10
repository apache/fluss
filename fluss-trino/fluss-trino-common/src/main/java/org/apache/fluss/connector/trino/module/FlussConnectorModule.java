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

package org.apache.fluss.connector.trino.module;

import org.apache.fluss.connector.trino.annotation.ForFlussConnector;
import org.apache.fluss.connector.trino.config.FlussConnectorConfig;
import org.apache.fluss.connector.trino.connection.FlussClientManager;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.spi.type.TypeManager;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

/**
 * Guice module for Fluss connector dependency injection.
 */
public class FlussConnectorModule implements Module {

    private final String catalogName;
    private final TypeManager typeManager;

    public FlussConnectorModule(String catalogName, TypeManager typeManager) {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder) {
        // Bind catalog name
        binder.bind(String.class)
                .annotatedWith(ForFlussConnector.class)
                .toInstance(catalogName);

        // Bind TypeManager
        binder.bind(TypeManager.class)
                .annotatedWith(ForFlussConnector.class)
                .toInstance(typeManager);

        // Bind client manager
        binder.bind(FlussClientManager.class).in(Scopes.SINGLETON);

        // Configure connector config
        configBinder(binder).bindConfig(FlussConnectorConfig.class);
    }
}
