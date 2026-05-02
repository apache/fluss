/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator.system;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TablePath;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Registry of all system view definitions.
 *
 * <p>The resolver maintains two separate registries for system views:
 *
 * <ul>
 *   <li><b>View definitions</b> ({@link SystemViewDefinition}) — lightweight metadata (path,
 *       schema, schemaId) registered on all servers. Used for metadata operations such as {@code
 *       listTables}, {@code tableExists}, and {@code getTableInfo}.
 *   <li><b>View providers</b> ({@link SystemViewProvider}) — full implementations that can produce
 *       data via {@code scanRows}. Registered only on servers that have access to the required data
 *       sources.
 * </ul>
 */
@Internal
public class SystemTableResolver {

    /** Registered system view definitions (metadata only), keyed by table path. */
    private final Map<TablePath, SystemViewDefinition> viewDefinitions;

    /** Registered system view providers (data serving), keyed by table path. */
    private final Map<TablePath, SystemViewProvider> viewProviders;

    public SystemTableResolver() {
        this.viewDefinitions = new HashMap<>();
        this.viewProviders = new HashMap<>();
    }

    // -------------------------------------------------------------------------
    //  Registration
    // -------------------------------------------------------------------------

    /**
     * Registers a system view definition (metadata only).
     *
     * <p>This makes the view visible in metadata operations ({@code listTables}, {@code
     * tableExists}, {@code getTableInfo}) without requiring the server to produce data for the
     * view. All servers should register definitions for all known views.
     */
    public void registerViewDefinition(SystemViewDefinition definition) {
        viewDefinitions.put(definition.viewPath(), definition);
    }

    /**
     * Registers a system view provider (data serving).
     *
     * <p>A provider is a {@link SystemViewDefinition} that can also produce data. This method
     * registers both the definition and the provider. Only servers that can serve the view's data
     * should call this method.
     */
    public void registerViewProvider(SystemViewProvider provider) {
        viewDefinitions.put(provider.viewPath(), provider);
        viewProviders.put(provider.viewPath(), provider);
    }

    // -------------------------------------------------------------------------
    //  System view definition accessors
    // -------------------------------------------------------------------------

    /** Returns the system view definition for the given path, if registered. */
    public Optional<SystemViewDefinition> getViewDefinition(TablePath tablePath) {
        return Optional.ofNullable(viewDefinitions.get(tablePath));
    }

    /** Returns all registered system view definitions. */
    public List<SystemViewDefinition> getAllViewDefinitions() {
        return new ArrayList<>(viewDefinitions.values());
    }

    /** Checks whether the given path is a registered system view. */
    public boolean isSystemView(TablePath tablePath) {
        return viewDefinitions.containsKey(tablePath);
    }

    // -------------------------------------------------------------------------
    //  System view provider accessors
    // -------------------------------------------------------------------------

    /** Returns the system view provider for the given path, if registered. */
    public Optional<SystemViewProvider> getViewProvider(TablePath tablePath) {
        return Optional.ofNullable(viewProviders.get(tablePath));
    }

    /** Returns all registered system view providers. */
    public List<SystemViewProvider> getAllViewProviders() {
        return new ArrayList<>(viewProviders.values());
    }

    // -------------------------------------------------------------------------
    //  Combined accessors
    // -------------------------------------------------------------------------

    /**
     * Returns all system view names for the system database.
     *
     * <p>System views are always listed because their definitions are available in-memory on every
     * server.
     */
    public List<String> getAllSystemViewNames() {
        List<String> names = new ArrayList<>();
        for (TablePath path : viewDefinitions.keySet()) {
            names.add(path.getTableName());
        }
        return names;
    }
}
