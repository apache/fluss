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

package org.apache.fluss.flink.procedure;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Runtime resources supplied by a Fluss catalog to its procedures. */
public final class FlussProcedureContext {

    private final Admin admin;
    private final String defaultDatabase;
    private final Configuration flussConfiguration;
    private final Map<String, String> lakeCatalogProperties;
    private final ClassLoader classLoader;

    /** Creates an immutable procedure context. */
    public FlussProcedureContext(
            Admin admin,
            String defaultDatabase,
            Configuration flussConfiguration,
            Map<String, String> lakeCatalogProperties,
            ClassLoader classLoader) {
        this.admin = checkNotNull(admin);
        this.defaultDatabase = checkNotNull(defaultDatabase);
        this.flussConfiguration = new Configuration(checkNotNull(flussConfiguration));
        this.lakeCatalogProperties =
                Collections.unmodifiableMap(new HashMap<>(checkNotNull(lakeCatalogProperties)));
        this.classLoader = checkNotNull(classLoader);
    }

    /** Returns the Fluss admin client owned by the catalog. */
    public Admin getAdmin() {
        return admin;
    }

    /** Returns the default database configured for the catalog. */
    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    /** Returns a copy of the Fluss client configuration. */
    public Configuration getFlussConfiguration() {
        return new Configuration(flussConfiguration);
    }

    /** Returns the lake catalog properties configured on the Fluss catalog. */
    public Map<String, String> getLakeCatalogProperties() {
        return lakeCatalogProperties;
    }

    /** Returns the catalog classloader. */
    public ClassLoader getClassLoader() {
        return classLoader;
    }
}
