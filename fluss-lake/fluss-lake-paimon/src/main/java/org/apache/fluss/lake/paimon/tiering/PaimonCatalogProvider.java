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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.config.Configuration;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

import java.io.Serializable;

/** A provider for Paimon catalog. */
public class PaimonCatalogProvider implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Configuration paimonConfig;

    public PaimonCatalogProvider(Configuration paimonConfig) {
        this.paimonConfig = paimonConfig;
    }

    public Catalog get() {
        return CatalogFactory.createCatalog(
                CatalogContext.create(Options.fromMap(paimonConfig.toMap())));
    }
}
