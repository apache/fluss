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

package org.apache.fluss.flink.catalog;

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.paimon.flink.SystemCatalogTable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** A Paimon system catalog table that carries transient options for the Flink planner. */
public class PaimonSystemCatalogTable extends SystemCatalogTable {

    private final SystemCatalogTable originTable;
    private final Map<String, String> options;

    PaimonSystemCatalogTable(SystemCatalogTable originTable, Map<String, String> options) {
        super(originTable.table());
        this.originTable = originTable;
        this.options = Collections.unmodifiableMap(new HashMap<>(options));
    }

    /** Returns the original Paimon system catalog table. */
    public SystemCatalogTable unwrap() {
        return originTable;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public CatalogTable copy(Map<String, String> options) {
        // Keep the transient wrapper options separate from the underlying Paimon system table.
        return new PaimonSystemCatalogTable(originTable, options);
    }

    @Override
    public CatalogTable copy() {
        return new PaimonSystemCatalogTable(originTable, options);
    }
}
