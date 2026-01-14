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

package org.apache.fluss.flink.adapter;

import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;

/**
 * Adapter for {@link ResolvedCatalogMaterializedTable} because the constructor is compatibility in
 * flink 2.2. However, this constructor only used in test.
 *
 * <p>TODO: remove it until <a href="https://issues.apache.org/jira/browse/FLINK-38532">...</a> is
 * fixed.
 */
public class ResolvedCatalogMaterializedTableAdapter {

    public static ResolvedCatalogMaterializedTable create(
            CatalogMaterializedTable origin,
            ResolvedSchema resolvedSchema,
            CatalogMaterializedTable.RefreshMode refreshMode,
            IntervalFreshness freshness) {
        return new ResolvedCatalogMaterializedTable(origin, resolvedSchema, refreshMode, freshness);
    }
}
