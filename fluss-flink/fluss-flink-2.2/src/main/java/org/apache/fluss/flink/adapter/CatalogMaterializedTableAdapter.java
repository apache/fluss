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

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.TableDistribution;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** An adapter for {@link CatalogMaterializedTable#newBuilder()} constructor for flink2.2. */
public class CatalogMaterializedTableAdapter {

    private final CatalogMaterializedTable.Builder builder;

    private CatalogMaterializedTableAdapter() {
        this.builder = CatalogMaterializedTable.newBuilder();
    }

    public static CatalogMaterializedTableAdapter newAdapter() {
        return new CatalogMaterializedTableAdapter();
    }

    public CatalogMaterializedTableAdapter schema(Schema schema) {
        this.builder.schema(schema);
        return this;
    }

    public CatalogMaterializedTableAdapter comment(@Nullable String comment) {
        this.builder.comment(comment);
        return this;
    }

    public CatalogMaterializedTableAdapter partitionKeys(List<String> partitionKeys) {
        this.builder.partitionKeys(partitionKeys);
        return this;
    }

    public CatalogMaterializedTableAdapter options(Map<String, String> options) {
        this.builder.options(options);
        return this;
    }

    public CatalogMaterializedTableAdapter snapshot(@Nullable Long snapshot) {
        this.builder.snapshot(snapshot);
        return this;
    }

    public CatalogMaterializedTableAdapter originalQuery(String originalQuery) {
        return this;
    }

    public CatalogMaterializedTableAdapter expandedQuery(String expandedQuery) {
        return this;
    }

    public CatalogMaterializedTableAdapter definitionQuery(String definitionQuery) {
        this.builder.definitionQuery(definitionQuery);
        return this;
    }

    public CatalogMaterializedTableAdapter freshness(@Nullable IntervalFreshness freshness) {
        this.builder.freshness(freshness);
        return this;
    }

    public CatalogMaterializedTableAdapter logicalRefreshMode(
            CatalogMaterializedTable.LogicalRefreshMode logicalRefreshMode) {
        this.builder.logicalRefreshMode(logicalRefreshMode);
        return this;
    }

    public CatalogMaterializedTableAdapter refreshMode(
            @Nullable CatalogMaterializedTable.RefreshMode refreshMode) {
        this.builder.refreshMode(refreshMode);
        return this;
    }

    public CatalogMaterializedTableAdapter refreshStatus(
            CatalogMaterializedTable.RefreshStatus refreshStatus) {
        this.builder.refreshStatus(refreshStatus);
        return this;
    }

    public CatalogMaterializedTableAdapter refreshHandlerDescription(
            @Nullable String refreshHandlerDescription) {
        this.builder.refreshHandlerDescription(refreshHandlerDescription);
        return this;
    }

    public CatalogMaterializedTableAdapter serializedRefreshHandler(
            @Nullable byte[] serializedRefreshHandler) {
        this.builder.serializedRefreshHandler(serializedRefreshHandler);
        return this;
    }

    public CatalogMaterializedTableAdapter distribution(@Nullable TableDistribution distribution) {
        this.builder.distribution(distribution);
        return this;
    }

    public CatalogMaterializedTable build() {
        return this.builder.build();
    }
}
