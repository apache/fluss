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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.IntervalFreshness;

/** Options for flink catalog. */
public class FlinkCatalogOptions {

    public static final String MATERIALIZED_TABLE_PREFIX = "materialized-table.";

    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key("default-database")
                    .stringType()
                    .defaultValue("fluss")
                    .withDescription("Default database name used when none is specified.");

    // -------------------------------------------------------------------------------------------
    // Only used internally to support materialized table
    // -------------------------------------------------------------------------------------------

    public static final ConfigOption<String> MATERIALIZED_TABLE_DEFINITION_QUERY =
            ConfigOptions.key("materialized-table.definition-query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The definition query text of materialized table, text is expanded in contrast to the original SQL.");

    public static final ConfigOption<String> MATERIALIZED_TABLE_INTERVAL_FRESHNESS =
            ConfigOptions.key("materialized-table.interval-freshness")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The freshness interval of materialized table which is used to determine the physical refresh mode.");

    public static final ConfigOption<IntervalFreshness.TimeUnit>
            MATERIALIZED_TABLE_INTERVAL_FRESHNESS_TIME_UNIT =
                    ConfigOptions.key("materialized-table.interval-freshness.time-unit")
                            .enumType(IntervalFreshness.TimeUnit.class)
                            .noDefaultValue()
                            .withDescription("The time unit of freshness interval.");

    public static final ConfigOption<CatalogMaterializedTable.LogicalRefreshMode>
            MATERIALIZED_TABLE_LOGICAL_REFRESH_MODE =
                    ConfigOptions.key("materialized-table.logical-refresh-mode")
                            .enumType(CatalogMaterializedTable.LogicalRefreshMode.class)
                            .noDefaultValue()
                            .withDescription("The logical refresh mode of materialized table.");

    public static final ConfigOption<CatalogMaterializedTable.RefreshMode>
            MATERIALIZED_TABLE_REFRESH_MODE =
                    ConfigOptions.key("materialized-table.refresh-mode")
                            .enumType(CatalogMaterializedTable.RefreshMode.class)
                            .noDefaultValue()
                            .withDescription("The physical refresh mode of materialized table.");

    public static final ConfigOption<CatalogMaterializedTable.RefreshStatus>
            MATERIALIZED_TABLE_REFRESH_STATUS =
                    ConfigOptions.key("materialized-table.refresh-status")
                            .enumType(CatalogMaterializedTable.RefreshStatus.class)
                            .noDefaultValue()
                            .withDescription("The refresh status of materialized table.");

    public static final ConfigOption<String> MATERIALIZED_TABLE_REFRESH_HANDLER_DESCRIPTION =
            ConfigOptions.key("materialized-table.refresh-handler-description")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The summary description of materialized table's refresh handler");

    public static final ConfigOption<String> MATERIALIZED_TABLE_REFRESH_HANDLER_BYTES =
            ConfigOptions.key("materialized-table.refresh-handler-bytes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The serialized refresh handler of materialized table.");

    private FlinkCatalogOptions() {}
}
