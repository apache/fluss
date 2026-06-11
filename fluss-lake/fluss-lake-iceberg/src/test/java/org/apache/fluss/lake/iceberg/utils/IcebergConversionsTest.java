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

package org.apache.fluss.lake.iceberg.utils;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Locale;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

/** UT for {@link IcebergConversions}. */
class IcebergConversionsTest {

    @Test
    void testToPartition(@TempDir File tempWarehouseDir) {
        Catalog catalog = getIcebergCatalog(tempWarehouseDir);

        TablePath tablePath = TablePath.of("default", "fluss_non_partitioned_table");
        // for non-multiple partition column partitioned table
        Table table = createIcebergTable(catalog, tablePath, false);
        PartitionKey partitionKey = IcebergConversions.toPartition(table, null, 1);
        assertThat(partitionKey.toPath()).isEqualTo("__bucket=1");

        // for multiple partition columns partitioned table
        tablePath = TablePath.of("default", "fluss_partitioned_table");
        table = createIcebergTable(catalog, tablePath, true);
        partitionKey = IcebergConversions.toPartition(table, "china$region1", 2);
        assertThat(partitionKey.toPath()).isEqualTo("country=china/region=region1/__bucket=2");
    }

    @Test
    void testImplicitTimePartitionValueConversion(@TempDir File tempWarehouseDir) {
        Catalog catalog = getIcebergCatalog(tempWarehouseDir);
        assertImplicitTimePartitionValueConversion(
                catalog,
                AutoPartitionTimeUnit.HOUR,
                "2026060415",
                "event_hour=2026-06-04-15/__bucket=2");
        assertImplicitTimePartitionValueConversion(
                catalog, AutoPartitionTimeUnit.DAY, "20260604", "event_day=2026-06-04/__bucket=2");
        assertImplicitTimePartitionValueConversion(
                catalog, AutoPartitionTimeUnit.MONTH, "202606", "event_month=2026-06/__bucket=2");
        assertImplicitTimePartitionValueConversion(
                catalog, AutoPartitionTimeUnit.YEAR, "2026", "event_year=2026/__bucket=2");
    }

    @Test
    void testMixedPhysicalAndImplicitPartitionValueConversion(@TempDir File tempWarehouseDir) {
        Catalog catalog = getIcebergCatalog(tempWarehouseDir);
        TablePath tablePath = TablePath.of("default", "mixed_implicit_partition_table");
        Schema schema =
                new Schema(
                        required(1, "region", Types.StringType.get()),
                        required(2, "event_time", Types.TimestampType.withoutZone()),
                        optional(3, "__bucket", Types.IntegerType.get()));
        PartitionSpec spec =
                PartitionSpec.builderFor(schema)
                        .identity("region")
                        .day("event_time", "__fluss_implicit_partition_1")
                        .identity("__bucket")
                        .build();
        TableIdentifier tableIdentifier = toIceberg(tablePath);
        ((SupportsNamespaces) catalog).createNamespace(tableIdentifier.namespace());
        Table table = catalog.createTable(tableIdentifier, schema, spec);

        PartitionKey partitionKey = IcebergConversions.toPartition(table, "us$20260604", 2);

        assertThat(partitionKey.toPath())
                .isEqualTo("region=us/__fluss_implicit_partition_1=2026-06-04/__bucket=2");
        assertThat(
                        IcebergConversions.toFlussPartitionValue(
                                spec.fields().get(0), partitionKey.get(0, String.class)))
                .isEqualTo("us");
        assertThat(
                        IcebergConversions.toFlussPartitionValue(
                                spec.fields().get(1), partitionKey.get(1, Integer.class)))
                .isEqualTo("20260604");
    }

    private void assertImplicitTimePartitionValueConversion(
            Catalog catalog,
            AutoPartitionTimeUnit timeUnit,
            String flussPartitionValue,
            String expectedIcebergPath) {
        TablePath tablePath =
                TablePath.of(
                        "default",
                        "implicit_"
                                + timeUnit.name().toLowerCase(Locale.ROOT)
                                + "_partition_table");
        Schema schema =
                new Schema(
                        required(1, "event_time", Types.TimestampType.withoutZone()),
                        optional(2, "__bucket", Types.IntegerType.get()));
        PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
        String partitionFieldName = "event_" + timeUnit.name().toLowerCase(Locale.ROOT);
        switch (timeUnit) {
            case HOUR:
                specBuilder.hour("event_time", partitionFieldName);
                break;
            case DAY:
                specBuilder.day("event_time", partitionFieldName);
                break;
            case MONTH:
                specBuilder.month("event_time", partitionFieldName);
                break;
            case YEAR:
                specBuilder.year("event_time", partitionFieldName);
                break;
            default:
                throw new IllegalArgumentException("Unsupported test time unit: " + timeUnit);
        }
        PartitionSpec spec = specBuilder.identity("__bucket").build();
        TableIdentifier tableIdentifier = toIceberg(tablePath);
        try {
            ((SupportsNamespaces) catalog).createNamespace(tableIdentifier.namespace());
        } catch (AlreadyExistsException ignore) {
            // ignore
        }
        Table table = catalog.createTable(tableIdentifier, schema, spec);

        PartitionKey partitionKey = IcebergConversions.toPartition(table, flussPartitionValue, 2);
        assertThat(partitionKey.toPath()).isEqualTo(expectedIcebergPath);
        assertThat(
                        IcebergConversions.toFlussPartitionValue(
                                spec.fields().get(0), partitionKey.get(0, Integer.class)))
                .isEqualTo(flussPartitionValue);
    }

    private Catalog getIcebergCatalog(File tempWarehouseDir) {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", tempWarehouseDir.toURI().toString());
        configuration.setString("catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog");
        configuration.setString("name", "fluss_test_catalog");
        return IcebergCatalogUtils.createIcebergCatalog(configuration);
    }

    private Table createIcebergTable(
            Catalog catalog, TablePath tablePath, boolean isMultiplePartitionKeyTable) {
        Schema schema =
                new Schema(
                        required(1, "id", Types.LongType.get()),
                        optional(2, "name", Types.StringType.get()),
                        optional(3, "country", Types.StringType.get()),
                        optional(4, "region", Types.StringType.get()),
                        optional(5, "__bucket", Types.IntegerType.get()));
        PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
        if (isMultiplePartitionKeyTable) {
            specBuilder.identity("country").identity("region");
        }

        // we always us __bucket as partition spec
        specBuilder.identity("__bucket");

        TableIdentifier tableIdentifier = toIceberg(tablePath);

        try {
            ((SupportsNamespaces) catalog).createNamespace(tableIdentifier.namespace());
        } catch (AlreadyExistsException ignore) {
            // ignore
        }

        catalog.createTable(tableIdentifier, schema, specBuilder.build());

        return catalog.loadTable(tableIdentifier);
    }
}
