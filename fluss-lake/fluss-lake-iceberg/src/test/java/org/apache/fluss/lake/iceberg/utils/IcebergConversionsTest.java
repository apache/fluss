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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.conf.IcebergConfiguration;
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
import java.util.Map;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.iceberg.CatalogUtil.buildIcebergCatalog;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

/** UT for {@link IcebergConversions}. */
class IcebergConversionsTest {
    ;

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

    private Catalog getIcebergCatalog(File tempWarehouseDir) {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", tempWarehouseDir.toURI().toString());
        configuration.setString("catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog");
        configuration.setString("name", "fluss_test_catalog");

        Map<String, String> icebergProps = configuration.toMap();
        String catalogName = icebergProps.getOrDefault("name", "default_iceberg_catalog");
        return buildIcebergCatalog(
                catalogName, icebergProps, IcebergConfiguration.from(configuration).get());
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
