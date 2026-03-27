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

package org.apache.fluss.lake.iceberg.version;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.tiering.IcebergCatalogProvider;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link FormatVersionManager}. */
class FormatVersionManagerTest {

    private @TempDir File tempWarehouseDir;
    private Catalog catalog;

    @BeforeEach
    void setUp() {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", "file://" + tempWarehouseDir.getAbsolutePath());
        configuration.setString("type", "hadoop");
        configuration.setString("name", "test_catalog");
        IcebergCatalogProvider provider = new IcebergCatalogProvider(configuration);
        catalog = provider.get();

        // Create namespace
        Namespace namespace = Namespace.of("test_db");
        if (catalog instanceof SupportsNamespaces) {
            ((SupportsNamespaces) catalog).createNamespace(namespace);
        }
    }

    @Test
    void testDetectDefaultFormatVersion() {
        // Create table without explicit format version - should default to V2
        Table table = createTableWithFormatVersion(null);

        assertThat(FormatVersionManager.detectFormatVersion(table))
                .isEqualTo(FormatVersionManager.DEFAULT_FORMAT_VERSION);
    }

    @Test
    void testDetectV2FormatVersion() {
        Table table = createTableWithFormatVersion(2);

        assertThat(FormatVersionManager.detectFormatVersion(table)).isEqualTo(2);
        assertThat(FormatVersionManager.supportsDeletionVectors(table)).isFalse();
    }

    @Test
    void testDetectV3FormatVersion() {
        Table table = createTableWithFormatVersion(3);

        assertThat(FormatVersionManager.detectFormatVersion(table)).isEqualTo(3);
        assertThat(FormatVersionManager.supportsDeletionVectors(table)).isTrue();
    }

    @Test
    void testIsV3OrHigher() {
        assertThat(FormatVersionManager.isV3OrHigher(1)).isFalse();
        assertThat(FormatVersionManager.isV3OrHigher(2)).isFalse();
        assertThat(FormatVersionManager.isV3OrHigher(3)).isTrue();
        assertThat(FormatVersionManager.isV3OrHigher(4)).isTrue();
    }

    @Test
    void testSupportsDeletionVectorsV2Table() {
        Table v2Table = createTableWithFormatVersion(2);
        assertThat(FormatVersionManager.supportsDeletionVectors(v2Table)).isFalse();
    }

    @Test
    void testSupportsDeletionVectorsV3Table() {
        Table v3Table = createTableWithFormatVersion(3);
        assertThat(FormatVersionManager.supportsDeletionVectors(v3Table)).isTrue();
    }

    private Table createTableWithFormatVersion(Integer formatVersion) {
        Schema schema =
                new Schema(
                        Types.NestedField.required(1, "id", Types.LongType.get()),
                        Types.NestedField.optional(2, "name", Types.StringType.get()));

        String tableName = "test_table_v" + (formatVersion != null ? formatVersion : "default");
        TableIdentifier tableId = TableIdentifier.of("test_db", tableName);

        Map<String, String> properties = new HashMap<>();
        if (formatVersion != null) {
            properties.put("format-version", String.valueOf(formatVersion));
        }

        catalog.createTable(tableId, schema, PartitionSpec.unpartitioned(), properties);
        return catalog.loadTable(tableId);
    }
}
