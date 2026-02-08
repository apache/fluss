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

package org.apache.fluss.client.lookup;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/** Tests for {@link TableLookup}. */
public class TableLookupTest {

    @Test
    void testCreateLookuperWithoutLookupColumns() {
        // Test default behavior: null lookupColumnNames should create PrimaryKeyLookuper
        TableInfo tableInfo = createTestTableInfo(false, false);
        TableLookup tableLookup = createTableLookup(tableInfo, null);

        Lookuper lookuper = tableLookup.createLookuper();
        assertThat(lookuper).isInstanceOf(PrimaryKeyLookuper.class);
    }

    @Test
    void testCreateLookuperWithPrimaryKey() {
        // Test: lookupBy with complete primary key should create PrimaryKeyLookuper
        TableInfo tableInfo = createTestTableInfo(false, false);
        TableLookup tableLookup = createTableLookup(tableInfo, Arrays.asList("a", "c", "d"));

        Lookuper lookuper = tableLookup.createLookuper();
        assertThat(lookuper).isInstanceOf(PrimaryKeyLookuper.class);
    }

    @Test
    void testCreateLookuperWithPrefixKeyNonPartitioned() {
        // Test: lookupBy with prefix key (non-partitioned) should create PrefixKeyLookuper
        TableInfo tableInfo = createTestTableInfo(false, false);
        TableLookup tableLookup = createTableLookup(tableInfo, Collections.singletonList("a"));

        Lookuper lookuper = tableLookup.createLookuper();
        assertThat(lookuper).isInstanceOf(PrefixKeyLookuper.class);
    }

    @Test
    void testCreateLookuperWithPrefixKeyPartitioned() {
        // Test: lookupBy with prefix key (partitioned) should create PrefixKeyLookuper
        // Primary key: [a, c, d], Partition key: [d], Bucket key: [a]
        // Lookup columns: [a, d] (bucket key + partition key)
        TableInfo tableInfo = createTestTableInfo(true, false);
        TableLookup tableLookup = createTableLookup(tableInfo, Arrays.asList("a", "d"));

        Lookuper lookuper = tableLookup.createLookuper();
        assertThat(lookuper).isInstanceOf(PrefixKeyLookuper.class);
    }

    @Test
    void testCreateLookuperWithPrimaryKeyPartitioned() {
        // Test: lookupBy with complete primary key (partitioned) should create PrimaryKeyLookuper
        TableInfo tableInfo = createTestTableInfo(true, true);
        TableLookup tableLookup = createTableLookup(tableInfo, Arrays.asList("a", "c", "d"));

        Lookuper lookuper = tableLookup.createLookuper();
        assertThat(lookuper).isInstanceOf(PrimaryKeyLookuper.class);
    }

    @Test
    void testCreateLookuperWithInvalidLookupColumns() {
        // Test: invalid lookup columns should throw IllegalArgumentException
        TableInfo tableInfo = createTestTableInfo(false, false);

        // Test with columns that are neither primary key nor valid prefix key
        TableLookup tableLookup1 = createTableLookup(tableInfo, Arrays.asList("b", "c"));
        assertThatThrownBy(tableLookup1::createLookuper)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must contain all bucket keys")
                .hasMessageContaining("in order");

        // Test with partial primary key but not a valid prefix
        TableLookup tableLookup2 = createTableLookup(tableInfo, Arrays.asList("c", "d"));
        assertThatThrownBy(tableLookup2::createLookuper)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must contain all bucket keys")
                .hasMessageContaining("in order");
    }

    @Test
    void testCreateLookuperWithOutOfOrderPrimaryKey() {
        // Test: out-of-order primary key columns should fail
        TableInfo tableInfo = createTestTableInfo(false, false);
        TableLookup tableLookup = createTableLookup(tableInfo, Arrays.asList("c", "a", "d"));

        assertThatThrownBy(tableLookup::createLookuper)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must contain all bucket keys")
                .hasMessageContaining("in order");
    }

    @Test
    void testCreateLookuperWithEmptyLookupColumns() {
        // Test: empty lookup columns should fail
        TableInfo tableInfo = createTestTableInfo(false, false);
        TableLookup tableLookup = createTableLookup(tableInfo, Collections.emptyList());

        assertThatThrownBy(tableLookup::createLookuper)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must contain all bucket keys")
                .hasMessageContaining("in order");
    }

    @Test
    void testCreateLookuperWithTwoColumnPrefix() {
        // Test: two column prefix key
        // Primary key: [a, c, d], Bucket key: [a, c]
        // Lookup columns: [a, c]
        TableInfo tableInfo = createTestTableInfoWithBucketKey(Arrays.asList("a", "c"));
        TableLookup tableLookup = createTableLookup(tableInfo, Arrays.asList("a", "c"));

        Lookuper lookuper = tableLookup.createLookuper();
        assertThat(lookuper).isInstanceOf(PrefixKeyLookuper.class);
    }

    @Test
    void testCreateLookuperWithLogTable() {
        // Test: log table (no primary key) should reject any lookup
        TableInfo logTableInfo = createLogTableInfo();
        TableLookup tableLookup = createTableLookup(logTableInfo, Collections.singletonList("a"));

        assertThatThrownBy(tableLookup::createLookuper)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Log table")
                .hasMessageContaining("doesn't support lookup");
    }

    private TableLookup createTableLookup(
            TableInfo tableInfo, java.util.List<String> lookupColumns) {
        SchemaGetter schemaGetter = mock(SchemaGetter.class);
        MetadataUpdater metadataUpdater = mock(MetadataUpdater.class);
        LookupClient lookupClient = mock(LookupClient.class);

        if (lookupColumns == null) {
            return new TableLookup(tableInfo, schemaGetter, metadataUpdater, lookupClient);
        } else {
            return (TableLookup)
                    new TableLookup(tableInfo, schemaGetter, metadataUpdater, lookupClient)
                            .lookupBy(lookupColumns);
        }
    }

    /**
     * Create a test table info.
     *
     * @param isPartitioned whether the table is partitioned by "d"
     * @param isDefaultBucketKey whether using default bucket key
     * @return TableInfo with: - Columns: [a INT, b STRING, c STRING, d STRING] - Primary key: [a,
     *     c, d] - Partition key: [d] (if isPartitioned) - Bucket key: [a, c] (if isDefaultBucketKey
     *     and isPartitioned) [a, c, d] (if isDefaultBucketKey and !isPartitioned) [a] (if
     *     !isDefaultBucketKey)
     */
    private TableInfo createTestTableInfo(boolean isPartitioned, boolean isDefaultBucketKey) {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.STRING())
                        .column("d", DataTypes.STRING())
                        .primaryKey("a", "c", "d")
                        .build();

        TableDescriptor.Builder builder = TableDescriptor.builder().schema(schema);

        if (isPartitioned) {
            builder.partitionedBy("d");
        }

        if (isDefaultBucketKey) {
            builder.distributedBy(3);
        } else {
            builder.distributedBy(3, "a");
        }

        return TableInfo.of(
                TablePath.of("test_db", "test_table"),
                1L,
                1,
                builder.build(),
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }

    /**
     * Create a test table info with specific bucket key.
     *
     * @param bucketKeys the bucket key columns
     * @return TableInfo with: - Columns: [a INT, b STRING, c STRING, d STRING] - Primary key: [a,
     *     c, d] - Bucket key: specified by parameter
     */
    private TableInfo createTestTableInfoWithBucketKey(java.util.List<String> bucketKeys) {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.STRING())
                        .column("d", DataTypes.STRING())
                        .primaryKey("a", "c", "d")
                        .build();

        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, bucketKeys.toArray(new String[0]));

        return TableInfo.of(
                TablePath.of("test_db", "test_table"),
                1L,
                1,
                builder.build(),
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }

    /**
     * Create a log table info (without primary key).
     *
     * @return TableInfo without primary key
     */
    private TableInfo createLogTableInfo() {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "b").build();

        return TableInfo.of(
                TablePath.of("test_db", "log_table"),
                2L,
                1,
                descriptor,
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }
}
