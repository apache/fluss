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

package org.apache.fluss.server.kv.rowmerger;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests for {@link RowMerger#create} method. */
class RowMergerCreateTest {

    private static final short SCHEMA_ID = (short) 1;

    private BinaryValue toBinaryValue(BinaryRow row) {
        return new BinaryValue(SCHEMA_ID, row);
    }

    private static final Schema TEST_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("value", DataTypes.BIGINT(), AggFunctions.SUM())
                    .primaryKey("id")
                    .build();

    private static TestingSchemaGetter createSchemaGetter(Schema schema) {
        return new TestingSchemaGetter(new SchemaInfo(schema, SCHEMA_ID));
    }

    @Test
    void testCreateAggregateRowMergerWithMaxFunction() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BIGINT(), AggFunctions.MAX())
                        .primaryKey("id")
                        .build();

        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.TABLE_MERGE_ENGINE.key(), MergeEngineType.AGGREGATION.name());
        TableConfig tableConfig = new TableConfig(conf);

        RowMerger merger =
                RowMerger.create(tableConfig, KvFormat.COMPACTED, createSchemaGetter(schema));
        merger.configureTargetColumns(null, SCHEMA_ID, schema);

        assertThat(merger).isInstanceOf(AggregateRowMerger.class);

        // Verify max aggregation
        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, 10L});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, 20L});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));

        assertThat(merged.row.getInt(0)).isEqualTo(1);
        assertThat(merged.row.getLong(1)).isEqualTo(20L); // max(10, 20)
    }

    @Test
    void testCreateAggregateRowMergerWithDeleteBehaviorAllow() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.TABLE_MERGE_ENGINE.key(), MergeEngineType.AGGREGATION.name());
        conf.set(ConfigOptions.TABLE_DELETE_BEHAVIOR, DeleteBehavior.ALLOW);
        TableConfig tableConfig = new TableConfig(conf);

        RowMerger merger =
                RowMerger.create(tableConfig, KvFormat.COMPACTED, createSchemaGetter(TEST_SCHEMA));

        assertThat(merger).isInstanceOf(AggregateRowMerger.class);
        assertThat(merger.deleteBehavior()).isEqualTo(DeleteBehavior.ALLOW);

        // Verify delete behavior - should remove the record
        BinaryRow row = compactedRow(TEST_SCHEMA.getRowType(), new Object[] {1, 10L});
        BinaryValue deleted = merger.delete(toBinaryValue(row));

        assertThat(deleted).isNull();
    }

    @Test
    void testCreateAggregateRowMergerWithDeleteBehavior() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.TABLE_MERGE_ENGINE.key(), MergeEngineType.AGGREGATION.name());
        conf.setString(ConfigOptions.TABLE_DELETE_BEHAVIOR.key(), DeleteBehavior.IGNORE.name());
        TableConfig tableConfig = new TableConfig(conf);

        RowMerger merger =
                RowMerger.create(tableConfig, KvFormat.COMPACTED, createSchemaGetter(TEST_SCHEMA));

        assertThat(merger).isInstanceOf(AggregateRowMerger.class);
        assertThat(merger.deleteBehavior()).isEqualTo(DeleteBehavior.IGNORE);
    }

    @Test
    void testCreateFirstRowRowMerger() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.TABLE_MERGE_ENGINE.key(), MergeEngineType.FIRST_ROW.name());
        TableConfig tableConfig = new TableConfig(conf);

        RowMerger merger =
                RowMerger.create(tableConfig, KvFormat.COMPACTED, createSchemaGetter(TEST_SCHEMA));

        assertThat(merger).isInstanceOf(FirstRowRowMerger.class);
    }

    @Test
    void testCreateVersionedRowMerger() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("version", DataTypes.BIGINT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.TABLE_MERGE_ENGINE.key(), MergeEngineType.VERSIONED.name());
        conf.setString(ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key(), "version");
        TableConfig tableConfig = new TableConfig(conf);

        RowMerger merger =
                RowMerger.create(tableConfig, KvFormat.COMPACTED, createSchemaGetter(schema));

        assertThat(merger).isInstanceOf(VersionedRowMerger.class);
    }

    @Test
    void testCreateVersionedRowMergerWithoutVersionColumnThrowsException() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.TABLE_MERGE_ENGINE.key(), MergeEngineType.VERSIONED.name());
        // Missing version column configuration
        TableConfig tableConfig = new TableConfig(conf);

        assertThatThrownBy(
                        () ->
                                RowMerger.create(
                                        tableConfig,
                                        KvFormat.COMPACTED,
                                        createSchemaGetter(TEST_SCHEMA)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be set for versioned merge engine");
    }

    @Test
    void testCreateDefaultRowMerger() {
        // No merge engine specified
        Configuration conf = new Configuration();
        TableConfig tableConfig = new TableConfig(conf);

        RowMerger merger =
                RowMerger.create(tableConfig, KvFormat.COMPACTED, createSchemaGetter(TEST_SCHEMA));

        assertThat(merger).isInstanceOf(DefaultRowMerger.class);
    }

    @Test
    void testCreateAggregateRowMergerWithCompositePrimaryKeyAndMultipleAggTypes() {
        // Create schema with composite primary key and various aggregation function types
        Schema schema =
                Schema.newBuilder()
                        .column("id1", DataTypes.INT())
                        .column("id2", DataTypes.STRING())
                        .column("sum_count", DataTypes.BIGINT(), AggFunctions.SUM())
                        .column("max_val", DataTypes.INT(), AggFunctions.MAX())
                        .column("min_val", DataTypes.INT(), AggFunctions.MIN())
                        .column("last_name", DataTypes.STRING(), AggFunctions.LAST_VALUE())
                        .primaryKey("id1", "id2")
                        .build();

        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.TABLE_MERGE_ENGINE.key(), MergeEngineType.AGGREGATION.name());
        TableConfig tableConfig = new TableConfig(conf);

        RowMerger merger =
                RowMerger.create(tableConfig, KvFormat.COMPACTED, createSchemaGetter(schema));

        // Verify the merger is AggregateRowMerger
        assertThat(merger).isInstanceOf(AggregateRowMerger.class);
    }

    @Test
    void testCreateAggregateRowMergerCaseInsensitive() {
        Configuration conf = new Configuration();
        // Test case-insensitive merge engine type
        conf.setString(
                ConfigOptions.TABLE_MERGE_ENGINE.key(),
                MergeEngineType.AGGREGATION.name().toLowerCase());
        TableConfig tableConfig = new TableConfig(conf);

        RowMerger merger =
                RowMerger.create(tableConfig, KvFormat.COMPACTED, createSchemaGetter(TEST_SCHEMA));

        assertThat(merger).isInstanceOf(AggregateRowMerger.class);
    }
}
