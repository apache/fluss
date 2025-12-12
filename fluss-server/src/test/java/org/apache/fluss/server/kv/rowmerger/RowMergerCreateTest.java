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
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;
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
                    .column("value", DataTypes.BIGINT())
                    .primaryKey("id")
                    .build();

    @Test
    void testCreateAggregateRowMerger() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.TABLE_MERGE_ENGINE.key(), MergeEngineType.AGGREGATE.name());
        conf.setString("table.merge-engine.aggregate.value", "sum");
        TableConfig tableConfig = new TableConfig(conf);

        RowMerger merger = RowMerger.create(tableConfig, TEST_SCHEMA, KvFormat.COMPACTED);

        // Verify the merger is AggregateRowMerger
        assertThat(merger).isInstanceOf(AggregateRowMerger.class);

        // Verify it works correctly
        BinaryRow row1 = compactedRow(TEST_SCHEMA.getRowType(), new Object[] {1, 10L});
        BinaryRow row2 = compactedRow(TEST_SCHEMA.getRowType(), new Object[] {1, 20L});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));

        assertThat(merged.row.getInt(0)).isEqualTo(1);
        assertThat(merged.row.getLong(1)).isEqualTo(30L); // 10 + 20
    }

    @Test
    void testCreateAggregateRowMergerWithDefaultFunction() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.TABLE_MERGE_ENGINE.key(), MergeEngineType.AGGREGATE.name());
        conf.setString("table.merge-engine.aggregate.default-function", "max");
        TableConfig tableConfig = new TableConfig(conf);

        RowMerger merger = RowMerger.create(tableConfig, TEST_SCHEMA, KvFormat.COMPACTED);

        assertThat(merger).isInstanceOf(AggregateRowMerger.class);

        // Verify max aggregation
        BinaryRow row1 = compactedRow(TEST_SCHEMA.getRowType(), new Object[] {1, 10L});
        BinaryRow row2 = compactedRow(TEST_SCHEMA.getRowType(), new Object[] {1, 20L});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));

        assertThat(merged.row.getInt(0)).isEqualTo(1);
        assertThat(merged.row.getLong(1)).isEqualTo(20L); // max(10, 20)
    }

    @Test
    void testCreateAggregateRowMergerWithRemoveRecordOnDelete() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.TABLE_MERGE_ENGINE.key(), MergeEngineType.AGGREGATE.name());
        conf.setBoolean(ConfigOptions.TABLE_AGG_REMOVE_RECORD_ON_DELETE, true);
        TableConfig tableConfig = new TableConfig(conf);

        RowMerger merger = RowMerger.create(tableConfig, TEST_SCHEMA, KvFormat.COMPACTED);

        assertThat(merger).isInstanceOf(AggregateRowMerger.class);

        // Verify delete behavior
        BinaryRow row = compactedRow(TEST_SCHEMA.getRowType(), new Object[] {1, 10L});
        BinaryValue deleted = merger.delete(toBinaryValue(row));

        assertThat(deleted).isNull();
    }

    @Test
    void testCreateAggregateRowMergerWithDeleteBehavior() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.TABLE_MERGE_ENGINE.key(), MergeEngineType.AGGREGATE.name());
        conf.setString(ConfigOptions.TABLE_DELETE_BEHAVIOR.key(), DeleteBehavior.IGNORE.name());
        TableConfig tableConfig = new TableConfig(conf);

        RowMerger merger = RowMerger.create(tableConfig, TEST_SCHEMA, KvFormat.COMPACTED);

        assertThat(merger).isInstanceOf(AggregateRowMerger.class);
        assertThat(merger.deleteBehavior()).isEqualTo(DeleteBehavior.IGNORE);
    }

    @Test
    void testCreateFirstRowRowMerger() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.TABLE_MERGE_ENGINE.key(), MergeEngineType.FIRST_ROW.name());
        TableConfig tableConfig = new TableConfig(conf);

        RowMerger merger = RowMerger.create(tableConfig, TEST_SCHEMA, KvFormat.COMPACTED);

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

        RowMerger merger = RowMerger.create(tableConfig, schema, KvFormat.COMPACTED);

        assertThat(merger).isInstanceOf(VersionedRowMerger.class);
    }

    @Test
    void testCreateVersionedRowMergerWithoutVersionColumnThrowsException() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.TABLE_MERGE_ENGINE.key(), MergeEngineType.VERSIONED.name());
        // Missing version column configuration
        TableConfig tableConfig = new TableConfig(conf);

        assertThatThrownBy(() -> RowMerger.create(tableConfig, TEST_SCHEMA, KvFormat.COMPACTED))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be set for versioned merge engine");
    }

    @Test
    void testCreateDefaultRowMerger() {
        // No merge engine specified
        Configuration conf = new Configuration();
        TableConfig tableConfig = new TableConfig(conf);

        RowMerger merger = RowMerger.create(tableConfig, TEST_SCHEMA, KvFormat.COMPACTED);

        assertThat(merger).isInstanceOf(DefaultRowMerger.class);
    }

    @Test
    void testCreateAggregateRowMergerWithMultipleFields() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("count", DataTypes.BIGINT())
                        .column("max_val", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.TABLE_MERGE_ENGINE.key(), MergeEngineType.AGGREGATE.name());
        conf.setString("table.merge-engine.aggregate.count", "sum");
        conf.setString("table.merge-engine.aggregate.max_val", "max");
        conf.setString("table.merge-engine.aggregate.name", "last_value");
        TableConfig tableConfig = new TableConfig(conf);

        RowMerger merger = RowMerger.create(tableConfig, schema, KvFormat.COMPACTED);

        assertThat(merger).isInstanceOf(AggregateRowMerger.class);

        // Test with actual data
        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, 10L, 100, "Alice"});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, 20L, 200, "Bob"});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));

        assertThat(merged.row.getInt(0)).isEqualTo(1); // id (primary key)
        assertThat(merged.row.getLong(1)).isEqualTo(30L); // count (sum)
        assertThat(merged.row.getInt(2)).isEqualTo(200); // max_val (max)
        assertThat(merged.row.getString(3).toString()).isEqualTo("Bob"); // name (last_value)
    }

    @Test
    void testCreateAggregateRowMergerCaseInsensitive() {
        Configuration conf = new Configuration();
        // Test case-insensitive merge engine type
        conf.setString(ConfigOptions.TABLE_MERGE_ENGINE.key(), "aggregate");
        conf.setString("table.merge-engine.aggregate.value", "sum");
        TableConfig tableConfig = new TableConfig(conf);

        RowMerger merger = RowMerger.create(tableConfig, TEST_SCHEMA, KvFormat.COMPACTED);

        assertThat(merger).isInstanceOf(AggregateRowMerger.class);
    }

    @Test
    void testCreateAggregateRowMergerWithCompositePrimaryKey() {
        Schema schema =
                Schema.newBuilder()
                        .column("id1", DataTypes.INT())
                        .column("id2", DataTypes.STRING())
                        .column("value", DataTypes.BIGINT())
                        .primaryKey("id1", "id2")
                        .build();

        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.TABLE_MERGE_ENGINE.key(), MergeEngineType.AGGREGATE.name());
        conf.setString("table.merge-engine.aggregate.value", "sum");
        TableConfig tableConfig = new TableConfig(conf);

        RowMerger merger = RowMerger.create(tableConfig, schema, KvFormat.COMPACTED);

        assertThat(merger).isInstanceOf(AggregateRowMerger.class);

        // Test with composite key
        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, "key", 10L});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, "key", 20L});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));

        assertThat(merged.row.getInt(0)).isEqualTo(1); // id1
        assertThat(merged.row.getString(1).toString()).isEqualTo("key"); // id2
        assertThat(merged.row.getLong(2)).isEqualTo(30L); // value (sum)
    }
}
