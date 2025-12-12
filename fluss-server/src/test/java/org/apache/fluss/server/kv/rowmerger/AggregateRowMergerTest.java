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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AggregateRowMerger}. */
class AggregateRowMergerTest {

    private static final short SCHEMA_ID = (short) 1;

    private BinaryValue toBinaryValue(BinaryRow row) {
        return new BinaryValue(SCHEMA_ID, row);
    }

    private static final Schema SCHEMA_SUM =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("count", DataTypes.BIGINT())
                    .column("total", DataTypes.DOUBLE())
                    .primaryKey("id")
                    .build();

    private static final RowType ROW_TYPE_SUM = SCHEMA_SUM.getRowType();

    @Test
    void testSumAggregation() {
        Configuration conf = new Configuration();
        conf.setString("table.merge-engine.aggregate.count", "sum");
        conf.setString("table.merge-engine.aggregate.total", "sum");
        TableConfig tableConfig = new TableConfig(conf);

        AggregateRowMerger merger = createMerger(SCHEMA_SUM, tableConfig);

        // First row: id=1, count=5, total=10.5
        BinaryRow row1 = compactedRow(ROW_TYPE_SUM, new Object[] {1, 5L, 10.5});

        // Second row: id=1, count=3, total=7.5
        BinaryRow row2 = compactedRow(ROW_TYPE_SUM, new Object[] {1, 3L, 7.5});

        // Merge: count should be 8 (5+3), total should be 18.0 (10.5+7.5)
        BinaryValue value1 = toBinaryValue(row1);
        BinaryValue result = merger.merge(null, value1);
        assertThat(result).isSameAs(value1);

        BinaryValue value2 = toBinaryValue(row2);
        BinaryValue merged = merger.merge(value1, value2);

        // Verify merged result
        assertThat(merged.row.getInt(0)).isEqualTo(1); // id stays the same
        assertThat(merged.row.getLong(1)).isEqualTo(8L); // count = 5 + 3
        assertThat(merged.row.getDouble(2)).isEqualTo(18.0); // total = 10.5 + 7.5
    }

    @Test
    void testMaxMinAggregation() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("max_val", DataTypes.INT())
                        .column("min_val", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        Configuration conf = new Configuration();
        conf.setString("table.merge-engine.aggregate.max_val", "max");
        conf.setString("table.merge-engine.aggregate.min_val", "min");
        TableConfig tableConfig = new TableConfig(conf);

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        // First row: id=1, max_val=10, min_val=3
        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, 10, 3});

        // Second row: id=1, max_val=15, min_val=1
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, 15, 1});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));

        // Verify: max should be 15, min should be 1
        assertThat(merged.row.getInt(0)).isEqualTo(1);
        assertThat(merged.row.getInt(1)).isEqualTo(15); // max
        assertThat(merged.row.getInt(2)).isEqualTo(1); // min
    }

    @Test
    void testLastNonNullValueAggregation() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        // No explicit aggregate function, should default to last_non_null_value
        TableConfig tableConfig = new TableConfig(new Configuration());

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        // First row: id=1, name="Alice"
        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, "Alice"});

        // Second row: id=1, name="Bob"
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, "Bob"});

        // Third row: id=1, name=null
        BinaryRow row3 = compactedRow(schema.getRowType(), new Object[] {1, null});

        BinaryValue merged1 = merger.merge(toBinaryValue(row1), toBinaryValue(row2));
        assertThat(merged1.row.getString(1).toString()).isEqualTo("Bob");

        // Merge with null should keep "Bob"
        BinaryValue merged2 = merger.merge(merged1, toBinaryValue(row3));
        assertThat(merged2.row.getString(1).toString()).isEqualTo("Bob");
    }

    @Test
    void testDeleteBehaviorRemoveRecord() {
        Configuration conf = new Configuration();
        conf.setString("table.merge-engine.aggregate.count", "sum");
        conf.setBoolean(ConfigOptions.TABLE_AGG_REMOVE_RECORD_ON_DELETE, true);
        TableConfig tableConfig = new TableConfig(conf);

        AggregateRowMerger merger = createMerger(SCHEMA_SUM, tableConfig);

        BinaryRow row = compactedRow(ROW_TYPE_SUM, new Object[] {1, 5L, 10.5});

        // Delete should remove the record
        BinaryValue deleted = merger.delete(toBinaryValue(row));
        assertThat(deleted).isNull();
    }

    @Test
    void testDeleteBehaviorNotAllowed() {
        Configuration conf = new Configuration();
        conf.setString("table.merge-engine.aggregate.count", "sum");
        // removeRecordOnDelete is false by default
        TableConfig tableConfig = new TableConfig(conf);

        AggregateRowMerger merger = createMerger(SCHEMA_SUM, tableConfig);

        BinaryRow row = compactedRow(ROW_TYPE_SUM, new Object[] {1, 5L, 10.5});

        // Delete should throw exception
        assertThatThrownBy(() -> merger.delete(toBinaryValue(row)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("DELETE is not supported");
    }

    @Test
    void testNullValueHandling() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();

        Configuration conf = new Configuration();
        conf.setString("table.merge-engine.aggregate.value", "sum");
        TableConfig tableConfig = new TableConfig(conf);

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        // First row: id=1, value=null
        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, null});

        // Second row: id=1, value=10
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, 10L});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));

        // Result should be 10 (null + 10 = 10)
        assertThat(merged.row.getLong(1)).isEqualTo(10L);
    }

    @Test
    void testPartialUpdate() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("count", DataTypes.BIGINT())
                        .column("total", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .build();

        Configuration conf = new Configuration();
        conf.setString("table.merge-engine.aggregate.count", "sum");
        conf.setString("table.merge-engine.aggregate.total", "sum");
        TableConfig tableConfig = new TableConfig(conf);

        AggregateRowMerger merger = createMerger(schema, tableConfig);

        // Configure partial update for id and count (excluding total)
        RowMerger partialMerger =
                merger.configureTargetColumns(new int[] {0, 1}, SCHEMA_ID, schema);

        // First row: id=1, count=10, total=100.0
        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, 10L, 100.0});

        // Second row (partial): id=1, count=5, total=50.0
        // Only count should be aggregated (10 + 5 = 15)
        // total should remain unchanged (100.0)
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, 5L, 50.0});

        BinaryValue merged = partialMerger.merge(toBinaryValue(row1), toBinaryValue(row2));

        assertThat(merged.row.getInt(0)).isEqualTo(1);
        assertThat(merged.row.getLong(1)).isEqualTo(15L); // 10 + 5 (aggregated)
        assertThat(merged.row.getDouble(2)).isEqualTo(100.0); // unchanged (not in targetColumns)
    }

    @Test
    void testConfigureTargetColumnsNull() {
        TableConfig tableConfig = new TableConfig(new Configuration());
        AggregateRowMerger merger = createMerger(SCHEMA_SUM, tableConfig);

        // Null target columns should return the same merger
        RowMerger result = merger.configureTargetColumns(null, SCHEMA_ID, SCHEMA_SUM);
        assertThat(result).isSameAs(merger);
    }

    @Test
    void testDeleteBehavior() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.TABLE_DELETE_BEHAVIOR.key(), "IGNORE");
        TableConfig tableConfig = new TableConfig(conf);

        AggregateRowMerger merger = createMerger(SCHEMA_SUM, tableConfig);

        assertThat(merger.deleteBehavior()).isEqualTo(DeleteBehavior.IGNORE);
    }

    private AggregateRowMerger createMerger(Schema schema, TableConfig tableConfig) {
        List<String> primaryKeys = schema.getPrimaryKeyColumnNames();
        return new AggregateRowMerger(
                schema.getRowType(),
                AggregatorHelper.createAggregators(schema.getRowType(), primaryKeys, tableConfig),
                tableConfig.getAggregationRemoveRecordOnDelete(),
                tableConfig.getDeleteBehavior().orElse(DeleteBehavior.IGNORE),
                tableConfig.getKvFormat());
    }
}
