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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AggregateRowMerger}. */
class AggregateRowMergerTest {

    private static final short SCHEMA_ID = (short) 1;

    private BinaryValue toBinaryValue(BinaryRow row) {
        return new BinaryValue(SCHEMA_ID, row);
    }

    private static final Schema SCHEMA_SUM =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("count", DataTypes.BIGINT(), AggFunctions.SUM())
                    .column("total", DataTypes.DOUBLE(), AggFunctions.SUM())
                    .primaryKey("id")
                    .build();

    private static final RowType ROW_TYPE_SUM = SCHEMA_SUM.getRowType();

    @Test
    void testBasicAggregation() {
        // Create a single schema with all aggregation types
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("sum_count", DataTypes.BIGINT(), AggFunctions.SUM())
                        .column("sum_total", DataTypes.DOUBLE(), AggFunctions.SUM())
                        .column("max_val", DataTypes.INT(), AggFunctions.MAX())
                        .column("min_val", DataTypes.INT(), AggFunctions.MIN())
                        .column("name", DataTypes.STRING()) // defaults to last_value_ignore_nulls
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());
        AggregateRowMerger merger = createMerger(schema, tableConfig);
        merger.configureTargetColumns(null, SCHEMA_ID, schema);
        RowType rowType = schema.getRowType();

        // First row: id=1, sum_count=5, sum_total=10.5, max_val=10, min_val=3, name="Alice"
        BinaryRow row1 = compactedRow(rowType, new Object[] {1, 5L, 10.5, 10, 3, "Alice"});

        // Second row: id=1, sum_count=3, sum_total=7.5, max_val=15, min_val=1, name="Bob"
        BinaryRow row2 = compactedRow(rowType, new Object[] {1, 3L, 7.5, 15, 1, "Bob"});

        // Merge rows
        BinaryValue value1 = toBinaryValue(row1);
        BinaryValue result = merger.merge(null, value1);
        assertThat(result).isSameAs(value1);

        BinaryValue value2 = toBinaryValue(row2);
        BinaryValue merged = merger.merge(value1, value2);

        // Enhanced assertions: verify all fields including schemaId and field count
        assertThat(merged.schemaId).isEqualTo(SCHEMA_ID);
        assertThat(merged.row.getFieldCount()).isEqualTo(6);
        assertThat(merged.row.getInt(0)).isEqualTo(1); // id stays the same
        assertThat(merged.row.getLong(1)).isEqualTo(8L); // sum_count = 5 + 3
        assertThat(merged.row.getDouble(2)).isEqualTo(18.0); // sum_total = 10.5 + 7.5
        assertThat(merged.row.getInt(3)).isEqualTo(15); // max_val = max(10, 15)
        assertThat(merged.row.getInt(4)).isEqualTo(1); // min_val = min(3, 1)
        assertThat(merged.row.getString(5).toString()).isEqualTo("Bob"); // name = last non-null

        // Third row: id=1, name=null (other fields don't matter for last_value test)
        BinaryRow row3 = compactedRow(rowType, new Object[] {1, 0L, 0.0, 0, 0, null});

        // Merge with null name should keep "Bob"
        BinaryValue merged2 = merger.merge(merged, toBinaryValue(row3));
        // Enhanced assertion: verify null handling for last_value_ignore_nulls
        assertThat(merged2.schemaId).isEqualTo(SCHEMA_ID);
        assertThat(merged2.row.getString(5).toString())
                .isEqualTo("Bob"); // name should still be "Bob" (null ignored)
        assertThat(merged2.row.isNullAt(5)).isFalse(); // Should not be null
    }

    @Test
    void testDeleteBehaviorRemoveRecord() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.TABLE_DELETE_BEHAVIOR, DeleteBehavior.ALLOW);
        TableConfig tableConfig = new TableConfig(conf);

        AggregateRowMerger merger = createMerger(SCHEMA_SUM, tableConfig);

        BinaryRow row = compactedRow(ROW_TYPE_SUM, new Object[] {1, 5L, 10.5});

        // Delete should remove the record
        BinaryValue deleted = merger.delete(toBinaryValue(row));
        // Enhanced assertion: verify complete removal
        assertThat(deleted).isNull();
        // Verify deleteBehavior is correctly configured
        assertThat(merger.deleteBehavior()).isEqualTo(DeleteBehavior.ALLOW);
    }

    @Test
    void testDeleteBehaviorNotAllowed() {
        // deleteBehavior defaults to IGNORE
        TableConfig tableConfig = new TableConfig(new Configuration());

        AggregateRowMerger merger = createMerger(SCHEMA_SUM, tableConfig);

        // Enhanced assertion: verify deleteBehavior
        assertThat(merger.deleteBehavior()).isEqualTo(DeleteBehavior.IGNORE);
        // Delete method should not be called when deleteBehavior is IGNORE,
        // as KvTablet.processDeletion() will skip it.
        // However, if called directly, it will remove the record.
        BinaryRow row = compactedRow(ROW_TYPE_SUM, new Object[] {1, 5L, 10.5});
        BinaryValue deleted = merger.delete(toBinaryValue(row));
        assertThat(deleted).isNull();
    }

    @Test
    void testNullValueHandling() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BIGINT(), AggFunctions.SUM())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());

        AggregateRowMerger merger = createMerger(schema, tableConfig);
        merger.configureTargetColumns(null, SCHEMA_ID, schema);

        // First row: id=1, value=null
        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, null});

        // Second row: id=1, value=10
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, 10L});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));

        // Enhanced assertions: verify schemaId and null handling
        assertThat(merged.schemaId).isEqualTo(SCHEMA_ID);
        assertThat(merged.row.getFieldCount()).isEqualTo(2);
        assertThat(merged.row.getInt(0)).isEqualTo(1); // id unchanged
        assertThat(merged.row.getLong(1)).isEqualTo(10L); // Result should be 10 (null + 10 = 10)
    }

    @Test
    void testPartialUpdate() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("count", DataTypes.BIGINT(), AggFunctions.SUM())
                        .column("total", DataTypes.DOUBLE(), AggFunctions.SUM())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());

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

        // Enhanced assertions: verify all fields including schemaId
        assertThat(merged.schemaId).isEqualTo(SCHEMA_ID);
        assertThat(merged.row.getFieldCount()).isEqualTo(3);
        assertThat(merged.row.getInt(0)).isEqualTo(1); // id unchanged
        assertThat(merged.row.getLong(1)).isEqualTo(15L); // count: 10 + 5 (aggregated)
        assertThat(merged.row.getDouble(2))
                .isEqualTo(100.0); // total unchanged (not in targetColumns)
        // Verify non-target column is truly unchanged (not aggregated)
        assertThat(merged.row.getDouble(2)).isEqualTo(row1.getDouble(2));
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
    void testPartialUpdateWithSchemaEvolutionAddColumn() {
        // Create old schema: id(columnId=0), count(columnId=1), total(columnId=2)
        Schema oldSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("count", DataTypes.BIGINT(), AggFunctions.SUM())
                        .column("total", DataTypes.DOUBLE(), AggFunctions.SUM())
                        .primaryKey("id")
                        .build();

        // Create new schema: add a new column "new_field" (columnId=3) at the end
        // Ensure columnId inheritance: use fromColumns to preserve columnIds from old schema
        Schema newSchema =
                Schema.newBuilder()
                        .fromColumns(
                                java.util.Arrays.asList(
                                        // id: preserve columnId=0
                                        new Schema.Column("id", DataTypes.INT(), null, 0),
                                        // count: preserve columnId=1
                                        new Schema.Column(
                                                "count",
                                                DataTypes.BIGINT(),
                                                null,
                                                1,
                                                AggFunctions.SUM()),
                                        // total: preserve columnId=2
                                        new Schema.Column(
                                                "total",
                                                DataTypes.DOUBLE(),
                                                null,
                                                2,
                                                AggFunctions.SUM()),
                                        // new_field: new column with columnId=3
                                        new Schema.Column(
                                                "new_field",
                                                DataTypes.BIGINT(),
                                                null,
                                                3,
                                                AggFunctions.SUM())))
                        .primaryKey("id")
                        .build();

        short oldSchemaId = (short) 1;
        short newSchemaId = (short) 2;

        // Create schema getter with both schemas
        // Initialize with newSchema as latest schema for AggregateRowMerger constructor
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(newSchema, newSchemaId));
        // Add old schema to cache
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(oldSchema, oldSchemaId));
        // Ensure newSchema is latest
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(newSchema, newSchemaId));

        TableConfig tableConfig = new TableConfig(new Configuration());
        AggregateRowMerger merger =
                new AggregateRowMerger(tableConfig, tableConfig.getKvFormat(), schemaGetter);

        // Configure partial update for id and count (excluding total and new_field)
        RowMerger partialMerger =
                merger.configureTargetColumns(new int[] {0, 1}, newSchemaId, newSchema);

        // Create old row with old schema: id=1, count=10, total=100.0
        BinaryRow oldRow = compactedRow(oldSchema.getRowType(), new Object[] {1, 10L, 100.0});
        BinaryValue oldValue = new BinaryValue(oldSchemaId, oldRow);

        // Create new row with new schema: id=1, count=5, total=50.0, new_field=20
        BinaryRow newRow = compactedRow(newSchema.getRowType(), new Object[] {1, 5L, 50.0, 20L});
        BinaryValue newValue = new BinaryValue(newSchemaId, newRow);

        // Merge old and new rows
        BinaryValue merged = partialMerger.merge(oldValue, newValue);

        // Enhanced assertions: verify schema evolution and all fields
        assertThat(merged.schemaId).isEqualTo(newSchemaId);
        assertThat(merged.row.getFieldCount()).isEqualTo(4);

        // Verify old fields
        assertThat(merged.row.getInt(0)).isEqualTo(1); // id stays the same
        assertThat(merged.row.getLong(1))
                .isEqualTo(15L); // count = 10 + 5 (aggregated, target column)
        assertThat(merged.row.getDouble(2))
                .isEqualTo(100.0); // total unchanged (not in targetColumns)
        // Verify non-target column is truly unchanged
        assertThat(merged.row.getDouble(2)).isEqualTo(oldRow.getDouble(2));

        // Verify new field: not in targetColumns and not in old schema, so should be null
        // In partial aggregation, non-target columns should not use values from newRow
        assertThat(merged.row.isNullAt(3)).isTrue(); // new_field should be null
    }

    @Test
    void testPartialUpdateWithSchemaEvolutionAddColumnTargetNewField() {
        // Create old schema: id(columnId=0), count(columnId=1)
        Schema oldSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("count", DataTypes.BIGINT(), AggFunctions.SUM())
                        .primaryKey("id")
                        .build();

        // Create new schema: add a new column "new_field" (columnId=2) at the end
        // Ensure columnId inheritance: use fromColumns to preserve columnIds from old schema
        Schema newSchema =
                Schema.newBuilder()
                        .fromColumns(
                                java.util.Arrays.asList(
                                        // id: preserve columnId=0
                                        new Schema.Column("id", DataTypes.INT(), null, 0),
                                        // count: preserve columnId=1
                                        new Schema.Column(
                                                "count",
                                                DataTypes.BIGINT(),
                                                null,
                                                1,
                                                AggFunctions.SUM()),
                                        // new_field: new column with columnId=2
                                        new Schema.Column(
                                                "new_field",
                                                DataTypes.BIGINT(),
                                                null,
                                                2,
                                                AggFunctions.SUM())))
                        .primaryKey("id")
                        .build();

        short oldSchemaId = (short) 1;
        short newSchemaId = (short) 2;

        // Create schema getter with both schemas
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(newSchema, newSchemaId));
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(oldSchema, oldSchemaId));
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(newSchema, newSchemaId));

        TableConfig tableConfig = new TableConfig(new Configuration());
        AggregateRowMerger merger =
                new AggregateRowMerger(tableConfig, tableConfig.getKvFormat(), schemaGetter);

        // Configure partial update for id and new_field (excluding count)
        RowMerger partialMerger =
                merger.configureTargetColumns(new int[] {0, 2}, newSchemaId, newSchema);

        // Create old row with old schema: id=1, count=10
        BinaryRow oldRow = compactedRow(oldSchema.getRowType(), new Object[] {1, 10L});
        BinaryValue oldValue = new BinaryValue(oldSchemaId, oldRow);

        // Create new row with new schema: id=1, count=5, new_field=20
        BinaryRow newRow = compactedRow(newSchema.getRowType(), new Object[] {1, 5L, 20L});
        BinaryValue newValue = new BinaryValue(newSchemaId, newRow);

        // Merge old and new rows
        BinaryValue merged = partialMerger.merge(oldValue, newValue);

        // Enhanced assertions: verify schema evolution and field handling
        assertThat(merged.schemaId).isEqualTo(newSchemaId);
        assertThat(merged.row.getFieldCount()).isEqualTo(3);

        // Verify fields
        assertThat(merged.row.getInt(0)).isEqualTo(1); // id stays the same
        assertThat(merged.row.getLong(1)).isEqualTo(10L); // count unchanged (not in targetColumns)
        // Verify non-target column is truly unchanged
        assertThat(merged.row.getLong(1)).isEqualTo(oldRow.getLong(1));
        assertThat(merged.row.getLong(2))
                .isEqualTo(20L); // new_field = null + 20 (aggregated, target column)
    }

    @Test
    void testSchemaEvolutionAddColumnWithMultipleAggTypes() {
        // Create old schema: id(columnId=0), sum_count(columnId=1), max_val(columnId=2)
        Schema oldSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("sum_count", DataTypes.BIGINT(), AggFunctions.SUM())
                        .column("max_val", DataTypes.INT(), AggFunctions.MAX())
                        .primaryKey("id")
                        .build();

        // Create new schema: add new columns "min_val" (columnId=3) and "new_sum" (columnId=4)
        // Ensure columnId inheritance: use fromColumns to preserve columnIds from old schema
        Schema newSchema =
                Schema.newBuilder()
                        .fromColumns(
                                java.util.Arrays.asList(
                                        // id: preserve columnId=0
                                        new Schema.Column("id", DataTypes.INT(), null, 0),
                                        // sum_count: preserve columnId=1
                                        new Schema.Column(
                                                "sum_count",
                                                DataTypes.BIGINT(),
                                                null,
                                                1,
                                                AggFunctions.SUM()),
                                        // max_val: preserve columnId=2
                                        new Schema.Column(
                                                "max_val",
                                                DataTypes.INT(),
                                                null,
                                                2,
                                                AggFunctions.MAX()),
                                        // min_val: new column with columnId=3
                                        new Schema.Column(
                                                "min_val",
                                                DataTypes.INT(),
                                                null,
                                                3,
                                                AggFunctions.MIN()),
                                        // new_sum: new column with columnId=4
                                        new Schema.Column(
                                                "new_sum",
                                                DataTypes.BIGINT(),
                                                null,
                                                4,
                                                AggFunctions.SUM())))
                        .primaryKey("id")
                        .build();

        short oldSchemaId = (short) 1;
        short newSchemaId = (short) 2;

        // Create schema getter with both schemas
        // Initialize with newSchema as latest schema for AggregateRowMerger constructor
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(newSchema, newSchemaId));
        // Add old schema to cache
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(oldSchema, oldSchemaId));
        // Ensure newSchema is latest
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(newSchema, newSchemaId));

        TableConfig tableConfig = new TableConfig(new Configuration());
        AggregateRowMerger merger =
                new AggregateRowMerger(tableConfig, tableConfig.getKvFormat(), schemaGetter);
        merger.configureTargetColumns(null, newSchemaId, newSchema);

        // Create old row with old schema: id=1, sum_count=100, max_val=50
        BinaryRow oldRow = compactedRow(oldSchema.getRowType(), new Object[] {1, 100L, 50});
        BinaryValue oldValue = new BinaryValue(oldSchemaId, oldRow);

        // Create new row with new schema: id=1, sum_count=20, max_val=80, min_val=10, new_sum=30
        BinaryRow newRow = compactedRow(newSchema.getRowType(), new Object[] {1, 20L, 80, 10, 30L});
        BinaryValue newValue = new BinaryValue(newSchemaId, newRow);

        // Merge old and new rows
        BinaryValue merged = merger.merge(oldValue, newValue);

        // Enhanced assertions: verify schema evolution and all aggregation functions
        assertThat(merged.schemaId).isEqualTo(newSchemaId);
        assertThat(merged.row.getFieldCount()).isEqualTo(5);

        // Verify old fields are aggregated correctly
        assertThat(merged.row.getInt(0)).isEqualTo(1); // id stays the same
        assertThat(merged.row.getLong(1)).isEqualTo(120L); // sum_count = 100 + 20
        assertThat(merged.row.getInt(2)).isEqualTo(80); // max_val = max(50, 80)

        // Verify new fields are aggregated correctly (null handling)
        assertThat(merged.row.getInt(3)).isEqualTo(10); // min_val = min(null, 10) = 10
        assertThat(merged.row.isNullAt(3)).isFalse(); // Should not be null
        assertThat(merged.row.getLong(4)).isEqualTo(30L); // new_sum = null + 30 = 30
        assertThat(merged.row.isNullAt(4)).isFalse(); // Should not be null
    }

    // ========== Schema Evolution Tests ==========

    @Test
    void testSchemaEvolutionColumnReorder() {
        // Old schema: [id(0), name(1), age(2)]
        Schema oldSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT()) // columnId = 0
                        .column("name", DataTypes.STRING()) // columnId = 1
                        .column("age", DataTypes.INT()) // columnId = 2
                        .primaryKey("id")
                        .build();

        // New schema: [id(0), age(2), name(1)] - columns reordered!
        Schema newSchema =
                Schema.newBuilder()
                        .fromColumns(
                                java.util.Arrays.asList(
                                        new Schema.Column("id", DataTypes.INT(), null, 0),
                                        new Schema.Column(
                                                "age",
                                                DataTypes.INT(),
                                                null,
                                                2,
                                                AggFunctions.SUM()),
                                        new Schema.Column(
                                                "name",
                                                DataTypes.STRING(),
                                                null,
                                                1,
                                                AggFunctions.LAST_VALUE())))
                        .primaryKey("id")
                        .build();

        short oldSchemaId = (short) 1;
        short newSchemaId = (short) 2;

        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(newSchema, newSchemaId));
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(oldSchema, oldSchemaId));
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(newSchema, newSchemaId));

        TableConfig tableConfig = new TableConfig(new Configuration());
        AggregateRowMerger merger =
                new AggregateRowMerger(tableConfig, tableConfig.getKvFormat(), schemaGetter);
        merger.configureTargetColumns(null, newSchemaId, newSchema);

        // Old row: id=1, name="Alice", age=20
        BinaryRow oldRow = compactedRow(oldSchema.getRowType(), new Object[] {1, "Alice", 20});
        BinaryValue oldValue = new BinaryValue(oldSchemaId, oldRow);

        // New row: id=1, age=30, name="Bob" (reordered!)
        BinaryRow newRow = compactedRow(newSchema.getRowType(), new Object[] {1, 30, "Bob"});
        BinaryValue newValue = new BinaryValue(newSchemaId, newRow);

        // Merge
        BinaryValue merged = merger.merge(oldValue, newValue);

        // Enhanced assertions: verify column ID matching and schema evolution
        assertThat(merged.schemaId).isEqualTo(newSchemaId);
        assertThat(merged.row.getFieldCount()).isEqualTo(3);
        // Verify: should correctly match by column ID, not position
        assertThat(merged.row.getInt(0)).isEqualTo(1); // id
        assertThat(merged.row.getInt(1)).isEqualTo(50); // age: 20 + 30 = 50 (SUM)
        assertThat(merged.row.getString(2).toString()).isEqualTo("Bob"); // name: last_value
        // Verify column reordering: old schema [id, name, age], new schema [id, age, name]
        // Should match by columnId, not position
    }

    @Test
    void testSchemaEvolutionDropMiddleColumn() {
        // Old schema: [id(0), dropped(1), count(2)]
        Schema oldSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("dropped", DataTypes.STRING())
                        .column("count", DataTypes.BIGINT(), AggFunctions.SUM())
                        .primaryKey("id")
                        .build();

        // New schema: [id(0), count(2)] - dropped column removed
        Schema newSchema =
                Schema.newBuilder()
                        .fromColumns(
                                java.util.Arrays.asList(
                                        new Schema.Column("id", DataTypes.INT(), null, 0),
                                        new Schema.Column(
                                                "count",
                                                DataTypes.BIGINT(),
                                                null,
                                                2,
                                                AggFunctions.SUM())))
                        .primaryKey("id")
                        .build();

        short oldSchemaId = (short) 1;
        short newSchemaId = (short) 2;

        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(newSchema, newSchemaId));
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(oldSchema, oldSchemaId));
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(newSchema, newSchemaId));

        TableConfig tableConfig = new TableConfig(new Configuration());
        AggregateRowMerger merger =
                new AggregateRowMerger(tableConfig, tableConfig.getKvFormat(), schemaGetter);
        merger.configureTargetColumns(null, newSchemaId, newSchema);

        // Old row: id=1, dropped="ignore", count=100
        BinaryRow oldRow = compactedRow(oldSchema.getRowType(), new Object[] {1, "ignore", 100L});
        BinaryValue oldValue = new BinaryValue(oldSchemaId, oldRow);

        // New row: id=1, count=50
        BinaryRow newRow = compactedRow(newSchema.getRowType(), new Object[] {1, 50L});
        BinaryValue newValue = new BinaryValue(newSchemaId, newRow);

        // Merge
        BinaryValue merged = merger.merge(oldValue, newValue);

        // Enhanced assertions: verify column dropping and aggregation
        assertThat(merged.schemaId).isEqualTo(newSchemaId);
        assertThat(merged.row.getFieldCount()).isEqualTo(2);
        // Verify: should correctly aggregate count despite position change
        assertThat(merged.row.getInt(0)).isEqualTo(1); // id
        assertThat(merged.row.getLong(1)).isEqualTo(150L); // count: 100 + 50
        // Verify dropped column is not present in result
    }

    @Test
    void testSchemaEvolutionInsertMiddleColumn() {
        // Old schema: [id(0), count(2)]
        Schema oldSchema =
                Schema.newBuilder()
                        .fromColumns(
                                java.util.Arrays.asList(
                                        new Schema.Column("id", DataTypes.INT(), null, 0),
                                        new Schema.Column(
                                                "count",
                                                DataTypes.BIGINT(),
                                                null,
                                                2,
                                                AggFunctions.SUM())))
                        .primaryKey("id")
                        .highestFieldId((short) 2)
                        .build();

        // New schema: [id(0), inserted(1), count(2)] - new column inserted in middle
        Schema newSchema =
                Schema.newBuilder()
                        .fromColumns(
                                java.util.Arrays.asList(
                                        new Schema.Column("id", DataTypes.INT(), null, 0),
                                        new Schema.Column(
                                                "inserted",
                                                DataTypes.STRING(),
                                                null,
                                                1,
                                                AggFunctions.LAST_VALUE()),
                                        new Schema.Column(
                                                "count",
                                                DataTypes.BIGINT(),
                                                null,
                                                2,
                                                AggFunctions.SUM())))
                        .primaryKey("id")
                        .build();

        short oldSchemaId = (short) 1;
        short newSchemaId = (short) 2;

        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(newSchema, newSchemaId));
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(oldSchema, oldSchemaId));
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(newSchema, newSchemaId));

        TableConfig tableConfig = new TableConfig(new Configuration());
        AggregateRowMerger merger =
                new AggregateRowMerger(tableConfig, tableConfig.getKvFormat(), schemaGetter);
        merger.configureTargetColumns(null, newSchemaId, newSchema);

        // Old row: id=1, count=100
        BinaryRow oldRow = compactedRow(oldSchema.getRowType(), new Object[] {1, 100L});
        BinaryValue oldValue = new BinaryValue(oldSchemaId, oldRow);

        // New row: id=1, inserted="new", count=50
        BinaryRow newRow = compactedRow(newSchema.getRowType(), new Object[] {1, "new", 50L});
        BinaryValue newValue = new BinaryValue(newSchemaId, newRow);

        // Merge
        BinaryValue merged = merger.merge(oldValue, newValue);

        // Enhanced assertions: verify column insertion and aggregation
        assertThat(merged.schemaId).isEqualTo(newSchemaId);
        assertThat(merged.row.getFieldCount()).isEqualTo(3);
        assertThat(merged.row.getInt(0)).isEqualTo(1); // id
        assertThat(merged.row.getString(1).toString()).isEqualTo("new"); // inserted (new column)
        assertThat(merged.row.isNullAt(1)).isFalse(); // Should not be null
        assertThat(merged.row.getLong(2)).isEqualTo(150L); // count: 100 + 50
        // Verify new column is correctly inserted at middle position
    }

    @Test
    void testSchemaEvolutionPartialUpdateWithReorder() {
        // Old schema: [id(0), a(1), b(2), c(3)]
        Schema oldSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("a", DataTypes.BIGINT(), AggFunctions.SUM())
                        .column("b", DataTypes.BIGINT(), AggFunctions.SUM())
                        .column("c", DataTypes.BIGINT(), AggFunctions.SUM())
                        .primaryKey("id")
                        .build();

        // New schema: [id(0), c(3), b(2), a(1)] - all columns reordered!
        Schema newSchema =
                Schema.newBuilder()
                        .fromColumns(
                                java.util.Arrays.asList(
                                        new Schema.Column("id", DataTypes.INT(), null, 0),
                                        new Schema.Column(
                                                "c",
                                                DataTypes.BIGINT(),
                                                null,
                                                3,
                                                AggFunctions.SUM()),
                                        new Schema.Column(
                                                "b",
                                                DataTypes.BIGINT(),
                                                null,
                                                2,
                                                AggFunctions.SUM()),
                                        new Schema.Column(
                                                "a",
                                                DataTypes.BIGINT(),
                                                null,
                                                1,
                                                AggFunctions.SUM())))
                        .primaryKey("id")
                        .build();

        short oldSchemaId = (short) 1;
        short newSchemaId = (short) 2;

        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(newSchema, newSchemaId));
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(oldSchema, oldSchemaId));
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(newSchema, newSchemaId));

        TableConfig tableConfig = new TableConfig(new Configuration());
        AggregateRowMerger merger =
                new AggregateRowMerger(tableConfig, tableConfig.getKvFormat(), schemaGetter);

        // Configure partial update: only update "a" and "c" (at positions 3 and 1 in new schema)
        // But we specify by original indices in new schema
        RowMerger partialMerger =
                merger.configureTargetColumns(
                        new int[] {0, 1, 3}, // id, c, a in new schema positions
                        newSchemaId,
                        newSchema);

        // Old row: id=1, a=10, b=20, c=30
        BinaryRow oldRow = compactedRow(oldSchema.getRowType(), new Object[] {1, 10L, 20L, 30L});
        BinaryValue oldValue = new BinaryValue(oldSchemaId, oldRow);

        // New row: id=1, c=5, b=999, a=3 (reordered, b should be ignored)
        BinaryRow newRow = compactedRow(newSchema.getRowType(), new Object[] {1, 5L, 999L, 3L});
        BinaryValue newValue = new BinaryValue(newSchemaId, newRow);

        // Merge
        BinaryValue merged = partialMerger.merge(oldValue, newValue);

        // Enhanced assertions: verify column ID matching and partial update behavior
        assertThat(merged.schemaId).isEqualTo(newSchemaId);
        assertThat(merged.row.getFieldCount()).isEqualTo(4);
        // Verify: should match by column ID, not position
        assertThat(merged.row.getInt(0)).isEqualTo(1); // id
        assertThat(merged.row.getLong(1)).isEqualTo(35L); // c: 30 + 5 = 35 (target, aggregated)
        assertThat(merged.row.getLong(2)).isEqualTo(20L); // b: kept old value (not target)
        // Verify non-target column is truly unchanged
        assertThat(merged.row.getLong(2)).isEqualTo(oldRow.getLong(2));
        assertThat(merged.row.getLong(3)).isEqualTo(13L); // a: 10 + 3 = 13 (target, aggregated)
    }

    @Test
    void testPartialAggregateRowMergerDeleteAllScenarios() {
        // Create schema with multiple columns for comprehensive delete testing
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("target_sum", DataTypes.BIGINT(), AggFunctions.SUM())
                        .column("target_max", DataTypes.INT(), AggFunctions.MAX())
                        .column("non_target", DataTypes.STRING())
                        .column("non_target_count", DataTypes.BIGINT(), AggFunctions.SUM())
                        .primaryKey("id")
                        .build();

        short schemaId = SCHEMA_ID;

        // ========== Scenario 1: deleteBehavior = ALLOW, partial delete with non-target values
        // ==========
        {
            Configuration conf = new Configuration();
            conf.set(ConfigOptions.TABLE_DELETE_BEHAVIOR, DeleteBehavior.ALLOW);
            TableConfig tableConfig = new TableConfig(conf);

            AggregateRowMerger merger = createMerger(schema, tableConfig);
            RowMerger partialMerger =
                    merger.configureTargetColumns(new int[] {0, 1, 2}, schemaId, schema);

            BinaryRow row =
                    compactedRow(schema.getRowType(), new Object[] {1, 100L, 50, "keep", 200L});
            BinaryValue oldValue = new BinaryValue(schemaId, row);

            BinaryValue deleted = partialMerger.delete(oldValue);

            // Should perform partial delete: target columns (except PK) set to null, non-target
            // columns kept
            assertThat(deleted).isNotNull();
            assertThat(deleted.schemaId).isEqualTo(schemaId);
            assertThat(deleted.row.getFieldCount()).isEqualTo(5);

            // Primary key should be kept
            assertThat(deleted.row.getInt(0)).isEqualTo(1);

            // Target columns (non-PK) should be set to null
            assertThat(deleted.row.isNullAt(1)).isTrue(); // target_sum
            assertThat(deleted.row.isNullAt(2)).isTrue(); // target_max

            // Non-target columns should be kept unchanged
            assertThat(deleted.row.getString(3).toString()).isEqualTo("keep");
            assertThat(deleted.row.getLong(4)).isEqualTo(200L);
        }

        // ========== Scenario 2: deleteBehavior = IGNORE (default), partial delete with non-target
        // values ==========
        {
            TableConfig tableConfig = new TableConfig(new Configuration());
            AggregateRowMerger merger = createMerger(schema, tableConfig);
            // Target columns: id(0), target_sum(1), target_max(2)
            // Non-target columns: non_target(3), non_target_count(4)
            RowMerger partialMerger =
                    merger.configureTargetColumns(new int[] {0, 1, 2}, schemaId, schema);

            BinaryRow row =
                    compactedRow(schema.getRowType(), new Object[] {1, 100L, 50, "keep_me", 200L});
            BinaryValue oldValue = new BinaryValue(schemaId, row);

            BinaryValue deleted = partialMerger.delete(oldValue);

            // Should perform partial delete: target columns (except PK) set to null, non-target
            // columns kept
            assertThat(deleted).isNotNull();
            assertThat(deleted.schemaId).isEqualTo(schemaId);
            assertThat(deleted.row.getFieldCount()).isEqualTo(5);

            // Primary key should be kept
            assertThat(deleted.row.getInt(0)).isEqualTo(1);

            // Target columns (non-PK) should be set to null
            assertThat(deleted.row.isNullAt(1)).isTrue(); // target_sum
            assertThat(deleted.row.isNullAt(2)).isTrue(); // target_max

            // Non-target columns should be kept unchanged
            assertThat(deleted.row.getString(3).toString()).isEqualTo("keep_me");
            assertThat(deleted.row.getLong(4)).isEqualTo(200L);
        }

        // ========== Scenario 3: deleteBehavior = IGNORE (default), all non-target columns are null
        // ==========
        {
            TableConfig tableConfig = new TableConfig(new Configuration());
            AggregateRowMerger merger = createMerger(schema, tableConfig);
            // Target columns: id(0), target_sum(1)
            RowMerger partialMerger =
                    merger.configureTargetColumns(new int[] {0, 1}, schemaId, schema);

            // All non-target columns are null
            BinaryRow row =
                    compactedRow(schema.getRowType(), new Object[] {1, 100L, null, null, null});
            BinaryValue oldValue = new BinaryValue(schemaId, row);

            BinaryValue deleted = partialMerger.delete(oldValue);

            // Should return null (all non-target columns are null)
            assertThat(deleted).isNull();
        }

        // ========== Scenario 4: deleteBehavior = IGNORE (default), Schema Evolution + Partial
        // Delete ==========
        {
            // Old schema: id(columnId=0), target_sum(columnId=1), non_target(columnId=2)
            Schema oldSchema =
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("target_sum", DataTypes.BIGINT(), AggFunctions.SUM())
                            .column("non_target", DataTypes.STRING())
                            .primaryKey("id")
                            .build();

            // New schema: id(columnId=0), target_sum(columnId=1), target_max(columnId=3, new),
            // non_target(columnId=2), non_target_count(columnId=4, new)
            // Ensure columnId inheritance: use fromColumns to preserve columnIds from old schema
            Schema newSchema =
                    Schema.newBuilder()
                            .fromColumns(
                                    java.util.Arrays.asList(
                                            // id: preserve columnId=0
                                            new Schema.Column("id", DataTypes.INT(), null, 0),
                                            // target_sum: preserve columnId=1
                                            new Schema.Column(
                                                    "target_sum",
                                                    DataTypes.BIGINT(),
                                                    null,
                                                    1,
                                                    AggFunctions.SUM()),
                                            // target_max: new column with columnId=3
                                            new Schema.Column(
                                                    "target_max",
                                                    DataTypes.INT(),
                                                    null,
                                                    3,
                                                    AggFunctions.MAX()),
                                            // non_target: preserve columnId=2
                                            new Schema.Column(
                                                    "non_target", DataTypes.STRING(), null, 2),
                                            // non_target_count: new column with columnId=4
                                            new Schema.Column(
                                                    "non_target_count",
                                                    DataTypes.BIGINT(),
                                                    null,
                                                    4,
                                                    AggFunctions.SUM())))
                            .primaryKey("id")
                            .build();

            short oldSchemaId = (short) 1;
            short newSchemaId = (short) 2;

            TestingSchemaGetter schemaGetter =
                    new TestingSchemaGetter(new SchemaInfo(newSchema, newSchemaId));
            schemaGetter.updateLatestSchemaInfo(new SchemaInfo(oldSchema, oldSchemaId));
            schemaGetter.updateLatestSchemaInfo(new SchemaInfo(newSchema, newSchemaId));

            TableConfig tableConfig = new TableConfig(new Configuration());
            AggregateRowMerger merger =
                    new AggregateRowMerger(tableConfig, tableConfig.getKvFormat(), schemaGetter);

            // Configure partial update for id and target_sum (columns 0, 1 in new schema)
            RowMerger partialMerger =
                    merger.configureTargetColumns(new int[] {0, 1}, newSchemaId, newSchema);

            // Old row with old schema: id=1, target_sum=100, non_target="keep"
            BinaryRow oldRow = compactedRow(oldSchema.getRowType(), new Object[] {1, 100L, "keep"});
            BinaryValue oldValue = new BinaryValue(oldSchemaId, oldRow);

            BinaryValue deleted = partialMerger.delete(oldValue);

            // Should perform partial delete using new schema
            assertThat(deleted).isNotNull();
            assertThat(deleted.schemaId).isEqualTo(newSchemaId);
            assertThat(deleted.row.getFieldCount()).isEqualTo(5);

            // Primary key kept
            assertThat(deleted.row.getInt(0)).isEqualTo(1);

            // Target column (target_sum) set to null
            assertThat(deleted.row.isNullAt(1)).isTrue();

            // New column (target_max) should be null (didn't exist in old schema)
            assertThat(deleted.row.isNullAt(2)).isTrue();

            // Non-target column (non_target) kept
            assertThat(deleted.row.getString(3).toString()).isEqualTo("keep");

            // New non-target column (non_target_count) should be null
            assertThat(deleted.row.isNullAt(4)).isTrue();
        }

        // ========== Scenario 5: deleteBehavior = IGNORE (default), composite primary key
        // ==========
        {
            Schema compositePkSchema =
                    Schema.newBuilder()
                            .column("id1", DataTypes.INT())
                            .column("id2", DataTypes.STRING())
                            .column("target_sum", DataTypes.BIGINT(), AggFunctions.SUM())
                            .column("non_target", DataTypes.STRING())
                            .primaryKey("id1", "id2")
                            .build();

            TableConfig tableConfig = new TableConfig(new Configuration());
            AggregateRowMerger merger = createMerger(compositePkSchema, tableConfig);
            // Target columns: id1(0), id2(1), target_sum(2)
            RowMerger partialMerger =
                    merger.configureTargetColumns(
                            new int[] {0, 1, 2}, SCHEMA_ID, compositePkSchema);

            BinaryRow row =
                    compactedRow(
                            compositePkSchema.getRowType(),
                            new Object[] {1, "pk2", 100L, "keep_me"});
            BinaryValue oldValue = new BinaryValue(SCHEMA_ID, row);

            BinaryValue deleted = partialMerger.delete(oldValue);

            // Both primary key columns should be kept
            assertThat(deleted).isNotNull();
            assertThat(deleted.row.getInt(0)).isEqualTo(1); // id1
            assertThat(deleted.row.getString(1).toString()).isEqualTo("pk2"); // id2
            // Target column (non-PK) should be null
            assertThat(deleted.row.isNullAt(2)).isTrue(); // target_sum
            // Non-target column should be kept
            assertThat(deleted.row.getString(3).toString()).isEqualTo("keep_me");
        }
    }

    private AggregateRowMerger createMerger(Schema schema, TableConfig tableConfig) {
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(schema, SCHEMA_ID));
        return new AggregateRowMerger(tableConfig, tableConfig.getKvFormat(), schemaGetter);
    }
}
