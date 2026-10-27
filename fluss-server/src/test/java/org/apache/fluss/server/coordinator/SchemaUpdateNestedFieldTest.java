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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.ColumnPath;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for nested field schema updates. */
class SchemaUpdateNestedFieldTest {

    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @Test
    void testAddNestedFieldToRow() {
        Schema schema = baseSchema();

        Schema updated =
                SchemaUpdate.applySchemaChanges(
                        schema,
                        Collections.singletonList(
                                TableChange.addNestedField(
                                        ColumnPath.of("profile", "address"),
                                        "country",
                                        DataTypes.STRING(),
                                        "country name")));

        RowType profileType = (RowType) updated.getColumn("profile").getDataType();
        RowType addressType = (RowType) profileType.getField("address").getType();
        DataField countryField = addressType.getField("country");

        assertThat(countryField.getType()).isEqualTo(DataTypes.STRING());
        assertThat(countryField.getDescription()).hasValue("country name");
        assertThat(countryField.getFieldId()).isEqualTo(schema.getHighestFieldId() + 1);
        assertThat(updated.getHighestFieldId()).isEqualTo(schema.getHighestFieldId() + 1);

        RowType originalProfileType = (RowType) schema.getColumn("profile").getDataType();
        RowType originalAddressType = (RowType) originalProfileType.getField("address").getType();
        assertThat(addressType.getField("city").getFieldId())
                .isEqualTo(originalAddressType.getField("city").getFieldId());
    }

    @Test
    void testRejectInvalidNestedFieldAdd() {
        Schema schema = baseSchema();

        assertThatThrownBy(
                        () ->
                                SchemaUpdate.applySchemaChanges(
                                        schema,
                                        Collections.singletonList(
                                                TableChange.addNestedField(
                                                        ColumnPath.of("profile", "address"),
                                                        "country",
                                                        DataTypes.STRING().copy(false),
                                                        null))))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("must be nullable");

        assertThatThrownBy(
                        () ->
                                SchemaUpdate.applySchemaChanges(
                                        schema,
                                        Collections.singletonList(
                                                TableChange.addNestedField(
                                                        ColumnPath.of("profile", "address"),
                                                        "city",
                                                        DataTypes.STRING(),
                                                        null))))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("already exists");

        assertThatThrownBy(
                        () ->
                                SchemaUpdate.applySchemaChanges(
                                        schema,
                                        Collections.singletonList(
                                                TableChange.addNestedField(
                                                        ColumnPath.of("id"),
                                                        "country",
                                                        DataTypes.STRING(),
                                                        null))))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("primary key");

        assertThatThrownBy(
                        () ->
                                SchemaUpdate.applySchemaChanges(
                                        schema,
                                        Collections.singletonList(
                                                TableChange.addNestedField(
                                                        ColumnPath.of("missing"),
                                                        "country",
                                                        DataTypes.STRING(),
                                                        null))))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("Cannot find parent path");
    }

    @Test
    void testRejectNestedFieldAddUnderKeyAndLakeTable() {
        Schema schema = baseSchema();
        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addNestedField(
                                ColumnPath.of("profile"), "country", DataTypes.STRING(), null));

        assertThatThrownBy(
                        () ->
                                NestedSchemaChangeValidator.validate(
                                        tableInfo(
                                                schema,
                                                Collections.singletonList("profile"),
                                                Collections.emptyList(),
                                                new Configuration()),
                                        changes))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("key column");

        Configuration lakeConf = new Configuration();
        lakeConf.set(ConfigOptions.TABLE_DATALAKE_ENABLED, true);
        assertThatThrownBy(
                        () ->
                                NestedSchemaChangeValidator.validate(
                                        tableInfo(
                                                schema,
                                                Collections.emptyList(),
                                                Collections.emptyList(),
                                                lakeConf),
                                        changes))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("datalake enabled table");
    }

    @Test
    void testPreserveSchemaMetadataAndAllocateNestedFieldIds() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .primaryKeyNamed("pk", Collections.singletonList("id"))
                        .column("sequence", DataTypes.BIGINT())
                        .enableAutoIncrement("sequence")
                        .column("total", DataTypes.BIGINT(), AggFunctions.SUM())
                        .withComment("total amount")
                        .column(
                                "profile",
                                DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "name", DataTypes.STRING(), "name comment"))
                                        .copy(false))
                        .withComment("profile comment")
                        .build();
        Schema updated =
                SchemaUpdate.applySchemaChanges(
                        schema,
                        Arrays.asList(
                                TableChange.addNestedField(
                                        ColumnPath.of("profile"),
                                        "address",
                                        DataTypes.ROW(DataTypes.FIELD("city", DataTypes.STRING())),
                                        "address comment"),
                                TableChange.addNestedField(
                                        ColumnPath.of("profile", "address"),
                                        "zip",
                                        DataTypes.INT(),
                                        null),
                                TableChange.addColumn(
                                        "count",
                                        DataTypes.BIGINT(),
                                        null,
                                        TableChange.ColumnPosition.last(),
                                        AggFunctions.SUM())));

        assertThat(updated.getPrimaryKey()).isEqualTo(schema.getPrimaryKey());
        assertThat(updated.getAutoIncrementColumnNames())
                .isEqualTo(schema.getAutoIncrementColumnNames());
        assertThat(updated.getColumn("id")).isEqualTo(schema.getColumn("id"));
        assertThat(updated.getColumn("total")).isEqualTo(schema.getColumn("total"));
        assertThat(updated.getColumn("count").getAggFunction()).hasValue(AggFunctions.SUM());
        assertThat(updated.getColumn("profile").getColumnId())
                .isEqualTo(schema.getColumn("profile").getColumnId());
        assertThat(updated.getColumn("profile").getComment()).hasValue("profile comment");
        RowType profile = (RowType) updated.getColumn("profile").getDataType();
        RowType original = (RowType) schema.getColumn("profile").getDataType();
        assertThat(profile.isNullable()).isFalse();
        assertThat(profile.getField("name")).isEqualTo(original.getField("name"));
        assertThat(original.getFieldNames()).containsExactly("name");
        DataField address = profile.getField("address");
        RowType addressType = (RowType) address.getType();
        int nextId = schema.getHighestFieldId();
        assertThat(address.getFieldId()).isEqualTo(++nextId);
        assertThat(address.getDescription()).hasValue("address comment");
        assertThat(addressType.getField("city").getFieldId()).isEqualTo(++nextId);
        assertThat(addressType.getField("zip").getFieldId()).isEqualTo(++nextId);
        assertThat(updated.getColumn("count").getColumnId()).isEqualTo(++nextId);
        assertThat(updated.getHighestFieldId()).isEqualTo(nextId);
    }

    @Test
    void testAddTopLevelRowThenNestedFieldWithoutPrimaryKey() {
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        Schema updated =
                SchemaUpdate.applySchemaChanges(
                        schema,
                        Arrays.asList(
                                TableChange.addColumn(
                                        "profile",
                                        DataTypes.ROW(DataTypes.FIELD("name", DataTypes.STRING())),
                                        null,
                                        TableChange.ColumnPosition.last()),
                                TableChange.addNestedField(
                                        ColumnPath.of("profile"), "age", DataTypes.INT(), null)));
        assertThat(((RowType) updated.getColumn("profile").getDataType()).getFieldNames())
                .containsExactly("name", "age");
        assertThat(updated.getPrimaryKey()).isEmpty();
        assertThat(schema.getColumns()).hasSize(1);
    }

    @ParameterizedTest
    @MethodSource("invalidParentPaths")
    void testRejectInvalidParentPath(ColumnPath path, String message) {
        Schema schema = baseSchema();
        int highestId = schema.getHighestFieldId();
        assertThatThrownBy(
                        () ->
                                SchemaUpdate.applySchemaChanges(
                                        schema,
                                        Collections.singletonList(
                                                TableChange.addNestedField(
                                                        path, "added", DataTypes.STRING(), null))))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining(message);
        assertThat(schema.getHighestFieldId()).isEqualTo(highestId);
    }

    private static Stream<Arguments> invalidParentPaths() {
        return Stream.of(
                Arguments.of(ColumnPath.of("profile", "missing"), "Cannot find parent path"),
                Arguments.of(
                        ColumnPath.of("profile", "name", "missing"), "Cannot find parent path"),
                Arguments.of(ColumnPath.of("profile", "name"), "Parent path must refer to a ROW"),
                Arguments.of(
                        ColumnPath.of(ColumnPath.Segment.arrayElement()),
                        "must start with a top-level field"),
                Arguments.of(
                        ColumnPath.of(
                                ColumnPath.Segment.field("profile"),
                                ColumnPath.Segment.arrayElement()),
                        "Only ROW field path segment"),
                Arguments.of(
                        ColumnPath.of(
                                ColumnPath.Segment.field("profile"), ColumnPath.Segment.mapValue()),
                        "Only ROW field path segment"));
    }

    @Test
    void testValidateMixedChangesAndRejectProtectedPaths() {
        Schema schema = baseSchema();
        TableChange nested =
                TableChange.addNestedField(ColumnPath.of("profile"), "age", DataTypes.INT(), null);
        TableChange topLevel =
                TableChange.addColumn(
                        "other", DataTypes.STRING(), null, TableChange.ColumnPosition.last());
        NestedSchemaChangeValidator.validate(
                tableInfo(
                        schema,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new Configuration()),
                Arrays.asList(topLevel, nested));

        Configuration lake = new Configuration();
        lake.set(ConfigOptions.TABLE_DATALAKE_ENABLED, true);
        NestedSchemaChangeValidator.validate(
                tableInfo(schema, Collections.emptyList(), Collections.emptyList(), lake),
                Collections.singletonList(topLevel));
        NestedSchemaChangeValidator.validate(
                tableInfo(schema, Collections.emptyList(), Collections.emptyList(), lake),
                Collections.emptyList());

        assertThatThrownBy(
                        () ->
                                NestedSchemaChangeValidator.validate(
                                        tableInfo(
                                                schema,
                                                Collections.emptyList(),
                                                Collections.singletonList("profile"),
                                                new Configuration()),
                                        Collections.singletonList(nested)))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("key column: profile");
        assertThatThrownBy(
                        () ->
                                NestedSchemaChangeValidator.validate(
                                        tableInfo(
                                                schema,
                                                Collections.emptyList(),
                                                Collections.emptyList(),
                                                new Configuration()),
                                        Collections.singletonList(
                                                TableChange.addNestedField(
                                                        ColumnPath.of("id"),
                                                        "value",
                                                        DataTypes.INT(),
                                                        null))))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("key column: id");
        assertThatThrownBy(
                        () ->
                                NestedSchemaChangeValidator.validate(
                                        tableInfo(
                                                schema,
                                                Collections.emptyList(),
                                                Collections.emptyList(),
                                                new Configuration()),
                                        Collections.singletonList(
                                                TableChange.addNestedField(
                                                        ColumnPath.of(
                                                                ColumnPath.Segment.mapValue()),
                                                        "value",
                                                        DataTypes.INT(),
                                                        null))))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("must start with a top-level field");
    }

    @Test
    void testMetadataManagerRegistersUpdatedSchema() throws Exception {
        Schema schema = baseSchema();
        TablePath tablePath = TablePath.of("db", "nested_fields");
        ZooKeeperClient zkClient =
                ZOO_KEEPER_EXTENSION
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        List<TableChange> changes =
                Collections.singletonList(
                        TableChange.addNestedField(
                                ColumnPath.of("profile"), "age", DataTypes.INT(), null));
        try (LakeCatalogDynamicLoader loader =
                new LakeCatalogDynamicLoader(new Configuration(), null, true)) {
            MetadataManager manager = new MetadataManager(zkClient, new Configuration(), loader);
            createTable(manager, zkClient, tablePath, false);
            int originalSchemaId = manager.getTable(tablePath).getSchemaId();

            manager.alterTableSchema(tablePath, changes, false, null);

            TableInfo updated = manager.getTable(tablePath);
            assertThat(updated.getSchemaId()).isEqualTo(originalSchemaId + 1);
            assertThat(updated.getSchema())
                    .isEqualTo(SchemaUpdate.applySchemaChanges(schema, changes));
            assertThat(zkClient.getSchemaById(tablePath, originalSchemaId))
                    .get()
                    .extracting(SchemaInfo::getSchema)
                    .isEqualTo(schema);

            List<TableChange> invalidChanges =
                    Arrays.asList(
                            TableChange.addNestedField(
                                    ColumnPath.of("profile"), "nickname", DataTypes.STRING(), null),
                            changes.get(0));
            assertThatThrownBy(
                            () -> manager.alterTableSchema(tablePath, invalidChanges, false, null))
                    .isInstanceOf(InvalidAlterTableException.class)
                    .hasMessageContaining("already exists");
            assertThat(manager.getTable(tablePath).getSchema()).isEqualTo(updated.getSchema());
            assertThat(zkClient.getCurrentSchemaId(tablePath)).isEqualTo(updated.getSchemaId());
            assertThat(zkClient.getSchemaById(tablePath, updated.getSchemaId() + 1)).isNotPresent();
        }
    }

    @Test
    void testMetadataManagerRejectsLakeChangeBeforePersisting() throws Exception {
        TablePath tablePath = TablePath.of("db", "lake_nested_fields");
        ZooKeeperClient zkClient =
                ZOO_KEEPER_EXTENSION
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        try (LakeCatalogDynamicLoader loader =
                new LakeCatalogDynamicLoader(new Configuration(), null, true)) {
            MetadataManager manager = new MetadataManager(zkClient, new Configuration(), loader);
            createTable(manager, zkClient, tablePath, true);
            TableInfo original = manager.getTable(tablePath);

            assertThatThrownBy(
                            () ->
                                    manager.alterTableSchema(
                                            tablePath,
                                            Collections.singletonList(
                                                    TableChange.addNestedField(
                                                            ColumnPath.of("profile"),
                                                            "age",
                                                            DataTypes.INT(),
                                                            null)),
                                            false,
                                            null))
                    .isInstanceOf(InvalidAlterTableException.class)
                    .hasMessageContaining("datalake enabled table");
            assertThat(manager.getTable(tablePath).getSchema()).isEqualTo(original.getSchema());
            assertThat(zkClient.getCurrentSchemaId(tablePath)).isEqualTo(original.getSchemaId());
            assertThat(zkClient.getSchemaById(tablePath, original.getSchemaId() + 1))
                    .isNotPresent();
        }
    }

    private static void createTable(
            MetadataManager manager,
            ZooKeeperClient zkClient,
            TablePath tablePath,
            boolean lakeEnabled) {
        manager.createDatabase(
                tablePath.getDatabaseName(), DatabaseDescriptor.builder().build(), true);
        manager.createTable(
                tablePath,
                zkClient.getDefaultRemoteDataDir(),
                TableDescriptor.builder()
                        .schema(baseSchema())
                        .distributedBy(1, "id")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, lakeEnabled)
                        .build(),
                null,
                false);
    }

    private static Schema baseSchema() {
        return Schema.newBuilder()
                .primaryKey("id")
                .column("id", DataTypes.INT())
                .column(
                        "profile",
                        DataTypes.ROW(
                                DataTypes.FIELD("name", DataTypes.STRING()),
                                DataTypes.FIELD(
                                        "address",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("city", DataTypes.STRING())))))
                .build();
    }

    private static TableInfo tableInfo(
            Schema schema,
            List<String> bucketKeys,
            List<String> partitionKeys,
            Configuration configuration) {
        return new TableInfo(
                TablePath.of("db", "t"),
                1L,
                0,
                schema,
                bucketKeys,
                partitionKeys,
                1,
                configuration,
                new Configuration(),
                null,
                null,
                0L,
                0L);
    }
}
