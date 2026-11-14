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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link IcebergPartitionSpecValidator}. */
class IcebergPartitionSpecValidatorTest {

    private static final int BUCKET_NUM = 3;
    private static final TablePath TABLE_PATH = TablePath.of("iceberg", "partition_spec_test");

    private static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
            new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "c1", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "c2", Types.StringType.get()),
                    Types.NestedField.required(3, "c3", Types.StringType.get()),
                    Types.NestedField.required(4, "__bucket", Types.IntegerType.get()),
                    Types.NestedField.required(5, "__offset", Types.LongType.get()),
                    Types.NestedField.required(6, "__timestamp", Types.TimestampType.withZone()));

    @Test
    void testMatchingPartitionSpec() {
        PartitionSpec icebergSpec =
                PartitionSpec.builderFor(ICEBERG_SCHEMA)
                        .identity("c3")
                        .bucket("c1", BUCKET_NUM)
                        .build();

        assertThatCode(
                        () ->
                                IcebergPartitionSpecValidator.validate(
                                        mockTable(icebergSpec), createTableInfo()))
                .doesNotThrowAnyException();
    }

    @Test
    void testAllowDifferentPartitionFieldNames() {
        PartitionSpec icebergSpec =
                PartitionSpec.builderFor(ICEBERG_SCHEMA)
                        .identity("c3", "renamed_partition")
                        .bucket("c1", BUCKET_NUM, "renamed_bucket")
                        .build();

        assertThatCode(
                        () ->
                                IcebergPartitionSpecValidator.validate(
                                        mockTable(icebergSpec), createTableInfo()))
                .doesNotThrowAnyException();
    }

    @Test
    void testRejectRenamedIcebergColumn() {
        org.apache.iceberg.Schema renamedSchema =
                new org.apache.iceberg.Schema(
                        Types.NestedField.required(1, "c1", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "renamed_c2", Types.StringType.get()),
                        Types.NestedField.required(3, "c3", Types.StringType.get()),
                        Types.NestedField.required(4, "__bucket", Types.IntegerType.get()),
                        Types.NestedField.required(5, "__offset", Types.LongType.get()),
                        Types.NestedField.required(
                                6, "__timestamp", Types.TimestampType.withZone()));

        assertThatThrownBy(
                        () ->
                                IcebergPartitionSpecValidator.validate(
                                        mockTable(renamedSchema, PartitionSpec.unpartitioned()),
                                        createTableInfo()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("Iceberg schema is incompatible")
                .hasMessageContaining("renamed_c2");
    }

    @Test
    void testRejectDifferentIcebergColumnType() {
        org.apache.iceberg.Schema changedSchema =
                new org.apache.iceberg.Schema(
                        Types.NestedField.required(1, "c1", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "c2", Types.IntegerType.get()),
                        Types.NestedField.required(3, "c3", Types.StringType.get()),
                        Types.NestedField.required(4, "__bucket", Types.IntegerType.get()),
                        Types.NestedField.required(5, "__offset", Types.LongType.get()),
                        Types.NestedField.required(
                                6, "__timestamp", Types.TimestampType.withZone()));

        assertThatThrownBy(
                        () ->
                                IcebergPartitionSpecValidator.validate(
                                        mockTable(changedSchema, PartitionSpec.unpartitioned()),
                                        createTableInfo()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("Iceberg schema is incompatible");
    }

    @Test
    void testRejectDifferentIcebergColumnNullability() {
        org.apache.iceberg.Schema changedSchema =
                new org.apache.iceberg.Schema(
                        Types.NestedField.required(1, "c1", Types.IntegerType.get()),
                        Types.NestedField.required(2, "c2", Types.StringType.get()),
                        Types.NestedField.required(3, "c3", Types.StringType.get()),
                        Types.NestedField.required(4, "__bucket", Types.IntegerType.get()),
                        Types.NestedField.required(5, "__offset", Types.LongType.get()),
                        Types.NestedField.required(
                                6, "__timestamp", Types.TimestampType.withZone()));

        assertThatThrownBy(
                        () ->
                                IcebergPartitionSpecValidator.validate(
                                        mockTable(changedSchema, PartitionSpec.unpartitioned()),
                                        createTableInfo()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("Iceberg schema is incompatible");
    }

    @Test
    void testRejectDifferentPartitionFieldCount() {
        PartitionSpec icebergSpec =
                PartitionSpec.builderFor(ICEBERG_SCHEMA).bucket("c1", BUCKET_NUM).build();

        assertIncompatible(icebergSpec);
    }

    @Test
    void testRejectReorderedPartitionFields() {
        PartitionSpec icebergSpec =
                PartitionSpec.builderFor(ICEBERG_SCHEMA)
                        .bucket("c1", BUCKET_NUM)
                        .identity("c3")
                        .build();

        assertIncompatible(icebergSpec);
    }

    @Test
    void testRejectDifferentPartitionSourceColumn() {
        PartitionSpec icebergSpec =
                PartitionSpec.builderFor(ICEBERG_SCHEMA)
                        .identity("c2")
                        .bucket("c1", BUCKET_NUM)
                        .build();

        assertIncompatible(icebergSpec);
    }

    @Test
    void testRejectDifferentPartitionTransform() {
        PartitionSpec icebergSpec =
                PartitionSpec.builderFor(ICEBERG_SCHEMA)
                        .identity("c3")
                        .bucket("c1", BUCKET_NUM * 2)
                        .build();

        assertIncompatible(icebergSpec);
    }

    private static void assertIncompatible(PartitionSpec icebergSpec) {
        assertThatThrownBy(
                        () ->
                                IcebergPartitionSpecValidator.validate(
                                        mockTable(icebergSpec), createTableInfo()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("Iceberg partition spec is incompatible");
    }

    private static Table mockTable(PartitionSpec icebergSpec) {
        return mockTable(ICEBERG_SCHEMA, icebergSpec);
    }

    private static Table mockTable(
            org.apache.iceberg.Schema icebergSchema, PartitionSpec icebergSpec) {
        Table icebergTable = mock(Table.class);
        when(icebergTable.schema()).thenReturn(icebergSchema);
        when(icebergTable.spec()).thenReturn(icebergSpec);
        return icebergTable;
    }

    private static TableInfo createTableInfo() {
        Schema flussSchema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .column("c3", DataTypes.STRING())
                        .primaryKey("c1", "c3")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(BUCKET_NUM)
                        .partitionedBy("c3")
                        .build();
        return TableInfo.of(TABLE_PATH, 0, 1, tableDescriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
    }
}
