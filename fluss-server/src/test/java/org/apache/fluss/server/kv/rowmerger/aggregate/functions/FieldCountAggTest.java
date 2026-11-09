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

package org.apache.fluss.server.kv.rowmerger.aggregate.functions;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.kv.rowmerger.AggregateRowMerger;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests COUNT through the row merger, including first writes and partial updates. */
class FieldCountAggTest {

    private static final short SCHEMA_ID = 1;
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("cnt", DataTypes.BIGINT(), AggFunctions.COUNT())
                    .column("total", DataTypes.BIGINT(), AggFunctions.SUM())
                    .primaryKey("id")
                    .build();

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testCountNonNullInputs(KvFormat format) {
        RowMerger merger = createMerger(format, null);
        BinaryValue value = merger.merge(null, input(format, null, 5L));
        assertThat(value.row.getLong(1)).isZero();
        assertThat(value.row.getLong(2)).isEqualTo(5L);

        value = merger.merge(copy(value), input(format, 100L, 2L));
        assertThat(value.row.getLong(1)).isEqualTo(1L);
        value = merger.merge(copy(value), input(format, 0L, 3L));
        assertThat(value.row.getLong(1)).isEqualTo(2L);
        value = merger.merge(copy(value), input(format, -7L, 4L));
        assertThat(value.row.getLong(1)).isEqualTo(3L);
        value = merger.merge(copy(value), input(format, null, 1L));
        assertThat(value.row.getLong(1)).isEqualTo(3L);
        assertThat(value.row.getLong(2)).isEqualTo(15L);

        BinaryValue first = merger.merge(null, input(format, 100L, 5L));
        assertThat(first.row.getLong(1)).isEqualTo(1L);
        assertThat(first.row.getInt(0)).isEqualTo(1);
        assertThat(first.schemaId).isEqualTo(SCHEMA_ID);
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testCountPartialUpdates(KvFormat format) {
        RowMerger countMerger = createMerger(format, new int[] {0, 1});
        BinaryValue value = countMerger.merge(null, input(format, 42L, null));
        assertThat(value.row.getLong(1)).isEqualTo(1L);
        value = countMerger.merge(copy(value), input(format, 99L, null));
        assertThat(value.row.getLong(1)).isEqualTo(2L);

        RowMerger sumMerger = createMerger(format, new int[] {0, 2});
        value = sumMerger.merge(copy(value), input(format, null, 10L));
        assertThat(value.row.getLong(1)).isEqualTo(2L);
        assertThat(value.row.getLong(2)).isEqualTo(10L);

        value = countMerger.delete(copy(value));
        assertThat(value.row.isNullAt(1)).isTrue();
        value = countMerger.merge(copy(value), input(format, 88L, null));
        assertThat(value.row.getLong(1)).isEqualTo(1L);
        assertThat(value.row.getLong(2)).isEqualTo(10L);
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testFirstPartialUpdateWithoutCountCanBeDeleted(KvFormat format) {
        RowMerger sumMerger = createMerger(format, new int[] {0, 2});
        BinaryValue value = sumMerger.merge(null, input(format, null, 10L));
        assertThat(value.row.getInt(0)).isEqualTo(1);
        assertThat(value.schemaId).isEqualTo(SCHEMA_ID);
        assertThat(value.row.getLong(2)).isEqualTo(10L);
        assertThat(sumMerger.delete(copy(value))).isNull();
        assertThat(value.row.isNullAt(1)).isTrue();

        RowMerger countMerger = createMerger(format, new int[] {0, 1});
        value = countMerger.merge(copy(value), input(format, 42L, null));
        assertThat(value.row.getLong(1)).isEqualTo(1L);
        assertThat(value.row.getLong(2)).isEqualTo(10L);
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testFirstPartialUpdateWithNullCount(KvFormat format) {
        RowMerger countMerger = createMerger(format, new int[] {0, 1});
        BinaryValue value = countMerger.merge(null, input(format, null, null));
        assertThat(value.row.getLong(1)).isZero();
        assertThat(value.row.isNullAt(2)).isTrue();

        RowMerger sumMerger = createMerger(format, new int[] {0, 2});
        value = sumMerger.merge(copy(value), input(format, null, 10L));
        assertThat(value.row.getLong(1)).isZero();
        value = sumMerger.delete(copy(value));
        assertThat(value).isNotNull();
        assertThat(value.row.getLong(1)).isZero();
        assertThat(value.row.isNullAt(2)).isTrue();
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testFirstPartialUpdateIgnoresNonTargetValues(KvFormat format) {
        RowMerger sumMerger = createMerger(format, new int[] {0, 2});
        BinaryValue value = sumMerger.merge(null, input(format, 99L, 10L));
        assertThat(value.row.isNullAt(1)).isTrue();
        assertThat(value.row.getLong(2)).isEqualTo(10L);
        assertThat(sumMerger.delete(copy(value))).isNull();

        RowMerger countMerger = createMerger(format, new int[] {0, 1});
        value = countMerger.merge(null, input(format, 42L, 99L));
        assertThat(value.row.getLong(1)).isEqualTo(1L);
        assertThat(value.row.isNullAt(2)).isTrue();
        assertThat(countMerger.delete(copy(value))).isNull();
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testFirstPartialUpdateWithSchemaEvolution(KvFormat format) {
        Schema inputSchema =
                Schema.newBuilder()
                        .fromColumns(
                                Arrays.asList(
                                        SCHEMA.getColumns().get(1),
                                        SCHEMA.getColumns().get(0),
                                        SCHEMA.getColumns().get(2)))
                        .primaryKey("id")
                        .build();
        Schema latestSchema =
                Schema.newBuilder()
                        .fromColumns(
                                Arrays.asList(
                                        SCHEMA.getColumns().get(2),
                                        SCHEMA.getColumns().get(1),
                                        SCHEMA.getColumns().get(0)))
                        .column("extra", DataTypes.BIGINT(), AggFunctions.SUM())
                        .primaryKey("id")
                        .build();
        short latestSchemaId = 2;
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(inputSchema, SCHEMA_ID));
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(latestSchema, latestSchemaId));
        RowMerger merger =
                new AggregateRowMerger(new TableConfig(new Configuration()), format, schemaGetter)
                        .configureTargetColumns(new int[] {0, 2}, latestSchemaId, latestSchema);
        RowEncoder encoder = RowEncoder.create(format, inputSchema.getRowType());
        encoder.startNewRow();
        encoder.encodeField(0, 99L);
        encoder.encodeField(1, 1);
        encoder.encodeField(2, 10L);
        BinaryValue input = new BinaryValue(SCHEMA_ID, encoder.finishRow());

        BinaryValue value = merger.merge(null, input);
        assertThat(value.schemaId).isEqualTo(SCHEMA_ID);
        assertThat(value.row.isNullAt(0)).isTrue();
        assertThat(value.row.getInt(1)).isEqualTo(1);
        assertThat(value.row.getLong(2)).isEqualTo(10L);
        assertThat(merger.delete(copy(value))).isNull();

        value = merger.merge(copy(value), input);
        assertThat(value.schemaId).isEqualTo(latestSchemaId);
        assertThat(value.row.getLong(0)).isEqualTo(20L);
        assertThat(value.row.isNullAt(1)).isTrue();
        assertThat(value.row.getInt(2)).isEqualTo(1);
        assertThat(value.row.isNullAt(3)).isTrue();
        assertThat(merger.delete(copy(value))).isNull();

        RowMerger countMerger =
                new AggregateRowMerger(new TableConfig(new Configuration()), format, schemaGetter)
                        .configureTargetColumns(new int[] {1, 2}, latestSchemaId, latestSchema);
        encoder.startNewRow();
        encoder.encodeField(0, 99L);
        encoder.encodeField(1, 1);
        encoder.encodeField(2, null);
        value = countMerger.merge(null, new BinaryValue(SCHEMA_ID, encoder.finishRow()));
        assertThat(value.schemaId).isEqualTo(SCHEMA_ID);
        assertThat(value.row.getLong(0)).isEqualTo(1L);
        assertThat(value.row.getInt(1)).isEqualTo(1);
        assertThat(value.row.isNullAt(2)).isTrue();
        assertThat(countMerger.delete(copy(value))).isNull();
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testCountWithSchemaEvolution(KvFormat format) {
        Schema oldSchema =
                Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("id").build();
        short oldSchemaId = 0;
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(oldSchema, oldSchemaId));
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(SCHEMA, SCHEMA_ID));
        AggregateRowMerger merger =
                new AggregateRowMerger(new TableConfig(new Configuration()), format, schemaGetter);
        merger.configureTargetColumns(null, SCHEMA_ID, SCHEMA);
        RowEncoder oldEncoder = RowEncoder.create(format, oldSchema.getRowType());
        oldEncoder.startNewRow();
        oldEncoder.encodeField(0, 1);
        BinaryValue oldValue = new BinaryValue(oldSchemaId, oldEncoder.finishRow());

        BinaryValue value = merger.merge(oldValue, input(format, 99L, 3L));
        assertThat(value.schemaId).isEqualTo(SCHEMA_ID);
        assertThat(value.row.getLong(1)).isEqualTo(1L);
        value = merger.merge(copy(value), oldValue);
        assertThat(value.row.getLong(1)).isEqualTo(1L);
        assertThat(value.row.getLong(2)).isEqualTo(3L);
    }

    private RowMerger createMerger(KvFormat format, int[] targetColumns) {
        AggregateRowMerger merger =
                new AggregateRowMerger(
                        new TableConfig(new Configuration()),
                        format,
                        new TestingSchemaGetter(new SchemaInfo(SCHEMA, SCHEMA_ID)));
        return merger.configureTargetColumns(targetColumns, SCHEMA_ID, SCHEMA);
    }

    private BinaryValue input(KvFormat format, Long count, Long total) {
        RowEncoder encoder = RowEncoder.create(format, SCHEMA.getRowType());
        encoder.startNewRow();
        encoder.encodeField(0, 1);
        encoder.encodeField(1, count);
        encoder.encodeField(2, total);
        return new BinaryValue(SCHEMA_ID, encoder.finishRow());
    }

    private BinaryValue copy(BinaryValue value) {
        return new BinaryValue(value.schemaId, value.row.copy());
    }
}
