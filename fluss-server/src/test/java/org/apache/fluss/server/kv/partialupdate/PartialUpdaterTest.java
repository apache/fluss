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

package org.apache.fluss.server.kv.partialupdate;

import org.apache.fluss.exception.InvalidTargetColumnException;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowDeserializer;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.StringType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PartialUpdater} target column validation. */
class PartialUpdaterTest {

    private static final short SCHEMA_ID = 1;

    private static final String OMITTED_NOT_NULL_MESSAGE =
            "Partial Update requires all columns omitted from the target columns to be nullable, "
                    + "but omitted column c is NOT NULL.";

    /** {@code a INT NOT NULL} (primary key), {@code b STRING}, {@code c STRING NOT NULL}. */
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .column("b", DataTypes.STRING())
                    .column("c", new StringType(false))
                    .primaryKey("a")
                    .build();

    /** {@code (a, b)} primary key, {@code c STRING NOT NULL}, {@code d STRING}. */
    private static final Schema COMPOSITE_PK_SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .column("b", DataTypes.STRING())
                    .column("c", new StringType(false))
                    .column("d", DataTypes.STRING())
                    .primaryKey("a", "b")
                    .build();

    /** Same shape as {@link #SCHEMA} but with a nullable {@code c}. */
    private static final Schema NULLABLE_SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .column("b", DataTypes.STRING())
                    .column("c", DataTypes.STRING())
                    .primaryKey("a")
                    .build();

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testNullabilityIsRequiredOnlyForOmittedColumns(KvFormat kvFormat) {
        // c is NOT NULL, so it is accepted as a target column and rejected when omitted
        assertThatCode(() -> new PartialUpdater(kvFormat, SCHEMA_ID, SCHEMA, new int[] {0, 2}))
                .doesNotThrowAnyException();
        assertThatCode(() -> new PartialUpdater(kvFormat, SCHEMA_ID, SCHEMA, new int[] {0, 1, 2}))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> new PartialUpdater(kvFormat, SCHEMA_ID, SCHEMA, new int[] {0, 1}))
                .isInstanceOf(InvalidTargetColumnException.class)
                .hasMessage(OMITTED_NOT_NULL_MESSAGE);

        // every column of a composite primary key is NOT NULL and always a target column
        assertThatCode(
                        () ->
                                new PartialUpdater(
                                        kvFormat,
                                        SCHEMA_ID,
                                        COMPOSITE_PK_SCHEMA,
                                        new int[] {0, 1, 2}))
                .doesNotThrowAnyException();
        assertThatThrownBy(
                        () ->
                                new PartialUpdater(
                                        kvFormat,
                                        SCHEMA_ID,
                                        COMPOSITE_PK_SCHEMA,
                                        new int[] {0, 1, 3}))
                .isInstanceOf(InvalidTargetColumnException.class)
                .hasMessage(OMITTED_NOT_NULL_MESSAGE);
    }

    @Test
    void testTargetColumnsMustContainPrimaryKey() {
        assertThatThrownBy(() -> createPartialUpdater(SCHEMA, new int[] {1, 2}))
                .isInstanceOf(InvalidTargetColumnException.class)
                .hasMessage(
                        "The target write columns [b, c] must contain the primary key columns [a].");
        assertThatThrownBy(() -> createPartialUpdater(COMPOSITE_PK_SCHEMA, new int[] {0, 2, 3}))
                .isInstanceOf(InvalidTargetColumnException.class)
                .hasMessage(
                        "The target write columns [a, c, d] must contain the primary key columns [a, b].");
    }

    @Test
    void testAutoIncrementColumnMustBeNullable() {
        // an auto increment column is always omitted and only gets its value after the merge,
        // so updateRow writes null into it first
        assertThatThrownBy(
                        () ->
                                createPartialUpdater(
                                        autoIncrementSchema(new BigIntType(false)),
                                        new int[] {0, 1}))
                .isInstanceOf(InvalidTargetColumnException.class)
                .hasMessage(
                        "Partial Update requires the auto increment column c to be nullable, "
                                + "since it is always omitted from the target columns and assigned by the server.");
        assertThatCode(
                        () ->
                                createPartialUpdater(
                                        autoIncrementSchema(DataTypes.BIGINT()), new int[] {0, 1}))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testUpdateRowKeepsOmittedColumnsAndRejectsNullInNotNullTargetColumn(KvFormat kvFormat) {
        PartialUpdater partialUpdater =
                new PartialUpdater(kvFormat, SCHEMA_ID, SCHEMA, new int[] {0, 2});

        BinaryValue merged =
                partialUpdater.updateRow(
                        binaryValue(SCHEMA, 1, "old", "oldC"), row(1, null, "newC"));
        assertThat(merged.row.getString(1).toString()).isEqualTo("old");
        assertThat(merged.row.getString(2).toString()).isEqualTo("newC");

        // a null in a non-nullable slot has to be caught rather than encoded. the row is typed
        // with SCHEMA as the server types it, so the guard has to fire before any deserialization
        assertThatThrownBy(() -> partialUpdater.updateRow(null, asServerTypedRow(1, "b", null)))
                .isInstanceOf(InvalidTargetColumnException.class)
                .hasMessage("Target column c is NOT NULL but the written row has no value for it.");
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testDeleteRowRejectsNotNullTargetColumnUnlessWholeRowIsRemoved(KvFormat kvFormat) {
        PartialUpdater partialUpdater =
                new PartialUpdater(kvFormat, SCHEMA_ID, SCHEMA, new int[] {0, 2});

        assertThatThrownBy(() -> partialUpdater.deleteRow(binaryValue(SCHEMA, 1, "b", "c")))
                .isInstanceOf(InvalidTargetColumnException.class)
                .hasMessage(
                        "Partial Delete sets the target columns to null, so it requires all target columns "
                                + "except primary key to be nullable, but target column c is NOT NULL.");

        // b is already null, so the row is removed outright and nothing is nulled
        assertThat(partialUpdater.deleteRow(binaryValue(SCHEMA, 1, null, "c"))).isNull();
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testDeleteRowSetsNullableTargetColumnsToNull(KvFormat kvFormat) {
        PartialUpdater partialUpdater =
                new PartialUpdater(kvFormat, SCHEMA_ID, NULLABLE_SCHEMA, new int[] {0, 2});

        BinaryValue deleted = partialUpdater.deleteRow(binaryValue(NULLABLE_SCHEMA, 1, "b", "c"));

        assertThat(deleted).isNotNull();
        assertThat(deleted.row.getString(1).toString()).isEqualTo("b");
        assertThat(deleted.row.isNullAt(2)).isTrue();
    }

    private static PartialUpdater createPartialUpdater(Schema schema, int[] targetColumns) {
        return new PartialUpdater(KvFormat.COMPACTED, SCHEMA_ID, schema, targetColumns);
    }

    private static Schema autoIncrementSchema(DataType autoIncrementType) {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .column("c", autoIncrementType)
                .primaryKey("a")
                .enableAutoIncrement("c")
                .build();
    }

    /** A row of {@link #NULLABLE_SCHEMA}, usable as the partial value for any of the schemas. */
    private static BinaryValue row(int a, String b, String c) {
        return binaryValue(NULLABLE_SCHEMA, a, b, c);
    }

    /**
     * The same bytes as {@link #row}, but typed with {@link #SCHEMA} the way the server types an
     * incoming record. Reading any field of it deserializes the whole row and fails on the null
     * that {@code c} is not allowed to hold.
     */
    private static BinaryValue asServerTypedRow(int a, String b, String c) {
        CompactedRow encoded = compactedRow(NULLABLE_SCHEMA.getRowType(), new Object[] {a, b, c});
        byte[] bytes = new byte[encoded.getSizeInBytes()];
        encoded.copyTo(bytes, 0);
        DataType[] types = SCHEMA.getRowType().getChildren().toArray(new DataType[0]);
        return new BinaryValue(
                SCHEMA_ID, CompactedRow.from(types, bytes, new CompactedRowDeserializer(types)));
    }

    private static BinaryValue binaryValue(Schema schema, int a, String b, String c) {
        return new BinaryValue(
                SCHEMA_ID, compactedRow(schema.getRowType(), new Object[] {a, b, c}));
    }
}
