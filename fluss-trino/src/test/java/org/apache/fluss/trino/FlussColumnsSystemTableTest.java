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

package org.apache.fluss.trino;

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.fluss.trino.TestingFlussMetadata.usersTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Tests the public system-table schema and rows returned through the Trino cursor. */
final class FlussColumnsSystemTableTest {
    private final FlussColumnsSystemTable table =
            new FlussColumnsSystemTable(
                    new SchemaTableName("sales", "users$columns"), usersTable());
    private final ConnectorSession session = mock(ConnectorSession.class);

    @Test
    void testSystemTableMetadata() {
        assertThat(table.getDistribution()).isEqualTo(SystemTable.Distribution.SINGLE_COORDINATOR);
        assertThat(table.getTableMetadata().getTable())
                .isEqualTo(new SchemaTableName("sales", "users$columns"));
        assertThat(table.getTableMetadata().getColumns())
                .extracting(ColumnMetadata::getName)
                .containsExactly(
                        "column_name",
                        "ordinal_position",
                        "fluss_type",
                        "trino_type",
                        "is_nullable",
                        "primary_key_position",
                        "partition_key_position",
                        "bucket_key_position",
                        "comment");
        assertThat(table.getTableMetadata().getColumns())
                .extracting(ColumnMetadata::getType)
                .containsExactly(
                        VARCHAR, BIGINT, VARCHAR, VARCHAR, BOOLEAN, BIGINT, BIGINT, BIGINT,
                        VARCHAR);
        assertThat(table.getTableMetadata().getColumns())
                .extracting(ColumnMetadata::isNullable)
                .containsExactly(false, false, false, false, false, true, true, true, true);
    }

    @Test
    void testColumnRowsPreserveNamesAndKeyOrder() {
        try (RecordCursor cursor =
                table.cursor(FlussTransactionHandle.INSTANCE, session, TupleDomain.all())) {
            assertThat(cursor.advanceNextPosition()).isTrue();
            assertThat(cursor.getSlice(0).toStringUtf8()).isEqualTo("Region");
            assertThat(cursor.getLong(1)).isEqualTo(1);
            assertThat(cursor.getSlice(2).toStringUtf8()).isEqualTo("STRING NOT NULL");
            assertThat(cursor.getSlice(3).toStringUtf8()).isEqualTo("varchar");
            assertThat(cursor.getBoolean(4)).isFalse();
            assertThat(cursor.getLong(5)).isEqualTo(2);
            assertThat(cursor.getLong(6)).isEqualTo(1);
            assertThat(cursor.isNull(7)).isTrue();
            assertThat(cursor.isNull(8)).isTrue();

            assertThat(cursor.advanceNextPosition()).isTrue();
            assertThat(cursor.getSlice(0).toStringUtf8()).isEqualTo("ID");
            assertThat(cursor.getLong(1)).isEqualTo(2);
            assertThat(cursor.getSlice(2).toStringUtf8()).isEqualTo("BIGINT NOT NULL");
            assertThat(cursor.getSlice(3).toStringUtf8()).isEqualTo("bigint");
            assertThat(cursor.getBoolean(4)).isFalse();
            assertThat(cursor.getLong(5)).isEqualTo(1);
            assertThat(cursor.isNull(6)).isTrue();
            assertThat(cursor.getLong(7)).isEqualTo(1);
            assertThat(cursor.getSlice(8).toStringUtf8()).isEqualTo("User identifier");

            assertThat(cursor.advanceNextPosition()).isTrue();
            assertThat(cursor.getSlice(0).toStringUtf8()).isEqualTo("Name");
            assertThat(cursor.getLong(1)).isEqualTo(3);
            assertThat(cursor.getSlice(2).toStringUtf8()).isEqualTo("STRING");
            assertThat(cursor.getSlice(3).toStringUtf8()).isEqualTo("varchar");
            assertThat(cursor.getBoolean(4)).isTrue();
            assertThat(cursor.isNull(5)).isTrue();
            assertThat(cursor.isNull(6)).isTrue();
            assertThat(cursor.isNull(7)).isTrue();
            assertThat(cursor.isNull(8)).isTrue();
            assertThat(cursor.advanceNextPosition()).isFalse();
        }
    }

    @Test
    void testEmptyConstraintReturnsNoRows() {
        try (RecordCursor cursor =
                table.cursor(FlussTransactionHandle.INSTANCE, session, TupleDomain.none())) {
            assertThat(cursor.advanceNextPosition()).isFalse();
        }
    }

    @Test
    void testCursorsAreIndependent() {
        try (RecordCursor first =
                        table.cursor(FlussTransactionHandle.INSTANCE, session, TupleDomain.all());
                RecordCursor second =
                        table.cursor(FlussTransactionHandle.INSTANCE, session, TupleDomain.all())) {
            assertThat(first.advanceNextPosition()).isTrue();
            assertThat(first.advanceNextPosition()).isTrue();
            assertThat(second.advanceNextPosition()).isTrue();
            assertThat(second.getSlice(0).toStringUtf8()).isEqualTo("Region");
        }
    }
}
