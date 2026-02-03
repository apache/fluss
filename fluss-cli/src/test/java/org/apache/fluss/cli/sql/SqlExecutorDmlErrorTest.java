/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.cli.sql;

import org.apache.fluss.cli.config.ConnectionConfig;
import org.apache.fluss.cli.config.ConnectionManager;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqlExecutorDmlErrorTest {

    @Test
    void testInsertNonValuesClause() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        SqlExecutor executor =
                new SqlExecutor(
                        new StubConnectionManager(connection), new PrintWriter(new StringWriter()));

        assertThatThrownBy(() -> executor.executeSql("INSERT INTO db1.tbl SELECT id FROM db1.tbl"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Only VALUES clause is supported");
    }

    @Test
    void testUpsertWithoutPrimaryKey() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        SqlExecutor executor =
                new SqlExecutor(
                        new StubConnectionManager(connection), new PrintWriter(new StringWriter()));

        assertThatThrownBy(() -> executor.executeSql("UPSERT INTO db1.tbl VALUES (1)"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("UPSERT is only supported for tables with primary keys");
    }

    @Test
    void testDeleteWithoutPrimaryKey() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        SqlExecutor executor =
                new SqlExecutor(
                        new StubConnectionManager(connection), new PrintWriter(new StringWriter()));

        assertThatThrownBy(() -> executor.executeSql("DELETE FROM db1.tbl WHERE id = 1"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("DELETE is only supported for tables with primary keys");
    }

    @Test
    void testDeleteWithoutWhereClause() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("id").build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        SqlExecutor executor =
                new SqlExecutor(
                        new StubConnectionManager(connection), new PrintWriter(new StringWriter()));

        assertThatThrownBy(() -> executor.executeSql("DELETE FROM db1.tbl"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("DELETE without WHERE clause is not supported");
    }

    @Test
    void testDeleteInvalidTableName() throws Exception {
        Connection connection = mock(Connection.class);
        SqlExecutor executor =
                new SqlExecutor(
                        new StubConnectionManager(connection), new PrintWriter(new StringWriter()));

        assertThatThrownBy(() -> executor.executeSql("DELETE FROM tbl WHERE id = 1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Table name must be in format 'database.table'");
    }

    @Test
    void testUpdateInvalidTableName() throws Exception {
        Connection connection = mock(Connection.class);
        SqlExecutor executor =
                new SqlExecutor(
                        new StubConnectionManager(connection), new PrintWriter(new StringWriter()));

        assertThatThrownBy(() -> executor.executeSql("UPDATE tbl SET id = 1 WHERE id = 0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Table name must be in format 'database.table'");
    }

    @Test
    void testInsertInvalidTableName() throws Exception {
        Connection connection = mock(Connection.class);
        SqlExecutor executor =
                new SqlExecutor(
                        new StubConnectionManager(connection), new PrintWriter(new StringWriter()));

        assertThatThrownBy(() -> executor.executeSql("INSERT INTO tbl VALUES (1)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Table name must be in format 'database.table'");
    }

    @Test
    void testUpdateWithoutPrimaryKey() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        SqlExecutor executor =
                new SqlExecutor(
                        new StubConnectionManager(connection), new PrintWriter(new StringWriter()));

        assertThatThrownBy(() -> executor.executeSql("UPDATE db1.tbl SET id = 1 WHERE id = 0"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("UPDATE is only supported for tables with primary keys");
    }

    @Test
    void testUpsertInvalidTableName() throws Exception {
        Connection connection = mock(Connection.class);
        SqlExecutor executor =
                new SqlExecutor(
                        new StubConnectionManager(connection), new PrintWriter(new StringWriter()));

        assertThatThrownBy(() -> executor.executeSql("UPSERT INTO tbl VALUES (1)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Table name must be in format 'database.table'");
    }

    private static class StubConnectionManager extends ConnectionManager {
        private final Connection connection;

        StubConnectionManager(Connection connection) {
            super(new ConnectionConfig(new org.apache.fluss.config.Configuration()));
            this.connection = connection;
        }

        @Override
        public Connection getConnection() {
            return connection;
        }
    }
}
