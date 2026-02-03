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
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqlExecutorAlterTableTest {

    @Test
    void testAlterTableAddColumn() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.alterTable(any(TablePath.class), any(List.class), eq(false)))
                .thenReturn(CompletableFuture.completedFuture(null));

        String output = executeSql(admin, "ALTER TABLE db1.tbl ADD COLUMN c1 INT");

        assertThat(output).contains("Table altered successfully: db1.tbl");
    }

    @Test
    void testAlterTableModifyColumn() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.alterTable(any(TablePath.class), any(List.class), eq(false)))
                .thenReturn(CompletableFuture.completedFuture(null));

        String output = executeSql(admin, "ALTER TABLE db1.tbl MODIFY COLUMN c1 STRING");

        assertThat(output).contains("Table altered successfully: db1.tbl");
    }

    @Test
    void testAlterTableRenameColumn() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.alterTable(any(TablePath.class), any(List.class), eq(false)))
                .thenReturn(CompletableFuture.completedFuture(null));

        String output = executeSql(admin, "ALTER TABLE db1.tbl RENAME COLUMN c1 TO c2");

        assertThat(output).contains("Table altered successfully: db1.tbl");
    }

    @Test
    void testAlterTableDropColumn() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.alterTable(any(TablePath.class), any(List.class), eq(false)))
                .thenReturn(CompletableFuture.completedFuture(null));

        String output = executeSql(admin, "ALTER TABLE db1.tbl DROP COLUMN c1");

        assertThat(output).contains("Table altered successfully: db1.tbl");
    }

    @Test
    void testAlterTableSetOption() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.alterTable(any(TablePath.class), any(List.class), eq(false)))
                .thenReturn(CompletableFuture.completedFuture(null));

        String output = executeSql(admin, "ALTER TABLE db1.tbl SET ('k1' = 'v1')");

        assertThat(output).contains("Table altered successfully: db1.tbl");
    }

    @Test
    void testAlterTableResetOption() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.alterTable(any(TablePath.class), any(List.class), eq(false)))
                .thenReturn(CompletableFuture.completedFuture(null));

        String output = executeSql(admin, "ALTER TABLE db1.tbl RESET ('k1')");

        assertThat(output).contains("Table altered successfully: db1.tbl");
    }

    @Test
    void testAlterTableAddColumnsPlural() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.alterTable(any(TablePath.class), any(List.class), eq(false)))
                .thenReturn(CompletableFuture.completedFuture(null));

        String output = executeSql(admin, "ALTER TABLE db1.tbl ADD COLUMNS (c1 INT, c2 STRING)");

        assertThat(output).contains("Table altered successfully: db1.tbl");
    }

    @Test
    void testAlterTableModifyColumnsPlural() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.alterTable(any(TablePath.class), any(List.class), eq(false)))
                .thenReturn(CompletableFuture.completedFuture(null));

        String output = executeSql(admin, "ALTER TABLE db1.tbl MODIFY COLUMNS (c1 STRING)");

        assertThat(output).contains("Table altered successfully: db1.tbl");
    }

    private static String executeSql(Admin admin, String sql) throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.getAdmin()).thenReturn(admin);
        ConnectionManager connectionManager = new StubConnectionManager(connection);
        StringWriter writer = new StringWriter();
        SqlExecutor executor = new SqlExecutor(connectionManager, new PrintWriter(writer));
        executor.executeSql(sql);
        return writer.toString();
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
