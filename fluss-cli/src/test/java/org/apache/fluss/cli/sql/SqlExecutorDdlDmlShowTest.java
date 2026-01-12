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
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqlExecutorDdlDmlShowTest {

    @Test
    void testCreateDatabase() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.createDatabase(eq("db1"), any(DatabaseDescriptor.class), eq(false)))
                .thenReturn(CompletableFuture.completedFuture(null));

        String output = executeSql(admin, "CREATE DATABASE db1");

        assertThat(output).contains("Database created successfully: db1");
    }

    @Test
    void testCreateTable() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.createTable(any(TablePath.class), any(TableDescriptor.class), eq(false)))
                .thenReturn(CompletableFuture.completedFuture(null));

        String sql =
                "CREATE TABLE db1.tbl (id INT, name STRING, PRIMARY KEY (id)) WITH ('bucket.num' = '3')";

        String output = executeSql(admin, sql);

        assertThat(output).contains("Table created successfully: db1.tbl");
    }

    @Test
    void testDropTable() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.dropTable(eq(TablePath.of("db1", "tbl")), eq(false)))
                .thenReturn(CompletableFuture.completedFuture(null));

        String output = executeSql(admin, "DROP TABLE db1.tbl");

        assertThat(output).contains("Table dropped successfully: db1.tbl");
    }

    @Test
    void testShowDatabases() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.listDatabases())
                .thenReturn(CompletableFuture.completedFuture(Collections.singletonList("db1")));

        String output = executeSql(admin, "SHOW DATABASES");

        assertThat(output).contains("Databases:");
        assertThat(output).contains("db1");
    }

    @Test
    void testShowDatabaseExists() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.databaseExists("db1")).thenReturn(CompletableFuture.completedFuture(true));

        String output = executeSql(admin, "SHOW DATABASE EXISTS db1");

        assertThat(output).contains("true");
    }

    @Test
    void testShowDatabase() throws Exception {
        Admin admin = mock(Admin.class);
        DatabaseDescriptor descriptor = DatabaseDescriptor.builder().comment("demo").build();
        DatabaseInfo info = new DatabaseInfo("db1", descriptor, 1L, 2L);
        when(admin.getDatabaseInfo("db1")).thenReturn(CompletableFuture.completedFuture(info));

        String output = executeSql(admin, "SHOW DATABASE db1");

        assertThat(output).contains("Database: db1");
        assertThat(output).contains("Comment: demo");
    }

    @Test
    void testShowTables() throws Exception {
        Admin admin = mock(Admin.class);
        when(admin.listTables("db1"))
                .thenReturn(CompletableFuture.completedFuture(Collections.singletonList("tbl")));
        when(admin.databaseExists("db1")).thenReturn(CompletableFuture.completedFuture(true));

        String output = executeSql(admin, "SHOW TABLES FROM db1");

        assertThat(output).contains("Tables in database 'db1':");
        assertThat(output).contains("tbl");
    }

    @Test
    void testShowTableSchema() throws Exception {
        Admin admin = mock(Admin.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        SchemaInfo schemaInfo = new SchemaInfo(schema, 1);
        when(admin.tableExists(tablePath)).thenReturn(CompletableFuture.completedFuture(true));
        when(admin.getTableSchema(tablePath))
                .thenReturn(CompletableFuture.completedFuture(schemaInfo));

        String output = executeSql(admin, "SHOW TABLE SCHEMA db1.tbl");

        assertThat(output).contains("Table: db1.tbl");
        assertThat(output).contains("Schema ID: 1");
    }

    @Test
    void testShowTableSchemaWithId() throws Exception {
        Admin admin = mock(Admin.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("id").build();
        SchemaInfo schemaInfo = new SchemaInfo(schema, 5);
        when(admin.tableExists(tablePath)).thenReturn(CompletableFuture.completedFuture(true));
        when(admin.getTableSchema(tablePath, 5))
                .thenReturn(CompletableFuture.completedFuture(schemaInfo));

        String output = executeSql(admin, "SHOW TABLE SCHEMA db1.tbl ID 5");

        assertThat(output).contains("Table: db1.tbl");
        assertThat(output).contains("Schema ID: 5");
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
