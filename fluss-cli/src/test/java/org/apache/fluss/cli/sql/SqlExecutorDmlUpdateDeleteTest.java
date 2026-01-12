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
import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqlExecutorDmlUpdateDeleteTest {

    @Test
    void testUpdate() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        Lookup lookup = mock(Lookup.class);
        Lookuper lookuper = mock(Lookuper.class);
        when(table.newLookup()).thenReturn(lookup);
        when(lookup.createLookuper()).thenReturn(lookuper);

        GenericRow row = new GenericRow(2);
        row.setField(0, 1);
        row.setField(1, BinaryString.fromString("Alice"));
        LookupResult result = new LookupResult(row);
        when(lookuper.lookup(any(InternalRow.class)))
                .thenReturn(CompletableFuture.completedFuture(result));

        Upsert upsert = mock(Upsert.class);
        UpsertWriter writer = mock(UpsertWriter.class);
        when(table.newUpsert()).thenReturn(upsert);
        when(upsert.createWriter()).thenReturn(writer);

        String output = executeSql(connection, "UPDATE db1.tbl SET name = 'Bob' WHERE id = 1");

        assertThat(output).contains("1 row(s) updated");
    }

    @Test
    void testDelete() throws Exception {
        Connection connection = mock(Connection.class);
        Table table = mock(Table.class);
        TablePath tablePath = TablePath.of("db1", "tbl");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, 0L, 0L);
        when(connection.getTable(tablePath)).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);

        Lookup lookup = mock(Lookup.class);
        Lookuper lookuper = mock(Lookuper.class);
        when(table.newLookup()).thenReturn(lookup);
        when(lookup.createLookuper()).thenReturn(lookuper);

        GenericRow row = new GenericRow(2);
        row.setField(0, 1);
        row.setField(1, BinaryString.fromString("Alice"));
        LookupResult result = new LookupResult(row);
        when(lookuper.lookup(any(InternalRow.class)))
                .thenReturn(CompletableFuture.completedFuture(result));

        Upsert upsert = mock(Upsert.class);
        UpsertWriter writer = mock(UpsertWriter.class);
        when(table.newUpsert()).thenReturn(upsert);
        when(upsert.createWriter()).thenReturn(writer);

        String output = executeSql(connection, "DELETE FROM db1.tbl WHERE id = 1");

        assertThat(output).contains("1 row(s) deleted");
    }

    private static String executeSql(Connection connection, String sql) throws Exception {
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
