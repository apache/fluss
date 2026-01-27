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

package org.apache.fluss.cli.sql.executor;

import org.apache.fluss.cli.config.ConnectionManager;
import org.apache.fluss.cli.util.DataTypeConverter;
import org.apache.fluss.cli.util.WhereClauseEvaluator;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.flink.sql.parser.dml.RichSqlInsert;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Executor for DML (Data Manipulation Language) operations.
 *
 * <p>Handles INSERT, UPDATE, and DELETE statements.
 */
public class DmlExecutor {
    private final ConnectionManager connectionManager;
    private final PrintWriter out;

    public DmlExecutor(ConnectionManager connectionManager, PrintWriter out) {
        this.connectionManager = connectionManager;
        this.out = out;
    }

    public void executeInsert(RichSqlInsert insert, boolean forceUpsert) throws Exception {
        SqlIdentifier tableId = (SqlIdentifier) insert.getTargetTable();
        String fullTableName = tableId.toString();

        if (forceUpsert) {
            out.println("Upserting into table: " + fullTableName);
        } else {
            out.println("Inserting into table: " + fullTableName);
        }

        String[] parts = fullTableName.split("\\.");
        TablePath tablePath;
        if (parts.length == 2) {
            tablePath = TablePath.of(parts[0], parts[1]);
        } else {
            throw new IllegalArgumentException(
                    "Table name must be in format 'database.table': " + fullTableName);
        }

        try (Table table = connectionManager.getConnection().getTable(tablePath)) {
            TableInfo tableInfo = table.getTableInfo();
            Schema schema = tableInfo.getSchema();
            RowType rowType = schema.getRowType();

            SqlNodeList specifiedColumns = insert.getTargetColumnList();
            List<String> columnNames;
            if (specifiedColumns == null || specifiedColumns.isEmpty()) {
                columnNames = rowType.getFieldNames();
            } else {
                columnNames = new ArrayList<>();
                for (SqlNode col : specifiedColumns) {
                    if (col instanceof SqlIdentifier) {
                        columnNames.add(((SqlIdentifier) col).getSimple());
                    }
                }
            }

            SqlNode source = insert.getSource();
            if (!(source instanceof SqlCall) || ((SqlCall) source).getKind() != SqlKind.VALUES) {
                throw new UnsupportedOperationException(
                        "Only VALUES clause is supported for INSERT/UPSERT");
            }

            SqlCall valuesCall = (SqlCall) source;
            List<List<SqlNode>> allRowExpressions = parseInsertValues(valuesCall);

            boolean hasPrimaryKey = schema.getPrimaryKey().isPresent();
            int rowsInserted = 0;

            if (hasPrimaryKey || forceUpsert) {
                if (forceUpsert && !hasPrimaryKey) {
                    throw new UnsupportedOperationException(
                            "UPSERT is only supported for tables with primary keys");
                }
                UpsertWriter writer = table.newUpsert().createWriter();
                for (List<SqlNode> rowExpressions : allRowExpressions) {
                    GenericRow row = buildRow(rowExpressions, columnNames, rowType);
                    writer.upsert(row);
                    rowsInserted++;
                }
                writer.flush();
            } else {
                AppendWriter writer = table.newAppend().createWriter();
                for (List<SqlNode> rowExpressions : allRowExpressions) {
                    GenericRow row = buildRow(rowExpressions, columnNames, rowType);
                    writer.append(row);
                    rowsInserted++;
                }
                writer.flush();
            }

            if (forceUpsert) {
                out.println(rowsInserted + " row(s) upserted into " + fullTableName);
            } else {
                out.println(rowsInserted + " row(s) inserted into " + fullTableName);
            }
            out.flush();
        }
    }

    public void executeUpdate(SqlUpdate update) throws Exception {
        SqlIdentifier tableId = (SqlIdentifier) update.getTargetTable();
        String fullTableName = tableId.toString();

        out.println("Updating table: " + fullTableName);

        String[] parts = fullTableName.split("\\.");
        TablePath tablePath;
        if (parts.length == 2) {
            tablePath = TablePath.of(parts[0], parts[1]);
        } else {
            throw new IllegalArgumentException(
                    "Table name must be in format 'database.table': " + fullTableName);
        }

        try (Table table = connectionManager.getConnection().getTable(tablePath)) {
            TableInfo tableInfo = table.getTableInfo();
            Schema schema = tableInfo.getSchema();

            if (!schema.getPrimaryKey().isPresent()) {
                throw new UnsupportedOperationException(
                        "UPDATE is only supported for tables with primary keys");
            }

            if (update.getCondition() == null) {
                throw new UnsupportedOperationException(
                        "UPDATE without WHERE clause is not supported for safety reasons");
            }

            RowType rowType = schema.getRowType();
            SqlNodeList targetColumns = update.getTargetColumnList();
            SqlNodeList sourceExpressions = update.getSourceExpressionList();

            if (targetColumns == null
                    || targetColumns.isEmpty()
                    || sourceExpressions == null
                    || sourceExpressions.isEmpty()) {
                throw new IllegalArgumentException("UPDATE requires SET clause");
            }

            Map<String, String> updateColumns = new HashMap<>();
            for (int i = 0; i < targetColumns.size(); i++) {
                SqlNode colNode = targetColumns.get(i);
                SqlNode valNode = sourceExpressions.get(i);

                String columnName;
                if (colNode instanceof SqlIdentifier) {
                    columnName = ((SqlIdentifier) colNode).getSimple();
                } else {
                    columnName = colNode.toString();
                }

                String value = stripSqlQuotes(valNode.toString());
                updateColumns.put(columnName, value);
            }

            Map<String, String> whereEqualities =
                    WhereClauseEvaluator.extractEqualities(update.getCondition());

            List<String> pkColumns = schema.getPrimaryKeyColumnNames();
            InternalRow pkRow =
                    WhereClauseEvaluator.buildPrimaryKeyRow(pkColumns, whereEqualities, rowType);

            Lookuper lookuper = table.newLookup().createLookuper();
            LookupResult lookupResult = lookuper.lookup(pkRow).get();
            InternalRow existingRow = lookupResult.getSingletonRow();

            if (existingRow == null) {
                out.println("No rows found matching WHERE clause");
                out.flush();
                return;
            }

            GenericRow updatedRow = new GenericRow(rowType.getFieldCount());
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                String columnName = rowType.getFieldNames().get(i);
                DataType dataType = rowType.getTypeAt(i);
                if (updateColumns.containsKey(columnName)) {
                    String valueStr = updateColumns.get(columnName);
                    Object value = DataTypeConverter.convertFromString(valueStr, dataType);
                    updatedRow.setField(i, value);
                } else {
                    updatedRow.setField(
                            i, DataTypeConverter.getFieldValue(existingRow, i, dataType));
                }
            }

            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(updatedRow);
            writer.flush();

            out.println("1 row(s) updated in " + fullTableName);
            out.flush();
        }
    }

    public void executeDelete(SqlDelete delete) throws Exception {
        SqlIdentifier tableId = (SqlIdentifier) delete.getTargetTable();
        String fullTableName = tableId.toString();

        out.println("Deleting from table: " + fullTableName);

        String[] parts = fullTableName.split("\\.");
        TablePath tablePath;
        if (parts.length == 2) {
            tablePath = TablePath.of(parts[0], parts[1]);
        } else {
            throw new IllegalArgumentException(
                    "Table name must be in format 'database.table': " + fullTableName);
        }

        try (Table table = connectionManager.getConnection().getTable(tablePath)) {
            TableInfo tableInfo = table.getTableInfo();
            Schema schema = tableInfo.getSchema();

            if (!schema.getPrimaryKey().isPresent()) {
                throw new UnsupportedOperationException(
                        "DELETE is only supported for tables with primary keys");
            }

            if (delete.getCondition() == null) {
                throw new UnsupportedOperationException(
                        "DELETE without WHERE clause is not supported for safety reasons");
            }

            RowType rowType = schema.getRowType();
            Map<String, String> whereEqualities =
                    WhereClauseEvaluator.extractEqualities(delete.getCondition());

            List<String> pkColumns = schema.getPrimaryKeyColumnNames();
            InternalRow pkRow =
                    WhereClauseEvaluator.buildPrimaryKeyRow(pkColumns, whereEqualities, rowType);

            Lookuper lookuper = table.newLookup().createLookuper();
            LookupResult lookupResult = lookuper.lookup(pkRow).get();
            InternalRow existingRow = lookupResult.getSingletonRow();

            if (existingRow == null) {
                out.println("No rows found matching WHERE clause");
                out.flush();
                return;
            }

            GenericRow deleteRow = new GenericRow(rowType.getFieldCount());
            for (int i = 0; i < pkColumns.size(); i++) {
                String pkColumn = pkColumns.get(i);
                int fieldIndex = rowType.getFieldNames().indexOf(pkColumn);
                DataType dataType = rowType.getTypeAt(fieldIndex);
                deleteRow.setField(
                        fieldIndex,
                        DataTypeConverter.getFieldValue(existingRow, fieldIndex, dataType));
            }

            UpsertWriter writer = table.newUpsert().createWriter();
            writer.delete(deleteRow);
            writer.flush();

            out.println("1 row(s) deleted from " + fullTableName);
            out.flush();
        }
    }

    private List<List<SqlNode>> parseInsertValues(SqlCall valuesCall) {
        List<List<SqlNode>> allRowExpressions = new ArrayList<>();

        for (SqlNode operand : valuesCall.getOperandList()) {
            if (operand instanceof SqlCall) {
                SqlCall rowCall = (SqlCall) operand;
                if (rowCall.getKind() == SqlKind.ROW) {
                    List<SqlNode> rowExprs = new ArrayList<>();
                    for (SqlNode expr : rowCall.getOperandList()) {
                        rowExprs.add(expr);
                    }
                    allRowExpressions.add(rowExprs);
                }
            }
        }

        return allRowExpressions;
    }

    private GenericRow buildRow(
            List<SqlNode> expressions, List<String> columnNames, RowType rowType) {
        if (expressions.size() != columnNames.size()) {
            throw new IllegalArgumentException(
                    "Number of values ("
                            + expressions.size()
                            + ") does not match number of columns ("
                            + columnNames.size()
                            + ")");
        }

        GenericRow row = new GenericRow(rowType.getFieldCount());

        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            SqlNode expression = expressions.get(i);
            String valueStr = stripSqlQuotes(expression.toString());

            int fieldIndex = rowType.getFieldNames().indexOf(columnName);
            if (fieldIndex < 0) {
                throw new IllegalArgumentException(
                        "Column not found in table schema: " + columnName);
            }

            DataType dataType = rowType.getTypeAt(fieldIndex);
            Object value = DataTypeConverter.convertFromString(valueStr, dataType);
            row.setField(fieldIndex, value);
        }

        return row;
    }

    private String stripSqlQuotes(String value) {
        if (value == null) {
            return null;
        }
        value = value.trim();
        if ((value.startsWith("'") && value.endsWith("'"))
                || (value.startsWith("\"") && value.endsWith("\""))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }
}
