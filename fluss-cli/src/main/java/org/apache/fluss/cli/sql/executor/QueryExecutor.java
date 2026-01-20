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
import org.apache.fluss.cli.format.OutputFormat;
import org.apache.fluss.cli.format.OutputFormatter;
import org.apache.fluss.cli.util.DataTypeConverter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Executor for SELECT query operations.
 *
 * <p>This executor handles SELECT statements with optimization for point queries using the Lookup
 * API when all primary key columns are provided in the WHERE clause.
 */
public class QueryExecutor {
    private final ConnectionManager connectionManager;
    private final PrintWriter out;
    private final OutputFormat outputFormat;
    private final boolean quiet;
    private final long streamingTimeoutSeconds;

    public QueryExecutor(ConnectionManager connectionManager, PrintWriter out) {
        this(connectionManager, out, OutputFormat.TABLE, false, 30);
    }

    public QueryExecutor(
            ConnectionManager connectionManager, PrintWriter out, OutputFormat outputFormat) {
        this(connectionManager, out, outputFormat, false, 30);
    }

    public QueryExecutor(
            ConnectionManager connectionManager,
            PrintWriter out,
            OutputFormat outputFormat,
            boolean quiet) {
        this(connectionManager, out, outputFormat, quiet, 30);
    }

    public QueryExecutor(
            ConnectionManager connectionManager,
            PrintWriter out,
            OutputFormat outputFormat,
            boolean quiet,
            long streamingTimeoutSeconds) {
        this.connectionManager = connectionManager;
        this.out = out;
        this.outputFormat = outputFormat;
        this.quiet = quiet;
        this.streamingTimeoutSeconds = streamingTimeoutSeconds;
    }

    /**
     * Executes a SELECT statement.
     *
     * @param select the parsed SELECT statement
     * @throws Exception if the query execution fails
     */
    public void executeSelect(SqlSelect select) throws Exception {
        executeSelect(select, select);
    }

    /**
     * Executes a SELECT statement.
     *
     * @param select the parsed SELECT statement
     * @param originalNode the original SqlNode (may be SqlOrderBy wrapping the SELECT)
     * @throws Exception if the query execution fails
     */
    public void executeSelect(SqlSelect select, SqlNode originalNode) throws Exception {
        SqlNode fromNode = select.getFrom();

        if (!(fromNode instanceof SqlIdentifier)) {
            throw new IllegalArgumentException(
                    "FROM clause must reference a table (database.table format)");
        }

        SqlIdentifier tableId = (SqlIdentifier) fromNode;
        String fullName = tableId.toString();
        String[] parts = fullName.split("\\.");
        TablePath tablePath;

        if (parts.length == 2) {
            tablePath = TablePath.of(parts[0], parts[1]);
        } else {
            throw new IllegalArgumentException(
                    "Table name must be in format 'database.table': " + fullName);
        }

        List<String> projectedColumns = null;
        SqlNodeList selectList = select.getSelectList();

        if (selectList != null && selectList.size() == 1) {
            SqlNode item = selectList.get(0);
            if (item instanceof SqlIdentifier && ((SqlIdentifier) item).isStar()) {
                projectedColumns = null;
            } else {
                projectedColumns = extractColumnNames(selectList);
            }
        } else if (selectList != null) {
            projectedColumns = extractColumnNames(selectList);
        }

        // Extract LIMIT clause from SqlOrderBy if present
        SqlNode fetch = null;
        if (originalNode instanceof org.apache.calcite.sql.SqlOrderBy) {
            org.apache.calcite.sql.SqlOrderBy orderBy =
                    (org.apache.calcite.sql.SqlOrderBy) originalNode;
            fetch = orderBy.fetch;
        } else {
            fetch = select.getFetch();
        }

        Integer limit = null;
        if (fetch != null) {
            limit = extractLimitValue(fetch);
        }

        if (!quiet) {
            out.println("Executing SELECT on table: " + tablePath);
            out.flush();
        }

        try (org.apache.fluss.client.table.Table flussTable =
                connectionManager.getConnection().getTable(tablePath)) {
            TableInfo tableInfo = flussTable.getTableInfo();
            org.apache.fluss.metadata.Schema schema = tableInfo.getSchema();
            SqlNode whereClause = select.getWhere();

            // Determine if this is a log table (no primary key)
            boolean isLogTable = !schema.getPrimaryKey().isPresent();
            // Streaming mode: Log table without LIMIT
            boolean streamingMode = isLogTable && limit == null;

            if (whereClause != null
                    && schema.getPrimaryKey().isPresent()
                    && canUseLookup(whereClause, schema)) {
                executeSelectWithLookup(
                        flussTable, whereClause, schema, projectedColumns, tablePath, limit);
            } else {
                if (!quiet) {
                    if (whereClause != null && isLogTable) {
                        out.println(
                                "Warning: WHERE clause on log table - using client-side filtering");
                    } else if (whereClause != null) {
                        out.println(
                                "Warning: WHERE clause without all primary key columns - using full"
                                        + " table scan");
                    }

                    if (streamingMode) {
                        out.println(
                                "Streaming mode: Continuously polling for new data (Ctrl+C to exit)");
                        out.println("Idle timeout: " + streamingTimeoutSeconds + " seconds");
                    }
                }

                List<String> columnsToFetch = projectedColumns;
                if (whereClause != null && projectedColumns != null) {
                    List<String> whereColumns =
                            org.apache.fluss.cli.util.WhereClauseEvaluator.extractReferencedColumns(
                                    whereClause);
                    columnsToFetch = new ArrayList<>(projectedColumns);
                    for (String col : whereColumns) {
                        if (!columnsToFetch.contains(col)) {
                            columnsToFetch.add(col);
                        }
                    }
                }

                executeSelectWithScan(
                        flussTable,
                        tableInfo,
                        projectedColumns,
                        columnsToFetch,
                        whereClause,
                        limit,
                        streamingMode);
            }
        }
    }

    private boolean canUseLookup(SqlNode whereClause, org.apache.fluss.metadata.Schema schema) {
        try {
            java.util.Map<String, String> whereEqualities =
                    org.apache.fluss.cli.util.WhereClauseEvaluator.extractEqualities(whereClause);
            List<String> pkColumns = schema.getPrimaryKeyColumnNames();

            for (String pkColumn : pkColumns) {
                if (!whereEqualities.containsKey(pkColumn)) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private Integer extractLimitValue(SqlNode fetch) {
        if (fetch instanceof org.apache.calcite.sql.SqlNumericLiteral) {
            org.apache.calcite.sql.SqlNumericLiteral literal =
                    (org.apache.calcite.sql.SqlNumericLiteral) fetch;
            return literal.intValue(true);
        } else if (fetch instanceof org.apache.calcite.sql.SqlLiteral) {
            org.apache.calcite.sql.SqlLiteral literal = (org.apache.calcite.sql.SqlLiteral) fetch;
            Object value = literal.getValue();
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
        }
        throw new IllegalArgumentException("Invalid LIMIT value: " + fetch);
    }

    private void executeSelectWithLookup(
            org.apache.fluss.client.table.Table flussTable,
            SqlNode whereClause,
            org.apache.fluss.metadata.Schema schema,
            List<String> projectedColumns,
            TablePath tablePath,
            Integer limit)
            throws Exception {
        if (!quiet) {
            out.println("Using point query optimization (Lookup API)");
        }

        if (limit != null && limit == 0) {
            if (!quiet) {
                out.println("LIMIT 0: Skipping query execution");
            }
            RowType displayRowType;
            if (projectedColumns == null) {
                displayRowType = schema.getRowType();
            } else {
                displayRowType = projectRowType(schema.getRowType(), projectedColumns);
            }
            OutputFormatter formatter = createFormatter(displayRowType);
            printHeader(formatter);
            printFooter(formatter, 0);
            return;
        }

        RowType fullRowType = schema.getRowType();
        java.util.Map<String, String> whereEqualities =
                org.apache.fluss.cli.util.WhereClauseEvaluator.extractEqualities(whereClause);
        List<String> pkColumns = schema.getPrimaryKeyColumnNames();

        InternalRow pkRow =
                org.apache.fluss.cli.util.WhereClauseEvaluator.buildPrimaryKeyRow(
                        pkColumns, whereEqualities, fullRowType);

        org.apache.fluss.client.lookup.Lookuper lookuper = flussTable.newLookup().createLookuper();
        org.apache.fluss.client.lookup.LookupResult lookupResult = lookuper.lookup(pkRow).get();
        InternalRow resultRow = lookupResult.getSingletonRow();

        RowType displayRowType;
        if (projectedColumns == null) {
            displayRowType = fullRowType;
        } else {
            displayRowType = projectRowType(fullRowType, projectedColumns);
        }

        OutputFormatter formatter = createFormatter(displayRowType);
        printHeader(formatter);

        long totalRows = 0;
        if (resultRow != null) {
            if (projectedColumns == null) {
                printRow(formatter, resultRow);
            } else {
                GenericRow projectedRow = new GenericRow(projectedColumns.size());
                for (int i = 0; i < projectedColumns.size(); i++) {
                    String colName = projectedColumns.get(i);
                    int srcIndex = fullRowType.getFieldNames().indexOf(colName);
                    DataType dataType = fullRowType.getTypeAt(srcIndex);
                    projectedRow.setField(
                            i, DataTypeConverter.getFieldValue(resultRow, srcIndex, dataType));
                }
                printRow(formatter, projectedRow);
            }
            totalRows = 1;
        }

        printFooter(formatter, totalRows);
    }

    private void executeSelectWithScan(
            org.apache.fluss.client.table.Table flussTable,
            TableInfo tableInfo,
            List<String> projectedColumns,
            List<String> columnsToFetch,
            SqlNode whereClause,
            Integer limit,
            boolean streamingMode)
            throws Exception {
        RowType displayRowType;
        if (projectedColumns == null) {
            displayRowType = tableInfo.getSchema().getRowType();
        } else {
            displayRowType = projectRowType(tableInfo.getSchema().getRowType(), projectedColumns);
        }

        RowType fetchRowType;
        if (columnsToFetch == null) {
            fetchRowType = tableInfo.getSchema().getRowType();
        } else {
            fetchRowType = projectRowType(tableInfo.getSchema().getRowType(), columnsToFetch);
        }

        OutputFormatter formatter = createFormatter(displayRowType);
        printHeader(formatter);

        org.apache.fluss.client.table.scanner.log.LogScanner scanner;
        if (columnsToFetch == null) {
            scanner = flussTable.newScan().createLogScanner();
        } else {
            scanner = flussTable.newScan().project(columnsToFetch).createLogScanner();
        }

        int numBuckets = tableInfo.getNumBuckets();
        for (int i = 0; i < numBuckets; i++) {
            scanner.subscribeFromBeginning(i);
        }

        long totalRows = 0;
        long pollTimeoutMs = 5000;
        long idleTimeoutMs = streamingMode ? (streamingTimeoutSeconds * 1000) : 10000;
        long lastRecordTime = System.currentTimeMillis();
        int maxLimit = limit != null ? limit : Integer.MAX_VALUE;

        try {
            while (totalRows < maxLimit) {
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastRecordTime > idleTimeoutMs) {
                    if (streamingMode && !quiet) {
                        out.println(
                                "\nIdle timeout reached ("
                                        + streamingTimeoutSeconds
                                        + "s). Exiting.");
                        out.flush();
                    }
                    break;
                }

                org.apache.fluss.client.table.scanner.log.ScanRecords scanRecords =
                        scanner.poll(java.time.Duration.ofMillis(pollTimeoutMs));

                if (scanRecords.count() == 0) {
                    if (!streamingMode) {
                        break;
                    }
                    continue;
                }

                lastRecordTime = System.currentTimeMillis();

                for (org.apache.fluss.metadata.TableBucket bucket : scanRecords.buckets()) {
                    for (org.apache.fluss.client.table.scanner.ScanRecord record :
                            scanRecords.records(bucket)) {
                        if (totalRows >= maxLimit) {
                            break;
                        }

                        InternalRow fetchedRow = record.getRow();

                        if (whereClause != null
                                && !org.apache.fluss.cli.util.WhereClauseEvaluator.evaluateWhere(
                                        whereClause, fetchedRow, fetchRowType)) {
                            continue;
                        }

                        InternalRow displayRow = fetchedRow;
                        if (projectedColumns != null && columnsToFetch != projectedColumns) {
                            displayRow = projectRow(fetchedRow, fetchRowType, projectedColumns);
                        }

                        printRow(formatter, displayRow);
                        totalRows++;

                        if (totalRows >= maxLimit) {
                            break;
                        }
                    }
                    if (totalRows >= maxLimit) {
                        break;
                    }
                }
            }
        } finally {
            scanner.close();
        }

        printFooter(formatter, totalRows);
    }

    private List<String> extractColumnNames(SqlNodeList selectList) {
        List<String> columnNames = new ArrayList<>();
        for (SqlNode item : selectList) {
            if (item instanceof SqlIdentifier) {
                columnNames.add(((SqlIdentifier) item).getSimple());
            } else {
                columnNames.add(item.toString());
            }
        }
        return columnNames;
    }

    private RowType projectRowType(RowType originalRowType, List<String> projectedColumns) {
        List<DataField> projectedFields = new ArrayList<>();

        for (String columnName : projectedColumns) {
            boolean found = false;
            for (int i = 0; i < originalRowType.getFieldCount(); i++) {
                if (originalRowType.getFieldNames().get(i).equals(columnName)) {
                    projectedFields.add(new DataField(columnName, originalRowType.getTypeAt(i)));
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException("Column not found: " + columnName);
            }
        }

        return new RowType(projectedFields);
    }

    private InternalRow projectRow(
            InternalRow sourceRow, RowType sourceRowType, List<String> targetColumns) {
        org.apache.fluss.row.GenericRow projectedRow =
                new org.apache.fluss.row.GenericRow(targetColumns.size());

        for (int i = 0; i < targetColumns.size(); i++) {
            String columnName = targetColumns.get(i);
            int sourceIndex = sourceRowType.getFieldNames().indexOf(columnName);
            if (sourceIndex < 0) {
                throw new IllegalArgumentException("Column not found: " + columnName);
            }

            DataType dataType = sourceRowType.getTypeAt(sourceIndex);
            Object value =
                    org.apache.fluss.cli.util.DataTypeConverter.getFieldValue(
                            sourceRow, sourceIndex, dataType);
            projectedRow.setField(i, value);
        }

        return projectedRow;
    }

    private OutputFormatter createFormatter(RowType rowType) {
        switch (outputFormat) {
            case TABLE:
                return new org.apache.fluss.cli.format.TableFormatter(rowType, out);
            case CSV:
                return new org.apache.fluss.cli.format.CsvFormatter(rowType, out);
            case TSV:
                return new org.apache.fluss.cli.format.TsvFormatter(rowType, out);
            case JSON:
                return new org.apache.fluss.cli.format.JsonFormatter(rowType, out);
            default:
                return new org.apache.fluss.cli.format.TableFormatter(rowType, out);
        }
    }

    private void printHeader(OutputFormatter formatter) {
        formatter.printHeader();
    }

    private void printRow(OutputFormatter formatter, InternalRow row) {
        formatter.printRow(row);
    }

    private void printFooter(OutputFormatter formatter, long rowCount) {
        formatter.printFooter(rowCount);
    }
}
