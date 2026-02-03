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
import org.apache.fluss.cli.util.SqlTypeMapper;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.exception.DatabaseNotEmptyException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataType;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.flink.sql.parser.ddl.SqlCreateDatabase;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlDropDatabase;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Executor for DDL (Data Definition Language) operations.
 *
 * <p>Handles CREATE, DROP, and ALTER operations for databases and tables.
 */
public class DdlExecutor {
    private static final Pattern RENAME_COLUMN_PATTERN =
            Pattern.compile("RENAME\\s+COLUMN\\s+(\\S+)\\s+TO\\s+(\\S+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern COLUMN_FIRST_PATTERN =
            Pattern.compile("^(.*)\\s+FIRST$", Pattern.CASE_INSENSITIVE);
    private static final Pattern COLUMN_AFTER_PATTERN =
            Pattern.compile("^(.*)\\s+AFTER\\s+(\\S+)$", Pattern.CASE_INSENSITIVE);

    private final ConnectionManager connectionManager;
    private final PrintWriter out;
    private final org.apache.fluss.cli.sql.CalciteSqlParser sqlParser;

    public DdlExecutor(ConnectionManager connectionManager, PrintWriter out) {
        this.connectionManager = connectionManager;
        this.out = out;
        this.sqlParser = new org.apache.fluss.cli.sql.CalciteSqlParser();
    }

    public void executeCreateDatabase(SqlCreateDatabase createDb) throws Exception {
        Admin admin = connectionManager.getConnection().getAdmin();
        String dbName = createDb.getDatabaseName().getSimple();
        boolean ifNotExists = createDb.isIfNotExists();

        out.println("Creating database: " + dbName);

        DatabaseDescriptor descriptor = DatabaseDescriptor.builder().build();

        CompletableFuture<Void> future = admin.createDatabase(dbName, descriptor, ifNotExists);
        future.get();

        out.println("Database created successfully: " + dbName);
        out.flush();
    }

    public void executeCreateTable(SqlCreateTable createTable) throws Exception {
        Admin admin = connectionManager.getConnection().getAdmin();
        String fullTableName = createTable.getTableName().toString();
        boolean ifNotExists = createTable.isIfNotExists();

        out.println("Creating table: " + fullTableName);

        String[] parts = fullTableName.split("\\.");
        TablePath tablePath;
        if (parts.length == 2) {
            tablePath = TablePath.of(parts[0], parts[1]);
        } else {
            throw new IllegalArgumentException(
                    "Table name must be in format 'database.table': " + fullTableName);
        }

        Schema.Builder schemaBuilder = Schema.newBuilder();
        SqlNodeList columnList = createTable.getColumnList();

        if (columnList == null || columnList.isEmpty()) {
            throw new IllegalArgumentException("CREATE TABLE requires at least one column");
        }

        for (SqlNode col : columnList) {
            if (col instanceof SqlTableColumn.SqlRegularColumn) {
                SqlTableColumn.SqlRegularColumn regularCol = (SqlTableColumn.SqlRegularColumn) col;
                String columnName = regularCol.getName().getSimple();
                DataType dataType = SqlTypeMapper.toFlussDataType(regularCol.getType());
                schemaBuilder.column(columnName, dataType);
            }
        }

        List<SqlTableConstraint> constraints = createTable.getTableConstraints();
        for (SqlTableConstraint constraint : constraints) {
            if (constraint.isPrimaryKey()) {
                List<String> pkColumns = new ArrayList<>();
                SqlNodeList pkColumnList = constraint.getColumns();
                for (SqlNode pkCol : pkColumnList) {
                    if (pkCol instanceof SqlIdentifier) {
                        pkColumns.add(((SqlIdentifier) pkCol).getSimple());
                    }
                }
                schemaBuilder.primaryKey(pkColumns);
            }
        }

        Schema schema = schemaBuilder.build();

        TableDescriptor.Builder tableDescriptorBuilder = TableDescriptor.builder().schema(schema);

        List<String> partitionKeys = new ArrayList<>();
        Map<String, String> tableProperties = new HashMap<>();
        Integer bucketCount = null;
        List<String> bucketKeys = new ArrayList<>();

        if (tableProperties.containsKey("bucket.num")) {
            bucketCount = Integer.parseInt(tableProperties.get("bucket.num"));
            tableProperties.remove("bucket.num");
        }
        if (tableProperties.containsKey("bucket.key")) {
            String bucketKeyStr = tableProperties.get("bucket.key");
            bucketKeys.addAll(java.util.Arrays.asList(bucketKeyStr.split(",")));
            tableProperties.remove("bucket.key");
        }

        if (!partitionKeys.isEmpty()) {
            tableDescriptorBuilder.partitionedBy(partitionKeys);
        }

        if (!tableProperties.isEmpty()) {
            tableDescriptorBuilder.properties(tableProperties);
        }

        if (bucketCount == null) {
            bucketCount = 3;
        }

        if (schema.getPrimaryKey().isPresent()) {
            if (bucketKeys.isEmpty()) {
                List<String> pkColumns = schema.getPrimaryKeyColumnNames();
                List<String> defaultBucketKeys = new ArrayList<>(pkColumns);
                defaultBucketKeys.removeAll(partitionKeys);
                tableDescriptorBuilder.distributedBy(bucketCount, defaultBucketKeys);
            } else {
                tableDescriptorBuilder.distributedBy(bucketCount, bucketKeys);
            }
        } else {
            if (!bucketKeys.isEmpty()) {
                tableDescriptorBuilder.distributedBy(bucketCount, bucketKeys);
            } else {
                tableDescriptorBuilder.distributedBy(bucketCount, Collections.emptyList());
            }
        }

        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

        CompletableFuture<Void> future = admin.createTable(tablePath, tableDescriptor, ifNotExists);
        future.get();

        out.println("Table created successfully: " + fullTableName);
        out.flush();
    }

    public void executeDropTable(SqlDropTable drop) throws Exception {
        Admin admin = connectionManager.getConnection().getAdmin();
        String fullName = drop.getTableName().toString();
        boolean ifExists = drop.getIfExists();

        out.println("Dropping table: " + fullName);

        String[] parts = fullName.split("\\.");
        TablePath tablePath;
        if (parts.length == 2) {
            tablePath = TablePath.of(parts[0], parts[1]);
        } else {
            throw new IllegalArgumentException(
                    "Table name must be in format 'database.table': " + fullName);
        }

        CompletableFuture<Void> future = admin.dropTable(tablePath, ifExists);
        future.get();

        out.println("Table dropped successfully: " + fullName);
        out.flush();
    }

    public void executeDropDatabase(SqlDropDatabase drop) throws Exception {
        Admin admin = connectionManager.getConnection().getAdmin();
        String dbName = drop.getDatabaseName().getSimple();
        boolean ifExists = drop.getIfExists();
        boolean cascade = drop.isCascade();

        out.println("Dropping database: " + dbName);

        CompletableFuture<Void> future = admin.dropDatabase(dbName, ifExists, cascade);
        try {
            future.get();
        } catch (ExecutionException exception) {
            if (exception.getCause() instanceof DatabaseNotEmptyException) {
                out.println("Database drop failed: " + dbName);
                out.println("Reason: database is not empty.");
                out.println("Hint: drop tables first or use DROP DATABASE ... CASCADE.");
                out.flush();
                return;
            }
            throw exception;
        }

        out.println("Database dropped successfully: " + dbName);
        out.flush();
    }

    public void executeAlterTable(
            String sql,
            TablePath tablePath,
            String action,
            PartitionSpec partitionSpec,
            String specContent)
            throws Exception {
        String actionUpper = action.toUpperCase();
        Admin admin = connectionManager.getConnection().getAdmin();

        if (actionUpper.startsWith("ADD PARTITION")) {
            boolean ignoreIfExists = actionUpper.contains("IF NOT EXISTS");
            validatePartitionSpec(tablePath, partitionSpec, true);
            admin.createPartition(tablePath, partitionSpec, ignoreIfExists).get();
            out.println("Partition added to " + tablePath + ": " + partitionSpec);
            out.flush();
            return;
        }

        if (actionUpper.startsWith("DROP PARTITION")) {
            boolean ignoreIfNotExists = actionUpper.contains("IF EXISTS");
            validatePartitionSpec(tablePath, partitionSpec, true);
            admin.dropPartition(tablePath, partitionSpec, ignoreIfNotExists).get();
            out.println("Partition dropped from " + tablePath + ": " + partitionSpec);
            out.flush();
            return;
        }

        List<TableChange> changes = new ArrayList<>();

        if (actionUpper.startsWith("SET")) {
            String content = extractParenthesizedContent(stripLeadingKeyword(action, "SET"));
            Map<String, String> properties = parseKeyValueMap(content);
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                changes.add(TableChange.set(entry.getKey(), entry.getValue()));
            }
        } else if (actionUpper.startsWith("RESET")) {
            String content = extractParenthesizedContent(stripLeadingKeyword(action, "RESET"));
            List<String> keys = parseKeyList(content);
            for (String key : keys) {
                changes.add(TableChange.reset(key));
            }
        } else if (actionUpper.startsWith("ADD COLUMNS")) {
            String columnClause = action.substring("ADD COLUMNS".length()).trim();
            String content = extractParenthesizedContent(columnClause);
            for (String definition : splitCommaSeparated(content)) {
                changes.add(parseAddColumnChange(definition));
            }
        } else if (actionUpper.startsWith("ADD COLUMN") || actionUpper.startsWith("ADD ")) {
            String columnClause =
                    actionUpper.startsWith("ADD COLUMN")
                            ? action.substring("ADD COLUMN".length()).trim()
                            : action.substring("ADD".length()).trim();
            changes.add(parseAddColumnChange(columnClause));
        } else if (actionUpper.startsWith("MODIFY COLUMNS")) {
            String columnClause = action.substring("MODIFY COLUMNS".length()).trim();
            String content = extractParenthesizedContent(columnClause);
            for (String definition : splitCommaSeparated(content)) {
                changes.add(parseModifyColumnChange(definition));
            }
        } else if (actionUpper.startsWith("MODIFY COLUMN") || actionUpper.startsWith("MODIFY ")) {
            String columnClause =
                    actionUpper.startsWith("MODIFY COLUMN")
                            ? action.substring("MODIFY COLUMN".length()).trim()
                            : action.substring("MODIFY".length()).trim();
            changes.add(parseModifyColumnChange(columnClause));
        } else if (actionUpper.startsWith("DROP COLUMN")) {
            String columnClause = action.substring("DROP COLUMN".length()).trim();
            if (columnClause.toUpperCase().startsWith("IF EXISTS ")) {
                columnClause = columnClause.substring("IF EXISTS".length()).trim();
            }
            String[] columnParts = splitFirstToken(columnClause);
            if (columnParts[0].isEmpty()) {
                throw new IllegalArgumentException(
                        "ALTER TABLE DROP COLUMN requires column name. Example: "
                                + "ALTER TABLE db.tbl DROP COLUMN col. Got: "
                                + action);
            }
            changes.add(TableChange.dropColumn(columnParts[0]));
        } else if (actionUpper.startsWith("RENAME COLUMN")) {
            Matcher matcher = RENAME_COLUMN_PATTERN.matcher(action);
            if (!matcher.find()) {
                throw new IllegalArgumentException(
                        "ALTER TABLE RENAME COLUMN requires 'old TO new': " + action);
            }
            changes.add(TableChange.renameColumn(matcher.group(1), matcher.group(2)));
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported ALTER TABLE action: "
                            + action
                            + ". Supported: ADD/DROP PARTITION, ADD COLUMN(S), MODIFY COLUMN(S), "
                            + "DROP COLUMN, RENAME COLUMN, SET, RESET.");
        }

        if (changes.isEmpty()) {
            throw new IllegalArgumentException("No ALTER TABLE changes parsed: " + action);
        }

        admin.alterTable(tablePath, changes, false).get();
        out.println("Table altered successfully: " + tablePath);
        out.flush();
    }

    private void validatePartitionSpec(
            TablePath tablePath, PartitionSpec partitionSpec, boolean requireAll) throws Exception {
        Admin admin = connectionManager.getConnection().getAdmin();
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        List<String> partitionKeys = tableInfo.getPartitionKeys();
        if (partitionKeys.isEmpty()) {
            throw new IllegalArgumentException("Table " + tablePath + " is not partitioned");
        }
        Map<String, String> specMap = partitionSpec.getSpecMap();
        for (String key : specMap.keySet()) {
            if (!partitionKeys.contains(key)) {
                throw new IllegalArgumentException(
                        "Unknown partition key '"
                                + key
                                + "' for table "
                                + tablePath
                                + ". Expected keys: "
                                + String.join(", ", partitionKeys));
            }
        }
        if (requireAll) {
            for (String key : partitionKeys) {
                if (!specMap.containsKey(key)) {
                    throw new IllegalArgumentException(
                            "Partition spec must include all partition keys: "
                                    + String.join(", ", partitionKeys));
                }
            }
        }
    }

    private TableChange parseAddColumnChange(String columnClause) throws Exception {
        String[] columnParts = splitFirstToken(columnClause);
        if (columnParts[0].isEmpty() || columnParts[1].isEmpty()) {
            throw new IllegalArgumentException(
                    "ALTER TABLE ADD COLUMN requires column name and type. Example: "
                            + "ALTER TABLE db.tbl ADD COLUMN c INT. Got: "
                            + columnClause);
        }

        String typePart = columnParts[1].trim();
        TableChange.ColumnPosition position = TableChange.ColumnPosition.last();
        Matcher firstMatcher = COLUMN_FIRST_PATTERN.matcher(typePart);
        Matcher afterMatcher = COLUMN_AFTER_PATTERN.matcher(typePart);

        if (firstMatcher.matches()) {
            typePart = firstMatcher.group(1).trim();
            position = TableChange.ColumnPosition.first();
        } else if (afterMatcher.matches()) {
            typePart = afterMatcher.group(1).trim();
            position = TableChange.ColumnPosition.after(afterMatcher.group(2));
        }

        DataType dataType = parseDataType(typePart);
        return TableChange.addColumn(columnParts[0], dataType, null, position);
    }

    private TableChange parseModifyColumnChange(String columnClause) throws Exception {
        String[] columnParts = splitFirstToken(columnClause);
        if (columnParts[0].isEmpty() || columnParts[1].isEmpty()) {
            throw new IllegalArgumentException(
                    "ALTER TABLE MODIFY COLUMN requires column name and type. Example: "
                            + "ALTER TABLE db.tbl MODIFY COLUMN c STRING. Got: "
                            + columnClause);
        }

        String typePart = columnParts[1].trim();
        TableChange.ColumnPosition position = null;
        Matcher firstMatcher = COLUMN_FIRST_PATTERN.matcher(typePart);
        Matcher afterMatcher = COLUMN_AFTER_PATTERN.matcher(typePart);

        if (firstMatcher.matches()) {
            typePart = firstMatcher.group(1).trim();
            position = TableChange.ColumnPosition.first();
        } else if (afterMatcher.matches()) {
            typePart = afterMatcher.group(1).trim();
            position = TableChange.ColumnPosition.after(afterMatcher.group(2));
        }

        DataType dataType = parseDataType(typePart);
        return TableChange.modifyColumn(columnParts[0], dataType, null, position);
    }

    private DataType parseDataType(String typeLiteral) throws Exception {
        String ddl = "CREATE TABLE dummy (c " + typeLiteral + ")";
        List<SqlNode> nodes = sqlParser.parse(ddl);
        if (nodes.isEmpty() || !(nodes.get(0) instanceof SqlCreateTable)) {
            throw new IllegalArgumentException("Unable to parse data type: " + typeLiteral);
        }

        SqlCreateTable createTable = (SqlCreateTable) nodes.get(0);
        SqlNodeList columnList = createTable.getColumnList();
        if (columnList == null || columnList.isEmpty()) {
            throw new IllegalArgumentException("Unable to parse data type: " + typeLiteral);
        }

        SqlTableColumn.SqlRegularColumn column =
                (SqlTableColumn.SqlRegularColumn) columnList.get(0);
        return SqlTypeMapper.toFlussDataType(column.getType());
    }

    private String[] splitFirstToken(String input) {
        String trimmed = input.trim();
        int spaceIndex = trimmed.indexOf(' ');
        if (spaceIndex < 0) {
            return new String[] {trimmed, ""};
        }
        String first = trimmed.substring(0, spaceIndex).trim();
        String rest = trimmed.substring(spaceIndex + 1).trim();
        return new String[] {first, rest};
    }

    private String extractParenthesizedContent(String text) {
        int startIndex = text.indexOf('(');
        int endIndex = text.lastIndexOf(')');
        if (startIndex < 0 || endIndex <= startIndex) {
            throw new IllegalArgumentException("Expected parentheses in: " + text);
        }
        return text.substring(startIndex + 1, endIndex).trim();
    }

    private String stripLeadingKeyword(String text, String keyword) {
        String trimmed = text.trim();
        String upper = trimmed.toUpperCase();
        String keywordUpper = keyword.toUpperCase();
        if (upper.startsWith(keywordUpper)) {
            String remainder = trimmed.substring(keyword.length()).trim();
            if (remainder.toUpperCase().startsWith("WITH")) {
                remainder = remainder.substring(4).trim();
            }
            return remainder;
        }
        return trimmed;
    }

    private List<String> splitCommaSeparated(String input) {
        List<String> elements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        char quoteChar = 0;
        int depth = 0;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if ((c == '\'' || c == '"') && (i == 0 || input.charAt(i - 1) != '\\')) {
                if (!inQuote) {
                    inQuote = true;
                    quoteChar = c;
                } else if (c == quoteChar) {
                    inQuote = false;
                }
                current.append(c);
                continue;
            }

            if (inQuote) {
                current.append(c);
                continue;
            }

            if (c == '(' || c == '[') {
                depth++;
            } else if (c == ')' || c == ']') {
                depth--;
            }

            if (c == ',' && depth == 0) {
                String element = current.toString().trim();
                if (!element.isEmpty()) {
                    elements.add(element);
                }
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        String element = current.toString().trim();
        if (!element.isEmpty()) {
            elements.add(element);
        }
        return elements;
    }

    private Map<String, String> parseKeyValueMap(String content) {
        Map<String, String> map = new HashMap<>();
        if (content == null || content.trim().isEmpty()) {
            return map;
        }
        for (String entry : splitCommaSeparated(content)) {
            String[] parts = entry.split("=", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid key=value pair: " + entry);
            }
            String key = stripQuotes(parts[0].trim());
            String value = stripQuotes(parts[1].trim());
            map.put(key, value);
        }
        return map;
    }

    private List<String> parseKeyList(String content) {
        if (content == null || content.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> items = new ArrayList<>();
        for (String entry : splitCommaSeparated(content)) {
            items.add(stripQuotes(entry.trim()));
        }
        return items;
    }

    private String stripQuotes(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if ((trimmed.startsWith("'") && trimmed.endsWith("'"))
                || (trimmed.startsWith("\"") && trimmed.endsWith("\""))
                || (trimmed.startsWith("`") && trimmed.endsWith("`"))) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }
}
