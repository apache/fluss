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

package org.apache.fluss.cli.sql;

import org.apache.fluss.cli.config.ConnectionManager;
import org.apache.fluss.cli.format.OutputFormat;
import org.apache.fluss.cli.sql.ast.FlussStatement;
import org.apache.fluss.cli.sql.ast.FlussStatementNodes;
import org.apache.fluss.cli.sql.executor.AclExecutor;
import org.apache.fluss.cli.sql.executor.ClusterExecutor;
import org.apache.fluss.cli.sql.executor.DdlExecutor;
import org.apache.fluss.cli.sql.executor.DmlExecutor;
import org.apache.fluss.cli.sql.executor.MetadataExecutor;
import org.apache.fluss.cli.sql.executor.QueryExecutor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TablePath;

import org.apache.calcite.sql.SqlNode;

import java.io.PrintWriter;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Executes parsed SQL statements against a Fluss cluster. */
public class SqlExecutor {
    private static final Pattern PARTITION_CLAUSE_PATTERN =
            Pattern.compile("PARTITION\\s*\\(([^)]*)\\)", Pattern.CASE_INSENSITIVE);

    private final ConnectionManager connectionManager;
    private final CalciteSqlParser sqlParser;
    private final FlussStatementParser flussParser;
    private final PrintWriter out;
    private final OutputFormat outputFormat;
    private final boolean quiet;
    private final long streamingTimeoutSeconds;
    private final AclExecutor aclExecutor;
    private final ClusterExecutor clusterExecutor;
    private final QueryExecutor queryExecutor;
    private final DmlExecutor dmlExecutor;
    private final DdlExecutor ddlExecutor;
    private final MetadataExecutor metadataExecutor;

    public SqlExecutor(ConnectionManager connectionManager, PrintWriter out) {
        this(connectionManager, out, OutputFormat.TABLE, false, 30);
    }

    public SqlExecutor(
            ConnectionManager connectionManager, PrintWriter out, OutputFormat outputFormat) {
        this(connectionManager, out, outputFormat, false, 30);
    }

    public SqlExecutor(
            ConnectionManager connectionManager,
            PrintWriter out,
            OutputFormat outputFormat,
            boolean quiet) {
        this(connectionManager, out, outputFormat, quiet, 30);
    }

    public SqlExecutor(
            ConnectionManager connectionManager,
            PrintWriter out,
            OutputFormat outputFormat,
            boolean quiet,
            long streamingTimeoutSeconds) {
        this.connectionManager = connectionManager;
        this.sqlParser = new CalciteSqlParser();
        this.flussParser = new FlussStatementParser();
        this.out = out;
        this.outputFormat = outputFormat;
        this.quiet = quiet;
        this.streamingTimeoutSeconds = streamingTimeoutSeconds;
        this.aclExecutor = new AclExecutor(connectionManager, out);
        this.clusterExecutor = new ClusterExecutor(connectionManager, out);
        this.queryExecutor =
                new QueryExecutor(
                        connectionManager, out, outputFormat, quiet, streamingTimeoutSeconds);
        this.dmlExecutor = new DmlExecutor(connectionManager, out);
        this.ddlExecutor = new DdlExecutor(connectionManager, out);
        this.metadataExecutor = new MetadataExecutor(connectionManager, out, outputFormat);
    }

    public void executeSql(String sql) throws Exception {
        String originalSql = sql.trim();

        CalciteSqlParser.SqlStatementType rawType = sqlParser.classifyRawStatement(originalSql);
        if (rawType != CalciteSqlParser.SqlStatementType.UNKNOWN) {
            FlussStatement stmt = flussParser.parse(originalSql);

            if (stmt instanceof FlussStatementNodes.ShowDatabasesStatement) {
                metadataExecutor.executeShowDatabases(
                        (FlussStatementNodes.ShowDatabasesStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.ShowDatabaseExistsStatement) {
                metadataExecutor.executeShowDatabaseExists(
                        (FlussStatementNodes.ShowDatabaseExistsStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.ShowDatabaseInfoStatement) {
                metadataExecutor.executeShowDatabaseInfo(
                        (FlussStatementNodes.ShowDatabaseInfoStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.ShowTablesStatement) {
                metadataExecutor.executeShowTables((FlussStatementNodes.ShowTablesStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.ShowTableExistsStatement) {
                metadataExecutor.executeShowTableExists(
                        (FlussStatementNodes.ShowTableExistsStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.ShowTableSchemaStatement) {
                metadataExecutor.executeShowTableSchema(
                        (FlussStatementNodes.ShowTableSchemaStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.ShowServersStatement) {
                metadataExecutor.executeShowServers(
                        (FlussStatementNodes.ShowServersStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.UseDatabaseStatement) {
                metadataExecutor.executeUseDatabase(
                        (FlussStatementNodes.UseDatabaseStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.DescribeTableStatement) {
                metadataExecutor.executeDescribeTable(
                        (FlussStatementNodes.DescribeTableStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.ShowCreateTableStatement) {
                metadataExecutor.executeShowCreateTable(
                        (FlussStatementNodes.ShowCreateTableStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.ShowPartitionsStatement) {
                metadataExecutor.executeShowPartitions(
                        (FlussStatementNodes.ShowPartitionsStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.AlterTableStatement) {
                executeAlterTable(originalSql);
            } else if (stmt instanceof FlussStatementNodes.ShowKvSnapshotsStatement) {
                metadataExecutor.executeShowKvSnapshots(
                        (FlussStatementNodes.ShowKvSnapshotsStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.ShowKvSnapshotMetadataStatement) {
                metadataExecutor.executeShowKvSnapshotMetadata(
                        (FlussStatementNodes.ShowKvSnapshotMetadataStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.ShowLakeSnapshotStatement) {
                metadataExecutor.executeShowLakeSnapshot(
                        (FlussStatementNodes.ShowLakeSnapshotStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.ShowOffsetsStatement) {
                metadataExecutor.executeShowOffsets(
                        (FlussStatementNodes.ShowOffsetsStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.ShowAclsStatement) {
                aclExecutor.executeShowAcls((FlussStatementNodes.ShowAclsStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.CreateAclStatement) {
                aclExecutor.executeCreateAcl((FlussStatementNodes.CreateAclStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.DropAclStatement) {
                aclExecutor.executeDropAcl((FlussStatementNodes.DropAclStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.ShowClusterConfigsStatement) {
                clusterExecutor.executeShowClusterConfigs(
                        (FlussStatementNodes.ShowClusterConfigsStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.AlterClusterConfigsStatement) {
                clusterExecutor.executeAlterClusterConfigs(
                        (FlussStatementNodes.AlterClusterConfigsStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.RebalanceClusterStatement) {
                clusterExecutor.executeRebalance(
                        (FlussStatementNodes.RebalanceClusterStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.ShowRebalanceStatement) {
                clusterExecutor.executeShowRebalance(
                        (FlussStatementNodes.ShowRebalanceStatement) stmt);
            } else if (stmt instanceof FlussStatementNodes.CancelRebalanceStatement) {
                clusterExecutor.executeCancelRebalance(
                        (FlussStatementNodes.CancelRebalanceStatement) stmt);
            } else {
                out.println("Unsupported statement: " + rawType);
            }
            return;
        }

        List<SqlNode> statements = sqlParser.parse(sql);

        for (SqlNode stmt : statements) {
            CalciteSqlParser.SqlStatementType type = sqlParser.getStatementType(stmt);

            boolean isUpsert = sqlParser.isUpsertStatement(originalSql);
            if (isUpsert && type == CalciteSqlParser.SqlStatementType.INSERT) {
                type = CalciteSqlParser.SqlStatementType.INSERT;
            }

            switch (type) {
                case CREATE_DATABASE:
                    ddlExecutor.executeCreateDatabase(
                            (org.apache.flink.sql.parser.ddl.SqlCreateDatabase) stmt);
                    break;
                case CREATE_TABLE:
                    ddlExecutor.executeCreateTable(
                            (org.apache.flink.sql.parser.ddl.SqlCreateTable) stmt);
                    break;
                case DROP_TABLE:
                    ddlExecutor.executeDropTable(
                            (org.apache.flink.sql.parser.ddl.SqlDropTable) stmt);
                    break;
                case DROP_DATABASE:
                    ddlExecutor.executeDropDatabase(
                            (org.apache.flink.sql.parser.ddl.SqlDropDatabase) stmt);
                    break;
                case INSERT:
                    dmlExecutor.executeInsert(
                            (org.apache.flink.sql.parser.dml.RichSqlInsert) stmt, isUpsert);
                    break;
                case UPDATE:
                    dmlExecutor.executeUpdate((org.apache.calcite.sql.SqlUpdate) stmt);
                    break;
                case DELETE:
                    dmlExecutor.executeDelete((org.apache.calcite.sql.SqlDelete) stmt);
                    break;
                case SELECT:
                    SqlNode selectNode = stmt;
                    if (stmt instanceof org.apache.calcite.sql.SqlOrderBy) {
                        org.apache.calcite.sql.SqlOrderBy orderBy =
                                (org.apache.calcite.sql.SqlOrderBy) stmt;
                        selectNode = orderBy.query;
                    }
                    queryExecutor.executeSelect(
                            (org.apache.calcite.sql.SqlSelect) selectNode, stmt);
                    break;
                default:
                    System.err.println("Unsupported statement type: " + type);
            }
        }
    }

    private void executeAlterTable(String sql) throws Exception {
        String normalized = stripTrailingSemicolon(sql);
        String upper = normalized.toUpperCase();
        int keywordIndex = upper.indexOf("ALTER TABLE");
        String remaining = normalized.substring(keywordIndex + "ALTER TABLE".length()).trim();
        if (remaining.toUpperCase().startsWith("IF EXISTS ")) {
            remaining = remaining.substring("IF EXISTS".length()).trim();
        }
        String[] parts = splitFirstToken(remaining);
        if (parts[0].isEmpty() || parts[1].isEmpty()) {
            throw new IllegalArgumentException(
                    "ALTER TABLE requires table name and action. Examples: "
                            + "ALTER TABLE db.tbl ADD COLUMN c INT; "
                            + "ALTER TABLE db.tbl SET ('k'='v'). Got: "
                            + normalized);
        }

        TablePath tablePath = parseTablePath(parts[0]);
        String action = parts[1].trim();

        PartitionSpec partitionSpec = null;
        Matcher partitionMatcher = PARTITION_CLAUSE_PATTERN.matcher(action);
        if (partitionMatcher.find()) {
            partitionSpec = parsePartitionSpec(partitionMatcher.group(1));
        }

        String specContent = null;
        if (action.toUpperCase().contains("PARTITION")) {
            try {
                specContent = extractParenthesizedContent(action);
            } catch (IllegalArgumentException ignored) {
            }
        }

        ddlExecutor.executeAlterTable(sql, tablePath, action, partitionSpec, specContent);
    }

    private TablePath parseTablePath(String tableName) {
        String[] parts = tableName.split("\\.");
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Table name must be in format 'database.table': " + tableName);
        }
        return TablePath.of(parts[0], parts[1]);
    }

    private String stripTrailingSemicolon(String sql) {
        String trimmed = sql.trim();
        if (trimmed.endsWith(";")) {
            return trimmed.substring(0, trimmed.length() - 1).trim();
        }
        return trimmed;
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

    private PartitionSpec parsePartitionSpec(String content) {
        java.util.Map<String, String> specMap = parseKeyValueMap(content);
        if (specMap.isEmpty()) {
            throw new IllegalArgumentException("Partition spec cannot be empty");
        }
        return new PartitionSpec(specMap);
    }

    private java.util.Map<String, String> parseKeyValueMap(String content) {
        java.util.Map<String, String> map = new java.util.HashMap<>();
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

    private java.util.List<String> splitCommaSeparated(String input) {
        java.util.List<String> elements = new java.util.ArrayList<>();
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
