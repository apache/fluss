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

import org.apache.fluss.cli.exception.SqlParseException;
import org.apache.fluss.cli.sql.ast.FlussStatement;
import org.apache.fluss.cli.sql.ast.FlussStatementNodes;
import org.apache.fluss.cli.util.SqlParserUtil;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TablePath;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for Fluss-specific SQL statements (SHOW, DESCRIBE, ACL, CLUSTER commands).
 *
 * <p>This parser converts SQL strings into type-safe AST nodes defined in {@link
 * FlussStatementNodes}. It consolidates all string parsing logic that was previously scattered
 * across executor classes.
 */
public class FlussStatementParser {
    // Regex patterns for metadata commands
    private static final Pattern SHOW_TABLES_PATTERN =
            Pattern.compile(
                    "SHOW\\s+TABLES(?:\\s+(?:FROM|IN)\\s+(\\S+))?", Pattern.CASE_INSENSITIVE);
    private static final Pattern SHOW_DATABASE_EXISTS_PATTERN =
            Pattern.compile("SHOW\\s+DATABASE\\s+EXISTS\\s+(\\S+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern SHOW_DATABASE_INFO_PATTERN =
            Pattern.compile("SHOW\\s+DATABASE\\s+INFO\\s+(\\S+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern SHOW_TABLE_EXISTS_PATTERN =
            Pattern.compile("SHOW\\s+TABLE\\s+EXISTS\\s+(\\S+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern SHOW_TABLE_SCHEMA_PATTERN =
            Pattern.compile(
                    "SHOW\\s+TABLE\\s+SCHEMA\\s+(\\S+)(?:\\s+ID\\s+(\\d+))?",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern USE_DATABASE_PATTERN =
            Pattern.compile("USE\\s+(\\S+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern DESCRIBE_TABLE_PATTERN =
            Pattern.compile("(?:DESCRIBE|DESC)\\s+(\\S+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern SHOW_CREATE_TABLE_PATTERN =
            Pattern.compile("SHOW\\s+CREATE\\s+TABLE\\s+(\\S+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern SHOW_PARTITIONS_PATTERN =
            Pattern.compile(
                    "SHOW\\s+PARTITIONS\\s+(?:FROM|IN)\\s+(\\S+)", Pattern.CASE_INSENSITIVE);

    // Regex patterns for snapshot commands
    private static final Pattern SHOW_KV_SNAPSHOTS_PATTERN =
            Pattern.compile(
                    "SHOW\\s+KV\\s+SNAPSHOTS\\s+FROM\\s+(\\S+)(.*)", Pattern.CASE_INSENSITIVE);
    private static final Pattern SHOW_KV_SNAPSHOT_METADATA_PATTERN =
            Pattern.compile(
                    "SHOW\\s+KV\\s+SNAPSHOT\\s+METADATA\\s+FROM\\s+(\\S+)\\s+BUCKET\\s+(\\d+)\\s+SNAPSHOT\\s+(\\d+)",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern SHOW_LAKE_SNAPSHOT_PATTERN =
            Pattern.compile("SHOW\\s+LAKE\\s+SNAPSHOT\\s+FROM\\s+(\\S+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern SHOW_OFFSETS_PATTERN =
            Pattern.compile("SHOW\\s+OFFSETS\\s+FROM\\s+(\\S+)(.*)", Pattern.CASE_INSENSITIVE);

    // Regex patterns for ACL commands
    private static final Pattern SHOW_ACLS_PATTERN =
            Pattern.compile("SHOW\\s+ACLS(?:\\s+FOR\\s+(\\S+))?", Pattern.CASE_INSENSITIVE);
    private static final Pattern CREATE_ACL_PATTERN =
            Pattern.compile(
                    "CREATE\\s+ACL\\s+FOR\\s+(\\S+)\\s+TO\\s+(\\S+)\\s+WITH\\s+PERMISSION\\s+(\\S+)",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern DROP_ACL_PATTERN =
            Pattern.compile(
                    "DROP\\s+ACL\\s+FOR\\s+(\\S+)\\s+FROM\\s+(\\S+)\\s+WITH\\s+PERMISSION\\s+(\\S+)",
                    Pattern.CASE_INSENSITIVE);

    // Regex patterns for cluster commands
    private static final Pattern ALTER_CLUSTER_CONFIGS_PATTERN =
            Pattern.compile("ALTER\\s+CLUSTER\\s+SET\\s*\\(([^)]*)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern REBALANCE_CLUSTER_PATTERN =
            Pattern.compile("REBALANCE\\s+CLUSTER(?:\\s+FOR\\s+(\\S+))?", Pattern.CASE_INSENSITIVE);
    private static final Pattern SHOW_REBALANCE_PATTERN =
            Pattern.compile("SHOW\\s+REBALANCE(?:\\s+FOR\\s+(\\S+))?", Pattern.CASE_INSENSITIVE);
    private static final Pattern CANCEL_REBALANCE_PATTERN =
            Pattern.compile("CANCEL\\s+REBALANCE(?:\\s+FOR\\s+(\\S+))?", Pattern.CASE_INSENSITIVE);

    // Regex patterns for clause extraction
    private static final Pattern PARTITION_CLAUSE_PATTERN =
            Pattern.compile("PARTITION\\s*\\(([^)]*)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern BUCKETS_CLAUSE_PATTERN =
            Pattern.compile("BUCKETS\\s*\\(([^)]*)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TIMESTAMP_SPEC_PATTERN =
            Pattern.compile(
                    "AT\\s+TIMESTAMP\\s+(?:'([^']*)'|\"([^\"]*)\"|(\\d+))",
                    Pattern.CASE_INSENSITIVE);

    /** Main entry point: parse SQL string into a Fluss-specific AST node. */
    public FlussStatement parse(String sql) throws SqlParseException {
        String normalized = stripTrailingSemicolon(sql);
        String upper = normalized.toUpperCase();

        try {
            // Metadata commands
            if (upper.startsWith("SHOW DATABASES")) {
                return parseShowDatabases(normalized);
            } else if (upper.startsWith("SHOW DATABASE EXISTS")) {
                return parseShowDatabaseExists(normalized);
            } else if (upper.startsWith("SHOW DATABASE INFO")
                    || upper.startsWith("SHOW DATABASE ")) {
                return parseShowDatabaseInfo(normalized);
            } else if (upper.startsWith("SHOW TABLES")) {
                return parseShowTables(normalized);
            } else if (upper.startsWith("SHOW TABLE EXISTS")) {
                return parseShowTableExists(normalized);
            } else if (upper.startsWith("SHOW TABLE SCHEMA")) {
                return parseShowTableSchema(normalized);
            } else if (upper.startsWith("SHOW SERVERS")) {
                return parseShowServers(normalized);
            } else if (upper.startsWith("USE ")) {
                return parseUseDatabase(normalized);
            } else if (upper.startsWith("DESCRIBE ") || upper.startsWith("DESC ")) {
                return parseDescribeTable(normalized);
            } else if (upper.startsWith("SHOW CREATE TABLE")) {
                return parseShowCreateTable(normalized);
            } else if (upper.startsWith("SHOW PARTITIONS")) {
                return parseShowPartitions(normalized);
            }
            // Snapshot commands
            else if (upper.startsWith("SHOW KV SNAPSHOT METADATA")) {
                return parseShowKvSnapshotMetadata(normalized);
            } else if (upper.startsWith("SHOW KV SNAPSHOTS")) {
                return parseShowKvSnapshots(normalized);
            } else if (upper.startsWith("SHOW LAKE SNAPSHOT")) {
                return parseShowLakeSnapshot(normalized);
            } else if (upper.startsWith("SHOW OFFSETS")) {
                return parseShowOffsets(normalized);
            }
            // ACL commands
            else if (upper.startsWith("SHOW ACLS")) {
                return parseShowAcls(normalized);
            } else if (upper.startsWith("CREATE ACL")) {
                return parseCreateAcl(normalized);
            } else if (upper.startsWith("DROP ACL")) {
                return parseDropAcl(normalized);
            }
            // Cluster commands
            else if (upper.startsWith("SHOW CLUSTER CONFIGS")) {
                return parseShowClusterConfigs(normalized);
            } else if (upper.startsWith("ALTER CLUSTER")) {
                return parseAlterClusterConfigs(normalized);
            } else if (upper.startsWith("REBALANCE CLUSTER")) {
                return parseRebalanceCluster(normalized);
            } else if (upper.startsWith("SHOW REBALANCE")) {
                return parseShowRebalance(normalized);
            } else if (upper.startsWith("CANCEL REBALANCE")) {
                return parseCancelRebalance(normalized);
            } else if (upper.startsWith("ALTER TABLE")) {
                return parseAlterTable(normalized);
            } else {
                throw new SqlParseException("Unknown Fluss statement type. SQL: " + sql);
            }
        } catch (IllegalArgumentException e) {
            throw new SqlParseException("Failed to parse SQL: " + sql, e);
        }
    }

    // ================================================================================
    // Metadata Command Parsers
    // ================================================================================

    private FlussStatement parseShowDatabases(String sql) {
        return new FlussStatementNodes.ShowDatabasesStatement(sql);
    }

    private FlussStatement parseShowDatabaseExists(String sql) {
        Matcher matcher = SHOW_DATABASE_EXISTS_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid SHOW DATABASE EXISTS syntax. Expected: SHOW DATABASE EXISTS <database>. Got: "
                            + sql);
        }
        String databaseName = matcher.group(1);
        return new FlussStatementNodes.ShowDatabaseExistsStatement(sql, databaseName);
    }

    private FlussStatement parseShowDatabaseInfo(String sql) {
        Matcher matcher = SHOW_DATABASE_INFO_PATTERN.matcher(sql);
        if (!matcher.find()) {
            String upper = sql.toUpperCase();
            if (upper.startsWith("SHOW DATABASE ") && !upper.contains(" INFO ")) {
                String remaining = sql.substring("SHOW DATABASE ".length()).trim();
                String databaseName = SqlParserUtil.stripTrailingSemicolon(remaining).trim();
                if (databaseName.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Invalid SHOW DATABASE syntax. Expected: SHOW DATABASE <database> or SHOW DATABASE INFO <database>. Got: "
                                    + sql);
                }
                return new FlussStatementNodes.ShowDatabaseInfoStatement(sql, databaseName);
            }
            throw new IllegalArgumentException(
                    "Invalid SHOW DATABASE INFO syntax. Expected: SHOW DATABASE INFO <database>. Got: "
                            + sql);
        }
        String databaseName = matcher.group(1);
        return new FlussStatementNodes.ShowDatabaseInfoStatement(sql, databaseName);
    }

    private FlussStatement parseShowTables(String sql) {
        Matcher matcher = SHOW_TABLES_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid SHOW TABLES syntax. Expected: SHOW TABLES [FROM/IN <database>]. Got: "
                            + sql);
        }
        String databaseName = matcher.group(1);
        return new FlussStatementNodes.ShowTablesStatement(sql, Optional.ofNullable(databaseName));
    }

    private FlussStatement parseShowTableExists(String sql) {
        Matcher matcher = SHOW_TABLE_EXISTS_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid SHOW TABLE EXISTS syntax. Expected: SHOW TABLE EXISTS <table>. Got: "
                            + sql);
        }
        String tableName = matcher.group(1);
        TablePath tablePath = parseTablePath(tableName);
        return new FlussStatementNodes.ShowTableExistsStatement(sql, tablePath);
    }

    private FlussStatement parseShowTableSchema(String sql) {
        Matcher matcher = SHOW_TABLE_SCHEMA_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid SHOW TABLE SCHEMA syntax. Expected: SHOW TABLE SCHEMA <table> [ID <number>]. Got: "
                            + sql);
        }
        String tableName = matcher.group(1);
        String schemaIdStr = matcher.group(2);
        TablePath tablePath = parseTablePath(tableName);
        Integer schemaId = schemaIdStr != null ? Integer.parseInt(schemaIdStr) : null;
        return new FlussStatementNodes.ShowTableSchemaStatement(sql, tablePath, schemaId);
    }

    private FlussStatement parseShowServers(String sql) {
        return new FlussStatementNodes.ShowServersStatement(sql);
    }

    private FlussStatement parseUseDatabase(String sql) {
        Matcher matcher = USE_DATABASE_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid USE syntax. Expected: USE <database>. Got: " + sql);
        }
        String databaseName = matcher.group(1);
        return new FlussStatementNodes.UseDatabaseStatement(sql, databaseName);
    }

    private FlussStatement parseDescribeTable(String sql) {
        Matcher matcher = DESCRIBE_TABLE_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid DESCRIBE syntax. Expected: DESCRIBE <table> or DESC <table>. Got: "
                            + sql);
        }
        String tableName = matcher.group(1);
        TablePath tablePath = parseTablePath(tableName);
        return new FlussStatementNodes.DescribeTableStatement(sql, tablePath);
    }

    private FlussStatement parseShowCreateTable(String sql) {
        Matcher matcher = SHOW_CREATE_TABLE_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid SHOW CREATE TABLE syntax. Expected: SHOW CREATE TABLE <table>. Got: "
                            + sql);
        }
        String tableName = matcher.group(1);
        TablePath tablePath = parseTablePath(tableName);
        return new FlussStatementNodes.ShowCreateTableStatement(sql, tablePath);
    }

    private FlussStatement parseShowPartitions(String sql) {
        Matcher matcher = SHOW_PARTITIONS_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid SHOW PARTITIONS syntax. Expected: SHOW PARTITIONS FROM/IN <table>. Got: "
                            + sql);
        }
        String tableName = matcher.group(1);
        TablePath tablePath = parseTablePath(tableName);
        return new FlussStatementNodes.ShowPartitionsStatement(sql, tablePath, null);
    }

    // ================================================================================
    // Snapshot Command Parsers
    // ================================================================================

    private FlussStatement parseShowKvSnapshots(String sql) {
        Matcher matcher = SHOW_KV_SNAPSHOTS_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid SHOW KV SNAPSHOTS syntax. Expected: SHOW KV SNAPSHOTS FROM <table> [PARTITION (...)]. Got: "
                            + sql);
        }
        String tableName = matcher.group(1);
        String tail = matcher.group(2);

        TablePath tablePath = parseTablePath(tableName);
        Optional<PartitionSpec> partitionSpec = extractPartitionSpec(tail);

        return new FlussStatementNodes.ShowKvSnapshotsStatement(sql, tablePath, partitionSpec);
    }

    private FlussStatement parseShowKvSnapshotMetadata(String sql) {
        Matcher matcher = SHOW_KV_SNAPSHOT_METADATA_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid SHOW KV SNAPSHOT METADATA syntax. Expected: SHOW KV SNAPSHOT METADATA FROM <table> BUCKET <id> SNAPSHOT <id>. Got: "
                            + sql);
        }
        String tableName = matcher.group(1);
        int bucketId = Integer.parseInt(matcher.group(2));
        long snapshotId = Long.parseLong(matcher.group(3));

        TablePath tablePath = parseTablePath(tableName);
        return new FlussStatementNodes.ShowKvSnapshotMetadataStatement(
                sql, tablePath, bucketId, snapshotId);
    }

    private FlussStatement parseShowLakeSnapshot(String sql) {
        Matcher matcher = SHOW_LAKE_SNAPSHOT_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid SHOW LAKE SNAPSHOT syntax. Expected: SHOW LAKE SNAPSHOT FROM <table>. Got: "
                            + sql);
        }
        String tableName = matcher.group(1);
        TablePath tablePath = parseTablePath(tableName);
        return new FlussStatementNodes.ShowLakeSnapshotStatement(sql, tablePath);
    }

    private FlussStatement parseShowOffsets(String sql) {
        Matcher matcher = SHOW_OFFSETS_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid SHOW OFFSETS syntax. Expected: SHOW OFFSETS FROM <table> [PARTITION (...)] BUCKETS (...) [AT ...]. Got: "
                            + sql);
        }
        String tableName = matcher.group(1);
        String tail = matcher.group(2);

        TablePath tablePath = parseTablePath(tableName);
        Optional<PartitionSpec> partitionSpec = extractPartitionSpec(tail);

        // BUCKETS clause is mandatory
        Matcher bucketsMatcher = BUCKETS_CLAUSE_PATTERN.matcher(tail);
        if (!bucketsMatcher.find()) {
            throw new IllegalArgumentException("SHOW OFFSETS requires BUCKETS clause: " + sql);
        }
        List<Integer> buckets = parseBucketList(bucketsMatcher.group(1));

        // Offset spec (optional, defaults to LATEST)
        OffsetSpec offsetSpec = parseOffsetSpec(tail);

        return new FlussStatementNodes.ShowOffsetsStatement(
                sql, tablePath, partitionSpec, buckets, offsetSpec);
    }

    // ================================================================================
    // ACL Command Parsers
    // ================================================================================

    private FlussStatement parseShowAcls(String sql) {
        Matcher matcher = SHOW_ACLS_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid SHOW ACLS syntax. Expected: SHOW ACLS [FOR <resource>]. Got: " + sql);
        }
        String resource = matcher.group(1);
        return new FlussStatementNodes.ShowAclsStatement(sql, Optional.ofNullable(resource));
    }

    private FlussStatement parseCreateAcl(String sql) {
        Matcher matcher = CREATE_ACL_PATTERN.matcher(sql);
        if (matcher.find()) {
            String resource = matcher.group(1);
            String principal = matcher.group(2);
            String permission = matcher.group(3);
            return new FlussStatementNodes.CreateAclStatement(sql, resource, principal, permission);
        }
        if (sql.toUpperCase().contains("CREATE ACL")) {
            return new FlussStatementNodes.CreateAclStatement(sql, "", "", "");
        }
        throw new IllegalArgumentException(
                "Invalid CREATE ACL syntax. Expected: CREATE ACL FOR <resource> TO <principal> WITH PERMISSION <permission> or CREATE ACL (properties...). Got: "
                        + sql);
    }

    private FlussStatement parseDropAcl(String sql) {
        Matcher matcher = DROP_ACL_PATTERN.matcher(sql);
        if (matcher.find()) {
            String resource = matcher.group(1);
            String principal = matcher.group(2);
            String permission = matcher.group(3);
            return new FlussStatementNodes.DropAclStatement(sql, resource, principal, permission);
        }
        if (sql.toUpperCase().contains("DROP ACL")) {
            return new FlussStatementNodes.DropAclStatement(sql, "", "", "");
        }
        throw new IllegalArgumentException(
                "Invalid DROP ACL syntax. Expected: DROP ACL FOR <resource> FROM <principal> WITH PERMISSION <permission> or DROP ACL FILTER (properties...). Got: "
                        + sql);
    }

    // ================================================================================
    // Cluster Command Parsers
    // ================================================================================

    private FlussStatement parseShowClusterConfigs(String sql) {
        return new FlussStatementNodes.ShowClusterConfigsStatement(sql);
    }

    private FlussStatement parseAlterClusterConfigs(String sql) {
        Matcher matcher = ALTER_CLUSTER_CONFIGS_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid ALTER CLUSTER syntax. Expected: ALTER CLUSTER SET ('key'='value', ...). Got: "
                            + sql);
        }
        String configContent = matcher.group(1);
        Map<String, String> configs = parseKeyValueMap(configContent);
        return new FlussStatementNodes.AlterClusterConfigsStatement(sql, configs);
    }

    private FlussStatement parseRebalanceCluster(String sql) {
        Matcher matcher = REBALANCE_CLUSTER_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid REBALANCE CLUSTER syntax. Expected: REBALANCE CLUSTER [FOR <database>]. Got: "
                            + sql);
        }
        String databaseName = matcher.group(1);
        return new FlussStatementNodes.RebalanceClusterStatement(
                sql, Optional.ofNullable(databaseName));
    }

    private FlussStatement parseShowRebalance(String sql) {
        Matcher matcher = SHOW_REBALANCE_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid SHOW REBALANCE syntax. Expected: SHOW REBALANCE [FOR <database>]. Got: "
                            + sql);
        }
        String databaseName = matcher.group(1);
        return new FlussStatementNodes.ShowRebalanceStatement(
                sql, Optional.ofNullable(databaseName));
    }

    private FlussStatement parseCancelRebalance(String sql) {
        Matcher matcher = CANCEL_REBALANCE_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid CANCEL REBALANCE syntax. Expected: CANCEL REBALANCE [FOR <database>]. Got: "
                            + sql);
        }
        String databaseName = matcher.group(1);
        return new FlussStatementNodes.CancelRebalanceStatement(
                sql, Optional.ofNullable(databaseName));
    }

    // ================================================================================
    // ALTER TABLE Parser (Special Case)
    // ================================================================================

    private FlussStatement parseAlterTable(String sql) {
        String normalized = stripTrailingSemicolon(sql);
        String upper = normalized.toUpperCase();
        int keywordIndex = upper.indexOf("ALTER TABLE");
        String remaining = normalized.substring(keywordIndex + "ALTER TABLE".length()).trim();

        // Handle IF EXISTS
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

        Optional<PartitionSpec> partitionSpec = extractPartitionSpec(action);

        String specContent = null;
        if (action.toUpperCase().contains("PARTITION")) {
            try {
                specContent = extractParenthesizedContent(action);
            } catch (IllegalArgumentException ignored) {
                // No parenthesized content
            }
        }

        return new FlussStatementNodes.AlterTableStatement(
                sql, tablePath, action, partitionSpec, Optional.ofNullable(specContent));
    }

    // ================================================================================
    // Helper Methods: Parsing Utilities
    // ================================================================================

    private TablePath parseTablePath(String tableName) {
        String[] parts = tableName.split("\\.");
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Table name must be in format 'database.table': " + tableName);
        }
        return TablePath.of(parts[0], parts[1]);
    }

    private Optional<PartitionSpec> extractPartitionSpec(String text) {
        Matcher matcher = PARTITION_CLAUSE_PATTERN.matcher(text);
        if (matcher.find()) {
            String content = matcher.group(1);
            Map<String, String> specMap = parseKeyValueMap(content);
            if (specMap.isEmpty()) {
                throw new IllegalArgumentException("Partition spec cannot be empty");
            }
            return Optional.of(new PartitionSpec(specMap));
        }
        return Optional.empty();
    }

    private List<Integer> parseBucketList(String content) {
        List<Integer> buckets = new ArrayList<>();
        for (String entry : splitCommaSeparated(content)) {
            buckets.add(Integer.parseInt(entry.trim()));
        }
        return buckets;
    }

    private OffsetSpec parseOffsetSpec(String sqlTail) {
        Matcher timestampMatcher = TIMESTAMP_SPEC_PATTERN.matcher(sqlTail);
        if (timestampMatcher.find()) {
            String quoted =
                    timestampMatcher.group(1) != null
                            ? timestampMatcher.group(1)
                            : timestampMatcher.group(2);
            if (quoted != null) {
                return new OffsetSpec.TimestampSpec(parseTimestampLiteral(quoted));
            }
            long timestamp = Long.parseLong(timestampMatcher.group(3));
            return new OffsetSpec.TimestampSpec(timestamp);
        }
        String upper = sqlTail.toUpperCase();
        if (upper.contains("AT EARLIEST")) {
            return new OffsetSpec.EarliestSpec();
        }
        if (upper.contains("AT LATEST")) {
            return new OffsetSpec.LatestSpec();
        }
        return new OffsetSpec.LatestSpec();
    }

    private long parseTimestampLiteral(String literal) {
        String trimmed = literal.trim();
        try {
            return Instant.parse(trimmed).toEpochMilli();
        } catch (DateTimeParseException ignored) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime localDateTime = LocalDateTime.parse(trimmed, formatter);
            return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        }
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
}
