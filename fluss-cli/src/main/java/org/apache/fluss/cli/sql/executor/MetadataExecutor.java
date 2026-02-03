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
import org.apache.fluss.cli.sql.ast.FlussStatementNodes;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.KvSnapshotMetadata;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.exception.KvSnapshotNotExistException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.metadata.DatabaseInfo;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Executor for metadata query operations (SHOW/DESCRIBE commands). */
public class MetadataExecutor {
    private static final Pattern TIMESTAMP_SPEC_PATTERN =
            Pattern.compile(
                    "AT\\s+TIMESTAMP\\s+(?:'([^']*)'|\"([^\"]*)\"|(\\d+))",
                    Pattern.CASE_INSENSITIVE);

    private final ConnectionManager connectionManager;
    private final PrintWriter out;
    private final OutputFormat outputFormat;
    private String currentDatabase;

    public MetadataExecutor(ConnectionManager connectionManager, PrintWriter out) {
        this(connectionManager, out, OutputFormat.TABLE);
    }

    public MetadataExecutor(
            ConnectionManager connectionManager, PrintWriter out, OutputFormat outputFormat) {
        this.connectionManager = connectionManager;
        this.out = out;
        this.outputFormat = outputFormat;
    }

    public void setCurrentDatabase(String currentDatabase) {
        this.currentDatabase = currentDatabase;
    }

    public String getCurrentDatabase() {
        return currentDatabase;
    }

    public void executeShowDatabases(FlussStatementNodes.ShowDatabasesStatement stmt)
            throws Exception {
        Admin admin = connectionManager.getConnection().getAdmin();
        java.util.List<String> databases = admin.listDatabases().get();

        out.println("Databases:");
        out.println("-----------");
        for (String db : databases) {
            out.println(db);
        }
        out.println();
        out.println(databases.size() + " database(s)");
        out.flush();
    }

    public void executeShowDatabaseExists(FlussStatementNodes.ShowDatabaseExistsStatement stmt)
            throws Exception {
        String databaseName = stmt.getDatabaseName();
        Admin admin = connectionManager.getConnection().getAdmin();
        boolean exists = admin.databaseExists(databaseName).get();
        out.println(exists);
        out.flush();
    }

    public void executeShowDatabaseInfo(FlussStatementNodes.ShowDatabaseInfoStatement stmt)
            throws Exception {
        String databaseName = stmt.getDatabaseName();
        Admin admin = connectionManager.getConnection().getAdmin();
        DatabaseInfo databaseInfo = admin.getDatabaseInfo(databaseName).get();

        out.println("Database: " + databaseInfo.getDatabaseName());
        databaseInfo
                .getDatabaseDescriptor()
                .getComment()
                .ifPresent(comment -> out.println("Comment: " + comment));

        out.println("Properties:");
        if (databaseInfo.getDatabaseDescriptor().getCustomProperties().isEmpty()) {
            out.println("  (none)");
        } else {
            for (java.util.Map.Entry<String, String> entry :
                    databaseInfo.getDatabaseDescriptor().getCustomProperties().entrySet()) {
                out.println("  " + entry.getKey() + "=" + entry.getValue());
            }
        }

        out.println("Created: " + java.time.Instant.ofEpochMilli(databaseInfo.getCreatedTime()));
        out.println("Modified: " + java.time.Instant.ofEpochMilli(databaseInfo.getModifiedTime()));
        out.flush();
    }

    public void executeShowServers(FlussStatementNodes.ShowServersStatement stmt) throws Exception {
        Admin admin = connectionManager.getConnection().getAdmin();
        java.util.List<ServerNode> servers = admin.getServerNodes().get();

        out.println("Server Nodes:");
        out.println("ID\tHost\tPort\tType");
        out.println("--\t----\t----\t----");
        for (ServerNode server : servers) {
            out.printf(
                    "%d\t%s\t%d\t%s%n",
                    server.id(), server.host(), server.port(), server.serverType());
        }
        out.flush();
    }

    public void executeUseDatabase(FlussStatementNodes.UseDatabaseStatement stmt) throws Exception {
        String databaseName = stmt.getDatabaseName();
        Admin admin = connectionManager.getConnection().getAdmin();
        if (!admin.databaseExists(databaseName).get()) {
            throw new IllegalArgumentException("Database does not exist: " + databaseName);
        }
        currentDatabase = databaseName;
        out.println("Using database: " + databaseName);
        out.flush();
    }

    public void executeShowTables(FlussStatementNodes.ShowTablesStatement stmt) throws Exception {
        Admin admin = connectionManager.getConnection().getAdmin();
        String databaseName = stmt.getDatabaseName().orElse(null);
        if (databaseName == null) {
            if (currentDatabase != null) {
                databaseName = currentDatabase;
            } else {
                throw new IllegalArgumentException(
                        "SHOW TABLES requires database specification. Use: SHOW TABLES FROM database,"
                                + " SHOW TABLES IN database, or USING database");
            }
        }

        java.util.List<String> tables = admin.listTables(databaseName).get();

        out.println("Tables in database '" + databaseName + "':");
        out.println("----------------------------------");
        for (String table : tables) {
            out.println(table);
        }
        out.println();
        out.println(tables.size() + " table(s)");
        out.flush();
    }

    public void executeShowTableExists(FlussStatementNodes.ShowTableExistsStatement stmt)
            throws Exception {
        TablePath tablePath = stmt.getTablePath();
        Admin admin = connectionManager.getConnection().getAdmin();
        boolean exists = admin.tableExists(tablePath).get();
        out.println(exists);
        out.flush();
    }

    public void executeShowTableSchema(FlussStatementNodes.ShowTableSchemaStatement stmt)
            throws Exception {
        TablePath tablePath = stmt.getTablePath();
        Integer schemaId = stmt.getSchemaId().orElse(null);
        Admin admin = connectionManager.getConnection().getAdmin();
        SchemaInfo schemaInfo =
                schemaId == null
                        ? admin.getTableSchema(tablePath).get()
                        : admin.getTableSchema(tablePath, schemaId).get();

        printSchemaInfo(tablePath, schemaInfo.getSchema(), schemaInfo.getSchemaId());
    }

    public void executeDescribeTable(FlussStatementNodes.DescribeTableStatement stmt)
            throws Exception {
        TablePath tablePath = stmt.getTablePath();

        Admin admin = connectionManager.getConnection().getAdmin();
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        org.apache.fluss.metadata.Schema schema = tableInfo.getSchema();

        out.println("Table: " + tablePath);
        out.println(
                "Type: "
                        + (schema.getPrimaryKey().isPresent() ? "Primary Key Table" : "Log Table"));
        out.println(repeat("=", 60));
        out.println();

        out.println("Columns:");
        out.println(String.format("%-30s %-20s %-10s", "Column Name", "Data Type", "Nullable"));
        out.println(repeat("-", 60));

        org.apache.fluss.types.RowType rowType = schema.getRowType();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            String columnName = rowType.getFieldNames().get(i);
            org.apache.fluss.types.DataType dataType = rowType.getTypeAt(i);
            boolean nullable = dataType.isNullable();
            out.println(
                    String.format(
                            "%-30s %-20s %-10s",
                            columnName, dataType.getTypeRoot(), nullable ? "YES" : "NO"));
        }

        out.println();
        if (schema.getPrimaryKey().isPresent()) {
            java.util.List<String> pkColumns = schema.getPrimaryKeyColumnNames();
            out.println("Primary Key: " + String.join(", ", pkColumns));
        } else {
            out.println("Primary Key: None (Log Table)");
        }

        out.println();
        out.println("Table Properties:");
        out.println("  Buckets: " + tableInfo.getNumBuckets());
        out.println("  Table ID: " + tableInfo.getTableId());
        out.println("  Schema ID: " + tableInfo.getSchemaId());
        out.flush();
    }

    public void executeShowCreateTable(FlussStatementNodes.ShowCreateTableStatement stmt)
            throws Exception {
        TablePath tablePath = stmt.getTablePath();

        Admin admin = connectionManager.getConnection().getAdmin();
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        org.apache.fluss.metadata.Schema schema = tableInfo.getSchema();

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(tablePath).append(" (\n");

        org.apache.fluss.types.RowType rowType = schema.getRowType();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (i > 0) {
                ddl.append(",\n");
            }
            String columnName = rowType.getFieldNames().get(i);
            org.apache.fluss.types.DataType dataType = rowType.getTypeAt(i);
            ddl.append("  ").append(columnName).append(" ").append(dataType.getTypeRoot());
        }

        if (schema.getPrimaryKey().isPresent()) {
            java.util.List<String> pkColumns = schema.getPrimaryKeyColumnNames();
            ddl.append(",\n  PRIMARY KEY (").append(String.join(", ", pkColumns)).append(")");
        }

        ddl.append("\n);");

        out.println(ddl.toString());
        out.flush();
    }

    public void executeShowPartitions(FlussStatementNodes.ShowPartitionsStatement stmt)
            throws Exception {
        TablePath tablePath = stmt.getTablePath();
        Admin admin = connectionManager.getConnection().getAdmin();

        PartitionSpec partitionSpec = stmt.getPartitionSpec().orElse(null);

        if (partitionSpec != null) {
            validatePartitionSpec(tablePath, partitionSpec, false);
        }

        List<PartitionInfo> partitions =
                partitionSpec == null
                        ? admin.listPartitionInfos(tablePath).get()
                        : admin.listPartitionInfos(tablePath, partitionSpec).get();

        out.println("Partitions in table '" + tablePath + "':");
        if (partitions.isEmpty()) {
            out.println("  (none)");
            out.flush();
            return;
        }

        out.println("Partition\tID");
        for (PartitionInfo partition : partitions) {
            out.println(partition.getPartitionName() + "\t" + partition.getPartitionId());
        }
        out.flush();
    }

    public void executeShowKvSnapshots(FlussStatementNodes.ShowKvSnapshotsStatement stmt)
            throws Exception {
        TablePath tablePath = stmt.getTablePath();
        PartitionSpec partitionSpec = stmt.getPartitionSpec().orElse(null);

        Admin admin = connectionManager.getConnection().getAdmin();
        KvSnapshots snapshots;
        if (partitionSpec == null) {
            snapshots = admin.getLatestKvSnapshots(tablePath).get();
        } else {
            validatePartitionSpec(tablePath, partitionSpec, true);
            String partitionName = resolvePartitionName(tablePath, partitionSpec);
            snapshots = admin.getLatestKvSnapshots(tablePath, partitionName).get();
        }

        out.println(
                "KV Snapshots for "
                        + tablePath
                        + (partitionSpec == null ? "" : " partition " + partitionSpec));
        out.println("Bucket\tSnapshotId\tLogOffset");

        List<Integer> bucketIds = new ArrayList<>(snapshots.getBucketIds());
        Collections.sort(bucketIds);
        for (Integer bucketId : bucketIds) {
            String snapshotText =
                    snapshots.getSnapshotId(bucketId).isPresent()
                            ? String.valueOf(snapshots.getSnapshotId(bucketId).getAsLong())
                            : "NONE";
            String logOffsetText =
                    snapshots.getLogOffset(bucketId).isPresent()
                            ? String.valueOf(snapshots.getLogOffset(bucketId).getAsLong())
                            : "EARLIEST";
            out.println(bucketId + "\t" + snapshotText + "\t" + logOffsetText);
        }
        out.flush();
    }

    public void executeShowKvSnapshotMetadata(
            FlussStatementNodes.ShowKvSnapshotMetadataStatement stmt) throws Exception {
        TablePath tablePath = stmt.getTablePath();
        int bucketId = stmt.getBucketId();
        long snapshotId = stmt.getSnapshotId();

        Admin admin = connectionManager.getConnection().getAdmin();
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), bucketId);
        KvSnapshotMetadata metadata;
        try {
            metadata = admin.getKvSnapshotMetadata(tableBucket, snapshotId).get();
        } catch (ExecutionException exception) {
            if (exception.getCause() instanceof KvSnapshotNotExistException) {
                out.println(
                        "KV Snapshot Metadata for "
                                + tablePath
                                + " bucket "
                                + bucketId
                                + " snapshot "
                                + snapshotId
                                + ": NONE");
                out.println("Reason: snapshot id not found for this bucket.");
                out.println("Hint: run SHOW KV SNAPSHOTS to see available snapshot IDs.");
                out.flush();
                return;
            }
            throw exception;
        }

        out.println(
                "KV Snapshot Metadata for "
                        + tablePath
                        + " bucket "
                        + bucketId
                        + " snapshot "
                        + snapshotId);
        out.println(metadata.toString());
        out.flush();
    }

    public void executeShowLakeSnapshot(FlussStatementNodes.ShowLakeSnapshotStatement stmt)
            throws Exception {
        TablePath tablePath = stmt.getTablePath();
        Admin admin = connectionManager.getConnection().getAdmin();
        LakeSnapshot snapshot;
        try {
            snapshot = admin.getLatestLakeSnapshot(tablePath).get();
        } catch (ExecutionException exception) {
            if (exception.getCause() instanceof LakeTableSnapshotNotExistException) {
                out.println("Lake Snapshot for " + tablePath + ": NONE");
                out.println("Reason: no lake snapshot has been committed yet.");
                out.println(
                        "Hint: enable lake tiering and wait for the first snapshot to complete.");
                out.flush();
                return;
            }
            throw exception;
        }

        out.println("Lake Snapshot for " + tablePath + ": " + snapshot.getSnapshotId());
        for (Map.Entry<TableBucket, Long> entry : snapshot.getTableBucketsOffset().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.flush();
    }

    public void executeShowOffsets(FlussStatementNodes.ShowOffsetsStatement stmt) throws Exception {
        TablePath tablePath = stmt.getTablePath();
        PartitionSpec partitionSpec = stmt.getPartitionSpec().orElse(null);
        List<Integer> buckets = stmt.getBuckets();
        OffsetSpec offsetSpec = stmt.getOffsetSpec();

        Admin admin = connectionManager.getConnection().getAdmin();
        ListOffsetsResult offsetsResult;
        if (partitionSpec == null) {
            offsetsResult = admin.listOffsets(tablePath, buckets, offsetSpec);
        } else {
            validatePartitionSpec(tablePath, partitionSpec, true);
            String partitionName = resolvePartitionName(tablePath, partitionSpec);
            offsetsResult = admin.listOffsets(tablePath, partitionName, buckets, offsetSpec);
        }

        Map<Integer, Long> offsets = offsetsResult.all().get();
        out.println(
                "Offsets for "
                        + tablePath
                        + (partitionSpec == null ? "" : " partition " + partitionSpec));
        out.println("Bucket\tOffset");
        for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
            out.println(entry.getKey() + "\t" + entry.getValue());
        }
        out.flush();
    }

    // Helper methods

    private void printSchemaInfo(
            TablePath tablePath, org.apache.fluss.metadata.Schema schema, int schemaId) {
        out.println("Table: " + tablePath);
        out.println("Schema ID: " + schemaId);
        out.println(
                "Type: "
                        + (schema.getPrimaryKey().isPresent() ? "Primary Key Table" : "Log Table"));
        out.println(repeat("=", 60));
        out.println();

        out.println("Columns:");
        out.println(String.format("%-30s %-20s %-10s", "Column Name", "Data Type", "Nullable"));
        out.println(repeat("-", 60));

        org.apache.fluss.types.RowType rowType = schema.getRowType();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            String columnName = rowType.getFieldNames().get(i);
            org.apache.fluss.types.DataType dataType = rowType.getTypeAt(i);
            boolean nullable = dataType.isNullable();
            out.println(
                    String.format(
                            "%-30s %-20s %-10s",
                            columnName, dataType.getTypeRoot(), nullable ? "YES" : "NO"));
        }

        out.println();
        if (schema.getPrimaryKey().isPresent()) {
            java.util.List<String> pkColumns = schema.getPrimaryKeyColumnNames();
            out.println("Primary Key: " + String.join(", ", pkColumns));
        } else {
            out.println("Primary Key: None (Log Table)");
        }
        out.println();
        out.flush();
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
            return java.time.Instant.parse(trimmed).toEpochMilli();
        } catch (java.time.format.DateTimeParseException ignored) {
            java.time.format.DateTimeFormatter formatter =
                    java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            java.time.LocalDateTime localDateTime =
                    java.time.LocalDateTime.parse(trimmed, formatter);
            return localDateTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
        }
    }

    private String resolvePartitionName(TablePath tablePath, PartitionSpec partitionSpec)
            throws Exception {
        Admin admin = connectionManager.getConnection().getAdmin();
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        org.apache.fluss.metadata.ResolvedPartitionSpec resolvedSpec =
                org.apache.fluss.metadata.ResolvedPartitionSpec.fromPartitionSpec(
                        tableInfo.getPartitionKeys(), partitionSpec);
        return resolvedSpec.getPartitionName();
    }

    private static String repeat(String str, int times) {
        StringBuilder sb = new StringBuilder(str.length() * times);
        for (int i = 0; i < times; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}
