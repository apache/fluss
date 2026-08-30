/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.cli;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** CLI tool for managing Fluss tables. */
@Internal
public class TableCommand {

    public static void main(String[] args) {
        System.exit(mainNoExit(args));
    }

    static int mainNoExit(String[] args) {
        try {
            execute(args);
            return 0;
        } catch (Throwable e) {
            System.err.println(
                    "Error while executing table command: "
                            + CommandUtils.unwrapExceptionMessage(e));
            return 1;
        }
    }

    static void execute(String[] args) throws Exception {
        TableCommandOptions opts = new TableCommandOptions(args);
        if (opts.hasHelpOption()) {
            CommandUtils.printUsageAndExit(opts.options(), "fluss-table.sh");
        }
        if (opts.hasVersionOption()) {
            CommandUtils.printVersionAndExit();
        }
        CommandUtils.validateActions(
                opts.commandLine(), opts.createOpt, opts.listOpt, opts.describeOpt, opts.dropOpt);

        try (TableService service = new TableService(opts)) {
            if (opts.hasCreateOption()) {
                service.createTable(opts);
            } else if (opts.hasListOption()) {
                service.listTables(opts);
            } else if (opts.hasDescribeOption()) {
                service.describeTable(opts);
            } else if (opts.hasDropOption()) {
                service.dropTable(opts);
            }
        }
    }

    /** Service class that wraps the Admin client for table operations. */
    static class TableService implements AutoCloseable {
        private final Admin admin;
        private final Connection connection;

        TableService(TableCommandOptions opts) {
            this.connection = CommandUtils.createConnection(opts);
            this.admin = connection.getAdmin();
        }

        void createTable(TableCommandOptions opts) throws Exception {
            TablePath tablePath = opts.tablePath();
            String schemaStr = opts.schema();
            List<String> primaryKeys = opts.primaryKeys();

            Schema schema = SchemaParser.parseSchema(schemaStr, primaryKeys);

            TableDescriptor.Builder builder = TableDescriptor.builder().schema(schema);

            List<String> partitionKeys = opts.partitionKeys();
            if (!partitionKeys.isEmpty()) {
                builder.partitionedBy(partitionKeys);
            }

            Integer buckets = opts.buckets();
            if (buckets != null) {
                builder.distributedBy(buckets);
            }

            Map<String, String> props =
                    CommandUtils.parseProperties(opts.commandLine().getOptionValues("property"));
            props.forEach(builder::property);

            admin.createTable(tablePath, builder.build(), opts.ifNotExists())
                    .get(CommandUtils.DEFAULT_TIMEOUT_SECS, TimeUnit.SECONDS);
            System.out.println("Created table \"" + tablePath + "\".");
        }

        void listTables(TableCommandOptions opts) throws Exception {
            String database = opts.database();
            List<String> tables =
                    admin.listTables(database)
                            .get(CommandUtils.DEFAULT_TIMEOUT_SECS, TimeUnit.SECONDS);
            tables.forEach(System.out::println);
        }

        void describeTable(TableCommandOptions opts) throws Exception {
            TablePath tablePath = opts.tablePath();
            TableInfo tableInfo =
                    admin.getTableInfo(tablePath)
                            .get(CommandUtils.DEFAULT_TIMEOUT_SECS, TimeUnit.SECONDS);

            Schema schema = tableInfo.getSchema();
            boolean hasPK = tableInfo.hasPrimaryKey();

            System.out.println("Table: " + tablePath);
            System.out.println("Type: " + (hasPK ? "PrimaryKey" : "Log"));

            System.out.println("Schema:");
            for (Schema.Column col : schema.getColumns()) {
                System.out.println("  " + col.getName() + " " + col.getDataType());
            }

            if (hasPK) {
                System.out.println("PrimaryKey: " + String.join(", ", tableInfo.getPrimaryKeys()));
            }

            List<String> partitionKeys = tableInfo.getPartitionKeys();
            if (!partitionKeys.isEmpty()) {
                System.out.println("PartitionKeys: " + String.join(", ", partitionKeys));
            }

            int numBuckets = tableInfo.getNumBuckets();
            if (numBuckets > 0) {
                System.out.println("Buckets: " + numBuckets);
            }

            List<String> bucketKeys = tableInfo.getBucketKeys();
            if (!bucketKeys.isEmpty()) {
                System.out.println("BucketKeys: " + String.join(", ", bucketKeys));
            }

            Optional<String> comment = tableInfo.getComment();
            if (comment.isPresent()) {
                System.out.println("Comment: " + comment.get());
            }

            Configuration props = tableInfo.getProperties();
            Configuration customProps = tableInfo.getCustomProperties();
            Map<String, String> propsMap = props.toMap();
            Map<String, String> customPropsMap = customProps.toMap();
            if (!propsMap.isEmpty() || !customPropsMap.isEmpty()) {
                System.out.println("Properties:");
                propsMap.forEach((k, v) -> System.out.println("  " + k + " = " + v));
                customPropsMap.forEach((k, v) -> System.out.println("  " + k + " = " + v));
            }

            printBucketDistribution(tablePath);
        }

        private void printBucketDistribution(TablePath tablePath) {
            MetadataUpdater metadataUpdater = ((FlussConnection) connection).getMetadataUpdater();
            metadataUpdater.updateTableOrPartitionMetadata(tablePath, null);
            Cluster cluster = metadataUpdater.getCluster();
            PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath);
            java.util.List<BucketLocation> bucketLocations =
                    cluster.getAvailableBucketsForPhysicalTablePath(physicalTablePath);
            if (bucketLocations == null || bucketLocations.isEmpty()) {
                return;
            }
            System.out.println("Bucket Distribution:");
            System.out.printf("  %-8s%-8s%-12s%s%n", "Bucket", "Leader", "Replicas", "Isr");
            for (BucketLocation loc : bucketLocations) {
                String leader = loc.getLeader() == null ? "none" : String.valueOf(loc.getLeader());
                System.out.printf(
                        "  %-8d%-8s%-12s%s%n",
                        loc.getBucketId(),
                        leader,
                        formatIntArray(loc.getReplicas()),
                        formatIntArray(loc.getIsr()));
            }
        }

        private static String formatIntArray(int[] arr) {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < arr.length; i++) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(arr[i]);
            }
            sb.append("]");
            return sb.toString();
        }

        void dropTable(TableCommandOptions opts) throws Exception {
            TablePath tablePath = opts.tablePath();
            admin.dropTable(tablePath, opts.ifExists())
                    .get(CommandUtils.DEFAULT_TIMEOUT_SECS, TimeUnit.SECONDS);
            System.out.println("Dropped table \"" + tablePath + "\".");
        }

        @Override
        public void close() throws Exception {
            connection.close();
        }
    }

    /** CLI option parser for table commands. */
    static class TableCommandOptions extends CommandDefaultOptions {
        final Option createOpt =
                Option.builder().longOpt("create").desc("Create a new table.").build();
        final Option listOpt =
                Option.builder().longOpt("list").desc("List all tables in a database.").build();
        final Option describeOpt =
                Option.builder().longOpt("describe").desc("Describe a table.").build();
        final Option dropOpt = Option.builder().longOpt("drop").desc("Drop a table.").build();
        final Option databaseOpt =
                Option.builder()
                        .longOpt("database")
                        .hasArg()
                        .argName("name")
                        .desc("The database name (for --list).")
                        .build();
        final Option tableOpt =
                Option.builder()
                        .longOpt("table")
                        .hasArg()
                        .argName("db.table")
                        .desc("The table path in format 'database.table'.")
                        .build();
        final Option schemaOpt =
                Option.builder()
                        .longOpt("schema")
                        .hasArg()
                        .argName("definition")
                        .desc("Column definitions: 'col1 INT, col2 STRING NOT NULL, ...'")
                        .build();
        final Option primaryKeyOpt =
                Option.builder()
                        .longOpt("primary-key")
                        .hasArg()
                        .argName("columns")
                        .desc("Primary key columns (comma-separated).")
                        .build();
        final Option partitionByOpt =
                Option.builder()
                        .longOpt("partition-by")
                        .hasArg()
                        .argName("columns")
                        .desc("Partition columns (comma-separated).")
                        .build();
        final Option bucketsOpt =
                Option.builder()
                        .longOpt("buckets")
                        .hasArg()
                        .argName("num")
                        .desc("Number of buckets.")
                        .build();
        final Option propertyOpt =
                Option.builder()
                        .longOpt("property")
                        .hasArg()
                        .argName("key=value")
                        .desc("Table property (repeatable).")
                        .build();
        final Option ifExistsOpt =
                Option.builder()
                        .longOpt("if-exists")
                        .desc("Only execute if the table exists.")
                        .build();
        final Option ifNotExistsOpt =
                Option.builder()
                        .longOpt("if-not-exists")
                        .desc("Only execute if the table does not exist.")
                        .build();

        TableCommandOptions(String[] args) throws ParseException {
            super();
            options.addOption(createOpt);
            options.addOption(listOpt);
            options.addOption(describeOpt);
            options.addOption(dropOpt);
            options.addOption(databaseOpt);
            options.addOption(tableOpt);
            options.addOption(schemaOpt);
            options.addOption(primaryKeyOpt);
            options.addOption(partitionByOpt);
            options.addOption(bucketsOpt);
            options.addOption(propertyOpt);
            options.addOption(ifExistsOpt);
            options.addOption(ifNotExistsOpt);
            parse(args);
        }

        boolean hasCreateOption() {
            return commandLine.hasOption(createOpt);
        }

        boolean hasListOption() {
            return commandLine.hasOption(listOpt);
        }

        boolean hasDescribeOption() {
            return commandLine.hasOption(describeOpt);
        }

        boolean hasDropOption() {
            return commandLine.hasOption(dropOpt);
        }

        TablePath tablePath() {
            String table = commandLine.getOptionValue(tableOpt);
            if (table == null) {
                throw new IllegalArgumentException("--table is required.");
            }
            String[] parts = table.split("\\.", 2);
            if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
                throw new IllegalArgumentException(
                        "Invalid table path: '" + table + "'. Expected format: 'database.table'.");
            }
            return TablePath.of(parts[0], parts[1]);
        }

        String database() {
            String db = commandLine.getOptionValue(databaseOpt);
            if (db == null) {
                throw new IllegalArgumentException("--database is required.");
            }
            return db;
        }

        String schema() {
            String s = commandLine.getOptionValue(schemaOpt);
            if (s == null) {
                throw new IllegalArgumentException("--schema is required for --create.");
            }
            return s;
        }

        List<String> primaryKeys() {
            String pk = commandLine.getOptionValue(primaryKeyOpt);
            if (pk == null) {
                return Collections.emptyList();
            }
            return Arrays.asList(trimElements(pk.split(",")));
        }

        List<String> partitionKeys() {
            String pb = commandLine.getOptionValue(partitionByOpt);
            if (pb == null) {
                return Collections.emptyList();
            }
            return Arrays.asList(trimElements(pb.split(",")));
        }

        private static String[] trimElements(String[] elements) {
            for (int i = 0; i < elements.length; i++) {
                elements[i] = elements[i].trim();
            }
            return elements;
        }

        Integer buckets() {
            String b = commandLine.getOptionValue(bucketsOpt);
            if (b == null) {
                return null;
            }
            return Integer.parseInt(b);
        }

        boolean ifExists() {
            return commandLine.hasOption(ifExistsOpt);
        }

        boolean ifNotExists() {
            return commandLine.hasOption(ifNotExistsOpt);
        }

        CommandLine commandLine() {
            return commandLine;
        }
    }
}
