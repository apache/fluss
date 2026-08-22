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
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseInfo;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** CLI tool for managing Fluss databases. */
@Internal
public class DatabaseCommand {

    public static void main(String[] args) {
        System.exit(mainNoExit(args));
    }

    static int mainNoExit(String[] args) {
        try {
            execute(args);
            return 0;
        } catch (Throwable e) {
            System.err.println(
                    "Error while executing database command: "
                            + CommandUtils.unwrapExceptionMessage(e));
            return 1;
        }
    }

    static void execute(String[] args) throws Exception {
        DatabaseCommandOptions opts = new DatabaseCommandOptions(args);
        if (opts.hasHelpOption()) {
            CommandUtils.printUsageAndExit(opts.options(), "fluss-database.sh");
        }
        if (opts.hasVersionOption()) {
            CommandUtils.printVersionAndExit();
        }
        CommandUtils.validateActions(
                opts.commandLine(), opts.createOpt, opts.listOpt, opts.describeOpt, opts.dropOpt);

        try (DatabaseService service = new DatabaseService(opts)) {
            if (opts.hasCreateOption()) {
                service.createDatabase(opts);
            } else if (opts.hasListOption()) {
                service.listDatabases();
            } else if (opts.hasDescribeOption()) {
                service.describeDatabase(opts);
            } else if (opts.hasDropOption()) {
                service.dropDatabase(opts);
            }
        }
    }

    /** Service class that wraps the Admin client for database operations. */
    static class DatabaseService implements AutoCloseable {
        private final Admin admin;
        private final Connection connection;

        DatabaseService(DatabaseCommandOptions opts) {
            this.connection = CommandUtils.createConnection(opts);
            this.admin = connection.getAdmin();
        }

        void createDatabase(DatabaseCommandOptions opts) throws Exception {
            String dbName = opts.database();
            Map<String, String> props =
                    CommandUtils.parseProperties(opts.commandLine().getOptionValues("property"));
            DatabaseDescriptor.Builder builder = DatabaseDescriptor.builder();
            props.forEach(builder::customProperty);
            admin.createDatabase(dbName, builder.build(), opts.ifNotExists())
                    .get(CommandUtils.DEFAULT_TIMEOUT_SECS, TimeUnit.SECONDS);
            System.out.println("Created database \"" + dbName + "\".");
        }

        void listDatabases() throws Exception {
            List<String> databases =
                    admin.listDatabases().get(CommandUtils.DEFAULT_TIMEOUT_SECS, TimeUnit.SECONDS);
            databases.forEach(System.out::println);
        }

        void describeDatabase(DatabaseCommandOptions opts) throws Exception {
            String dbName = opts.database();
            DatabaseInfo info =
                    admin.getDatabaseInfo(dbName)
                            .get(CommandUtils.DEFAULT_TIMEOUT_SECS, TimeUnit.SECONDS);
            System.out.println("Database: " + dbName);
            info.getDatabaseDescriptor()
                    .getComment()
                    .ifPresent(c -> System.out.println("Comment: " + c));
            Map<String, String> props = info.getDatabaseDescriptor().getCustomProperties();
            System.out.println("Properties:");
            if (props.isEmpty()) {
                System.out.println("  (none)");
            } else {
                props.forEach((k, v) -> System.out.println("  " + k + " = " + v));
            }
        }

        void dropDatabase(DatabaseCommandOptions opts) throws Exception {
            String dbName = opts.database();
            admin.dropDatabase(dbName, opts.ifExists(), opts.cascade())
                    .get(CommandUtils.DEFAULT_TIMEOUT_SECS, TimeUnit.SECONDS);
            System.out.println("Dropped database \"" + dbName + "\".");
        }

        @Override
        public void close() throws Exception {
            connection.close();
        }
    }

    /** CLI option parser for database commands. */
    static class DatabaseCommandOptions extends CommandDefaultOptions {
        final Option createOpt =
                Option.builder().longOpt("create").desc("Create a new database.").build();
        final Option listOpt = Option.builder().longOpt("list").desc("List all databases.").build();
        final Option describeOpt =
                Option.builder().longOpt("describe").desc("Describe a database.").build();
        final Option dropOpt = Option.builder().longOpt("drop").desc("Drop a database.").build();
        final Option databaseOpt =
                Option.builder()
                        .longOpt("database")
                        .hasArg()
                        .argName("name")
                        .desc("The database name.")
                        .build();
        final Option ifExistsOpt =
                Option.builder()
                        .longOpt("if-exists")
                        .desc("Only execute if the database exists.")
                        .build();
        final Option ifNotExistsOpt =
                Option.builder()
                        .longOpt("if-not-exists")
                        .desc("Only execute if the database does not exist.")
                        .build();
        final Option cascadeOpt =
                Option.builder()
                        .longOpt("cascade")
                        .desc("Drop all tables in the database (for --drop).")
                        .build();
        final Option propertyOpt =
                Option.builder()
                        .longOpt("property")
                        .hasArg()
                        .argName("key=value")
                        .desc("Database property (repeatable).")
                        .build();

        DatabaseCommandOptions(String[] args) throws ParseException {
            super();
            options.addOption(createOpt);
            options.addOption(listOpt);
            options.addOption(describeOpt);
            options.addOption(dropOpt);
            options.addOption(databaseOpt);
            options.addOption(ifExistsOpt);
            options.addOption(ifNotExistsOpt);
            options.addOption(cascadeOpt);
            options.addOption(propertyOpt);
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

        String database() {
            String db = commandLine.getOptionValue(databaseOpt);
            if (db == null) {
                throw new IllegalArgumentException("--database is required.");
            }
            return db;
        }

        boolean ifExists() {
            return commandLine.hasOption(ifExistsOpt);
        }

        boolean ifNotExists() {
            return commandLine.hasOption(ifNotExistsOpt);
        }

        boolean cascade() {
            return commandLine.hasOption(cascadeOpt);
        }

        CommandLine commandLine() {
            return commandLine;
        }
    }
}
