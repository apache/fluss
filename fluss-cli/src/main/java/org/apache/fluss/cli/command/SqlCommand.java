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

package org.apache.fluss.cli.command;

import org.apache.fluss.cli.config.ConnectionConfig;
import org.apache.fluss.cli.config.ConnectionManager;
import org.apache.fluss.cli.format.OutputFormat;
import org.apache.fluss.cli.repl.ReplShell;
import org.apache.fluss.cli.sql.SqlExecutor;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.Callable;

/** Command for executing SQL statements. */
@Command(name = "sql", description = "Execute SQL commands against Fluss cluster")
public class SqlCommand implements Callable<Integer> {

    @Option(
            names = {"-b", "--bootstrap-servers"},
            description = "Fluss bootstrap servers (host:port,host:port)",
            required = true)
    private String bootstrapServers;

    @Option(
            names = {"-f", "--file"},
            description = "Execute SQL from file")
    private File sqlFile;

    @Option(
            names = {"-e", "--execute"},
            description = "Execute SQL statement directly")
    private String sqlStatement;

    @Option(
            names = {"-c", "--config"},
            description = "Configuration properties file")
    private File configFile;

    @Option(
            names = {"-o", "--output-format"},
            description = "Output format: table (default), csv, json, tsv",
            defaultValue = "table")
    private String outputFormat;

    @Option(
            names = {"-q", "--quiet"},
            description = "Quiet mode: suppress status messages (useful for piping output)")
    private boolean quiet;

    @Option(
            names = {"--streaming-timeout"},
            description = "Idle timeout in seconds for streaming queries (default: 30)",
            defaultValue = "30")
    private long streamingTimeoutSeconds;

    @Parameters(description = "SQL statements to execute", arity = "0..1")
    private String sqlFromArgs;

    @Override
    public Integer call() throws Exception {
        ConnectionConfig connectionConfig;

        if (configFile != null) {
            connectionConfig = new ConnectionConfig(configFile);
        } else {
            connectionConfig = new ConnectionConfig(bootstrapServers);
        }

        try (ConnectionManager connectionManager = new ConnectionManager(connectionConfig)) {
            PrintWriter out = new PrintWriter(System.out, true);
            OutputFormat format = OutputFormat.fromString(outputFormat);
            SqlExecutor executor =
                    new SqlExecutor(connectionManager, out, format, quiet, streamingTimeoutSeconds);

            if (sqlFile != null) {
                String sql =
                        new String(Files.readAllBytes(sqlFile.toPath()), StandardCharsets.UTF_8);
                executor.executeSql(sql);
            } else if (sqlStatement != null) {
                executor.executeSql(sqlStatement);
            } else if (sqlFromArgs != null) {
                executor.executeSql(sqlFromArgs);
            } else {
                ReplShell repl = new ReplShell(executor);
                repl.run();
            }

            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            return 1;
        }
    }
}
