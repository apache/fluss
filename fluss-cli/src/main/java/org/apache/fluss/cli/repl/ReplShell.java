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

package org.apache.fluss.cli.repl;

import org.apache.fluss.cli.sql.SqlExecutor;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.IOException;

/** Interactive REPL shell for executing SQL commands. */
public class ReplShell {
    private final SqlExecutor executor;

    public ReplShell(SqlExecutor executor) {
        this.executor = executor;
    }

    public void run() throws IOException {
        Terminal terminal = TerminalBuilder.builder().system(true).build();

        LineReader reader = LineReaderBuilder.builder().terminal(terminal).build();

        System.out.println("Fluss SQL Shell");
        System.out.println("Type 'exit' or 'quit' to exit, '\\h' for help");
        System.out.println();

        StringBuilder multiLineBuffer = new StringBuilder();

        while (true) {
            try {
                String prompt = multiLineBuffer.length() == 0 ? "fluss> " : "    -> ";
                String line = reader.readLine(prompt);

                if (line == null
                        || line.trim().equalsIgnoreCase("exit")
                        || line.trim().equalsIgnoreCase("quit")) {
                    System.out.println("Goodbye!");
                    break;
                }

                if (line.trim().equals("\\h") || line.trim().equals("help")) {
                    printHelp();
                    continue;
                }

                if (line.trim().isEmpty()) {
                    continue;
                }

                multiLineBuffer.append(line).append(" ");

                if (line.trim().endsWith(";")) {
                    String sql = multiLineBuffer.toString();
                    multiLineBuffer.setLength(0);

                    try {
                        executor.executeSql(sql);
                    } catch (Exception e) {
                        System.err.println("Error executing SQL: " + e.getMessage());
                    }
                }
            } catch (UserInterruptException e) {
                multiLineBuffer.setLength(0);
                System.out.println("\nInterrupted. Type 'exit' to quit.");
            } catch (EndOfFileException e) {
                break;
            }
        }
    }

    private void printHelp() {
        System.out.println("Fluss SQL Shell Commands:");
        System.out.println("  \\h, help              - Show this help message");
        System.out.println("  exit, quit            - Exit the shell");
        System.out.println();
        System.out.println("SQL Commands:");
        System.out.println("  CREATE DATABASE <name>");
        System.out.println("  DROP DATABASE <name> [CASCADE]");
        System.out.println("  CREATE TABLE <name> ...");
        System.out.println("  DROP TABLE <name>");
        System.out.println("  SELECT * FROM <table>");
        System.out.println();
        System.out.println("End SQL statements with a semicolon (;)");
    }
}
