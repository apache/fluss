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

package org.apache.fluss.cli;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlussCliMain}. */
class FlussCliMainTest {

    @Test
    void testMainHelpOption() {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outStream));

        try {
            CommandLine cmd = new CommandLine(new FlussCliMain());
            int exitCode = cmd.execute("--help");

            assertThat(exitCode).isEqualTo(0);
            String output = outStream.toString();
            assertThat(output).contains("Fluss Command Line Interface");
            assertThat(output).contains("fluss-cli");
        } finally {
            System.setOut(originalOut);
        }
    }

    @Test
    void testMainVersionOption() {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outStream));

        try {
            CommandLine cmd = new CommandLine(new FlussCliMain());
            int exitCode = cmd.execute("--version");

            assertThat(exitCode).isEqualTo(0);
            String output = outStream.toString();
            assertThat(output).contains("Fluss CLI");
            assertThat(output).contains("0.9-SNAPSHOT");
        } finally {
            System.setOut(originalOut);
        }
    }

    @Test
    void testMainWithInvalidCommand() {
        ByteArrayOutputStream errStream = new ByteArrayOutputStream();
        PrintStream originalErr = System.err;
        System.setErr(new PrintStream(errStream));

        try {
            CommandLine cmd = new CommandLine(new FlussCliMain());
            int exitCode = cmd.execute("invalid-command");

            assertThat(exitCode).isNotEqualTo(0);
            String errorOutput = errStream.toString();
            assertThat(errorOutput).contains("invalid-command");
        } finally {
            System.setErr(originalErr);
        }
    }

    @Test
    void testMainWithSqlSubcommand() {
        ByteArrayOutputStream errStream = new ByteArrayOutputStream();
        PrintStream originalErr = System.err;
        System.setErr(new PrintStream(errStream));

        try {
            CommandLine cmd = new CommandLine(new FlussCliMain());
            int exitCode = cmd.execute("sql");

            assertThat(exitCode).isNotEqualTo(0);
            String errorOutput = errStream.toString();
            assertThat(errorOutput).contains("bootstrap-servers");
        } finally {
            System.setErr(originalErr);
        }
    }

    @Test
    void testMainWithHelpSubcommand() {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outStream));

        try {
            CommandLine cmd = new CommandLine(new FlussCliMain());
            int exitCode = cmd.execute("help");

            assertThat(exitCode).isEqualTo(0);
            String output = outStream.toString();
            assertThat(output).contains("fluss-cli");
        } finally {
            System.setOut(originalOut);
        }
    }

    @Test
    void testMainCommandLineExecution() {
        CommandLine cmd = new CommandLine(new FlussCliMain());

        assertThat(cmd.getCommandName()).isEqualTo("fluss-cli");
        assertThat(cmd.getSubcommands()).containsKey("sql");
        assertThat(cmd.getSubcommands()).containsKey("help");
    }

    @Test
    void testMainWithSqlHelpOption() {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outStream));

        try {
            CommandLine cmd = new CommandLine(new FlussCliMain());
            int exitCode = cmd.execute("help", "sql");

            assertThat(exitCode).isEqualTo(0);
            String output = outStream.toString();
            assertThat(output).contains("Execute SQL commands against Fluss cluster");
            assertThat(output).contains("--bootstrap-servers");
        } finally {
            System.setOut(originalOut);
        }
    }

    @Test
    void testMainMixinStandardHelpOptions() {
        CommandLine cmd = new CommandLine(new FlussCliMain());

        assertThat(cmd.getCommandSpec().mixinStandardHelpOptions()).isTrue();
    }

    @Test
    void testMainCommandDescription() {
        CommandLine cmd = new CommandLine(new FlussCliMain());
        String[] usageMessages = cmd.getCommandSpec().usageMessage().description();

        assertThat(usageMessages).contains("Fluss Command Line Interface");
    }
}
