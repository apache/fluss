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

import org.apache.fluss.cli.command.SqlCommand;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

/** Main entry point for the Fluss CLI tool. */
@Command(
        name = "fluss-cli",
        description = "Fluss Command Line Interface",
        mixinStandardHelpOptions = true,
        version = "Fluss CLI 0.9-SNAPSHOT",
        subcommands = {SqlCommand.class, HelpCommand.class})
public class FlussCliMain {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new FlussCliMain()).execute(args);
        System.exit(exitCode);
    }
}
