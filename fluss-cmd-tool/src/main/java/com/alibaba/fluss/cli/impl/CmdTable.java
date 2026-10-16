/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.cli.impl;

import com.alibaba.fluss.cli.CmdBase;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/** Table command. */
@Command(name = "table", description = "Table commands, e.g. list tables, get table")
public class CmdTable extends CmdBase {

    @Override
    public Integer execute() {
        return 0;
    }

    /** List table command. */
    @Command(name = "list", description = "list all tables.")
    public static class CmdListTable extends CmdBase {

        @Option(
                names = {"-f", "--format"},
                required = true)
        private String format = "json";

        @Override
        public Integer execute() {
            System.out.println("format: " + format);
            return 0;
        }
    }

    /** Get table cmd. */
    @Command(name = "get", description = "Get details of a table")
    public static class CmdGetTable extends CmdBase {
        @Parameters(index = "0", description = "Name of the table")
        private String tableName;

        @Override
        public Integer call() throws Exception {
            System.out.printf("Details of table '%s'...\n", tableName);
            super.call();
            return 0;
        }

        @Override
        public Integer execute() {
            return 0;
        }
    }
}
