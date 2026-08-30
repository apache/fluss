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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DatabaseCommand} option parsing. */
class DatabaseCommandTest {

    @Test
    void testParseCreateOptions() throws Exception {
        DatabaseCommand.DatabaseCommandOptions opts =
                new DatabaseCommand.DatabaseCommandOptions(
                        new String[] {
                            "--bootstrap-server", "localhost:9123", "--create", "--database", "mydb"
                        });

        assertThat(opts.hasCreateOption()).isTrue();
        assertThat(opts.hasListOption()).isFalse();
        assertThat(opts.database()).isEqualTo("mydb");
    }

    @Test
    void testParseListOptions() throws Exception {
        DatabaseCommand.DatabaseCommandOptions opts =
                new DatabaseCommand.DatabaseCommandOptions(
                        new String[] {"--bootstrap-server", "localhost:9123", "--list"});

        assertThat(opts.hasListOption()).isTrue();
    }

    @Test
    void testParseDropWithCascadeAndIfExists() throws Exception {
        DatabaseCommand.DatabaseCommandOptions opts =
                new DatabaseCommand.DatabaseCommandOptions(
                        new String[] {
                            "--bootstrap-server",
                            "localhost:9123",
                            "--drop",
                            "--database",
                            "mydb",
                            "--cascade",
                            "--if-exists"
                        });

        assertThat(opts.hasDropOption()).isTrue();
        assertThat(opts.database()).isEqualTo("mydb");
        assertThat(opts.cascade()).isTrue();
        assertThat(opts.ifExists()).isTrue();
    }

    @Test
    void testMissingDatabaseForCreateThrows() throws Exception {
        DatabaseCommand.DatabaseCommandOptions opts =
                new DatabaseCommand.DatabaseCommandOptions(
                        new String[] {"--bootstrap-server", "localhost:9123", "--create"});

        assertThatThrownBy(opts::database)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--database is required");
    }

    @Test
    void testIfNotExistsOption() throws Exception {
        DatabaseCommand.DatabaseCommandOptions opts =
                new DatabaseCommand.DatabaseCommandOptions(
                        new String[] {
                            "--bootstrap-server",
                            "localhost:9123",
                            "--create",
                            "--database",
                            "mydb",
                            "--if-not-exists"
                        });

        assertThat(opts.ifNotExists()).isTrue();
    }

    @Test
    void testMainNoExitReturnsOneOnError() {
        // No args at all -> parse error -> exit code 1
        int exitCode = DatabaseCommand.mainNoExit(new String[] {});
        assertThat(exitCode).isEqualTo(1);
    }
}
