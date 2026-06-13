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

import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableCommand} option parsing. */
class TableCommandTest {

    @Test
    void testParseCreateOptions() throws Exception {
        TableCommand.TableCommandOptions opts =
                new TableCommand.TableCommandOptions(
                        new String[] {
                            "--bootstrap-server",
                            "localhost:9123",
                            "--create",
                            "--table",
                            "mydb.users",
                            "--schema",
                            "id INT, name STRING",
                            "--primary-key",
                            "id",
                            "--buckets",
                            "4"
                        });

        assertThat(opts.hasCreateOption()).isTrue();
        assertThat(opts.tablePath()).isEqualTo(TablePath.of("mydb", "users"));
        assertThat(opts.schema()).isEqualTo("id INT, name STRING");
        assertThat(opts.primaryKeys()).containsExactly("id");
        assertThat(opts.buckets()).isEqualTo(4);
    }

    @Test
    void testParseListOptions() throws Exception {
        TableCommand.TableCommandOptions opts =
                new TableCommand.TableCommandOptions(
                        new String[] {
                            "--bootstrap-server", "localhost:9123", "--list", "--database", "mydb"
                        });

        assertThat(opts.hasListOption()).isTrue();
        assertThat(opts.database()).isEqualTo("mydb");
    }

    @Test
    void testTablePathParsing() throws Exception {
        TableCommand.TableCommandOptions opts =
                new TableCommand.TableCommandOptions(
                        new String[] {
                            "--bootstrap-server",
                            "localhost:9123",
                            "--describe",
                            "--table",
                            "mydb.orders"
                        });

        TablePath path = opts.tablePath();
        assertThat(path.getDatabaseName()).isEqualTo("mydb");
        assertThat(path.getTableName()).isEqualTo("orders");
    }

    @Test
    void testTablePathInvalidFormatNoDot() throws Exception {
        TableCommand.TableCommandOptions opts =
                new TableCommand.TableCommandOptions(
                        new String[] {
                            "--bootstrap-server",
                            "localhost:9123",
                            "--describe",
                            "--table",
                            "notable"
                        });

        assertThatThrownBy(opts::tablePath)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid table path");
    }

    @Test
    void testTablePathInvalidEmptyParts() throws Exception {
        TableCommand.TableCommandOptions opts =
                new TableCommand.TableCommandOptions(
                        new String[] {
                            "--bootstrap-server",
                            "localhost:9123",
                            "--describe",
                            "--table",
                            ".table"
                        });

        assertThatThrownBy(opts::tablePath)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid table path");
    }

    @Test
    void testMissingTableThrows() throws Exception {
        TableCommand.TableCommandOptions opts =
                new TableCommand.TableCommandOptions(
                        new String[] {"--bootstrap-server", "localhost:9123", "--describe"});

        assertThatThrownBy(opts::tablePath)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--table is required");
    }

    @Test
    void testMissingSchemaForCreateThrows() throws Exception {
        TableCommand.TableCommandOptions opts =
                new TableCommand.TableCommandOptions(
                        new String[] {
                            "--bootstrap-server",
                            "localhost:9123",
                            "--create",
                            "--table",
                            "mydb.users"
                        });

        assertThatThrownBy(opts::schema)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--schema is required");
    }

    @Test
    void testPartitionByOption() throws Exception {
        TableCommand.TableCommandOptions opts =
                new TableCommand.TableCommandOptions(
                        new String[] {
                            "--bootstrap-server",
                            "localhost:9123",
                            "--create",
                            "--table",
                            "mydb.orders",
                            "--schema",
                            "id INT, dt STRING",
                            "--partition-by",
                            "dt"
                        });

        List<String> keys = opts.partitionKeys();
        assertThat(keys).containsExactly("dt");
    }

    @Test
    void testNoBucketsReturnsNull() throws Exception {
        TableCommand.TableCommandOptions opts =
                new TableCommand.TableCommandOptions(
                        new String[] {
                            "--bootstrap-server",
                            "localhost:9123",
                            "--create",
                            "--table",
                            "mydb.users",
                            "--schema",
                            "id INT"
                        });

        assertThat(opts.buckets()).isNull();
    }

    @Test
    void testMainNoExitReturnsOneOnError() {
        int exitCode = TableCommand.mainNoExit(new String[] {});
        assertThat(exitCode).isEqualTo(1);
    }
}
