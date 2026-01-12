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

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SqlCommand}. */
class SqlCommandTest {

    @Test
    void testSqlCommandConstruction() {
        SqlCommand sqlCommand = new SqlCommand();
        assertThat(sqlCommand).isNotNull();
    }

    @Test
    void testSqlCommandImplementsCallable() {
        SqlCommand sqlCommand = new SqlCommand();
        assertThat(sqlCommand).isInstanceOf(java.util.concurrent.Callable.class);
    }

    @Test
    void testSqlCommandHasBootstrapServersOption() throws Exception {
        SqlCommand sqlCommand = new SqlCommand();
        Field bootstrapServersField = SqlCommand.class.getDeclaredField("bootstrapServers");
        bootstrapServersField.setAccessible(true);

        assertThat(bootstrapServersField).isNotNull();
        assertThat(bootstrapServersField.getType()).isEqualTo(String.class);
    }

    @Test
    void testSqlCommandHasSqlFileOption() throws Exception {
        SqlCommand sqlCommand = new SqlCommand();
        Field sqlFileField = SqlCommand.class.getDeclaredField("sqlFile");
        sqlFileField.setAccessible(true);

        assertThat(sqlFileField).isNotNull();
        assertThat(sqlFileField.getType()).isEqualTo(java.io.File.class);
    }

    @Test
    void testSqlCommandHasSqlStatementOption() throws Exception {
        SqlCommand sqlCommand = new SqlCommand();
        Field sqlStatementField = SqlCommand.class.getDeclaredField("sqlStatement");
        sqlStatementField.setAccessible(true);

        assertThat(sqlStatementField).isNotNull();
        assertThat(sqlStatementField.getType()).isEqualTo(String.class);
    }

    @Test
    void testSqlCommandHasConfigFileOption() throws Exception {
        SqlCommand sqlCommand = new SqlCommand();
        Field configFileField = SqlCommand.class.getDeclaredField("configFile");
        configFileField.setAccessible(true);

        assertThat(configFileField).isNotNull();
        assertThat(configFileField.getType()).isEqualTo(java.io.File.class);
    }

    @Test
    void testSqlCommandHasSqlFromArgsParameter() throws Exception {
        SqlCommand sqlCommand = new SqlCommand();
        Field sqlFromArgsField = SqlCommand.class.getDeclaredField("sqlFromArgs");
        sqlFromArgsField.setAccessible(true);

        assertThat(sqlFromArgsField).isNotNull();
        assertThat(sqlFromArgsField.getType()).isEqualTo(String.class);
    }

    @Test
    void testCommandLineCreationWithSqlCommand() {
        SqlCommand sqlCommand = new SqlCommand();
        CommandLine cmd = new CommandLine(sqlCommand);

        assertThat((Object) cmd).isNotNull();
        assertThat((Object) cmd.getCommand()).isSameAs(sqlCommand);
    }

    @Test
    void testSqlCommandAnnotations() {
        SqlCommand sqlCommand = new SqlCommand();
        CommandLine.Command commandAnnotation =
                sqlCommand.getClass().getAnnotation(CommandLine.Command.class);

        assertThat(commandAnnotation).isNotNull();
        assertThat(commandAnnotation.name()).isEqualTo("sql");
        assertThat(commandAnnotation.description()[0]).contains("Execute SQL commands");
    }

    @Test
    void testBootstrapServersOptionAnnotation() throws Exception {
        Field bootstrapServersField = SqlCommand.class.getDeclaredField("bootstrapServers");
        CommandLine.Option optionAnnotation =
                bootstrapServersField.getAnnotation(CommandLine.Option.class);

        assertThat(optionAnnotation).isNotNull();
        assertThat(optionAnnotation.names()).contains("-b", "--bootstrap-servers");
        assertThat(optionAnnotation.required()).isTrue();
    }

    @Test
    void testSqlFileOptionAnnotation() throws Exception {
        Field sqlFileField = SqlCommand.class.getDeclaredField("sqlFile");
        CommandLine.Option optionAnnotation = sqlFileField.getAnnotation(CommandLine.Option.class);

        assertThat(optionAnnotation).isNotNull();
        assertThat(optionAnnotation.names()).contains("-f", "--file");
    }

    @Test
    void testSqlStatementOptionAnnotation() throws Exception {
        Field sqlStatementField = SqlCommand.class.getDeclaredField("sqlStatement");
        CommandLine.Option optionAnnotation =
                sqlStatementField.getAnnotation(CommandLine.Option.class);

        assertThat(optionAnnotation).isNotNull();
        assertThat(optionAnnotation.names()).contains("-e", "--execute");
    }

    @Test
    void testConfigFileOptionAnnotation() throws Exception {
        Field configFileField = SqlCommand.class.getDeclaredField("configFile");
        CommandLine.Option optionAnnotation =
                configFileField.getAnnotation(CommandLine.Option.class);

        assertThat(optionAnnotation).isNotNull();
        assertThat(optionAnnotation.names()).contains("-c", "--config");
    }

    @Test
    void testSqlFromArgsParameterAnnotation() throws Exception {
        Field sqlFromArgsField = SqlCommand.class.getDeclaredField("sqlFromArgs");
        CommandLine.Parameters parametersAnnotation =
                sqlFromArgsField.getAnnotation(CommandLine.Parameters.class);

        assertThat(parametersAnnotation).isNotNull();
        assertThat(parametersAnnotation.arity()).isEqualTo("0..1");
    }
}
