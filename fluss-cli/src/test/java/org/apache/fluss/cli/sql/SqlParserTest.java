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

package org.apache.fluss.cli.sql;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.sql.parser.ddl.SqlCreateDatabase;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlDropDatabase;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SqlParserTest {

    private CalciteSqlParser parser;

    @BeforeEach
    void setUp() {
        parser = new CalciteSqlParser();
    }

    @Test
    void testParseCreateDatabase() throws Exception {
        String sql = "CREATE DATABASE mydb";
        List<SqlNode> statements = parser.parse(sql);

        assertThat(statements).hasSize(1);
        assertThat(statements.get(0)).isInstanceOf(SqlCreateDatabase.class);
        assertThat(parser.getStatementType(statements.get(0)))
                .isEqualTo(CalciteSqlParser.SqlStatementType.CREATE_DATABASE);
    }

    @Test
    void testParseCreateDatabaseIfNotExists() throws Exception {
        String sql = "CREATE DATABASE IF NOT EXISTS mydb";
        List<SqlNode> statements = parser.parse(sql);

        assertThat(statements).hasSize(1);
        assertThat(statements.get(0)).isInstanceOf(SqlCreateDatabase.class);
        assertThat(parser.getStatementType(statements.get(0)))
                .isEqualTo(CalciteSqlParser.SqlStatementType.CREATE_DATABASE);
    }

    @Test
    void testParseDropDatabase() throws Exception {
        String sql = "DROP DATABASE mydb";
        List<SqlNode> statements = parser.parse(sql);

        assertThat(statements).hasSize(1);
        assertThat(statements.get(0)).isInstanceOf(SqlDropDatabase.class);
        assertThat(parser.getStatementType(statements.get(0)))
                .isEqualTo(CalciteSqlParser.SqlStatementType.DROP_DATABASE);
    }

    @Test
    void testParseCreateTable() throws Exception {
        String sql =
                "CREATE TABLE mydb.users (id INT, name STRING, PRIMARY KEY (id)) WITH ('bucket.num' = '3')";
        List<SqlNode> statements = parser.parse(sql);

        assertThat(statements).hasSize(1);
        assertThat(statements.get(0)).isInstanceOf(SqlCreateTable.class);
        assertThat(parser.getStatementType(statements.get(0)))
                .isEqualTo(CalciteSqlParser.SqlStatementType.CREATE_TABLE);
    }

    @Test
    void testParseCreateTableIfNotExists() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS mydb.users (id INT, name STRING)";
        List<SqlNode> statements = parser.parse(sql);

        assertThat(statements).hasSize(1);
        assertThat(statements.get(0)).isInstanceOf(SqlCreateTable.class);
        assertThat(parser.getStatementType(statements.get(0)))
                .isEqualTo(CalciteSqlParser.SqlStatementType.CREATE_TABLE);
    }

    @Test
    void testParseDropTable() throws Exception {
        String sql = "DROP TABLE mydb.users";
        List<SqlNode> statements = parser.parse(sql);

        assertThat(statements).hasSize(1);
        assertThat(statements.get(0)).isInstanceOf(SqlDropTable.class);
        assertThat(parser.getStatementType(statements.get(0)))
                .isEqualTo(CalciteSqlParser.SqlStatementType.DROP_TABLE);
    }

    @Test
    void testParseInsert() throws Exception {
        String sql = "INSERT INTO mydb.users VALUES (1, 'Alice')";
        List<SqlNode> statements = parser.parse(sql);

        assertThat(statements).hasSize(1);
        assertThat(statements.get(0)).isInstanceOf(RichSqlInsert.class);
        assertThat(parser.getStatementType(statements.get(0)))
                .isEqualTo(CalciteSqlParser.SqlStatementType.INSERT);
    }

    @Test
    void testParseUpdate() throws Exception {
        String sql = "UPDATE mydb.users SET name = 'Bob' WHERE id = 1";
        List<SqlNode> statements = parser.parse(sql);

        assertThat(statements).hasSize(1);
        assertThat(statements.get(0)).isInstanceOf(SqlUpdate.class);
        assertThat(parser.getStatementType(statements.get(0)))
                .isEqualTo(CalciteSqlParser.SqlStatementType.UPDATE);
    }

    @Test
    void testParseDelete() throws Exception {
        String sql = "DELETE FROM mydb.users WHERE id = 1";
        List<SqlNode> statements = parser.parse(sql);

        assertThat(statements).hasSize(1);
        assertThat(statements.get(0)).isInstanceOf(org.apache.calcite.sql.SqlDelete.class);
        assertThat(parser.getStatementType(statements.get(0)))
                .isEqualTo(CalciteSqlParser.SqlStatementType.DELETE);
    }

    @Test
    void testParseSelect() throws Exception {
        String sql = "SELECT * FROM mydb.users";
        List<SqlNode> statements = parser.parse(sql);

        assertThat(statements).hasSize(1);
        assertThat(statements.get(0)).isInstanceOf(SqlSelect.class);
        assertThat(parser.getStatementType(statements.get(0)))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SELECT);
    }

    @Test
    void testIsUpsertStatementWithUpsert() {
        assertThat(parser.isUpsertStatement("UPSERT INTO mydb.users VALUES (1, 'Alice')")).isTrue();
        assertThat(parser.isUpsertStatement("upsert into mydb.users VALUES (1, 'Alice')")).isTrue();
        assertThat(parser.isUpsertStatement("  UPSERT INTO mydb.users VALUES (1, 'Alice')"))
                .isTrue();
    }

    @Test
    void testIsUpsertStatementWithInsert() {
        assertThat(parser.isUpsertStatement("INSERT INTO mydb.users VALUES (1, 'Alice')"))
                .isFalse();
        assertThat(parser.isUpsertStatement("insert into mydb.users VALUES (1, 'Alice')"))
                .isFalse();
    }

    @Test
    void testParseShowStatementsReturnEmpty() throws Exception {
        assertThat(parser.parse("SHOW DATABASES")).isEmpty();
        assertThat(parser.parse("DESCRIBE mydb.users")).isEmpty();
    }

    @Test
    void testClassifyShowStatements() {
        assertThat(parser.classifyRawStatement("SHOW DATABASES"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_DATABASES);
        assertThat(parser.classifyRawStatement("SHOW TABLES FROM mydb"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_TABLES);
        assertThat(parser.classifyRawStatement("SHOW CREATE TABLE mydb.users"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_CREATE_TABLE);
        assertThat(parser.classifyRawStatement("DESCRIBE mydb.users"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.DESCRIBE_TABLE);
    }

    @Test
    void testParseInvalidSql() {
        String sql = "INVALID SQL STATEMENT";
        assertThatThrownBy(() -> parser.parse(sql)).isInstanceOf(SqlParseException.class);
    }

    @Test
    void testParseMultipleStatementsUnsupported() {
        String sql = "CREATE DATABASE mydb; CREATE TABLE mydb.users (id INT)";
        assertThatThrownBy(() -> parser.parse(sql)).isInstanceOf(SqlParseException.class);
    }
}
