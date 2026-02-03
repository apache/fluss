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

package org.apache.fluss.cli.util;

import org.apache.fluss.cli.sql.CalciteSqlParser;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for WHERE clause filtering functionality. */
class WhereClauseFilteringTest {

    private RowType createTestRowType() {
        return new RowType(
                Arrays.asList(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()),
                        new DataField("age", DataTypes.INT())));
    }

    private SqlNode extractWhereClause(String sql) throws Exception {
        CalciteSqlParser parser = new CalciteSqlParser();
        List<SqlNode> statements = parser.parse(sql);
        assertThat(statements).hasSize(1);

        SqlSelect select = (SqlSelect) statements.get(0);
        return select.getWhere();
    }

    @Test
    void testGreaterThan() throws Exception {
        RowType rowType = createTestRowType();
        SqlNode where = extractWhereClause("SELECT * FROM t WHERE age > 25");

        GenericRow row1 = new GenericRow(3);
        row1.setField(0, 1);
        row1.setField(1, BinaryString.fromString("Alice"));
        row1.setField(2, 30);

        GenericRow row2 = new GenericRow(3);
        row2.setField(0, 2);
        row2.setField(1, BinaryString.fromString("Bob"));
        row2.setField(2, 20);

        assertThat(WhereClauseEvaluator.evaluateWhere(where, row1, rowType)).isTrue();
        assertThat(WhereClauseEvaluator.evaluateWhere(where, row2, rowType)).isFalse();
    }

    @Test
    void testGreaterThanOrEqual() throws Exception {
        RowType rowType = createTestRowType();
        SqlNode where = extractWhereClause("SELECT * FROM t WHERE age >= 25");

        GenericRow row1 = new GenericRow(3);
        row1.setField(0, 1);
        row1.setField(1, BinaryString.fromString("Alice"));
        row1.setField(2, 25);

        GenericRow row2 = new GenericRow(3);
        row2.setField(0, 2);
        row2.setField(1, BinaryString.fromString("Bob"));
        row2.setField(2, 24);

        assertThat(WhereClauseEvaluator.evaluateWhere(where, row1, rowType)).isTrue();
        assertThat(WhereClauseEvaluator.evaluateWhere(where, row2, rowType)).isFalse();
    }

    @Test
    void testLessThan() throws Exception {
        RowType rowType = createTestRowType();
        SqlNode where = extractWhereClause("SELECT * FROM t WHERE age < 25");

        GenericRow row1 = new GenericRow(3);
        row1.setField(0, 1);
        row1.setField(1, BinaryString.fromString("Alice"));
        row1.setField(2, 20);

        GenericRow row2 = new GenericRow(3);
        row2.setField(0, 2);
        row2.setField(1, BinaryString.fromString("Bob"));
        row2.setField(2, 30);

        assertThat(WhereClauseEvaluator.evaluateWhere(where, row1, rowType)).isTrue();
        assertThat(WhereClauseEvaluator.evaluateWhere(where, row2, rowType)).isFalse();
    }

    @Test
    void testLessThanOrEqual() throws Exception {
        RowType rowType = createTestRowType();
        SqlNode where = extractWhereClause("SELECT * FROM t WHERE age <= 25");

        GenericRow row1 = new GenericRow(3);
        row1.setField(0, 1);
        row1.setField(1, BinaryString.fromString("Alice"));
        row1.setField(2, 25);

        GenericRow row2 = new GenericRow(3);
        row2.setField(0, 2);
        row2.setField(1, BinaryString.fromString("Bob"));
        row2.setField(2, 26);

        assertThat(WhereClauseEvaluator.evaluateWhere(where, row1, rowType)).isTrue();
        assertThat(WhereClauseEvaluator.evaluateWhere(where, row2, rowType)).isFalse();
    }

    @Test
    void testNotEquals() throws Exception {
        RowType rowType = createTestRowType();
        SqlNode where = extractWhereClause("SELECT * FROM t WHERE age <> 25");

        GenericRow row1 = new GenericRow(3);
        row1.setField(0, 1);
        row1.setField(1, BinaryString.fromString("Alice"));
        row1.setField(2, 30);

        GenericRow row2 = new GenericRow(3);
        row2.setField(0, 2);
        row2.setField(1, BinaryString.fromString("Bob"));
        row2.setField(2, 25);

        assertThat(WhereClauseEvaluator.evaluateWhere(where, row1, rowType)).isTrue();
        assertThat(WhereClauseEvaluator.evaluateWhere(where, row2, rowType)).isFalse();
    }

    @Test
    void testOrCondition() throws Exception {
        RowType rowType = createTestRowType();
        SqlNode where = extractWhereClause("SELECT * FROM t WHERE age < 25 OR age > 30");

        GenericRow row1 = new GenericRow(3);
        row1.setField(0, 1);
        row1.setField(1, BinaryString.fromString("Alice"));
        row1.setField(2, 20);

        GenericRow row2 = new GenericRow(3);
        row2.setField(0, 2);
        row2.setField(1, BinaryString.fromString("Bob"));
        row2.setField(2, 27);

        GenericRow row3 = new GenericRow(3);
        row3.setField(0, 3);
        row3.setField(1, BinaryString.fromString("Charlie"));
        row3.setField(2, 35);

        assertThat(WhereClauseEvaluator.evaluateWhere(where, row1, rowType)).isTrue();
        assertThat(WhereClauseEvaluator.evaluateWhere(where, row2, rowType)).isFalse();
        assertThat(WhereClauseEvaluator.evaluateWhere(where, row3, rowType)).isTrue();
    }

    @Test
    void testAndCondition() throws Exception {
        RowType rowType = createTestRowType();
        SqlNode where = extractWhereClause("SELECT * FROM t WHERE age > 25 AND age < 35");

        GenericRow row1 = new GenericRow(3);
        row1.setField(0, 1);
        row1.setField(1, BinaryString.fromString("Alice"));
        row1.setField(2, 30);

        GenericRow row2 = new GenericRow(3);
        row2.setField(0, 2);
        row2.setField(1, BinaryString.fromString("Bob"));
        row2.setField(2, 20);

        GenericRow row3 = new GenericRow(3);
        row3.setField(0, 3);
        row3.setField(1, BinaryString.fromString("Charlie"));
        row3.setField(2, 40);

        assertThat(WhereClauseEvaluator.evaluateWhere(where, row1, rowType)).isTrue();
        assertThat(WhereClauseEvaluator.evaluateWhere(where, row2, rowType)).isFalse();
        assertThat(WhereClauseEvaluator.evaluateWhere(where, row3, rowType)).isFalse();
    }

    @Test
    void testEqualityCondition() throws Exception {
        RowType rowType = createTestRowType();
        SqlNode where = extractWhereClause("SELECT * FROM t WHERE age = 25");

        GenericRow row1 = new GenericRow(3);
        row1.setField(0, 1);
        row1.setField(1, BinaryString.fromString("Alice"));
        row1.setField(2, 25);

        GenericRow row2 = new GenericRow(3);
        row2.setField(0, 2);
        row2.setField(1, BinaryString.fromString("Bob"));
        row2.setField(2, 30);

        assertThat(WhereClauseEvaluator.evaluateWhere(where, row1, rowType)).isTrue();
        assertThat(WhereClauseEvaluator.evaluateWhere(where, row2, rowType)).isFalse();
    }

    @Test
    void testStringComparison() throws Exception {
        RowType rowType = createTestRowType();
        SqlNode where = extractWhereClause("SELECT * FROM t WHERE name = 'Alice'");

        GenericRow row1 = new GenericRow(3);
        row1.setField(0, 1);
        row1.setField(1, BinaryString.fromString("Alice"));
        row1.setField(2, 30);

        GenericRow row2 = new GenericRow(3);
        row2.setField(0, 2);
        row2.setField(1, BinaryString.fromString("Bob"));
        row2.setField(2, 25);

        assertThat(WhereClauseEvaluator.evaluateWhere(where, row1, rowType)).isTrue();
        assertThat(WhereClauseEvaluator.evaluateWhere(where, row2, rowType)).isFalse();
    }

    @Test
    void testComplexCondition() throws Exception {
        RowType rowType = createTestRowType();
        SqlNode where =
                extractWhereClause("SELECT * FROM t WHERE (age > 20 AND age < 30) OR age > 40");

        GenericRow row1 = new GenericRow(3);
        row1.setField(0, 1);
        row1.setField(1, BinaryString.fromString("Alice"));
        row1.setField(2, 25);

        GenericRow row2 = new GenericRow(3);
        row2.setField(0, 2);
        row2.setField(1, BinaryString.fromString("Bob"));
        row2.setField(2, 35);

        GenericRow row3 = new GenericRow(3);
        row3.setField(0, 3);
        row3.setField(1, BinaryString.fromString("Charlie"));
        row3.setField(2, 45);

        assertThat(WhereClauseEvaluator.evaluateWhere(where, row1, rowType)).isTrue();
        assertThat(WhereClauseEvaluator.evaluateWhere(where, row2, rowType)).isFalse();
        assertThat(WhereClauseEvaluator.evaluateWhere(where, row3, rowType)).isTrue();
    }
}
