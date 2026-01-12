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
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WhereClauseEvaluatorTest {

    @Test
    void testExtractEqualitiesSingleCondition() throws Exception {
        String sql = "SELECT * FROM mydb.users WHERE id = 1";
        SqlNode whereClause = extractWhereClause(sql);

        Map<String, String> equalities = WhereClauseEvaluator.extractEqualities(whereClause);

        assertThat(equalities).hasSize(1);
        assertThat(equalities).containsEntry("id", "1");
    }

    @Test
    void testExtractEqualitiesMultipleConditions() throws Exception {
        String sql = "SELECT * FROM mydb.orders WHERE shop_id = 100 AND order_id = 1001";
        SqlNode whereClause = extractWhereClause(sql);

        Map<String, String> equalities = WhereClauseEvaluator.extractEqualities(whereClause);

        assertThat(equalities).hasSize(2);
        assertThat(equalities).containsEntry("shop_id", "100");
        assertThat(equalities).containsEntry("order_id", "1001");
    }

    @Test
    void testExtractEqualitiesWithStringValue() throws Exception {
        String sql = "SELECT * FROM mydb.users WHERE name = 'Alice'";
        SqlNode whereClause = extractWhereClause(sql);

        Map<String, String> equalities = WhereClauseEvaluator.extractEqualities(whereClause);

        assertThat(equalities).hasSize(1);
        assertThat(equalities).containsEntry("name", "Alice");
    }

    @Test
    void testExtractEqualitiesThreeConditions() throws Exception {
        String sql = "SELECT * FROM mydb.multi_pk WHERE col1 = 1 AND col2 = 'test' AND col3 = 100";
        SqlNode whereClause = extractWhereClause(sql);

        Map<String, String> equalities = WhereClauseEvaluator.extractEqualities(whereClause);

        assertThat(equalities).hasSize(3);
        assertThat(equalities).containsEntry("col1", "1");
        assertThat(equalities).containsEntry("col2", "test");
        assertThat(equalities).containsEntry("col3", "100");
    }

    @Test
    void testExtractEqualitiesReversedOperands() throws Exception {
        String sql = "SELECT * FROM mydb.users WHERE 1 = id";
        SqlNode whereClause = extractWhereClause(sql);

        Map<String, String> equalities = WhereClauseEvaluator.extractEqualities(whereClause);

        assertThat(equalities).hasSize(1);
        assertThat(equalities).containsEntry("id", "1");
    }

    @Test
    void testExtractEqualitiesUnsupportedOperator() throws Exception {
        String sql = "SELECT * FROM mydb.users WHERE id > 1";
        SqlNode whereClause = extractWhereClause(sql);

        assertThatThrownBy(() -> WhereClauseEvaluator.extractEqualities(whereClause))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Primary key extraction only supports equality");
    }

    @Test
    void testExtractEqualitiesOrCondition() throws Exception {
        String sql = "SELECT * FROM mydb.users WHERE id = 1 OR id = 2";
        SqlNode whereClause = extractWhereClause(sql);

        assertThatThrownBy(() -> WhereClauseEvaluator.extractEqualities(whereClause))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Primary key extraction only supports equality");
    }

    @Test
    void testExtractEqualitiesInOperator() throws Exception {
        String sql = "SELECT * FROM mydb.users WHERE id IN (1, 2, 3)";
        SqlNode whereClause = extractWhereClause(sql);

        assertThatThrownBy(() -> WhereClauseEvaluator.extractEqualities(whereClause))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("WHERE clause only supports simple equality conditions");
    }

    @Test
    void testBuildPrimaryKeyRowSingleColumn() {
        List<String> pkColumns = Arrays.asList("id");
        Map<String, String> whereEqualities = new HashMap<>();
        whereEqualities.put("id", "123");

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"id", "name"});

        InternalRow pkRow =
                WhereClauseEvaluator.buildPrimaryKeyRow(pkColumns, whereEqualities, rowType);

        assertThat(pkRow.getFieldCount()).isEqualTo(1);
        assertThat(pkRow.getInt(0)).isEqualTo(123);
    }

    @Test
    void testBuildPrimaryKeyRowMultipleColumns() {
        List<String> pkColumns = Arrays.asList("shop_id", "order_id");
        Map<String, String> whereEqualities = new HashMap<>();
        whereEqualities.put("shop_id", "100");
        whereEqualities.put("order_id", "1001");

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"shop_id", "order_id", "status"});

        InternalRow pkRow =
                WhereClauseEvaluator.buildPrimaryKeyRow(pkColumns, whereEqualities, rowType);

        assertThat(pkRow.getFieldCount()).isEqualTo(2);
        assertThat(pkRow.getLong(0)).isEqualTo(100L);
        assertThat(pkRow.getLong(1)).isEqualTo(1001L);
    }

    @Test
    void testBuildPrimaryKeyRowWithString() {
        List<String> pkColumns = Arrays.asList("user_id", "category");
        Map<String, String> whereEqualities = new HashMap<>();
        whereEqualities.put("user_id", "1");
        whereEqualities.put("category", "'electronics'");

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE()},
                        new String[] {"user_id", "category", "price"});

        InternalRow pkRow =
                WhereClauseEvaluator.buildPrimaryKeyRow(pkColumns, whereEqualities, rowType);

        assertThat(pkRow.getFieldCount()).isEqualTo(2);
        assertThat(pkRow.getInt(0)).isEqualTo(1);
        assertThat(pkRow.getString(1).toString()).isEqualTo("electronics");
    }

    @Test
    void testBuildPrimaryKeyRowMissingPrimaryKeyColumn() {
        List<String> pkColumns = Arrays.asList("shop_id", "order_id");
        Map<String, String> whereEqualities = new HashMap<>();
        whereEqualities.put("shop_id", "100");

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.BIGINT()},
                        new String[] {"shop_id", "order_id"});

        assertThatThrownBy(
                        () ->
                                WhereClauseEvaluator.buildPrimaryKeyRow(
                                        pkColumns, whereEqualities, rowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("WHERE clause must specify all primary key columns")
                .hasMessageContaining("order_id");
    }

    @Test
    void testBuildPrimaryKeyRowColumnNotFound() {
        List<String> pkColumns = Arrays.asList("invalid_column");
        Map<String, String> whereEqualities = new HashMap<>();
        whereEqualities.put("invalid_column", "123");

        RowType rowType =
                RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"valid_column"});

        assertThatThrownBy(
                        () ->
                                WhereClauseEvaluator.buildPrimaryKeyRow(
                                        pkColumns, whereEqualities, rowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column not found: invalid_column");
    }

    @Test
    void testBuildPrimaryKeyRowDifferentDataTypes() {
        List<String> pkColumns = Arrays.asList("id", "score", "active");
        Map<String, String> whereEqualities = new HashMap<>();
        whereEqualities.put("id", "1");
        whereEqualities.put("score", "99.5");
        whereEqualities.put("active", "true");

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.DOUBLE(), DataTypes.BOOLEAN()},
                        new String[] {"id", "score", "active"});

        InternalRow pkRow =
                WhereClauseEvaluator.buildPrimaryKeyRow(pkColumns, whereEqualities, rowType);

        assertThat(pkRow.getFieldCount()).isEqualTo(3);
        assertThat(pkRow.getInt(0)).isEqualTo(1);
        assertThat(pkRow.getDouble(1)).isEqualTo(99.5);
        assertThat(pkRow.getBoolean(2)).isTrue();
    }

    @Test
    void testBuildPrimaryKeyRowWithExtraWhereConditions() {
        List<String> pkColumns = Arrays.asList("id");
        Map<String, String> whereEqualities = new HashMap<>();
        whereEqualities.put("id", "123");
        whereEqualities.put("name", "'Alice'");

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"id", "name"});

        InternalRow pkRow =
                WhereClauseEvaluator.buildPrimaryKeyRow(pkColumns, whereEqualities, rowType);

        assertThat(pkRow.getFieldCount()).isEqualTo(1);
        assertThat(pkRow.getInt(0)).isEqualTo(123);
    }

    private SqlNode extractWhereClause(String sql) throws Exception {
        CalciteSqlParser parser = new CalciteSqlParser();
        List<SqlNode> statements = parser.parse(sql);
        assertThat(statements).hasSize(1);

        SqlSelect select = (SqlSelect) statements.get(0);
        return select.getWhere();
    }
}
