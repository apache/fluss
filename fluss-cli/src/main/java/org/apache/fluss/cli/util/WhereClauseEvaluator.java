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

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import org.apache.calcite.sql.SqlNode;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utility for evaluating WHERE clauses in SQL statements using Apache Calcite AST. */
public class WhereClauseEvaluator {

    /**
     * Extracts all column names referenced in a WHERE clause.
     *
     * @param whereExpression the WHERE clause AST node
     * @return list of column names referenced in the WHERE clause
     */
    public static List<String> extractReferencedColumns(SqlNode whereExpression) {
        List<String> columns = new ArrayList<>();
        if (whereExpression != null) {
            extractColumnsRecursive(whereExpression, columns);
        }
        return columns;
    }

    private static void extractColumnsRecursive(SqlNode expression, List<String> columns) {
        if (expression instanceof org.apache.calcite.sql.SqlBasicCall) {
            org.apache.calcite.sql.SqlBasicCall call =
                    (org.apache.calcite.sql.SqlBasicCall) expression;
            for (SqlNode operand : call.getOperandList()) {
                extractColumnsRecursive(operand, columns);
            }
        } else if (expression instanceof org.apache.calcite.sql.SqlIdentifier) {
            String columnName = ((org.apache.calcite.sql.SqlIdentifier) expression).getSimple();
            if (!columns.contains(columnName)) {
                columns.add(columnName);
            }
        }
    }

    public static Map<String, String> extractEqualities(SqlNode whereExpression) {
        Map<String, String> equalities = new HashMap<>();
        extractEqualitiesRecursive(whereExpression, equalities);
        return equalities;
    }

    private static void extractEqualitiesRecursive(
            SqlNode expression, Map<String, String> equalities) {
        if (expression instanceof org.apache.calcite.sql.SqlBasicCall) {
            org.apache.calcite.sql.SqlBasicCall call =
                    (org.apache.calcite.sql.SqlBasicCall) expression;

            if (call.getKind() == org.apache.calcite.sql.SqlKind.EQUALS) {
                SqlNode left = call.operand(0);
                SqlNode right = call.operand(1);

                String columnName = null;
                String value = null;

                if (left instanceof org.apache.calcite.sql.SqlIdentifier) {
                    columnName = ((org.apache.calcite.sql.SqlIdentifier) left).getSimple();
                    value = stripQuotes(right.toString());
                } else if (right instanceof org.apache.calcite.sql.SqlIdentifier) {
                    columnName = ((org.apache.calcite.sql.SqlIdentifier) right).getSimple();
                    value = stripQuotes(left.toString());
                }

                if (columnName != null && value != null) {
                    equalities.put(columnName, value);
                }
            } else if (call.getKind() == org.apache.calcite.sql.SqlKind.AND) {
                extractEqualitiesRecursive(call.operand(0), equalities);
                extractEqualitiesRecursive(call.operand(1), equalities);
            } else if (call.getKind() == org.apache.calcite.sql.SqlKind.GREATER_THAN
                    || call.getKind() == org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL
                    || call.getKind() == org.apache.calcite.sql.SqlKind.LESS_THAN
                    || call.getKind() == org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL
                    || call.getKind() == org.apache.calcite.sql.SqlKind.NOT_EQUALS
                    || call.getKind() == org.apache.calcite.sql.SqlKind.OR) {
                throw new UnsupportedOperationException(
                        "Primary key extraction only supports equality (=) and AND. "
                                + "For range queries or OR conditions, use full table scan. "
                                + "Unsupported operator: "
                                + call.getKind());
            } else {
                throw new UnsupportedOperationException(
                        "WHERE clause only supports simple equality conditions (=) connected by AND for primary key lookup. "
                                + "Unsupported operator: "
                                + call.getKind());
            }
        } else {
            throw new UnsupportedOperationException(
                    "WHERE clause only supports simple equality conditions (=) connected by AND for primary key lookup. "
                            + "Unsupported expression type: "
                            + expression.getClass().getSimpleName());
        }
    }

    private static String stripQuotes(String value) {
        if (value == null) {
            return null;
        }
        value = value.trim();
        if ((value.startsWith("'") && value.endsWith("'"))
                || (value.startsWith("\"") && value.endsWith("\""))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    public static InternalRow buildPrimaryKeyRow(
            List<String> pkColumns, Map<String, String> whereEqualities, RowType rowType) {

        for (String pkColumn : pkColumns) {
            if (!whereEqualities.containsKey(pkColumn)) {
                throw new IllegalArgumentException(
                        "WHERE clause must specify all primary key columns. Missing: " + pkColumn);
            }
        }

        List<Object> pkValues = new ArrayList<>();
        for (String pkColumn : pkColumns) {
            String valueStr = whereEqualities.get(pkColumn);
            int fieldIndex = rowType.getFieldNames().indexOf(pkColumn);
            if (fieldIndex < 0) {
                throw new IllegalArgumentException("Column not found: " + pkColumn);
            }
            DataType dataType = rowType.getTypeAt(fieldIndex);
            Object value = DataTypeConverter.convertFromString(valueStr, dataType);
            pkValues.add(value);
        }

        GenericRow pkRow = new GenericRow(pkValues.size());
        for (int i = 0; i < pkValues.size(); i++) {
            pkRow.setField(i, pkValues.get(i));
        }
        return pkRow;
    }

    /**
     * Evaluates a WHERE clause predicate against a row.
     *
     * @param whereExpression the WHERE clause AST node
     * @param row the row to evaluate
     * @param rowType the row type schema
     * @return true if the row matches the WHERE condition, false otherwise
     */
    public static boolean evaluateWhere(SqlNode whereExpression, InternalRow row, RowType rowType) {
        if (whereExpression == null) {
            return true;
        }

        if (whereExpression instanceof org.apache.calcite.sql.SqlBasicCall) {
            org.apache.calcite.sql.SqlBasicCall call =
                    (org.apache.calcite.sql.SqlBasicCall) whereExpression;

            switch (call.getKind()) {
                case AND:
                    return evaluateWhere(call.operand(0), row, rowType)
                            && evaluateWhere(call.operand(1), row, rowType);
                case OR:
                    return evaluateWhere(call.operand(0), row, rowType)
                            || evaluateWhere(call.operand(1), row, rowType);
                case EQUALS:
                    return evaluateComparison(call, row, rowType, 0);
                case NOT_EQUALS:
                    return !evaluateComparison(call, row, rowType, 0);
                case GREATER_THAN:
                    return evaluateComparison(call, row, rowType, 1);
                case GREATER_THAN_OR_EQUAL:
                    return evaluateComparison(call, row, rowType, 2);
                case LESS_THAN:
                    return evaluateComparison(call, row, rowType, -1);
                case LESS_THAN_OR_EQUAL:
                    return evaluateComparison(call, row, rowType, -2);
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported WHERE clause operator: " + call.getKind());
            }
        }

        throw new UnsupportedOperationException(
                "Unsupported WHERE clause expression type: "
                        + whereExpression.getClass().getSimpleName());
    }

    /**
     * Evaluates a comparison operation.
     *
     * @param call the comparison operator call
     * @param row the row to evaluate
     * @param rowType the row type schema
     * @param mode comparison mode: 0 = equals, 1 = greater, 2 = greater_or_equal, -1 = less, -2 =
     *     less_or_equal
     * @return true if comparison succeeds
     */
    private static boolean evaluateComparison(
            org.apache.calcite.sql.SqlBasicCall call, InternalRow row, RowType rowType, int mode) {
        SqlNode left = call.operand(0);
        SqlNode right = call.operand(1);

        String columnName = null;
        String valueStr = null;

        if (left instanceof org.apache.calcite.sql.SqlIdentifier) {
            columnName = ((org.apache.calcite.sql.SqlIdentifier) left).getSimple();
            valueStr = stripQuotes(right.toString());
        } else if (right instanceof org.apache.calcite.sql.SqlIdentifier) {
            columnName = ((org.apache.calcite.sql.SqlIdentifier) right).getSimple();
            valueStr = stripQuotes(left.toString());
            // Reverse comparison when column is on right side
            mode = -mode;
        }

        if (columnName == null || valueStr == null) {
            return false;
        }

        int fieldIndex = rowType.getFieldNames().indexOf(columnName);
        if (fieldIndex < 0) {
            throw new IllegalArgumentException("Column not found in WHERE clause: " + columnName);
        }

        DataType dataType = rowType.getTypeAt(fieldIndex);
        Object rowValue = DataTypeConverter.getFieldValue(row, fieldIndex, dataType);

        if (rowValue == null) {
            return false;
        }

        Object compareValue = DataTypeConverter.convertFromString(valueStr, dataType);

        return compare(rowValue, compareValue, mode);
    }

    /**
     * Compares two values according to the specified mode.
     *
     * @param rowValue the value from the row
     * @param compareValue the value to compare against
     * @param mode comparison mode
     * @return true if comparison succeeds
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static boolean compare(Object rowValue, Object compareValue, int mode) {
        if (rowValue == null || compareValue == null) {
            return false;
        }

        int cmp;
        if (rowValue instanceof Comparable) {
            try {
                cmp = ((Comparable) rowValue).compareTo(compareValue);
            } catch (ClassCastException e) {
                // Handle numeric type mismatches
                if (rowValue instanceof Number && compareValue instanceof Number) {
                    BigDecimal left = new BigDecimal(rowValue.toString());
                    BigDecimal right = new BigDecimal(compareValue.toString());
                    cmp = left.compareTo(right);
                } else {
                    return false;
                }
            }
        } else {
            // Fallback to string comparison
            cmp = rowValue.toString().compareTo(compareValue.toString());
        }

        switch (mode) {
            case 0: // EQUALS
                return cmp == 0;
            case 1: // GREATER_THAN
                return cmp > 0;
            case 2: // GREATER_THAN_OR_EQUAL
                return cmp >= 0;
            case -1: // LESS_THAN
                return cmp < 0;
            case -2: // LESS_THAN_OR_EQUAL
                return cmp <= 0;
            default:
                return false;
        }
    }
}
