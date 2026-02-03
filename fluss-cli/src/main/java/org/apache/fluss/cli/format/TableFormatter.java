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

package org.apache.fluss.cli.format;

import org.apache.fluss.cli.util.DataTypeConverter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/** Formats query results as ASCII tables. */
public class TableFormatter implements OutputFormatter {

    private final RowType rowType;
    private final PrintWriter out;
    private final List<String> columnNames;
    private final List<Integer> columnWidths;

    public TableFormatter(RowType rowType, PrintWriter out) {
        this.rowType = rowType;
        this.out = out;
        this.columnNames = rowType.getFieldNames();
        this.columnWidths = new ArrayList<>();

        for (String name : columnNames) {
            columnWidths.add(Math.max(name.length(), 10));
        }
    }

    @Override
    public void printHeader() {
        printSeparator();
        printRow(columnNames.toArray(new String[0]));
        printSeparator();
        out.flush();
    }

    @Override
    public void printRow(InternalRow row) {
        String[] values = new String[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            DataType dataType = rowType.getTypeAt(i);
            values[i] = DataTypeConverter.getFieldAsString(row, i, dataType);
            columnWidths.set(i, Math.max(columnWidths.get(i), values[i].length()));
        }
        printRow(values);
        out.flush();
    }

    private void printRow(String[] values) {
        out.print("|");
        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            int width = columnWidths.get(i);
            out.print(" ");
            out.print(padRight(value, width));
            out.print(" |");
        }
        out.println();
    }

    @Override
    public void printFooter(long rowCount) {
        printSeparator();
        out.println(rowCount + " row(s)");
        out.flush();
    }

    private void printSeparator() {
        out.print("+");
        for (int width : columnWidths) {
            out.print(repeat("-", width + 2));
            out.print("+");
        }
        out.println();
    }

    private static String padRight(String s, int n) {
        if (s.length() >= n) {
            return s.substring(0, n);
        }
        return s + repeat(" ", n - s.length());
    }

    private static String repeat(String s, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(s);
        }
        return sb.toString();
    }
}
