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

package com.alibaba.fluss.cli.format;

import com.alibaba.fluss.utils.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;

/**
 * Utility class for formatting and printing console tables in CLI (Command Line Interface)
 * applications.
 *
 * <p>Supports two output styles:
 *
 * <ul>
 *   <li>{@link CliOutputStyle#TABLE} — renders a formatted table with borders and aligned columns
 *   <li>{@link CliOutputStyle#TEXT} — outputs a pipe-separated plain text format
 * </ul>
 *
 * <p><b>Example Usage:</b>
 *
 * <pre>{@code
 * CliTable table = CliTable.builder()
 *     .style(CliTable.CliOutputStyle.TABLE)      // Optional: defaults to TABLE
 *     .addHeader("Name", "Age", "City")          // Set column headers
 *     .addRow("Alice", "30", "New York")         // Add data rows
 *     .addRow("Bob", "25", "San Francisco")
 *     .addRow("Charlie", "35", "Los Angeles")
 *     .build();
 * table.print();                                 // Print the table to System.out
 * }</pre>
 *
 * <p><b>Output (TABLE style):</b>
 *
 * <pre>
 * +---------+-----+---------------+
 * | Name    | Age | City          |
 * +---------+-----+---------------+
 * | Alice   | 30  | New York      |
 * | Bob     | 25  | San Francisco |
 * | Charlie | 35  | Los Angeles   |
 * +---------+-----+---------------+
 * </pre>
 *
 * <p><b>Output (TEXT style):</b>
 *
 * <pre>
 * Alice|30|New York
 * Bob|25|San Francisco
 * Charlie|35|Los Angeles
 * </pre>
 *
 * <p><b>Implementation Notes:</b>
 *
 * <ul>
 *   <li>Headers must be set before adding rows.
 *   <li>Each row must contain the same number of columns as the header, or an {@link
 *       IllegalArgumentException} will be thrown.
 *   <li>{@code null} or empty values in cells are rendered as blank strings.
 *   <li>In TABLE style, column widths are automatically adjusted based on the widest header or cell
 *       in each column.
 * </ul>
 */
public class CliTable {

    // Stores column headers for the table
    private final List<String> headers = new ArrayList<>();
    // Contains all data rows where each row is a list of string values
    private final List<List<String>> rows = new ArrayList<>();
    // Output style, Text or Table.
    private CliOutputStyle style = CliOutputStyle.TABLE;

    private CliTable() {}

    public static CliTableBuilder builder() {
        return new CliTableBuilder();
    }

    public void print() {
        style.print(headers, rows);
    }

    public enum CliOutputStyle {
        TEXT {
            @Override
            public void print(List<String> headers, List<List<String>> rows) {
                rows.stream()
                        .map(
                                row ->
                                        row.stream()
                                                .map(
                                                        cell ->
                                                                StringUtils.isEmpty(cell)
                                                                        ? StringUtils.EMPTY
                                                                        : cell)
                                                .collect(Collectors.joining("|")))
                        .forEach(System.out::println);
            }
        },
        TABLE {
            @Override
            public void print(List<String> headers, List<List<String>> rows) {
                List<List<String>> printRows =
                        rows.stream()
                                .map(
                                        row ->
                                                row.stream()
                                                        .map(cell -> cell != null ? cell : "")
                                                        .collect(Collectors.toList()))
                                .collect(Collectors.toList());

                List<Integer> colWidths =
                        IntStream.range(0, headers.size())
                                .mapToObj(
                                        i -> {
                                            int headerLen = headers.get(i).length();
                                            int maxDataLen =
                                                    printRows.stream()
                                                            .mapToInt(row -> row.get(i).length())
                                                            .max()
                                                            .orElse(0);
                                            return Math.max(headerLen, maxDataLen);
                                        })
                                .collect(Collectors.toList());

                String sepLine = formatLine(colWidths, true);
                String rowFormat = formatLine(colWidths, false);

                System.out.println(sepLine);
                System.out.printf(rowFormat, headers.toArray());
                System.out.println(sepLine);
                for (List<String> row : rows) {
                    System.out.printf(rowFormat, row.toArray());
                }
                System.out.println(sepLine);
            }
        };

        public static CliOutputStyle of(String style) {
            Preconditions.checkArgument(
                    CliOutputStyle.TABLE.name().equalsIgnoreCase(style)
                            || CliOutputStyle.TEXT.name().equalsIgnoreCase(style),
                    "Unsupported style: " + style);
            return valueOf(style.toUpperCase());
        }

        public abstract void print(List<String> headers, List<List<String>> rows);
    }

    private void setHeader(String... headers) {
        this.headers.addAll(Arrays.asList(headers));
    }

    private void addRow(String... columns) {
        if (columns.length != headers.size()) {
            throw new IllegalArgumentException(
                    String.format(
                            "column length (%d) != headers length (%d)",
                            columns.length, headers.size()));
        }
        rows.add(Arrays.asList(columns));
    }

    private void setStyle(CliOutputStyle style) {
        this.style = style;
    }

    private static String formatLine(List<Integer> widths, boolean isSeparator) {
        StringBuilder sb = new StringBuilder();
        for (int width : widths) {
            sb.append(
                    isSeparator
                            ? "+" + StringUtils.repeat("-", width + 2)
                            : String.format("| %%-%ds ", width));
        }
        sb.append(isSeparator ? "+" : "|\n");
        return sb.toString();
    }

    // Builder class.
    public static class CliTableBuilder {
        private final CliTable table;

        private CliTableBuilder() {
            this.table = new CliTable();
        }

        public CliTableBuilder style(CliTable.CliOutputStyle style) {
            this.table.setStyle(style);
            return this;
        }

        public CliTableBuilder addHeader(String... headers) {
            table.setHeader(headers);
            return this;
        }

        public CliTableBuilder addRow(String... columns) {
            table.addRow(columns);
            return this;
        }

        public CliTable build() {
            return table;
        }
    }
}
