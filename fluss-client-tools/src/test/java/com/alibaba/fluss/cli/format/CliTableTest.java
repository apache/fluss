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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alibaba.fluss.cli.base.BaseTest;
import org.junit.jupiter.api.Test;

public class CliTableTest extends BaseTest {

    @Test
    public void testTableStyleOutputFormatting() {
        CliTable table =
                CliTable.builder()
                        .style(CliTable.CliOutputStyle.TABLE)
                        .addHeader("Name", "Age", "City")
                        .addRow("Alice", "30", "New York")
                        .addRow("Bob", "25", "San Francisco")
                        .build();

        table.print();

        String output = outContent.toString();
        assertTrue(output.contains("+-------+-----+---------------+"));
        assertTrue(output.contains("| Name  | Age | City          |"));
        assertTrue(output.contains("| Alice | 30  | New York      |"));
        assertTrue(output.contains("| Bob   | 25  | San Francisco |"));
        assertTrue(output.contains("+-------+-----+---------------+"));
    }

    @Test
    public void testTextStyleOutputFormatting() {
        CliTable table =
                CliTable.builder()
                        .style(CliTable.CliOutputStyle.TEXT)
                        .addHeader("ID", "Status")
                        .addRow("1001", "Active")
                        .addRow("1002", "Inactive")
                        .build();

        table.print();

        String output = outContent.toString();
        String expected = "1001|Active\n1002|Inactive\n";
        assertEquals(expected, output);
    }

    @Test
    public void testNullAndEmptyValueHandling() {
        CliTable table =
                CliTable.builder()
                        .addHeader("Col1", "Col2", "Col3")
                        .addRow("", null, "Value")
                        .build();

        table.print();

        String output = outContent.toString();
        assertEquals(
                "+------+------+-------+\n"
                        + "| Col1 | Col2 | Col3  |\n"
                        + "+------+------+-------+\n"
                        + "|      | null | Value |\n"
                        + "+------+------+-------+",
                output.trim());
    }

    @Test
    public void testInvalidRowColumnCount() {
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    CliTable.builder()
                            .addHeader("A", "B", "C")
                            .addRow("1", "2") // Only two columns
                            .build();
                });
    }

    @Test
    public void testColumnWidthCalculation() {
        CliTable table =
                CliTable.builder()
                        .addHeader("Short", "VeryLongHeader", "Medium")
                        .addRow("LongValue", "Short", "X")
                        .build();

        table.print();

        String output = outContent.toString();
        // Verify column widths based on longest content
        assertTrue(output.contains("| Short     | VeryLongHeader | Medium |"));
        assertTrue(output.contains("| LongValue | Short          | X      |"));
    }

    @Test
    public void testOutputStyleEnum() {
        CliTable.CliOutputStyle style = CliTable.CliOutputStyle.of("TEXT");
        assertEquals(CliTable.CliOutputStyle.TEXT, style);
        style = CliTable.CliOutputStyle.of("table");
        assertEquals(CliTable.CliOutputStyle.TABLE, style);
    }

    @Test
    public void testInvalidStyleName() {
        assertThrows(
                IllegalArgumentException.class, () -> CliTable.CliOutputStyle.of("INVALID_STYLE"));
    }

    @Test
    public void testBuilderPatternUsage() {
        CliTable table =
                CliTable.builder()
                        .style(CliTable.CliOutputStyle.TABLE)
                        .addHeader("Header")
                        .addRow("Row1")
                        .addRow("Row2")
                        .build();

        assertNotNull(table);
        table.print();
        assertTrue(outContent.toString().contains("Header"));
        assertTrue(outContent.toString().contains("Row1"));
        assertTrue(outContent.toString().contains("Row2"));
    }

    @Test
    public void testMultiLineTableRendering() {
        CliTable table =
                CliTable.builder()
                        .addHeader("ID", "Description")
                        .addRow("001", "First item")
                        .addRow("002", "Second item with longer description")
                        .build();
        table.print();
        String output = outContent.toString();
        assertEquals(
                "+-----+-------------------------------------+\n"
                        + "| ID  | Description                         |\n"
                        + "+-----+-------------------------------------+\n"
                        + "| 001 | First item                          |\n"
                        + "| 002 | Second item with longer description |\n"
                        + "+-----+-------------------------------------+",
                output.trim());
    }
}
