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

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.assertj.core.api.Assertions.assertThat;

class TsvFormatterTest {

    @Test
    void testPrintHeader() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE()},
                        new String[] {"id", "name", "score"});

        StringWriter sw = new StringWriter();
        TsvFormatter formatter = new TsvFormatter(rowType, new PrintWriter(sw));

        formatter.printHeader();

        String output = sw.toString();
        assertThat(output.trim()).isEqualTo("id\tname\tscore");
    }

    @Test
    void testPrintSingleRow() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"id", "name"});

        StringWriter sw = new StringWriter();
        TsvFormatter formatter = new TsvFormatter(rowType, new PrintWriter(sw));

        GenericRow row = new GenericRow(2);
        row.setField(0, 123);
        row.setField(1, BinaryString.fromString("Alice"));

        formatter.printRow(row);

        String output = sw.toString();
        assertThat(output.trim()).isEqualTo("123\tAlice");
    }

    @Test
    void testPrintMultipleRows() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"id", "name"});

        StringWriter sw = new StringWriter();
        TsvFormatter formatter = new TsvFormatter(rowType, new PrintWriter(sw));

        formatter.printHeader();

        GenericRow row1 = new GenericRow(2);
        row1.setField(0, 1);
        row1.setField(1, BinaryString.fromString("Alice"));
        formatter.printRow(row1);

        GenericRow row2 = new GenericRow(2);
        row2.setField(0, 2);
        row2.setField(1, BinaryString.fromString("Bob"));
        formatter.printRow(row2);

        String output = sw.toString();
        String[] lines = output.trim().split("\n");
        assertThat(lines).hasSize(3);
        assertThat(lines[0]).isEqualTo("id\tname");
        assertThat(lines[1]).isEqualTo("1\tAlice");
        assertThat(lines[2]).isEqualTo("2\tBob");
    }

    @Test
    void testEscapeTab() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"id", "text"});

        StringWriter sw = new StringWriter();
        TsvFormatter formatter = new TsvFormatter(rowType, new PrintWriter(sw));

        GenericRow row = new GenericRow(2);
        row.setField(0, 1);
        row.setField(1, BinaryString.fromString("Hello\tWorld"));

        formatter.printRow(row);

        String output = sw.toString();
        assertThat(output.trim()).isEqualTo("1\tHello\\tWorld");
    }

    @Test
    void testEscapeNewline() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"id", "text"});

        StringWriter sw = new StringWriter();
        TsvFormatter formatter = new TsvFormatter(rowType, new PrintWriter(sw));

        GenericRow row = new GenericRow(2);
        row.setField(0, 1);
        row.setField(1, BinaryString.fromString("Line1\nLine2"));

        formatter.printRow(row);

        String output = sw.toString();
        assertThat(output.trim()).isEqualTo("1\tLine1\\nLine2");
    }

    @Test
    void testNullValue() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"id", "name"});

        StringWriter sw = new StringWriter();
        TsvFormatter formatter = new TsvFormatter(rowType, new PrintWriter(sw));

        GenericRow row = new GenericRow(2);
        row.setField(0, 1);
        row.setField(1, null);

        formatter.printRow(row);

        String output = sw.toString();
        assertThat(output.trim()).isEqualTo("1\tNULL");
    }

    @Test
    void testPrintFooter() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"id"});

        StringWriter sw = new StringWriter();
        TsvFormatter formatter = new TsvFormatter(rowType, new PrintWriter(sw));

        formatter.printFooter(100);

        String output = sw.toString();
        assertThat(output).isEmpty();
    }

    @Test
    void testCompleteOutput() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE()},
                        new String[] {"id", "name", "score"});

        StringWriter sw = new StringWriter();
        TsvFormatter formatter = new TsvFormatter(rowType, new PrintWriter(sw));

        formatter.printHeader();

        GenericRow row1 = new GenericRow(3);
        row1.setField(0, 1);
        row1.setField(1, BinaryString.fromString("Alice"));
        row1.setField(2, 95.5);
        formatter.printRow(row1);

        GenericRow row2 = new GenericRow(3);
        row2.setField(0, 2);
        row2.setField(1, BinaryString.fromString("Bob"));
        row2.setField(2, 87.3);
        formatter.printRow(row2);

        formatter.printFooter(2);

        String output = sw.toString();
        String[] lines = output.trim().split("\n");
        assertThat(lines).hasSize(3);
        assertThat(lines[0]).isEqualTo("id\tname\tscore");
        assertThat(lines[1]).isEqualTo("1\tAlice\t95.5");
        assertThat(lines[2]).isEqualTo("2\tBob\t87.3");
    }
}
