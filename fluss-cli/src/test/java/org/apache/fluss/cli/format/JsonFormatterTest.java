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

class JsonFormatterTest {

    @Test
    void testPrintHeader() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"id"});

        StringWriter sw = new StringWriter();
        JsonFormatter formatter = new JsonFormatter(rowType, new PrintWriter(sw));

        formatter.printHeader();

        String output = sw.toString();
        assertThat(output.trim()).isEqualTo("[");
    }

    @Test
    void testPrintSingleRow() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"id", "name"});

        StringWriter sw = new StringWriter();
        JsonFormatter formatter = new JsonFormatter(rowType, new PrintWriter(sw));

        formatter.printHeader();

        GenericRow row = new GenericRow(2);
        row.setField(0, 123);
        row.setField(1, BinaryString.fromString("Alice"));

        formatter.printRow(row);
        formatter.printFooter(1);

        String output = sw.toString();
        assertThat(output).contains("{\"id\": 123, \"name\": \"Alice\"}");
        assertThat(output).startsWith("[");
        assertThat(output.trim()).endsWith("]");
    }

    @Test
    void testPrintMultipleRows() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"id", "name"});

        StringWriter sw = new StringWriter();
        JsonFormatter formatter = new JsonFormatter(rowType, new PrintWriter(sw));

        formatter.printHeader();

        GenericRow row1 = new GenericRow(2);
        row1.setField(0, 1);
        row1.setField(1, BinaryString.fromString("Alice"));
        formatter.printRow(row1);

        GenericRow row2 = new GenericRow(2);
        row2.setField(0, 2);
        row2.setField(1, BinaryString.fromString("Bob"));
        formatter.printRow(row2);

        formatter.printFooter(2);

        String output = sw.toString();
        assertThat(output).contains("{\"id\": 1, \"name\": \"Alice\"}");
        assertThat(output).contains("{\"id\": 2, \"name\": \"Bob\"}");
        assertThat(output).contains(",");
    }

    @Test
    void testNumericTypeNotQuoted() {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(),
                            DataTypes.BIGINT(),
                            DataTypes.DOUBLE(),
                            DataTypes.FLOAT()
                        },
                        new String[] {"int_val", "long_val", "double_val", "float_val"});

        StringWriter sw = new StringWriter();
        JsonFormatter formatter = new JsonFormatter(rowType, new PrintWriter(sw));

        formatter.printHeader();

        GenericRow row = new GenericRow(4);
        row.setField(0, 123);
        row.setField(1, 456789L);
        row.setField(2, 99.99);
        row.setField(3, 1.5f);

        formatter.printRow(row);
        formatter.printFooter(1);

        String output = sw.toString();
        assertThat(output).contains("\"int_val\": 123");
        assertThat(output).contains("\"long_val\": 456789");
        assertThat(output).contains("\"double_val\": 99.99");
        assertThat(output).contains("\"float_val\": 1.5");
        assertThat(output).doesNotContain("\"123\"");
        assertThat(output).doesNotContain("\"456789\"");
    }

    @Test
    void testStringTypeQuoted() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"text"});

        StringWriter sw = new StringWriter();
        JsonFormatter formatter = new JsonFormatter(rowType, new PrintWriter(sw));

        formatter.printHeader();

        GenericRow row = new GenericRow(1);
        row.setField(0, BinaryString.fromString("Hello World"));

        formatter.printRow(row);
        formatter.printFooter(1);

        String output = sw.toString();
        assertThat(output).contains("\"text\": \"Hello World\"");
    }

    @Test
    void testEscapeJsonSpecialCharacters() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"text"});

        StringWriter sw = new StringWriter();
        JsonFormatter formatter = new JsonFormatter(rowType, new PrintWriter(sw));

        formatter.printHeader();

        GenericRow row = new GenericRow(1);
        row.setField(0, BinaryString.fromString("Line1\nLine2\t\"quoted\"\\backslash"));

        formatter.printRow(row);
        formatter.printFooter(1);

        String output = sw.toString();
        assertThat(output).contains("\\n");
        assertThat(output).contains("\\t");
        assertThat(output).contains("\\\"");
        assertThat(output).contains("\\\\");
    }

    @Test
    void testNullValue() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"id", "name"});

        StringWriter sw = new StringWriter();
        JsonFormatter formatter = new JsonFormatter(rowType, new PrintWriter(sw));

        formatter.printHeader();

        GenericRow row = new GenericRow(2);
        row.setField(0, 1);
        row.setField(1, null);

        formatter.printRow(row);
        formatter.printFooter(1);

        String output = sw.toString();
        assertThat(output).contains("\"name\": \"NULL\"");
    }

    @Test
    void testEmptyResult() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"id"});

        StringWriter sw = new StringWriter();
        JsonFormatter formatter = new JsonFormatter(rowType, new PrintWriter(sw));

        formatter.printHeader();
        formatter.printFooter(0);

        String output = sw.toString();
        assertThat(output.trim()).isEqualTo("[\n]");
    }

    @Test
    void testCompleteOutput() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE()},
                        new String[] {"id", "name", "score"});

        StringWriter sw = new StringWriter();
        JsonFormatter formatter = new JsonFormatter(rowType, new PrintWriter(sw));

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
        assertThat(output).startsWith("[");
        assertThat(output.trim()).endsWith("]");
        assertThat(output).contains("\"id\": 1");
        assertThat(output).contains("\"name\": \"Alice\"");
        assertThat(output).contains("\"score\": 95.5");
        assertThat(output).contains("\"id\": 2");
        assertThat(output).contains("\"name\": \"Bob\"");
        assertThat(output).contains("\"score\": 87.3");
    }
}
