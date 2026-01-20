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
import java.util.List;

/** Formats query results as JSON array with type-aware serialization. */
public class JsonFormatter implements OutputFormatter {

    private final RowType rowType;
    private final PrintWriter out;
    private final List<String> columnNames;
    private boolean firstRow = true;

    public JsonFormatter(RowType rowType, PrintWriter out) {
        this.rowType = rowType;
        this.out = out;
        this.columnNames = rowType.getFieldNames();
    }

    @Override
    public void printHeader() {
        out.println("[");
        out.flush();
    }

    @Override
    public void printRow(InternalRow row) {
        if (!firstRow) {
            out.println(",");
        }
        firstRow = false;

        out.print("  {");
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (i > 0) {
                out.print(", ");
            }
            DataType dataType = rowType.getTypeAt(i);
            String value = DataTypeConverter.getFieldAsString(row, i, dataType);
            out.print("\"" + columnNames.get(i) + "\": ");
            if (value == null || value.equals("null")) {
                out.print("null");
            } else if (isNumeric(dataType)) {
                out.print(value);
            } else {
                out.print("\"" + escapeJson(value) + "\"");
            }
        }
        out.print("}");
        out.flush();
    }

    @Override
    public void printFooter(long rowCount) {
        if (!firstRow) {
            out.println();
        }
        out.println("]");
        out.flush();
    }

    private boolean isNumeric(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return true;
            default:
                return false;
        }
    }

    private String escapeJson(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
