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

/** Formats query results as TSV (Tab-Separated Values) with escape sequences. */
public class TsvFormatter implements OutputFormatter {

    private final RowType rowType;
    private final PrintWriter out;
    private final List<String> columnNames;

    public TsvFormatter(RowType rowType, PrintWriter out) {
        this.rowType = rowType;
        this.out = out;
        this.columnNames = rowType.getFieldNames();
    }

    @Override
    public void printHeader() {
        out.println(String.join("\t", columnNames));
        out.flush();
    }

    @Override
    public void printRow(InternalRow row) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (i > 0) {
                sb.append("\t");
            }
            DataType dataType = rowType.getTypeAt(i);
            String value = DataTypeConverter.getFieldAsString(row, i, dataType);
            if (value == null) {
                sb.append("");
            } else {
                sb.append(value.replace("\t", "\\t").replace("\n", "\\n"));
            }
        }
        out.println(sb.toString());
        out.flush();
    }

    @Override
    public void printFooter(long rowCount) {
        out.flush();
    }
}
