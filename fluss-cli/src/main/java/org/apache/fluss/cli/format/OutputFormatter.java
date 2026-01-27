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

import org.apache.fluss.row.InternalRow;

/**
 * Interface for formatting query results in different output formats.
 *
 * <p>Implementations include table, CSV, JSON, and TSV formatters.
 */
public interface OutputFormatter {
    /** Prints the header (column names or opening bracket for JSON). */
    void printHeader();

    /** Prints a single data row. */
    void printRow(InternalRow row);

    /** Prints the footer (row count summary or closing bracket for JSON). */
    void printFooter(long rowCount);
}
