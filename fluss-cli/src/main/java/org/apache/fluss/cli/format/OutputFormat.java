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

/** Output format for CLI results. */
public enum OutputFormat {
    /** Human-readable table format (default). */
    TABLE,

    /** Comma-separated values format. */
    CSV,

    /** Tab-separated values format. */
    TSV,

    /** JSON format. */
    JSON;

    public static OutputFormat fromString(String format) {
        if (format == null) {
            return TABLE;
        }
        switch (format.toLowerCase()) {
            case "table":
                return TABLE;
            case "csv":
                return CSV;
            case "tsv":
                return TSV;
            case "json":
                return JSON;
            default:
                throw new IllegalArgumentException(
                        "Invalid output format: "
                                + format
                                + ". Valid options: table, csv, tsv, json");
        }
    }
}
