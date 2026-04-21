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

package org.apache.fluss.microbench.report;

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Writes a {@link PerfReport} to disk in multiple formats. Delegates to format-specific writers:
 *
 * <ul>
 *   <li>JSON — inline Jackson serialization
 *   <li>CSV — {@link CsvReportWriter}
 *   <li>HTML — {@link HtmlReportWriter}
 * </ul>
 *
 * <p>Metric classification and labeling logic lives in {@link MetricClassifier}. Shared formatting
 * utilities are in {@link ReportFormatUtils}.
 */
public class ReportWriter {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private ReportWriter() {}

    /**
     * Writes the report to the given output directory in the requested formats. A
     * config-snapshot.yaml is always written when the report carries a config snapshot.
     */
    public static void write(PerfReport report, Path outputDir, List<String> formats)
            throws IOException {
        Files.createDirectories(outputDir);

        // Always write config snapshot.
        if (report.configSnapshot() != null && !report.configSnapshot().isEmpty()) {
            Files.writeString(outputDir.resolve("config-snapshot.yaml"), report.configSnapshot());
        }

        for (String format : formats) {
            if ("json".equals(format)) {
                writeJson(report, outputDir);
            } else if ("csv".equals(format)) {
                CsvReportWriter.write(report, outputDir);
            } else if ("html".equals(format)) {
                HtmlReportWriter.write(report, outputDir);
            }
        }
    }

    private static void writeJson(PerfReport report, Path outputDir) throws IOException {
        String json = MAPPER.writeValueAsString(report);
        Files.writeString(outputDir.resolve("summary.json"), json);
    }
}
