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

/** Shared formatting utilities for report writers. */
final class ReportFormatUtils {

    private ReportFormatUtils() {}

    static String formatMs(long nanos) {
        double ms = nanos / 1_000_000.0;
        if (ms < 1) {
            return String.format("%.3f", ms);
        } else if (ms < 100) {
            return String.format("%.2f", ms);
        } else {
            return String.format("%.1f", ms);
        }
    }

    static String formatMicros(long nanos) {
        double us = nanos / 1_000.0;
        if (us < 1000) {
            return String.format("%.1f µs", us);
        }
        return formatMs(nanos) + " ms";
    }

    static String formatNumber(long value) {
        if (value < 1_000) {
            return String.valueOf(value);
        } else if (value < 1_000_000) {
            return String.format("%.1fK", value / 1_000.0);
        }
        return String.format("%.2fM", value / 1_000_000.0);
    }

    static String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        } else if (bytes < 1024L * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024));
        }
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }

    static String formatOneDecimal(double value) {
        return String.format("%.1f", value);
    }

    static long toMB(long bytes) {
        return bytes / (1024 * 1024);
    }

    static long toMBOrNotAvailable(long bytes) {
        if (bytes >= 0) {
            return bytes / (1024 * 1024);
        }
        // Negative means "not available"
        return -1;
    }

    /** Escapes HTML special characters. */
    static String escapeHtml(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }

    /** Escapes a string for use inside a JavaScript string literal. */
    static String escapeJavaScript(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n");
    }
}
