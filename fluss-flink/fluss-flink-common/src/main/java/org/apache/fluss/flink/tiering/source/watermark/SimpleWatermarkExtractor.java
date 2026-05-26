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

package org.apache.fluss.flink.tiering.source.watermark;

import org.apache.fluss.lake.watermark.WatermarkExtractor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts epoch-millis watermark values from {@link InternalRow} by parsing Flink watermark
 * definitions stored in table properties. Only two expression formats are supported:
 *
 * <ul>
 *   <li>{@code WATERMARK FOR ts AS ts} — direct column reference, zero delay
 *   <li>{@code WATERMARK FOR ts AS ts - INTERVAL '5' SECOND} — column minus interval delay
 * </ul>
 *
 * <p>The rowtime column must be a physical column (computed columns are not supported). Returns
 * {@code null} from {@link #create(TableInfo)} if the watermark configuration cannot be parsed.
 */
public class SimpleWatermarkExtractor implements WatermarkExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleWatermarkExtractor.class);

    private static final String WATERMARK_PROPERTY_PREFIX = "schema.watermark.";
    private static final String WATERMARK_ROWTIME_SUFFIX = ".rowtime";
    private static final String WATERMARK_STRATEGY_EXPR_SUFFIX = ".strategy.expr";
    private static final String WATERMARK_STRATEGY_DATA_TYPE_SUFFIX = ".strategy.data-type";

    private static final Pattern WATERMARK_EXPR_SIMPLE_COLUMN_PATTERN =
            Pattern.compile("(`\\w+`|\\w+)");
    private static final Pattern WATERMARK_EXPR_COLUMN_MINUS_INTERVAL_PATTERN =
            Pattern.compile(
                    "(`\\w+`|\\w+)\\s+-\\s+INTERVAL\\s+'(\\d+\\.?\\d*)'\\s+(SECOND|MINUTE|HOUR|DAY)",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern TIMESTAMP_TYPE_PATTERN =
            Pattern.compile("TIMESTAMP(?:_LTZ)?\\((\\d+)\\)", Pattern.CASE_INSENSITIVE);

    private final int fieldIndex;
    private final int precision;
    private final boolean isTimestampLtz;
    private final long delayMillis;

    private SimpleWatermarkExtractor(
            int fieldIndex, int precision, boolean isTimestampLtz, long delayMillis) {
        this.fieldIndex = fieldIndex;
        this.precision = precision;
        this.isTimestampLtz = isTimestampLtz;
        this.delayMillis = delayMillis;
    }

    /**
     * Creates a {@link SimpleWatermarkExtractor} from the table's properties. Returns null if no
     * watermark configuration is found or the watermark configuration cannot be parsed.
     */
    @Nullable
    public static SimpleWatermarkExtractor create(TableInfo tableInfo) {
        Map<String, String> props = tableInfo.getCustomProperties().toMap();
        RowType rowType = tableInfo.getRowType();

        boolean isWatermarkDefined = false;
        String rowtimeColumn = null;
        String watermarkIndex = null;

        for (Map.Entry<String, String> entry : props.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(WATERMARK_PROPERTY_PREFIX)
                    && key.endsWith(WATERMARK_ROWTIME_SUFFIX)) {
                isWatermarkDefined = true;
                rowtimeColumn = entry.getValue();
                watermarkIndex =
                        key.substring(
                                WATERMARK_PROPERTY_PREFIX.length(),
                                key.length() - WATERMARK_ROWTIME_SUFFIX.length());
                if (!String.valueOf(0).equals(watermarkIndex)) {
                    LOG.warn(
                            "There are more than 1 watermark definition for {}, which is not supported for watermark extraction.",
                            tableInfo.getTablePath());
                    return null;
                }
                break;
            }
        }

        if (!isWatermarkDefined) {
            return null;
        }

        int fieldIndex = rowType.getFieldIndex(rowtimeColumn);
        if (fieldIndex < 0) {
            LOG.warn(
                    "Watermark rowtime column '{}' not found in row type for {}, "
                            + "computed column is not supported for watermark extraction.",
                    tableInfo.getTablePath(),
                    rowtimeColumn);
            return null;
        }

        Long watermarkDelayMillis = parseWatermarkDelayMillis(props);
        if (watermarkDelayMillis == null) {
            LOG.warn(
                    "Cannot parse delay millis for {}, complex watermark expression is not supported for watermark extraction.",
                    tableInfo.getTablePath());
            return null;
        }

        boolean isTimestampLtz;
        int precision = 3;
        String dataTypeKey =
                WATERMARK_PROPERTY_PREFIX + watermarkIndex + WATERMARK_STRATEGY_DATA_TYPE_SUFFIX;
        String dataTypeValue = props.get(dataTypeKey);
        if (dataTypeValue != null) {
            String trimmed = dataTypeValue.trim().toUpperCase();
            isTimestampLtz = trimmed.contains("TIMESTAMP_LTZ");
            Matcher matcher = TIMESTAMP_TYPE_PATTERN.matcher(trimmed);
            if (matcher.find()) {
                precision = Integer.parseInt(matcher.group(1));
            }
        } else {
            LOG.warn("Watermark column data type is not defined for {}.", tableInfo.getTablePath());
            return null;
        }

        return new SimpleWatermarkExtractor(
                fieldIndex, precision, isTimestampLtz, watermarkDelayMillis);
    }

    /** Extracts the epoch-millis watermark from the given row. */
    @Override
    @Nullable
    public Long currentWatermark(InternalRow row) {
        if (row.isNullAt(fieldIndex)) {
            return null;
        }
        if (isTimestampLtz) {
            return row.getTimestampLtz(fieldIndex, precision).getEpochMillisecond() - delayMillis;
        } else {
            return row.getTimestampNtz(fieldIndex, precision).getMillisecond() - delayMillis;
        }
    }

    /**
     * Parses the watermark delay from the strategy expression. For example, {@code `col` - INTERVAL
     * '5' SECOND} yields 5000 milliseconds. Returns {@code null} if the expression is unsupported.
     */
    @Nullable
    private static Long parseWatermarkDelayMillis(Map<String, String> props) {
        for (Map.Entry<String, String> entry : props.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(WATERMARK_PROPERTY_PREFIX)
                    && key.endsWith(WATERMARK_STRATEGY_EXPR_SUFFIX)) {
                String expr = entry.getValue();
                if (WATERMARK_EXPR_SIMPLE_COLUMN_PATTERN.matcher(expr).matches()) {
                    return 0L;
                }
                Matcher matcher = WATERMARK_EXPR_COLUMN_MINUS_INTERVAL_PATTERN.matcher(expr);
                if (matcher.matches()) {
                    double amount = Double.parseDouble(matcher.group(2));
                    String unit = matcher.group(3).toUpperCase();
                    switch (unit) {
                        case "SECOND":
                            return (long) (amount * 1000);
                        case "MINUTE":
                            return (long) (amount * 60_000);
                        case "HOUR":
                            return (long) (amount * 3_600_000);
                        case "DAY":
                            return (long) (amount * 86_400_000);
                        default:
                            return null;
                    }
                }
                return null;
            }
        }
        return 0L;
    }
}
