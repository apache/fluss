/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.utils;

import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

import static java.time.temporal.ChronoField.NANO_OF_SECOND;

/**
 * We replace "." with "_" and replace ":" in date format with "-" so that
 * TablePath.detectInvalidName validation will accept the partition name.
 */
public class PartitionNameConverters {

    private PartitionNameConverters() {}

    public static String hexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xFF & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    public static String reformatFloat(Float value) {
        if (value.isNaN()) {
            return "NaN";
        } else if (value.isInfinite()) {
            return (value > 0) ? "Inf" : "-Inf";
        } else {
            return String.valueOf(value).replace(".", "_");
        }
    }

    public static String reformatDouble(Double value) {
        if (value.isNaN()) {
            return "NaN";
        } else if (value.isInfinite()) {
            return (value > 0) ? "Inf" : "-Inf";
        } else {
            return String.valueOf(value).replace(".", "_");
        }
    }

    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);

    public static String dayToString(int days) {
        OffsetDateTime day = EPOCH.plusDays(days);
        return String.format(
                Locale.ROOT,
                "%04d-%02d-%02d",
                day.getYear(),
                day.getMonth().getValue(),
                day.getDayOfMonth());
    }

    public static String milliToString(int milli) {
        int millisPerSecond = 1000;
        int millisPerMinute = 60 * millisPerSecond;
        int millisPerHour = 60 * millisPerMinute;

        int hour = Math.floorDiv(milli, millisPerHour);
        int min = Math.floorDiv(Math.floorMod(milli, millisPerHour), millisPerMinute);
        int seconds =
                Math.floorDiv(
                        Math.floorMod(Math.floorMod(milli, millisPerHour), millisPerMinute),
                        millisPerSecond);

        return String.format(
                Locale.ROOT,
                "%02d-%02d-%02d_%03d",
                hour,
                min,
                seconds,
                Math.floorMod(milli, millisPerSecond));
    }

    private static final DateTimeFormatter TimestampFormatter =
            new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-[MM]-[dd]")
                    .optionalStart()
                    .appendPattern("-[HH]-[mm]-[ss]_")
                    .appendFraction(NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .toFormatter();

    /** always add nanoseconds whether TimestampNTZ and TimestampLTZ are compact or not. */
    public static String timestampToString(TimestampNtz timestampNtz) {
        return ChronoUnit.MILLIS
                .addTo(EPOCH, timestampNtz.getMillisecond())
                .plusNanos(timestampNtz.getNanoOfMillisecond())
                .toLocalDateTime()
                .atOffset(ZoneOffset.UTC)
                .format(TimestampFormatter);
    }

    public static String timestampToString(TimestampLtz timestampLtz) {
        return ChronoUnit.MILLIS
                .addTo(EPOCH, timestampLtz.getEpochMillisecond())
                .plusNanos(timestampLtz.getNanoOfMillisecond())
                .toLocalDateTime()
                .atOffset(ZoneOffset.UTC)
                .format(TimestampFormatter);
    }

    // ---- Reverse parsing methods (inverse of the above formatting methods) ----

    /** Parses a hex string back to byte array. Reverse of {@link #hexString(byte[])}. */
    public static byte[] parseHexString(String hex) {
        int len = hex.length();
        byte[] bytes = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            bytes[i / 2] =
                    (byte)
                            ((Character.digit(hex.charAt(i), 16) << 4)
                                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return bytes;
    }

    /**
     * Parses "YYYY-MM-DD" date string back to days since epoch. Reverse of {@link
     * #dayToString(int)}.
     */
    public static int parseDayString(String dayStr) {
        String[] parts = dayStr.split("-");
        int year = Integer.parseInt(parts[0]);
        int month = Integer.parseInt(parts[1]);
        int day = Integer.parseInt(parts[2]);
        LocalDateTime date = LocalDateTime.of(year, month, day, 0, 0);
        long epochDay = date.toLocalDate().toEpochDay();
        return (int) epochDay;
    }

    /**
     * Parses "HH-MM-SS_mmm" time string back to milliseconds of day. Reverse of {@link
     * #milliToString(int)}.
     */
    public static int parseMilliString(String timeStr) {
        String[] mainParts = timeStr.split("_");
        String[] timeParts = mainParts[0].split("-");
        int hour = Integer.parseInt(timeParts[0]);
        int min = Integer.parseInt(timeParts[1]);
        int sec = Integer.parseInt(timeParts[2]);
        int millis = Integer.parseInt(mainParts[1]);
        return hour * 3600000 + min * 60000 + sec * 1000 + millis;
    }

    /** Parses reformatted float string back to Float. Reverse of {@link #reformatFloat(Float)}. */
    public static Float parseFloat(String value) {
        if ("NaN".equals(value)) {
            return Float.NaN;
        } else if ("Inf".equals(value)) {
            return Float.POSITIVE_INFINITY;
        } else if ("-Inf".equals(value)) {
            return Float.NEGATIVE_INFINITY;
        } else {
            return java.lang.Float.parseFloat(value.replace("_", "."));
        }
    }

    /**
     * Parses reformatted double string back to Double. Reverse of {@link #reformatDouble(Double)}.
     */
    public static Double parseDouble(String value) {
        if ("NaN".equals(value)) {
            return Double.NaN;
        } else if ("Inf".equals(value)) {
            return Double.POSITIVE_INFINITY;
        } else if ("-Inf".equals(value)) {
            return Double.NEGATIVE_INFINITY;
        } else {
            return java.lang.Double.parseDouble(value.replace("_", "."));
        }
    }

    /**
     * Parses timestamp string back to TimestampNtz. Reverse of {@link
     * #timestampToString(TimestampNtz)}.
     *
     * <p>Format: "yyyy-MM-dd-HH-mm-ss_nnnnnnnnn" where the nano part has 0-9 digits.
     */
    public static TimestampNtz parseTimestampNtz(String value) {
        long[] millisAndNano = parseTimestampString(value);
        return TimestampNtz.fromMillis(millisAndNano[0], (int) millisAndNano[1]);
    }

    /**
     * Parses timestamp string back to TimestampLtz. Reverse of {@link
     * #timestampToString(TimestampLtz)}.
     */
    public static TimestampLtz parseTimestampLtz(String value) {
        long[] millisAndNano = parseTimestampString(value);
        return TimestampLtz.fromEpochMillis(millisAndNano[0], (int) millisAndNano[1]);
    }

    /**
     * Parses a timestamp string into [epochMillis, nanoOfMillisecond].
     *
     * <p>Format: "yyyy-MM-dd-HH-mm-ss_nnnnnnnnn" (the time and nano parts are optional).
     */
    private static long[] parseTimestampString(String value) {
        int underscoreIdx = value.lastIndexOf('_');
        String dateTimePart;
        String nanoPart;
        if (underscoreIdx >= 0) {
            dateTimePart = value.substring(0, underscoreIdx);
            nanoPart = value.substring(underscoreIdx + 1);
        } else {
            dateTimePart = value;
            nanoPart = "";
        }

        String[] parts = dateTimePart.split("-");
        int year = Integer.parseInt(parts[0]);
        int month = Integer.parseInt(parts[1]);
        int day = Integer.parseInt(parts[2]);
        int hour = parts.length > 3 ? Integer.parseInt(parts[3]) : 0;
        int min = parts.length > 4 ? Integer.parseInt(parts[4]) : 0;
        int sec = parts.length > 5 ? Integer.parseInt(parts[5]) : 0;

        LocalDateTime dateTime = LocalDateTime.of(year, month, day, hour, min, sec);
        long epochMillis =
                dateTime.toLocalDate().toEpochDay() * 86400000L
                        + dateTime.toLocalTime().toSecondOfDay() * 1000L;

        int nanoOfMillisecond = 0;
        if (!nanoPart.isEmpty()) {
            // Pad to 9 digits (nano of second)
            StringBuilder sb = new StringBuilder(nanoPart);
            while (sb.length() < 9) {
                sb.append('0');
            }
            long nanoOfSecond = Long.parseLong(sb.toString());
            // First 3 digits are millis-of-second, rest are nano-of-millis
            epochMillis += nanoOfSecond / 1_000_000;
            nanoOfMillisecond = (int) (nanoOfSecond % 1_000_000);
        }

        return new long[] {epochMillis, nanoOfMillisecond};
    }
}
