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

package org.apache.fluss.types;

import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import java.time.Instant;
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
}
