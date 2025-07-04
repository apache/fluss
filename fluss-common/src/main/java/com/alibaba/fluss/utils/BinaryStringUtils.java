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

package com.alibaba.fluss.utils;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;

import java.time.DateTimeException;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Util for {@link BinaryString}. */
public class BinaryStringUtils {

    private static final List<BinaryString> TRUE_STRINGS =
            Stream.of("t", "true", "y", "yes", "1")
                    .map(BinaryString::fromString)
                    .collect(Collectors.toList());

    private static final List<BinaryString> FALSE_STRINGS =
            Stream.of("f", "false", "n", "no", "0")
                    .map(BinaryString::fromString)
                    .collect(Collectors.toList());

    /** Parse a {@link BinaryString} to boolean. */
    public static boolean toBoolean(BinaryString str) {
        BinaryString lowerCase = str.toLowerCase();
        if (TRUE_STRINGS.contains(lowerCase)) {
            return true;
        }
        if (FALSE_STRINGS.contains(lowerCase)) {
            return false;
        }
        throw new RuntimeException("Cannot parse '" + str + "' as BOOLEAN.");
    }

    public static int toDate(BinaryString input) throws DateTimeException {
        Integer date = DateTimeUtils.parseDate(input.toString());
        if (date == null) {
            throw new DateTimeException("For input string: '" + input + "'.");
        }

        return date;
    }

    public static int toTime(BinaryString input) throws DateTimeException {
        Integer date = DateTimeUtils.parseTime(input.toString());
        if (date == null) {
            throw new DateTimeException("For input string: '" + input + "'.");
        }

        return date;
    }

    /** Used by {@code CAST(x as TIMESTAMPNTZ)}. */
    public static TimestampNtz toTimestampNtz(BinaryString input, int precision)
            throws DateTimeException {
        return DateTimeUtils.parseTimestampData(input.toString(), precision);
    }

    /** Used by {@code CAST(x as TIMESTAMPLTZ)}. */
    public static TimestampLtz toTimestampltz(BinaryString input, int precision, TimeZone timeZone)
            throws DateTimeException {
        return DateTimeUtils.parseTimestampData(input.toString(), precision, timeZone);
    }
}
