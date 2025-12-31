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

package org.apache.fluss.utils;

import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import java.time.DateTimeException;
import java.util.Arrays;
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

    /** Used by {@code CAST(x as TIMESTAMP_NTZ)}. */
    public static TimestampNtz toTimestampNtz(BinaryString input, int precision)
            throws DateTimeException {
        return DateTimeUtils.parseTimestampData(input.toString(), precision);
    }

    /** Used by {@code CAST(x as TIMESTAMP_LTZ)}. */
    public static TimestampLtz toTimestampLtz(
            BinaryString input, int precision, TimeZone localTimeZone) throws DateTimeException {
        return DateTimeUtils.parseTimestampData(input.toString(), precision, localTimeZone);
    }

    /**
     * Concatenates input strings together into a single string. Returns NULL if any argument is
     * NULL.
     *
     * <p>This method is optimized to avoid unnecessary string conversions and memory allocations.
     * It directly operates on the underlying UTF-8 byte arrays of BinaryString objects.
     *
     * @param inputs the strings to concatenate
     * @return the concatenated string, or NULL if any input is NULL
     */
    public static BinaryString concat(BinaryString... inputs) {
        return concat(Arrays.asList(inputs));
    }

    /**
     * Concatenates input strings together into a single string. Returns NULL if any argument is
     * NULL.
     *
     * <p>This method is optimized to avoid unnecessary string conversions and memory allocations.
     * It directly operates on the underlying UTF-8 byte arrays of BinaryString objects.
     *
     * @param inputs the strings to concatenate
     * @return the concatenated string, or NULL if any input is NULL
     */
    public static BinaryString concat(Iterable<BinaryString> inputs) {
        // Compute the total length of the result.
        int totalLength = 0;
        for (BinaryString input : inputs) {
            if (input == null) {
                return null;
            }

            totalLength += input.getSizeInBytes();
        }

        // Allocate a new byte array, and copy the inputs one by one into it.
        final byte[] result = new byte[totalLength];
        int offset = 0;
        for (BinaryString input : inputs) {
            if (input != null) {
                int len = input.getSizeInBytes();
                BinarySegmentUtils.copyToBytes(
                        input.getSegments(), input.getOffset(), result, offset, len);
                offset += len;
            }
        }
        return BinaryString.fromBytes(result);
    }

    /**
     * Concatenates input strings together into a single string using the separator. Returns NULL if
     * the separator is NULL.
     *
     * <p>Note: CONCAT_WS() does not skip any empty strings, however it does skip any NULL values
     * after the separator. For example, concat_ws(",", "a", null, "c") would yield "a,c".
     *
     * @param separator the separator to use between strings
     * @param inputs the strings to concatenate
     * @return the concatenated string with separator, or NULL if separator is NULL
     */
    public static BinaryString concatWs(BinaryString separator, BinaryString... inputs) {
        return concatWs(separator, Arrays.asList(inputs));
    }

    /**
     * Concatenates input strings together into a single string using the separator. Returns NULL if
     * the separator is NULL.
     *
     * <p>Note: CONCAT_WS() does not skip any empty strings, however it does skip any NULL values
     * after the separator. For example, concat_ws(",", "a", null, "c") would yield "a,c".
     *
     * @param separator the separator to use between strings
     * @param inputs the strings to concatenate
     * @return the concatenated string with separator, or NULL if separator is NULL
     */
    public static BinaryString concatWs(BinaryString separator, Iterable<BinaryString> inputs) {
        if (null == separator) {
            return null;
        }

        int numInputBytes = 0; // total number of bytes from the inputs
        int numInputs = 0; // number of non-null inputs
        for (BinaryString input : inputs) {
            if (input != null) {
                numInputBytes += input.getSizeInBytes();
                numInputs++;
            }
        }

        if (numInputs == 0) {
            // Return an empty string if there is no input, or all the inputs are null.
            return BinaryString.EMPTY_UTF8;
        }

        // Allocate a new byte array, and copy the inputs one by one into it.
        // The size of the new array is the size of all inputs, plus the separators.
        final byte[] result =
                new byte[numInputBytes + (numInputs - 1) * separator.getSizeInBytes()];
        int offset = 0;

        int i = 0;
        for (BinaryString input : inputs) {
            if (input != null) {
                int len = input.getSizeInBytes();
                BinarySegmentUtils.copyToBytes(
                        input.getSegments(), input.getOffset(), result, offset, len);
                offset += len;

                i++;
                // Add separator if this is not the last input
                if (i < numInputs) {
                    int sepLen = separator.getSizeInBytes();
                    BinarySegmentUtils.copyToBytes(
                            separator.getSegments(), separator.getOffset(), result, offset, sepLen);
                    offset += sepLen;
                }
            }
        }
        return BinaryString.fromBytes(result);
    }
}
