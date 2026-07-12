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

package org.apache.fluss.server.utils;

import org.apache.fluss.config.ConfigOptions;

import java.time.Duration;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Utility methods for row-level TTL. */
public final class RowTtlUtils {

    private RowTtlUtils() {}

    /** Validates a row TTL duration and returns its millisecond value rounded up. */
    public static long validateAndCeilTtlDurationToMillis(Duration ttl) {
        checkNotNull(ttl, "ttl must not be null.");
        checkArgument(
                ttl.compareTo(Duration.ZERO) > 0,
                "'%s' must be positive.",
                ConfigOptions.TABLE_KV_ROW_TTL.key());

        try {
            return Math.addExact(
                    Math.multiplyExact(ttl.getSeconds(), 1000L),
                    (ttl.getNano() + 999_999L) / 1_000_000L);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "'%s' exceeds the maximum supported row TTL.",
                            ConfigOptions.TABLE_KV_ROW_TTL.key()),
                    e);
        }
    }
}
