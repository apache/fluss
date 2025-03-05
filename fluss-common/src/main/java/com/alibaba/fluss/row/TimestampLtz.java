/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.row;

import com.alibaba.fluss.annotation.PublicStable;
import com.alibaba.fluss.types.LocalZonedTimestampType;

import java.io.Serializable;
import java.time.Instant;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/**
 * An internal data structure representing data of {@link LocalZonedTimestampType}.
 *
 * <p>This data structure is immutable and consists of a milliseconds and nanos-of-millisecond since
 * {@code 1970-01-01 00:00:00}. It might be stored in a compact representation (as a long value) if
 * values are small enough.
 *
 * @since 0.1
 */
@PublicStable
public class TimestampLtz implements Comparable<TimestampLtz>, Serializable {

    private static final long serialVersionUID = 1L;

    public static final long MICROS_PER_MILLIS = 1000L;

    public static final long NANOS_PER_MICROS = 1000L;

    // this field holds the integral second and the milli-of-second.
    private final long millisecond;

    // this field holds the nano-of-millisecond.
    private final int nanoOfMillisecond;

    private TimestampLtz(long millisecond, int nanoOfMillisecond) {
        checkArgument(nanoOfMillisecond >= 0 && nanoOfMillisecond <= 999_999);
        this.millisecond = millisecond;
        this.nanoOfMillisecond = nanoOfMillisecond;
    }

    /** Returns the number of milliseconds since {@code 1970-01-01 00:00:00}. */
    public long getEpochMillisecond() {
        return millisecond;
    }

    /**
     * Returns the number of nanoseconds (the nanoseconds within the milliseconds).
     *
     * <p>The value range is from 0 to 999,999.
     */
    public int getNanoOfMillisecond() {
        return nanoOfMillisecond;
    }

    /**
     * Creates an instance of {@link TimestampLtz} from milliseconds.
     *
     * <p>The nanos-of-millisecond field will be set to zero.
     *
     * @param milliseconds the number of milliseconds since {@code 1970-01-01 00:00:00}; a negative
     *     number is the number of milliseconds before {@code 1970-01-01 00:00:00}
     */
    public static TimestampLtz fromEpochMillis(long milliseconds) {
        return new TimestampLtz(milliseconds, 0);
    }

    /**
     * Creates an instance of {@link TimestampLtz} from milliseconds and a nanos-of-millisecond.
     *
     * @param milliseconds the number of milliseconds since {@code 1970-01-01 00:00:00}; a negative
     *     number is the number of milliseconds before {@code 1970-01-01 00:00:00}
     * @param nanosOfMillisecond the nanoseconds within the millisecond, from 0 to 999,999
     */
    public static TimestampLtz fromEpochMillis(long milliseconds, int nanosOfMillisecond) {
        return new TimestampLtz(milliseconds, nanosOfMillisecond);
    }

    /** Creates an instance of {@link TimestampLtz} from micros. */
    public static TimestampLtz fromEpochMicros(long micros) {
        long mills = Math.floorDiv(micros, MICROS_PER_MILLIS);
        long nanos = (micros - mills * MICROS_PER_MILLIS) * NANOS_PER_MICROS;
        return TimestampLtz.fromEpochMillis(mills, (int) nanos);
    }

    /** Converts this {@link TimestampLtz} object to micros. */
    public long toEpochMicros() {
        long micros = Math.multiplyExact(millisecond, MICROS_PER_MILLIS);
        return micros + nanoOfMillisecond / NANOS_PER_MICROS;
    }

    /**
     * Returns whether the timestamp data is small enough to be stored in a long of milliseconds.
     */
    public static boolean isCompact(int precision) {
        return precision <= 3;
    }

    /**
     * Creates an instance of {@link TimestampLtz} from an instance of {@link Instant}.
     *
     * @param instant an instance of {@link Instant}
     */
    public static TimestampLtz fromInstant(Instant instant) {
        long epochSecond = instant.getEpochSecond();
        int nanoSecond = instant.getNano();

        long millisecond = epochSecond * 1_000 + nanoSecond / 1_000_000;
        int nanoOfMillisecond = nanoSecond % 1_000_000;

        return new TimestampLtz(millisecond, nanoOfMillisecond);
    }

    /** Converts this {@link TimestampLtz} object to a {@link Instant}. */
    public Instant toInstant() {
        long epochSecond = millisecond / 1000;
        int milliOfSecond = (int) (millisecond % 1000);
        if (milliOfSecond < 0) {
            --epochSecond;
            milliOfSecond += 1000;
        }
        long nanoAdjustment = milliOfSecond * 1_000_000 + nanoOfMillisecond;
        return Instant.ofEpochSecond(epochSecond, nanoAdjustment);
    }

    @Override
    public int compareTo(TimestampLtz that) {
        int cmp = Long.compare(this.millisecond, that.millisecond);
        if (cmp == 0) {
            cmp = this.nanoOfMillisecond - that.nanoOfMillisecond;
        }
        return cmp;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TimestampLtz)) {
            return false;
        }
        TimestampLtz that = (TimestampLtz) obj;
        return this.millisecond == that.millisecond
                && this.nanoOfMillisecond == that.nanoOfMillisecond;
    }

    @Override
    public String toString() {
        return toInstant().toString();
    }

    @Override
    public int hashCode() {
        int ret = (int) millisecond ^ (int) (millisecond >> 32);
        return 31 * ret + nanoOfMillisecond;
    }
}
