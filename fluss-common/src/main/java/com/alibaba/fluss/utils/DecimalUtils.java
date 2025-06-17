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
import com.alibaba.fluss.row.Decimal;

import java.math.BigDecimal;
import java.math.RoundingMode;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Utilities for {@link Decimal}. */
public class DecimalUtils {

    static final int MAX_COMPACT_PRECISION = 18;

    static final long[] POW10 = new long[MAX_COMPACT_PRECISION + 1];

    static {
        POW10[0] = 1;
        for (int i = 1; i < POW10.length; i++) {
            POW10[i] = 10 * POW10[i - 1];
        }
    }

    public static double doubleValue(Decimal decimal) {
        if (decimal.isCompact()) {
            return ((double) decimal.toUnscaledLong()) / POW10[decimal.scale()];
        } else {
            return decimal.toBigDecimal().doubleValue();
        }
    }

    public static Decimal add(Decimal v1, Decimal v2, int precision, int scale) {
        if (v1.isCompact()
                && v2.isCompact()
                && v1.scale() == v2.scale()
                && Decimal.isCompact(precision)) {
            assert scale == v1.scale(); // no need to rescale
            try {
                long ls =
                        Math.addExact(v1.toUnscaledLong(), v2.toUnscaledLong()); // checks overflow
                return Decimal.fromUnscaledLong(ls, precision, scale);
            } catch (ArithmeticException e) {
                // overflow, fall through
            }
        }
        BigDecimal bd = v1.toBigDecimal().add(v2.toBigDecimal());
        return Decimal.fromBigDecimal(bd, precision, scale);
    }

    public static Decimal subtract(Decimal v1, Decimal v2, int precision, int scale) {
        if (v1.isCompact()
                && v2.isCompact()
                && v1.scale() == v2.scale()
                && Decimal.isCompact(precision)) {
            assert scale == v1.scale(); // no need to rescale
            try {
                long ls =
                        Math.subtractExact(
                                v1.toUnscaledLong(), v2.toUnscaledLong()); // checks overflow
                return Decimal.fromUnscaledLong(ls, precision, scale);
            } catch (ArithmeticException e) {
                // overflow, fall through
            }
        }
        BigDecimal bd = v1.toBigDecimal().subtract(v2.toBigDecimal());
        return Decimal.fromBigDecimal(bd, precision, scale);
    }

    public static int compare(Decimal b1, long n2) {
        if (!b1.isCompact()) {
            return b1.toBigDecimal().compareTo(BigDecimal.valueOf(n2));
        }
        if (b1.scale() == 0) {
            return Long.compare(b1.toUnscaledLong(), n2);
        }

        long i1 = b1.toUnscaledLong() / POW10[b1.scale()];
        if (i1 == n2) {
            long l2 = n2 * POW10[b1.scale()]; // won't overflow
            return Long.compare(b1.toUnscaledLong(), l2);
        } else {
            return i1 > n2 ? +1 : -1;
        }
    }

    public static long castToIntegral(Decimal dec) {
        BigDecimal bd = dec.toBigDecimal();
        // rounding down. This is consistent with float=>int,
        // and consistent with SQLServer, Spark.
        bd = bd.setScale(0, RoundingMode.DOWN);
        return bd.longValue();
    }

    public static Decimal castToDecimal(Decimal dec, int precision, int scale) {
        return Decimal.fromBigDecimal(dec.toBigDecimal(), precision, scale);
    }

    public static Decimal castFrom(Decimal dec, int precision, int scale) {
        return Decimal.fromBigDecimal(dec.toBigDecimal(), precision, scale);
    }

    public static Decimal castFrom(String string, int precision, int scale) {
        return Decimal.fromBigDecimal(new BigDecimal(string), precision, scale);
    }

    public static Decimal castFrom(BinaryString string, int precision, int scale) {
        return castFrom(string.toString(), precision, scale);
    }

    public static Decimal castFrom(double val, int p, int s) {
        return Decimal.fromBigDecimal(BigDecimal.valueOf(val), p, s);
    }

    public static Decimal castFrom(long val, int p, int s) {
        return Decimal.fromBigDecimal(BigDecimal.valueOf(val), p, s);
    }

    public static boolean castToBoolean(Decimal dec) {
        return dec.toBigDecimal().compareTo(BigDecimal.ZERO) != 0;
    }
}
