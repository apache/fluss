/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.rowmerger.aggregate.functions;

import org.apache.fluss.row.Decimal;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Overflow-safe arithmetic for narrow numeric types ({@code byte} and {@code short}) that {@link
 * Math} does not cover, plus shared Decimal validation used by multiple aggregators.
 */
final class NarrowMathUtils {

    private NarrowMathUtils() {}

    static byte addExactByte(byte a, byte b) {
        int r = a + b;
        if (r < Byte.MIN_VALUE || r > Byte.MAX_VALUE) {
            throw new ArithmeticException("byte addition out of range");
        }
        return (byte) r;
    }

    static short addExactShort(short a, short b) {
        int r = a + b;
        if (r < Short.MIN_VALUE || r > Short.MAX_VALUE) {
            throw new ArithmeticException("short addition out of range");
        }
        return (short) r;
    }

    static byte subtractExactByte(byte a, byte b) {
        int r = a - b;
        if (r < Byte.MIN_VALUE || r > Byte.MAX_VALUE) {
            throw new ArithmeticException("byte subtraction out of range");
        }
        return (byte) r;
    }

    static short subtractExactShort(short a, short b) {
        int r = a - b;
        if (r < Short.MIN_VALUE || r > Short.MAX_VALUE) {
            throw new ArithmeticException("short subtraction out of range");
        }
        return (short) r;
    }

    static byte multiplyExactByte(byte a, byte b) {
        int r = a * b;
        if (r < Byte.MIN_VALUE || r > Byte.MAX_VALUE) {
            throw new ArithmeticException("byte multiplication out of range");
        }
        return (byte) r;
    }

    static short multiplyExactShort(short a, short b) {
        int r = a * b;
        if (r < Short.MIN_VALUE || r > Short.MAX_VALUE) {
            throw new ArithmeticException("short multiplication out of range");
        }
        return (short) r;
    }

    static void checkDecimalConsistency(Decimal a, Decimal b) {
        checkArgument(a.scale() == b.scale(), "Inconsistent scale: %s vs %s", a.scale(), b.scale());
        checkArgument(
                a.precision() == b.precision(),
                "Inconsistent precision: %s vs %s",
                a.precision(),
                b.precision());
    }
}
