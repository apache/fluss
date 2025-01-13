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

package com.alibaba.fluss.utils;

/** Collection of simple mathematical routines. */
public class MathUtils {

    /**
     * Computes the logarithm of the given value to the base of 2. This method throws an error, if
     * the given argument is not a power of 2.
     *
     * @param value The value to compute the logarithm for.
     * @return The logarithm to the base of 2.
     * @throws ArithmeticException Thrown, if the given value is zero.
     * @throws IllegalArgumentException Thrown, if the given value is not a power of two.
     */
    public static int log2strict(int value) throws ArithmeticException, IllegalArgumentException {
        if (value == 0) {
            throw new ArithmeticException("Logarithm of zero is undefined.");
        }
        if ((value & (value - 1)) != 0) {
            throw new IllegalArgumentException(
                    "The given value " + value + " is not a power of two.");
        }
        return 31 - Integer.numberOfLeadingZeros(value);
    }

    /**
     * A cheap way to deterministically convert a number to a positive value. When the input is
     * positive, the original value is returned. When the input number is negative, the returned
     * positive value is the original value bit AND against 0x7fffffff which is not its absolute
     * value.
     *
     * @param number a given number
     * @return a positive number.
     */
    public static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    /**
     * This function hashes an integer value.
     *
     * <p>It is crucial to use different hash functions to partition data across machines and the
     * internal partitioning of data structures. This hash function is intended for partitioning
     * across machines.
     *
     * @param code The integer to be hashed.
     * @return The non-negative hash code for the integer.
     */
    public static int murmurHash(int code) {
        code *= 0xcc9e2d51;
        code = Integer.rotateLeft(code, 15);
        code *= 0x1b873593;

        code = Integer.rotateLeft(code, 13);
        code = code * 5 + 0xe6546b64;

        code ^= 4;
        code = bitMix(code);

        if (code >= 0) {
            return code;
        } else if (code != Integer.MIN_VALUE) {
            return -code;
        } else {
            return 0;
        }
    }

    /**
     * Bit-mixing for pseudo-randomization of integers (e.g., to guard against bad hash functions).
     * Implementation is from Murmur's 32 bit finalizer.
     *
     * @param in the input value
     * @return the bit-mixed output value
     */
    public static int bitMix(int in) {
        in ^= in >>> 16;
        in *= 0x85ebca6b;
        in ^= in >>> 13;
        in *= 0xc2b2ae35;
        in ^= in >>> 16;
        return in;
    }

    /**
     * Get the absolute value of the given number. If the number is Int.MinValue return 0. This is
     * different from java.lang.Math.abs or scala.math.abs in that they return Int.MinValue (!).
     */
    public static int abs(int n) {
        return (n == Integer.MIN_VALUE) ? 0 : Math.abs(n);
    }

    /** Divide two integers and round up to the nearest integer value. */
    public static int ceilDiv(int x, int y) {
        final int q = x / y;
        // if the signs are the same and modulo not zero, round up
        if ((x ^ y) >= 0 && (q * y != x)) {
            return q + 1;
        }
        return q;
    }
}
