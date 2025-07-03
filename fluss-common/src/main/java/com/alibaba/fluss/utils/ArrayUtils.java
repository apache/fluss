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

import com.alibaba.fluss.annotation.Internal;

/** Utility class for Java arrays. */
@Internal
public final class ArrayUtils {

    public static String[] concat(String[] array1, String[] array2) {
        if (array1.length == 0) {
            return array2;
        }
        if (array2.length == 0) {
            return array1;
        }
        String[] resultArray = new String[array1.length + array2.length];
        System.arraycopy(array1, 0, resultArray, 0, array1.length);
        System.arraycopy(array2, 0, resultArray, array1.length, array2.length);
        return resultArray;
    }

    public static int[] concat(int[] array1, int[] array2) {
        if (array1.length == 0) {
            return array2;
        }
        if (array2.length == 0) {
            return array1;
        }
        int[] resultArray = new int[array1.length + array2.length];
        System.arraycopy(array1, 0, resultArray, 0, array1.length);
        System.arraycopy(array2, 0, resultArray, array1.length, array2.length);
        return resultArray;
    }

    /** Check if the second array is a subset of the first array. */
    public static boolean isSubset(int[] a, int[] b) {
        // Iterate over each element in the second array
        for (int elementB : b) {
            boolean found = false;
            // Check if the element exists in the first array
            for (int elementA : a) {
                if (elementB == elementA) {
                    found = true;
                    break;
                }
            }
            // If any element is not found, return false
            if (!found) {
                return false;
            }
        }
        // If all elements are found, return true
        return true;
    }

    /**
     * Remove the elements of the second array from the first array.
     *
     * @throws IllegalArgumentException if the element of the second array is not found in the first
     *     array.
     */
    public static int[] removeSet(int[] a, int[] b) {
        // Iterate over each element in the second array
        // and check if the element exists in the first array
        if (!isSubset(a, b)) {
            throw new IllegalArgumentException("Element not found in the first array");
        }
        // Remove the elements of the second array from the first array
        int[] resultArray = new int[a.length - b.length];
        int index = 0;
        for (int elementA : a) {
            boolean found = false;
            for (int elementB : b) {
                if (elementA == elementB) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                resultArray[index] = elementA;
                index++;
            }
        }
        return resultArray;
    }

    /**
     * Returns a new array that contains the intersection of the two arrays and in the order of the
     * first array.
     *
     * @throws IllegalArgumentException if the element of the second array is not found in the first
     *     array.
     */
    public static int[] intersection(int[] a, int[] b) {
        // Remove the elements from the first array that not exist in the second array
        return removeSet(a, removeSet(a, b));
    }

    /** Check if the second array is a prefix of the first array. */
    public static boolean isPrefix(int[] a, int[] b) {
        // Iterate over each element in the second array
        for (int i = 0; i < b.length; i++) {
            // Check if the element exists in the first array
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compares two character arrays for equality using a constant-time algorithm, which is needed
     * for comparing passwords. Two arrays are equal if they have the same length and all characters
     * at corresponding positions are equal.
     *
     * <p>All characters in the first array are examined to determine equality. The calculation time
     * depends only on the length of this first character array; it does not depend on the length of
     * the second character array or the contents of either array.
     *
     * @param first the first array to compare
     * @param second the second array to compare
     * @return true if the arrays are equal, or false otherwise
     */
    public static boolean isEqualConstantTime(char[] first, char[] second) {
        if (first == second) {
            return true;
        }
        if (first == null || second == null) {
            return false;
        }

        if (second.length == 0) {
            return first.length == 0;
        }

        // For safety, time-constant comparison that always compares all characters in first array.
        boolean matches = first.length == second.length;
        for (int i = 0; i < first.length; ++i) {
            int j = i < second.length ? i : 0;
            if (first[i] != second[j]) {
                matches = false;
            }
        }
        return matches;
    }
}
