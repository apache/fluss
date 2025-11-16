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

import org.apache.fluss.annotation.Internal;

/** Utility class for Java arrays. */
@Internal
public final class ArrayUtils {

    /** An empty immutable {@code String} array. */
    public static final String[] EMPTY_STRING_ARRAY = new String[0];

    /** An empty immutable {@code long} array. */
    public static final long[] EMPTY_LONG_ARRAY = new long[0];
    /** An empty immutable {@code Long} array. */
    public static final Long[] EMPTY_LONG_OBJECT_ARRAY = new Long[0];
    /** An empty immutable {@code int} array. */
    public static final int[] EMPTY_INT_ARRAY = new int[0];
    /** An empty immutable {@code Integer} array. */
    public static final Integer[] EMPTY_INTEGER_OBJECT_ARRAY = new Integer[0];
    /** An empty immutable {@code short} array. */
    public static final short[] EMPTY_SHORT_ARRAY = new short[0];
    /** An empty immutable {@code Short} array. */
    public static final Short[] EMPTY_SHORT_OBJECT_ARRAY = new Short[0];
    /** An empty immutable {@code byte} array. */
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    /** An empty immutable {@code Byte} array. */
    public static final Byte[] EMPTY_BYTE_OBJECT_ARRAY = new Byte[0];
    /** An empty immutable {@code double} array. */
    public static final double[] EMPTY_DOUBLE_ARRAY = new double[0];
    /** An empty immutable {@code Double} array. */
    public static final Double[] EMPTY_DOUBLE_OBJECT_ARRAY = new Double[0];
    /** An empty immutable {@code float} array. */
    public static final float[] EMPTY_FLOAT_ARRAY = new float[0];
    /** An empty immutable {@code Float} array. */
    public static final Float[] EMPTY_FLOAT_OBJECT_ARRAY = new Float[0];
    /** An empty immutable {@code boolean} array. */
    public static final boolean[] EMPTY_BOOLEAN_ARRAY = new boolean[0];
    /** An empty immutable {@code Boolean} array. */
    public static final Boolean[] EMPTY_BOOLEAN_OBJECT_ARRAY = new Boolean[0];

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

    /**
     * Converts an array of primitive booleans to objects.
     *
     * <p>This method returns {@code null} for a {@code null} input array.
     *
     * @param array a {@code boolean} array
     * @return a {@code Boolean} array, {@code null} if null array input
     */
    public static Boolean[] toObject(final boolean[] array) {
        if (array == null) {
            return null;
        } else if (array.length == 0) {
            return EMPTY_BOOLEAN_OBJECT_ARRAY;
        }
        final Boolean[] result = new Boolean[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = (array[i] ? Boolean.TRUE : Boolean.FALSE);
        }
        return result;
    }

    /**
     * Converts an array of primitive bytes to objects.
     *
     * <p>This method returns {@code null} for a {@code null} input array.
     *
     * @param array a {@code byte} array
     * @return a {@code Byte} array, {@code null} if null array input
     */
    public static Byte[] toObject(final byte[] array) {
        if (array == null) {
            return null;
        } else if (array.length == 0) {
            return EMPTY_BYTE_OBJECT_ARRAY;
        }
        final Byte[] result = new Byte[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    /**
     * Converts an array of primitive doubles to objects.
     *
     * <p>This method returns {@code null} for a {@code null} input array.
     *
     * @param array a {@code double} array
     * @return a {@code Double} array, {@code null} if null array input
     */
    public static Double[] toObject(final double[] array) {
        if (array == null) {
            return null;
        } else if (array.length == 0) {
            return EMPTY_DOUBLE_OBJECT_ARRAY;
        }
        final Double[] result = new Double[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    /**
     * Converts an array of primitive floats to objects.
     *
     * <p>This method returns {@code null} for a {@code null} input array.
     *
     * @param array a {@code float} array
     * @return a {@code Float} array, {@code null} if null array input
     */
    public static Float[] toObject(final float[] array) {
        if (array == null) {
            return null;
        } else if (array.length == 0) {
            return EMPTY_FLOAT_OBJECT_ARRAY;
        }
        final Float[] result = new Float[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    /**
     * Converts an array of primitive ints to objects.
     *
     * <p>This method returns {@code null} for a {@code null} input array.
     *
     * @param array an {@code int} array
     * @return an {@code Integer} array, {@code null} if null array input
     */
    public static Integer[] toObject(final int[] array) {
        if (array == null) {
            return null;
        } else if (array.length == 0) {
            return EMPTY_INTEGER_OBJECT_ARRAY;
        }
        final Integer[] result = new Integer[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    /**
     * Converts an array of primitive longs to objects.
     *
     * <p>This method returns {@code null} for a {@code null} input array.
     *
     * @param array a {@code long} array
     * @return a {@code Long} array, {@code null} if null array input
     */
    public static Long[] toObject(final long[] array) {
        if (array == null) {
            return null;
        } else if (array.length == 0) {
            return EMPTY_LONG_OBJECT_ARRAY;
        }
        final Long[] result = new Long[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    /**
     * Converts an array of primitive shorts to objects.
     *
     * <p>This method returns {@code null} for a {@code null} input array.
     *
     * @param array a {@code short} array
     * @return a {@code Short} array, {@code null} if null array input
     */
    public static Short[] toObject(final short[] array) {
        if (array == null) {
            return null;
        } else if (array.length == 0) {
            return EMPTY_SHORT_OBJECT_ARRAY;
        }
        final Short[] result = new Short[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    /**
     * Converts an array of object Booleans to primitives.
     *
     * <p>This method returns {@code null} for a {@code null} input array.
     *
     * @param array a {@code Boolean} array, may be {@code null}
     * @return a {@code boolean} array, {@code null} if null array input
     * @throws NullPointerException if array content is {@code null}
     */
    public static boolean[] toPrimitiveBoolean(final Object[] array) {
        if (array == null) {
            return null;
        } else if (array.length == 0) {
            return EMPTY_BOOLEAN_ARRAY;
        }
        final boolean[] result = new boolean[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = (boolean) array[i];
        }
        return result;
    }

    /**
     * Converts an array of object Bytes to primitives.
     *
     * <p>This method returns {@code null} for a {@code null} input array.
     *
     * @param array a {@code Byte} array, may be {@code null}
     * @return a {@code byte} array, {@code null} if null array input
     * @throws NullPointerException if array content is {@code null}
     */
    public static byte[] toPrimitiveByte(final Object[] array) {
        if (array == null) {
            return null;
        } else if (array.length == 0) {
            return EMPTY_BYTE_ARRAY;
        }
        final byte[] result = new byte[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = (byte) array[i];
        }
        return result;
    }

    /**
     * Converts an array of object Doubles to primitives.
     *
     * <p>This method returns {@code null} for a {@code null} input array.
     *
     * @param array a {@code Double} array, may be {@code null}
     * @return a {@code double} array, {@code null} if null array input
     * @throws NullPointerException if array content is {@code null}
     */
    public static double[] toPrimitiveDouble(final Object[] array) {
        if (array == null) {
            return null;
        } else if (array.length == 0) {
            return EMPTY_DOUBLE_ARRAY;
        }
        final double[] result = new double[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = (double) array[i];
        }
        return result;
    }

    /**
     * Converts an array of object Floats to primitives.
     *
     * <p>This method returns {@code null} for a {@code null} input array.
     *
     * @param array a {@code Float} array, may be {@code null}
     * @return a {@code float} array, {@code null} if null array input
     * @throws NullPointerException if array content is {@code null}
     */
    public static float[] toPrimitiveFloat(final Object[] array) {
        if (array == null) {
            return null;
        } else if (array.length == 0) {
            return EMPTY_FLOAT_ARRAY;
        }
        final float[] result = new float[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = (float) array[i];
        }
        return result;
    }

    /**
     * Converts an array of object Integers to primitives.
     *
     * <p>This method returns {@code null} for a {@code null} input array.
     *
     * @param array a {@code Integer} array, may be {@code null}
     * @return an {@code int} array, {@code null} if null array input
     * @throws NullPointerException if array content is {@code null}
     */
    public static int[] toPrimitiveInteger(final Object[] array) {
        if (array == null) {
            return null;
        } else if (array.length == 0) {
            return EMPTY_INT_ARRAY;
        }
        final int[] result = new int[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = (int) array[i];
        }
        return result;
    }

    /**
     * Converts an array of object Longs to primitives.
     *
     * <p>This method returns {@code null} for a {@code null} input array.
     *
     * @param array a {@code Long} array, may be {@code null}
     * @return a {@code long} array, {@code null} if null array input
     * @throws NullPointerException if array content is {@code null}
     */
    public static long[] toPrimitiveLong(final Object[] array) {
        if (array == null) {
            return null;
        } else if (array.length == 0) {
            return EMPTY_LONG_ARRAY;
        }
        final long[] result = new long[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = (long) array[i];
        }
        return result;
    }

    /**
     * Converts an array of object Shorts to primitives.
     *
     * <p>This method returns {@code null} for a {@code null} input array.
     *
     * @param array a {@code Short} array, may be {@code null}
     * @return a {@code byte} array, {@code null} if null array input
     * @throws NullPointerException if array content is {@code null}
     */
    public static short[] toPrimitiveShort(final Object[] array) {
        if (array == null) {
            return null;
        } else if (array.length == 0) {
            return EMPTY_SHORT_ARRAY;
        }
        final short[] result = new short[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = (short) array[i];
        }
        return result;
    }
}
