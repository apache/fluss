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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link com.alibaba.fluss.utils.ArrayUtils}. */
public class ArrayUtilsTest {

    @Test
    void testConcatWithEmptyArray() {
        String[] emptyArray = new String[] {};
        String[] nonEmptyArray = new String[] {"some value"};

        assertThat(ArrayUtils.concat(emptyArray, nonEmptyArray)).isSameAs(nonEmptyArray);

        assertThat(ArrayUtils.concat(nonEmptyArray, emptyArray)).isSameAs(nonEmptyArray);
    }

    @Test
    void testConcatArrays() {
        String[] array1 = new String[] {"A", "B", "C", "D", "E", "F", "G"};
        String[] array2 = new String[] {"1", "2", "3"};

        assertThat(ArrayUtils.concat(array1, array2))
                .isEqualTo(new String[] {"A", "B", "C", "D", "E", "F", "G", "1", "2", "3"});

        assertThat(ArrayUtils.concat(array2, array1))
                .isEqualTo(new String[] {"1", "2", "3", "A", "B", "C", "D", "E", "F", "G"});
    }

    @Test
    void testConcatIntWithEmptyArray() {
        int[] emptyArray = new int[] {};
        int[] nonEmptyArray = new int[] {1};

        assertThat(ArrayUtils.concat(emptyArray, nonEmptyArray)).isSameAs(nonEmptyArray);

        assertThat(ArrayUtils.concat(nonEmptyArray, emptyArray)).isSameAs(nonEmptyArray);
    }

    @Test
    void testConcatIntArrays() {
        int[] array1 = new int[] {1, 2, 3, 4, 5, 6, 7};
        int[] array2 = new int[] {8, 9, 10};

        assertThat(ArrayUtils.concat(array1, array2))
                .isEqualTo(new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

        assertThat(ArrayUtils.concat(array2, array1))
                .isEqualTo(new int[] {8, 9, 10, 1, 2, 3, 4, 5, 6, 7});
    }

    @Test
    void testIsSubset() {
        int[] a = new int[] {1, 2, 3, 4, 5};
        int[] b = new int[] {1, 3, 5};
        int[] c = new int[] {1, 3, 6};
        int[] d = new int[] {};

        assertThat(ArrayUtils.isSubset(a, b)).isTrue();
        assertThat(ArrayUtils.isSubset(a, c)).isFalse();
        assertThat(ArrayUtils.isSubset(a, d)).isTrue();
    }

    @Test
    void testRemoveSet() {
        int[] a = new int[] {1, 2, 3, 4, 5};
        int[] b = new int[] {1, 3, 5};
        int[] c = new int[] {1, 3, 6};
        int[] d = new int[] {};

        assertThat(ArrayUtils.removeSet(a, b)).isEqualTo(new int[] {2, 4});
        assertThat(ArrayUtils.removeSet(a, d)).isEqualTo(new int[] {1, 2, 3, 4, 5});
        assertThatThrownBy(() -> ArrayUtils.removeSet(a, c))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Element not found in the first array");
    }

    @Test
    void testIntersection() {
        int[] a = new int[] {1, 2, 3, 4, 5};
        int[] b = new int[] {1, 3, 5};
        int[] c = new int[] {5, 3, 1};
        int[] d = new int[] {};
        int[] e = new int[] {1, 3, 6};

        assertThat(ArrayUtils.intersection(a, b)).isEqualTo(new int[] {1, 3, 5});
        assertThat(ArrayUtils.intersection(a, c)).isEqualTo(new int[] {1, 3, 5});
        assertThat(ArrayUtils.intersection(a, d)).isEqualTo(new int[] {});
        assertThatThrownBy(() -> ArrayUtils.intersection(a, e))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Element not found in the first array");
    }

    @Test
    void testIsPrefix() {
        int[] a = new int[] {1, 2, 3, 4, 5};
        int[] b = new int[] {1, 2, 3};
        int[] c = new int[] {1, 3, 5};
        int[] d = new int[] {2, 3};
        int[] e = new int[] {};

        assertThat(ArrayUtils.isPrefix(a, b)).isTrue();
        assertThat(ArrayUtils.isPrefix(a, c)).isFalse();
        assertThat(ArrayUtils.isPrefix(a, d)).isFalse();
        assertThat(ArrayUtils.isPrefix(a, e)).isTrue();
    }
}
