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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AbstractIterator}. */
class AbstractIteratorTest {

    /** Simple iterator over a list of integers for testing. */
    private static class IntIterator extends AbstractIterator<Integer> {
        private final List<Integer> data;
        private int index = 0;

        IntIterator(List<Integer> data) {
            this.data = data;
        }

        @Override
        protected Integer makeNext() {
            if (index >= data.size()) {
                return allDone();
            }
            return data.get(index++);
        }
    }

    /** Iterator that throws on first makeNext() call to test FAILED state. */
    private static class FailingIterator extends AbstractIterator<Integer> {
        @Override
        protected Integer makeNext() {
            throw new RuntimeException("Intentional failure");
        }
    }

    @Test
    void testNormalIteration() {
        IntIterator iter = new IntIterator(Arrays.asList(1, 2, 3));
        List<Integer> result = new ArrayList<>();
        while (iter.hasNext()) {
            result.add(iter.next());
        }
        assertThat(result).containsExactly(1, 2, 3);
    }

    @Test
    void testEmptyIterator() {
        IntIterator iter = new IntIterator(new ArrayList<Integer>());
        assertThat(iter.hasNext()).isFalse();
        assertThatThrownBy(iter::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void testPeek() {
        IntIterator iter = new IntIterator(Arrays.asList(10, 20));
        assertThat(iter.peek()).isEqualTo(10);
        // peek does not advance
        assertThat(iter.peek()).isEqualTo(10);
        assertThat(iter.next()).isEqualTo(10);
        assertThat(iter.peek()).isEqualTo(20);
        assertThat(iter.next()).isEqualTo(20);
        assertThatThrownBy(iter::peek).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void testNextAfterExhaustion() {
        IntIterator iter = new IntIterator(Arrays.asList(1));
        iter.next();
        assertThatThrownBy(iter::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void testRemoveThrowsUnsupportedOperationException() {
        IntIterator iter = new IntIterator(Arrays.asList(1));
        assertThatThrownBy(iter::remove).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testMultipleHasNextCallsAreIdempotent() {
        IntIterator iter = new IntIterator(Arrays.asList(5));
        assertThat(iter.hasNext()).isTrue();
        assertThat(iter.hasNext()).isTrue();
        assertThat(iter.next()).isEqualTo(5);
        assertThat(iter.hasNext()).isFalse();
        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    void testFailedStateThrowsIllegalStateException() {
        FailingIterator iter = new FailingIterator();
        // First call triggers FAILED state via RuntimeException in makeNext()
        assertThatThrownBy(iter::hasNext).isInstanceOf(RuntimeException.class);
        // Subsequent calls should throw IllegalStateException (FAILED state)
        assertThatThrownBy(iter::hasNext).isInstanceOf(IllegalStateException.class);
    }
}
