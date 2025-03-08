/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.log;

import com.alibaba.fluss.exception.InvalidOffsetException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link com.alibaba.fluss.server.log.OffsetIndex}. */
final class OffsetIndexTest {
    private OffsetIndex index;
    private @TempDir File tempDir;
    private final long baseOffset = 45L;

    @BeforeEach
    public void before() throws IOException {
        index = new OffsetIndex(new File(tempDir, "test.index"), baseOffset, 30 * 8);
    }

    @Test
    void testRandomLookup() {
        assertThat(index.lookup(92L)).isEqualTo(new OffsetPosition(index.baseOffset(), 0));

        // append some random values
        int base = (int) index.baseOffset() + 1;
        int size = index.maxEntries();
        List<Long> randomOffset =
                monotonicList(base, size).stream()
                        .map(Integer::longValue)
                        .collect(Collectors.toList());
        List<Integer> position = monotonicList(0, size);
        for (int i = 0; i < randomOffset.size(); i++) {
            index.append(randomOffset.get(i), position.get(i));
        }

        // should be able to find all those values
        for (int i = 0; i < randomOffset.size(); i++) {
            assertThat(index.lookup(randomOffset.get(i)))
                    .isEqualTo(new OffsetPosition(randomOffset.get(i), position.get(i)));
        }
    }

    @Test
    void lookupExtremeCases() {
        assertThat(index.lookup(index.baseOffset()))
                .isEqualTo(new OffsetPosition(index.baseOffset(), 0));

        for (int i = 0; i < index.maxEntries(); i++) {
            index.append(index.baseOffset() + i + 1, i);
        }
        // check first and last entry
        assertThat(index.lookup(index.baseOffset()))
                .isEqualTo(new OffsetPosition(index.baseOffset(), 0));
        assertThat(index.lookup(index.baseOffset() + index.maxEntries()))
                .isEqualTo(
                        new OffsetPosition(
                                index.baseOffset() + index.maxEntries(), index.maxEntries() - 1));
    }

    @Test
    void testEntry() {
        for (int i = 0; i < index.maxEntries(); i++) {
            index.append(index.baseOffset() + i + 1, i);
        }

        for (int i = 0; i < index.maxEntries(); i++) {
            assertThat(index.entry(i)).isEqualTo(new OffsetPosition(index.baseOffset() + i + 1, i));
        }
    }

    @Test
    void testEntryOverflow() {
        assertThatThrownBy(() -> index.entry(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void appendTooMany() {
        for (int i = 0; i < index.maxEntries(); i++) {
            index.append(index.baseOffset() + i + 1, i);
        }

        assertThatThrownBy(() -> index.append(index.maxEntries() + 1, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Attempt to append to a full index");
    }

    @Test
    void appendOutOfOrder() {
        index.append(51, 0);
        assertThatThrownBy(() -> index.append(50, 1))
                .isInstanceOf(InvalidOffsetException.class)
                .hasMessageContaining("no larger than the last offset appended");
    }

    @Test
    void testFetchUpperBoundOffset() {
        OffsetPosition first = new OffsetPosition(baseOffset, 0);
        OffsetPosition second = new OffsetPosition(baseOffset + 1, 10);
        OffsetPosition third = new OffsetPosition(baseOffset + 2, 23);
        OffsetPosition fourth = new OffsetPosition(baseOffset + 3, 37);
        assertThat(index.fetchUpperBoundOffset(first, 5)).isEqualTo(Optional.empty());
        for (OffsetPosition offsetPosition : Arrays.asList(first, second, third, fourth)) {
            index.append(offsetPosition.getOffset(), offsetPosition.getPosition());
        }

        assertThat(index.fetchUpperBoundOffset(first, 5)).isEqualTo(Optional.of(second));
        assertThat(index.fetchUpperBoundOffset(first, 10)).isEqualTo(Optional.of(second));
        assertThat(index.fetchUpperBoundOffset(first, 23)).isEqualTo(Optional.of(third));
        assertThat(index.fetchUpperBoundOffset(first, 22)).isEqualTo(Optional.of(third));
        assertThat(index.fetchUpperBoundOffset(second, 24)).isEqualTo(Optional.of(fourth));
        assertThat(index.fetchUpperBoundOffset(fourth, 1)).isEqualTo(Optional.empty());
        assertThat(index.fetchUpperBoundOffset(first, 200)).isEqualTo(Optional.empty());
        assertThat(index.fetchUpperBoundOffset(second, 200)).isEqualTo(Optional.empty());
    }

    @Test
    void testReopen() throws IOException {
        OffsetPosition first = new OffsetPosition(51, 0);
        OffsetPosition sec = new OffsetPosition(52, 1);
        index.append(first.getOffset(), first.getPosition());
        index.append(sec.getOffset(), sec.getPosition());
        index.close();

        OffsetIndex indexReopen = new OffsetIndex(index.file(), index.baseOffset());
        assertThat(indexReopen.lookup(first.getOffset())).isEqualTo(first);
        assertThat(indexReopen.lookup(sec.getOffset())).isEqualTo(sec);
        assertThat(indexReopen.lastOffset()).isEqualTo(sec.getOffset());
        assertThat(indexReopen.entries()).isEqualTo(2);

        assertThatThrownBy(() -> index.append(53, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Attempt to append to a full index");
    }

    @Test
    void testTruncate() throws IOException {
        OffsetIndex index = new OffsetIndex(new File(tempDir, "test2.index"), 0L, 10 * 8);
        index.truncate();

        for (int i = 1; i < 10; i++) {
            index.append(i, i);
        }

        // now check the last offset after various truncate points and validate that we can still
        // append to the index.
        index.truncateTo(12);
        assertThat(index.lastOffset()).isEqualTo(9);
        assertThat(index.lookup(10)).isEqualTo(new OffsetPosition(9, 9));

        index.append(10, 10);
        index.truncateTo(10);
        assertThat(index.lookup(10)).isEqualTo(new OffsetPosition(9, 9));
        assertThat(index.lastOffset()).isEqualTo(9);
        index.append(10, 10);

        index.truncateTo(9);
        assertThat(index.lookup(10)).isEqualTo(new OffsetPosition(8, 8));
        assertThat(index.lastOffset()).isEqualTo(8);
        index.append(9, 9);

        index.truncateTo(5);
        assertThat(index.lookup(10)).isEqualTo(new OffsetPosition(4, 4));
        assertThat(index.lastOffset()).isEqualTo(4);
        index.append(5, 5);

        index.truncate();
        assertThat(index.entries()).isEqualTo(0);
        index.append(0, 0);
    }

    @Test
    void testForceUnmap() throws IOException {
        index.forceUnmap();
        assertThatThrownBy(() -> index.lookup(1)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testSanityLastOffsetEqualToBaseOffset() throws IOException {
        // Test index sanity for the case where the last offset appended to the index is equal to
        // the base offset
        long baseOffset = 20L;
        OffsetIndex index = new OffsetIndex(new File(tempDir, "test2.index"), baseOffset, 10 * 8);
        index.append(baseOffset, 0);
        index.sanityCheck();
    }

    public List<Integer> monotonicList(int base, int len) {
        Random rand = new Random(1L);
        List<Integer> vals = new ArrayList<>(len);
        int last = base;
        for (int i = 0; i < len; i++) {
            last += rand.nextInt(15) + 1;
            vals.add(last);
        }
        return vals;
    }
}
