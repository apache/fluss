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

package org.apache.fluss.server.log;

import org.apache.fluss.exception.InvalidOffsetException;
import org.apache.fluss.server.exception.CorruptIndexException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link org.apache.fluss.server.log.TimeIndex}. */
class TimeIndexTest {
    private TimeIndex index;
    private @TempDir File tempDir;
    private final int maxEntries = 30;
    private final long baseOffset = 45L;

    @BeforeEach
    public void before() throws IOException {
        index = new TimeIndex(new File(tempDir, "test.index"), baseOffset, maxEntries * 12);
    }

    @Test
    void testLookup() {
        // empty time index.
        assertThat(index.lookup(100L)).isEqualTo(new TimestampOffset(-1L, baseOffset));

        // Add several time index entries.
        appendEntries(maxEntries - 1);

        // look for timestamp smaller that the earliest entry.
        assertThat(index.lookup(9L)).isEqualTo(new TimestampOffset(-1, baseOffset));
        // look for timestamp in the middle of two entries.
        assertThat(index.lookup(25L)).isEqualTo(new TimestampOffset(20L, 65L));
        // look for timestamp same as the one in the entry.
        assertThat(index.lookup(30L)).isEqualTo(new TimestampOffset(30L, 75L));
    }

    @Test
    void testEntry() {
        appendEntries(maxEntries - 1);
        assertThat(index.entry(0)).isEqualTo(new TimestampOffset(10L, 55L));
        assertThat(index.entry(1)).isEqualTo(new TimestampOffset(20L, 65L));
        assertThat(index.entry(2)).isEqualTo(new TimestampOffset(30L, 75L));
        assertThat(index.entry(3)).isEqualTo(new TimestampOffset(40L, 85L));
    }

    @Test
    void testEntryOverflow() {
        assertThatThrownBy(() -> index.entry(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testTruncate() {
        appendEntries(maxEntries - 1);
        index.truncate();
        assertThat(index.entries()).isEqualTo(0);

        appendEntries(maxEntries - 1);
        index.truncateTo(10L + baseOffset);
        assertThat(index.entries()).isEqualTo(0);
    }

    @Test
    void testAppend() {
        appendEntries(maxEntries - 1);
        assertThatThrownBy(() -> index.maybeAppend(10000L, 1000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Attempt to append to a full time index (size = 29).");
        assertThatThrownBy(() -> index.maybeAppend(10000L, (maxEntries - 2) * 10, true))
                .isInstanceOf(InvalidOffsetException.class)
                .hasMessageContaining(
                        "Attempt to append an offset (280) to slot 29 no larger "
                                + "than the last offset appended (335)");
        index.maybeAppend(10000L, 1000L, true);
    }

    @Test
    void testSanityCheck() throws IOException {
        index.sanityCheck();
        appendEntries(5);
        TimestampOffset firstEntry = index.entry(0);
        index.sanityCheck();
        index.close();

        final AtomicBoolean shouldCorruptOffset = new AtomicBoolean(false);
        final AtomicBoolean shouldCorruptTimestamp = new AtomicBoolean(false);
        final AtomicBoolean shouldCorruptLength = new AtomicBoolean(false);
        index =
                new TimeIndex(index.file(), baseOffset, maxEntries * 12) {
                    @Override
                    public TimestampOffset lastEntry() {
                        TimestampOffset superLastEntry = super.lastEntry();
                        long offset =
                                shouldCorruptOffset.get()
                                        ? this.baseOffset() - 1
                                        : superLastEntry.offset;
                        long timestamp =
                                shouldCorruptTimestamp.get()
                                        ? firstEntry.timestamp - 1
                                        : superLastEntry.timestamp;
                        return new TimestampOffset(timestamp, offset);
                    }

                    @Override
                    public long length() {
                        long superLength = super.length();
                        return shouldCorruptLength.get() ? superLength - 1 : superLength;
                    }
                };

        shouldCorruptLength.set(true);
        assertThatThrownBy(index::sanityCheck).isInstanceOf(CorruptIndexException.class);
        shouldCorruptLength.set(false);

        shouldCorruptOffset.set(true);
        assertThatThrownBy(index::sanityCheck).isInstanceOf(CorruptIndexException.class);
        shouldCorruptOffset.set(false);

        shouldCorruptTimestamp.set(true);
        assertThatThrownBy(index::sanityCheck).isInstanceOf(CorruptIndexException.class);
        shouldCorruptTimestamp.set(false);

        index.sanityCheck();
        index.close();
    }

    private void appendEntries(int numEntries) {
        for (int i = 1; i <= numEntries; i++) {
            index.maybeAppend(i * 10L, baseOffset + i * 10L);
        }
    }
}
