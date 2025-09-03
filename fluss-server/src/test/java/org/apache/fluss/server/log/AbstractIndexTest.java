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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AbstractIndex}. */
public class AbstractIndexTest {

    private @TempDir File tempDir;

    @Test
    public void testResizeInvokeUnmap() throws IOException {
        File testIndex = new File(tempDir, "test.index");
        TestIndex idx = new TestIndex(testIndex, 0L, 100, true);
        MappedByteBuffer oldMmap = idx.mmap();
        assertThat(idx.mmap()).isNotNull();
        assertThat(idx.unmapInvoked).isFalse();

        boolean changed = idx.resize(80);
        assertThat(changed).isTrue();
        // Unmap should have been invoked after resize.
        assertThat(idx.unmapInvoked).isTrue();
        // old mmap should be unmapped.
        assertThat(idx.unmappedBuffer).isEqualTo(oldMmap);
        assertThat(idx.unmappedBuffer).isNotEqualTo(idx.mmap());
    }

    private static class TestIndex extends AbstractIndex {
        private boolean unmapInvoked = false;
        private MappedByteBuffer unmappedBuffer = null;

        public TestIndex(File file, long baseOffset, int maxIndexSize, boolean writable)
                throws IOException {
            super(file, baseOffset, maxIndexSize, writable);
        }

        @Override
        protected int entrySize() {
            return 1;
        }

        @Override
        protected IndexEntry parseEntry(ByteBuffer buffer, int n) {
            return null;
        }

        @Override
        public void sanityCheck() {
            // unused
        }

        @Override
        protected void truncate() {
            // unused
        }

        @Override
        public void truncateTo(long offset) {
            // unused
        }

        @Override
        public void forceUnmap() throws IOException {
            unmapInvoked = true;
            unmappedBuffer = mmap();
        }
    }
}
