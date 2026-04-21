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

package org.apache.fluss.microbench;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class TeeOutputStreamTest {

    @Test
    void writesToBothStreams() throws IOException {
        ByteArrayOutputStream primary = new ByteArrayOutputStream();
        ByteArrayOutputStream secondary = new ByteArrayOutputStream();
        try (TeeOutputStream tee = new TeeOutputStream(primary, secondary)) {
            tee.write("hello".getBytes());
        }
        assertThat(primary.toByteArray()).isEqualTo("hello".getBytes());
        assertThat(secondary.toByteArray()).isEqualTo("hello".getBytes());
    }

    @Test
    void flushesBothStreams() throws IOException {
        TrackingOutputStream primary = new TrackingOutputStream();
        TrackingOutputStream secondary = new TrackingOutputStream();
        try (TeeOutputStream tee = new TeeOutputStream(primary, secondary)) {
            tee.flush();
        }
        assertThat(primary.flushed).isTrue();
        assertThat(secondary.flushed).isTrue();
    }

    @Test
    void singleByteWrite() throws IOException {
        ByteArrayOutputStream primary = new ByteArrayOutputStream();
        ByteArrayOutputStream secondary = new ByteArrayOutputStream();
        try (TeeOutputStream tee = new TeeOutputStream(primary, secondary)) {
            tee.write(42);
        }
        assertThat(primary.toByteArray()).containsExactly(42);
        assertThat(secondary.toByteArray()).containsExactly(42);
    }

    /** Helper that tracks whether flush was called. */
    private static class TrackingOutputStream extends ByteArrayOutputStream {
        boolean flushed = false;

        @Override
        public void flush() throws IOException {
            super.flush();
            flushed = true;
        }
    }
}
