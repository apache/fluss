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

package org.apache.fluss.fs.bos;

import com.baidubce.services.bos.FakeBosClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BOSOutputStream}, including failure injection on part uploads. */
class BOSOutputStreamTest {
    private static final int PART = 8;

    private static byte[] bytes(int n) {
        byte[] b = new byte[n];
        new Random(n).nextBytes(b);
        return b;
    }

    @Test
    void smallObjectIsWrittenWithASinglePut() throws IOException {
        FakeBosClient c = new FakeBosClient();
        byte[] data = bytes(5);
        BOSOutputStream out = new BOSOutputStream(c, "test-bucket", "k", PART);
        out.write(data, 0, data.length);
        assertThat(out.getPos()).isEqualTo(5);
        assertThat(c.objects.containsKey("k")).as("nothing is visible before close()").isFalse();
        out.close();
        assertThat(c.objects.get("k")).isEqualTo(data);
        assertThat(c.calls).isEqualTo(Arrays.asList("put:k"));
    }

    @Test
    void emptyObject() throws IOException {
        FakeBosClient c = new FakeBosClient();
        new BOSOutputStream(c, "test-bucket", "k", PART).close();
        assertThat(c.objects.get("k").length).isEqualTo(0);
    }

    @Test
    void largeObjectUsesMultipartAndKeepsEveryByte() throws IOException {
        FakeBosClient c = new FakeBosClient();
        byte[] data = bytes(PART * 3 + 3); // 3 full parts + a tail
        BOSOutputStream out = new BOSOutputStream(c, "test-bucket", "k", PART);
        for (int i = 0; i < data.length; i += 5) { // odd chunking
            out.write(data, i, Math.min(5, data.length - i));
        }
        out.close();
        assertThat(c.objects.get("k")).isEqualTo(data);
        assertThat(c.calls)
                .isEqualTo(
                        Arrays.asList(
                                "init:k", "part:1", "part:2", "part:3", "part:4", "complete:4"));
    }

    @Test
    void exactMultipleOfPartSizeHasNoEmptyTailPart() throws IOException {
        FakeBosClient c = new FakeBosClient();
        byte[] data = bytes(PART * 2);
        try (BOSOutputStream out = new BOSOutputStream(c, "test-bucket", "k", PART)) {
            out.write(data, 0, data.length);
        }
        assertThat(c.objects.get("k")).isEqualTo(data);
        assertThat(c.calls).isEqualTo(Arrays.asList("init:k", "part:1", "part:2", "complete:2"));
    }

    @Test
    void bufferGrowsOnDemandAndStillKeepsEveryByte() throws IOException {
        // part size well above the initial buffer so that the buffer has to grow several times
        int part = 300_000;
        FakeBosClient c = new FakeBosClient();
        byte[] data = bytes(part * 2 + 17);
        try (BOSOutputStream out = new BOSOutputStream(c, "test-bucket", "k", part)) {
            out.write(data[0]); // single byte first
            for (int i = 1; i < data.length; i += 7_777) {
                out.write(data, i, Math.min(7_777, data.length - i));
            }
            assertThat(out.getPos()).isEqualTo(data.length);
        }
        assertThat(c.objects.get("k")).isEqualTo(data);
        assertThat(c.calls)
                .isEqualTo(Arrays.asList("init:k", "part:1", "part:2", "part:3", "complete:3"));
    }

    @Test
    void failedPartUploadMustNeverPublishATruncatedObject() throws Exception {
        FakeBosClient c = new FakeBosClient();
        c.failUploadPartNumber = 2;
        byte[] data = bytes(PART * 2 + 4);
        BOSOutputStream out = new BOSOutputStream(c, "test-bucket", "k", PART);
        // try-with-resources semantics: the write fails, then close() is still called by the caller
        assertThatThrownBy(() -> out.write(data, 0, data.length)).isInstanceOf(IOException.class);
        try {
            out.close();
        } catch (IOException expected) {
            // fine: close may report the earlier failure
        }
        assertThat(c.objects.containsKey("k"))
                .as("a failed write must not publish any object: " + c.calls)
                .isFalse();
        assertThat(c.calls.contains("abort")).isTrue();
        assertThat(
                        c.calls.stream()
                                .anyMatch(s -> s.startsWith("put:") || s.startsWith("complete:")))
                .as(c.calls.toString())
                .isFalse();
    }

    @Test
    void closeAfterAFailedWriteReportsTheFailure() {
        FakeBosClient c = new FakeBosClient();
        c.failUploadPartNumber = 1;
        BOSOutputStream out = new BOSOutputStream(c, "test-bucket", "k", PART);
        assertThatThrownBy(() -> out.write(bytes(PART + 1), 0, PART + 1))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(out::close).isInstanceOf(IOException.class);
        assertThat(c.objects.containsKey("k")).isFalse();
    }

    @Test
    void failedCompleteAbortsTheUpload() {
        FakeBosClient c = new FakeBosClient();
        c.failComplete = true;
        BOSOutputStream out = new BOSOutputStream(c, "test-bucket", "k", PART);
        assertThatThrownBy(
                        () -> {
                            out.write(bytes(PART * 2), 0, PART * 2);
                            out.close();
                        })
                .isInstanceOf(IOException.class);
        assertThat(c.calls.contains("abort")).isTrue();
        assertThat(c.objects.containsKey("k")).isFalse();
    }

    @Test
    void failedSinglePutIsReportedAndNothingIsPublished() {
        FakeBosClient c = new FakeBosClient();
        c.failPut = true;
        BOSOutputStream out = new BOSOutputStream(c, "test-bucket", "k", PART);
        assertThatThrownBy(
                        () -> {
                            out.write(bytes(3), 0, 3);
                            out.close();
                        })
                .isInstanceOf(IOException.class);
        assertThat(c.objects.containsKey("k")).isFalse();
    }

    @Test
    void closeIsIdempotentAndWriteAfterCloseFails() throws IOException {
        FakeBosClient c = new FakeBosClient();
        BOSOutputStream out = new BOSOutputStream(c, "test-bucket", "k", PART);
        out.write(1);
        out.close();
        out.close();
        assertThat(c.calls.stream().filter(s -> s.startsWith("put:")).count()).isEqualTo(1);
        assertThatThrownBy(() -> out.write(2)).isInstanceOf(IOException.class);
    }
}
