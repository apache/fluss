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

package org.apache.fluss.bucketing;

import org.apache.fluss.metadata.DataLakeFormat;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Maintains parity with Rust client tests to guarantee cross-client hashing consistency. */
class BucketingFunctionTest {
    @Test
    void testDefaultBucketing() {
        BucketingFunction defaultBucketing = BucketingFunction.of(null);

        assertThat(defaultBucketing.bucketing(new byte[] {(byte) 0, (byte) 10}, 7)).isEqualTo(1);
        assertThat(
                        defaultBucketing.bucketing(
                                new byte[] {(byte) 0, (byte) 10, (byte) 10, (byte) 10}, 12))
                .isEqualTo(0);
        assertThat(
                        defaultBucketing.bucketing(
                                "2bb87d68-baf9-4e64-90f9-f80910419fa6"
                                        .getBytes(StandardCharsets.UTF_8),
                                16))
                .isEqualTo(6);
        assertThat(
                        defaultBucketing.bucketing(
                                "The quick brown fox jumps over the lazy dog"
                                        .getBytes(StandardCharsets.UTF_8),
                                8))
                .isEqualTo(6);
    }

    @Test
    void testPaimonBucketing() {
        BucketingFunction paimonBucketing = BucketingFunction.of(DataLakeFormat.PAIMON);

        assertThat(paimonBucketing.bucketing(new byte[] {(byte) 0, (byte) 10}, 7)).isEqualTo(1);
        assertThat(
                        paimonBucketing.bucketing(
                                new byte[] {(byte) 0, (byte) 10, (byte) 10, (byte) 10}, 12))
                .isEqualTo(11);
        assertThat(
                        paimonBucketing.bucketing(
                                "2bb87d68-baf9-4e64-90f9-f80910419fa6"
                                        .getBytes(StandardCharsets.UTF_8),
                                16))
                .isEqualTo(12);
        assertThat(
                        paimonBucketing.bucketing(
                                "The quick brown fox jumps over the lazy dog"
                                        .getBytes(StandardCharsets.UTF_8),
                                8))
                .isEqualTo(0);
    }

    @Test
    void testLanceBucketing() {
        BucketingFunction lanceBucketing = BucketingFunction.of(DataLakeFormat.LANCE);

        assertThat(lanceBucketing.bucketing(new byte[] {(byte) 0, (byte) 10}, 7)).isEqualTo(1);
        assertThat(
                        lanceBucketing.bucketing(
                                new byte[] {(byte) 0, (byte) 10, (byte) 10, (byte) 10}, 12))
                .isEqualTo(0);
        assertThat(
                        lanceBucketing.bucketing(
                                "2bb87d68-baf9-4e64-90f9-f80910419fa6"
                                        .getBytes(StandardCharsets.UTF_8),
                                16))
                .isEqualTo(6);
        assertThat(
                        lanceBucketing.bucketing(
                                "The quick brown fox jumps over the lazy dog"
                                        .getBytes(StandardCharsets.UTF_8),
                                8))
                .isEqualTo(6);
    }

    @Test
    void testIcebergBucketing() {
        BucketingFunction icebergBucketing = BucketingFunction.of(DataLakeFormat.ICEBERG);

        assertThat(icebergBucketing.bucketing(new byte[] {(byte) 0, (byte) 10}, 7)).isEqualTo(3);
        assertThat(
                        icebergBucketing.bucketing(
                                new byte[] {(byte) 0, (byte) 10, (byte) 10, (byte) 10}, 12))
                .isEqualTo(4);
        assertThat(
                        icebergBucketing.bucketing(
                                "2bb87d68-baf9-4e64-90f9-f80910419fa6"
                                        .getBytes(StandardCharsets.UTF_8),
                                16))
                .isEqualTo(12);
        assertThat(
                        icebergBucketing.bucketing(
                                "The quick brown fox jumps over the lazy dog"
                                        .getBytes(StandardCharsets.UTF_8),
                                8))
                .isEqualTo(3);
    }
}
