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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link com.alibaba.fluss.utils.BytesUtils}. */
class BytesUtilsTest {

    @Test
    void toArray() {
        byte[] input = {0, 1, 2, 3, 4};
        ByteBuffer buffer = ByteBuffer.wrap(input);
        assertThat(BytesUtils.toArray(buffer)).isEqualTo(input);
        assertThat(buffer.position()).isEqualTo(0);
        assertThat(BytesUtils.toArray(buffer, 1, 2)).isEqualTo(new byte[] {1, 2});
        assertThat(buffer.position()).isEqualTo(0);
        buffer.position(2);
        assertThat(BytesUtils.toArray(buffer)).isEqualTo(new byte[] {2, 3, 4});
        assertThat(buffer.position()).isEqualTo(2);
    }

    @Test
    void toArrayDirectByteBuffer() {
        byte[] input = {0, 1, 2, 3, 4};
        ByteBuffer buffer = ByteBuffer.allocateDirect(5);
        buffer.put(input);
        buffer.rewind();
        assertThat(BytesUtils.toArray(buffer)).isEqualTo(input);
        assertThat(buffer.position()).isEqualTo(0);
        assertThat(BytesUtils.toArray(buffer, 1, 2)).isEqualTo(new byte[] {1, 2});
        assertThat(buffer.position()).isEqualTo(0);
        buffer.position(2);
        assertThat(BytesUtils.toArray(buffer)).isEqualTo(new byte[] {2, 3, 4});
        assertThat(buffer.position()).isEqualTo(2);
    }
}
