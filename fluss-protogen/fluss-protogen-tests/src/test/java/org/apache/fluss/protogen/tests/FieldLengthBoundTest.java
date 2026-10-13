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

package org.apache.fluss.protogen.tests;

import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that a field declaring more bytes than the buffer holds is rejected, not allocated for. */
public class FieldLengthBoundTest {

    @Test
    public void testBytesField() {
        assertOversizedFieldRejected(new B(), 1); // B.payload
    }

    @Test
    public void testRepeatedBytesField() {
        assertOversizedFieldRejected(new B(), 2); // B.extra_items
    }

    @Test
    public void testStringField() {
        assertOversizedFieldRejected(new S(), 1); // S.id
    }

    @Test
    public void testRepeatedStringField() {
        assertOversizedFieldRejected(new S(), 2); // S.names
    }

    private void assertOversizedFieldRejected(ApiMessage message, int fieldNumber) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte((fieldNumber << 3) | 2); // tag: field number + wire type 2 (length-delimited)
        buf.writeByte(100); // length prefix claims 100 bytes...
        buf.writeByte(0).writeByte(0); // ...but only two follow.

        assertThatThrownBy(() -> message.parseFrom(buf, buf.readableBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("remaining readable bytes");
    }
}
