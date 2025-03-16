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

package com.alibaba.fluss.protogen.tests;

import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for required fields. */
public class RequiredTest {

    private final byte[] b1 = new byte[4096];
    private final ByteBuf bb1 = Unpooled.wrappedBuffer(b1);
    private final byte[] b2 = new byte[4096];

    @BeforeEach
    public void setup() {
        bb1.clear();
        Arrays.fill(b1, (byte) 0);
        Arrays.fill(b2, (byte) 0);
    }

    @Test
    public void testMissingFields() throws Exception {
        R lpr = new R();
        try {
            lpr.writeTo(bb1);
            fail("Should fail to serialize");
        } catch (IllegalStateException e) {
            // Expected
        }

        assertThat(lpr.hasA()).isFalse();
        assertThat(lpr.hasB()).isFalse();
        assertThat(lpr.hasC()).isFalse();

        assertThat(lpr.getC()).isEqualTo(5); // Default is set

        lpr.setA(1);
        assertThat(lpr.hasA()).isTrue();
        lpr.clearA();
        assertThat(lpr.hasA()).isFalse();
        lpr.setA(2);

        Required.R pbr = Required.R.newBuilder().setA(2).build();

        verify(lpr, pbr);
    }

    @Test
    public void testDeserializeWithMissingFields() throws Exception {
        NR lpnr = new NR().setB(3);

        lpnr.writeTo(bb1);

        R lpr = new R();
        try {
            lpr.parseFrom(bb1, bb1.readableBytes());
            fail("Should fail to de-serialize");
        } catch (IllegalStateException e) {
            // Expected
        }
    }

    @Test
    public void testIgnoreUnknownFields() throws Exception {
        RExt lprext =
                new RExt().setA(1).setB(3).setExtD(10).setExtE(11).setExtF(111L).setExtG("hello");
        int s1 = lprext.totalSize();

        lprext.writeTo(bb1);

        R lpr = new R();
        lpr.parseFrom(bb1, bb1.readableBytes());

        assertThat(lpr.getA()).isEqualTo(1);
        assertThat(lpr.getB()).isEqualTo(3);
        assertThat(lpr.totalSize() < s1).isTrue();
    }

    private void verify(R lpr, Required.R pbr) throws Exception {
        assertThat(lpr.totalSize()).isEqualTo(pbr.getSerializedSize());

        lpr.writeTo(bb1);
        assertThat(bb1.readableBytes()).isEqualTo(lpr.totalSize());

        pbr.writeTo(CodedOutputStream.newInstance(b2));

        assertThat(b2).isEqualTo(b1);

        R parsed = new R();
        parsed.parseFrom(bb1, bb1.readableBytes());

        assertThat(parsed.hasA()).isEqualTo(pbr.hasA());
        assertThat(parsed.hasB()).isEqualTo(pbr.hasB());
        assertThat(parsed.hasC()).isEqualTo(pbr.hasC());

        if (parsed.hasA()) {
            assertThat(parsed.getA()).isEqualTo(pbr.getA());
        }
        if (parsed.hasB()) {
            assertThat(parsed.getB()).isEqualTo(pbr.getB());
        }
        if (parsed.hasC()) {
            assertThat(parsed.getC()).isEqualTo(pbr.getC());
        }
    }
}
