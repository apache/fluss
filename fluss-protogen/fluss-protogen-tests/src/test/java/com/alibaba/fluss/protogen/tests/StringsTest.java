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

package com.alibaba.fluss.protogen.tests;

import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Strings}. */
public class StringsTest {

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
    public void testStrings() throws Exception {
        S lps = new S().setId("id");
        lps.addName("a");
        lps.addName("b");
        lps.addName("c");

        assertThat(lps.getId()).isEqualTo("id");
        assertThat(lps.getNamesCount()).isEqualTo(3);
        assertThat(lps.getNameAt(0)).isEqualTo("a");
        assertThat(lps.getNameAt(1)).isEqualTo("b");
        assertThat(lps.getNameAt(2)).isEqualTo("c");

        Strings.S pbs =
                Strings.S
                        .newBuilder()
                        .setId("id")
                        .addNames("a")
                        .addNames("b")
                        .addNames("c")
                        .build();

        assertThat(lps.totalSize()).isEqualTo(pbs.getSerializedSize());

        lps.writeTo(bb1);
        assertThat(bb1.readableBytes()).isEqualTo(lps.totalSize());

        pbs.writeTo(CodedOutputStream.newInstance(b2));

        assertThat(b2).isEqualTo(b1);

        S parsed = new S();
        parsed.parseFrom(bb1, bb1.readableBytes());

        assertThat(parsed.getId()).isEqualTo("id");
        assertThat(parsed.getNamesCount()).isEqualTo(3);
        assertThat(parsed.getNameAt(0)).isEqualTo("a");
        assertThat(parsed.getNameAt(1)).isEqualTo("b");
        assertThat(parsed.getNameAt(2)).isEqualTo("c");
    }

    @Test
    public void testAddAllStrings() throws Exception {
        List<String> strings = new ArrayList<>();
        strings.add("a");
        strings.add("b");
        strings.add("c");

        S lps = new S().setId("id").addAllNames(strings);

        assertThat(lps.getId()).isEqualTo("id");
        assertThat(lps.getNamesCount()).isEqualTo(3);
        assertThat(lps.getNameAt(0)).isEqualTo("a");
        assertThat(lps.getNameAt(1)).isEqualTo("b");
        assertThat(lps.getNameAt(2)).isEqualTo("c");
        assertThat(lps.getNamesList()).isEqualTo(new ArrayList<>(strings));
    }

    @Test
    public void testClearStrings() throws Exception {
        S lps = new S();
        lps.addName("a");
        lps.addName("b");
        lps.addName("c");

        lps.clear();
        lps.addName("d");
        lps.addName("e");
        assertThat(lps.getNamesCount()).isEqualTo(2);
        assertThat(lps.getNameAt(0)).isEqualTo("d");
        assertThat(lps.getNameAt(1)).isEqualTo("e");
    }
}
