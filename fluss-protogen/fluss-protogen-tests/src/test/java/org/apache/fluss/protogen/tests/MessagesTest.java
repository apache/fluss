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
import org.apache.fluss.rpc.messages.ErrorMessage;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link Messages}. */
public class MessagesTest {

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
    public void testErrorMessages() {
        E e = new E();
        assertThat(e).isInstanceOf(ErrorMessage.class);
        ErrorMessage er = e;
        er.setErrorCode(1).setErrorMessage("error");
        assertThat(er.getErrorCode()).isEqualTo(1);
        assertThat(er.getErrorMessage()).isEqualTo("error");

        E.NE ne = new E.NE();
        assertThat(ne).isInstanceOf(ErrorMessage.class);
        e.setNestedErrors().setErrorCode(2).setErrorMessage("error2");
        assertThat(e.getNestedErrors().getErrorCode()).isEqualTo(2);
        assertThat(e.getNestedErrors().getErrorMessage()).isEqualTo("error2");

        assertThatThrownBy(() -> er.setErrorMessage(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Field 'error_message' cannot be null");
    }

    @Test
    public void testMessages() throws Exception {
        M lpm = new M();
        lpm.setX().setA("a").setB("b");

        assertThat(lpm.getItemsList()).isEqualTo(Collections.emptyList());

        lpm.addItem().setK("k1").setV("v1");

        lpm.addItem().setK("k2").setV("v2").setXx().setN(5);

        assertThat(lpm.hasX()).isTrue();
        assertThat(lpm.getX().hasA()).isTrue();
        assertThat(lpm.getX().hasB()).isTrue();
        assertThat(lpm.getItemsCount()).isEqualTo(2);

        assertThat(lpm.getItemAt(0).getK()).isEqualTo("k1");
        assertThat(lpm.getItemAt(0).getV()).isEqualTo("v1");
        assertThat(lpm.getItemAt(1).getK()).isEqualTo("k2");
        assertThat(lpm.getItemAt(1).getV()).isEqualTo("v2");
        assertThat(lpm.getItemAt(1).getXx().getN()).isEqualTo(5);
        assertThat(lpm.getX().getA()).isEqualTo("a");
        assertThat(lpm.getX().getB()).isEqualTo("b");

        List<M.KV> itemsList = lpm.getItemsList();
        assertThat(itemsList.size()).isEqualTo(2);
        assertThat(itemsList.get(0).getK()).isEqualTo("k1");
        assertThat(itemsList.get(0).getV()).isEqualTo("v1");
        assertThat(itemsList.get(1).getK()).isEqualTo("k2");
        assertThat(itemsList.get(1).getV()).isEqualTo("v2");
        assertThat(itemsList.get(1).getXx().getN()).isEqualTo(5);

        Messages.X pbmx = Messages.X.newBuilder().setA("a").setB("b").build();

        Messages.M.KV.XX pbmXx = Messages.M.KV.XX.newBuilder().setN(5).build();
        Messages.M.KV pbmKv1 = Messages.M.KV.newBuilder().setK("k1").setV("v1").build();
        Messages.M.KV pbmKv2 =
                Messages.M.KV.newBuilder().setK("k2").setV("v2").setXx(pbmXx).build();

        Messages.M pbm =
                Messages.M.newBuilder().setX(pbmx).addItems(pbmKv1).addItems(pbmKv2).build();

        assertThat(lpm.totalSize()).isEqualTo(pbm.getSerializedSize());

        lpm.writeTo(bb1);
        assertThat(bb1.readableBytes()).isEqualTo(lpm.totalSize());

        pbm.writeTo(CodedOutputStream.newInstance(b2));

        assertThat(b2).isEqualTo(b1);

        M parsed = new M();
        parsed.parseFrom(bb1, bb1.readableBytes());

        assertThat(parsed.hasX()).isTrue();
        assertThat(parsed.getX().hasA()).isTrue();
        assertThat(parsed.getX().hasB()).isTrue();
        assertThat(parsed.getItemsCount()).isEqualTo(2);

        assertThat(parsed.getItemAt(0).getK()).isEqualTo("k1");
        assertThat(parsed.getItemAt(0).getV()).isEqualTo("v1");
        assertThat(parsed.getItemAt(1).getK()).isEqualTo("k2");
        assertThat(parsed.getItemAt(1).getV()).isEqualTo("v2");
        assertThat(parsed.getItemAt(1).getXx().getN()).isEqualTo(5);
        assertThat(parsed.getX().getA()).isEqualTo("a");
        assertThat(parsed.getX().getB()).isEqualTo("b");
    }

    @Test
    public void testCopyFrom() throws Exception {
        M lp1 = new M();
        lp1.setX().setA("a").setB("b");

        M lp2 = new M();
        lp2.copyFrom(lp1);

        assertThat(lp2.hasX()).isTrue();
        assertThat(lp2.getX().hasA()).isTrue();
        assertThat(lp2.getX().hasB()).isTrue();
        assertThat(lp2.getX().getA()).isEqualTo("a");
        assertThat(lp2.getX().getB()).isEqualTo("b");
    }

    @Test
    public void testAddAll() throws Exception {
        List<M.KV> kvs = new ArrayList<>();
        kvs.add(new M.KV().setK("k1").setV("v1"));
        kvs.add(new M.KV().setK("k2").setV("v2"));
        kvs.add(new M.KV().setK("k3").setV("v3"));

        M lp = new M().addAllItems(kvs);

        assertThat(lp.getItemsCount()).isEqualTo(3);
        assertThat(lp.getItemAt(0).getK()).isEqualTo("k1");
        assertThat(lp.getItemAt(0).getV()).isEqualTo("v1");
        assertThat(lp.getItemAt(1).getK()).isEqualTo("k2");
        assertThat(lp.getItemAt(1).getV()).isEqualTo("v2");
        assertThat(lp.getItemAt(2).getK()).isEqualTo("k3");
        assertThat(lp.getItemAt(2).getV()).isEqualTo("v3");
    }

    @Test
    public void testClearNestedMessage() throws Exception {
        M m = new M();
        m.setX().setA("a").setB("b");

        m.clear();
        assertThat(m.hasX()).isFalse();

        m.setX();
        assertThat(m.hasX()).isTrue();
        assertThat(m.getX().hasA()).isFalse();
        assertThat(m.getX().hasB()).isFalse();
    }

    @Test
    public void testByteArrays() throws Exception {
        M lp1 = new M();
        lp1.setX().setA("a").setB("b");

        byte[] a1 = lp1.toByteArray();
        assertThat(a1.length).isEqualTo(lp1.totalSize());

        lp1.writeTo(bb1);
        assertThat(Unpooled.wrappedBuffer(a1)).isEqualTo(bb1);

        M parsed = new M();
        parsed.parseFrom(a1);
        assertThat(parsed.hasX()).isTrue();
        assertThat(parsed.getX().hasB()).isTrue();
        assertThat(parsed.getX().getA()).isEqualTo("a");
        assertThat(parsed.getX().getB()).isEqualTo("b");
    }

    @Test
    public void testApiMessage() {
        M lp1 = new M();
        lp1.setX().setA("a").setB("b");

        assertThat(lp1).isInstanceOf(ApiMessage.class);

        ApiMessage am = lp1;

        byte[] a1 = am.toByteArray();
        assertThat(a1.length).isEqualTo(am.totalSize());

        am.writeTo(bb1);
        assertThat(Unpooled.wrappedBuffer(a1)).isEqualTo(bb1);

        M parsed = new M();
        am = parsed;
        am.parseFrom(a1);
        assertThat(parsed.hasX()).isTrue();
        assertThat(parsed.getX().hasB()).isTrue();
        assertThat(parsed.getX().getA()).isEqualTo("a");
        assertThat(parsed.getX().getB()).isEqualTo("b");
    }
}
