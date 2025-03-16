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

/** Tests for {@link AddressBook}. */
public class AddressBookTest {

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
    public void testAddressBook() throws Exception {
        AddressBook ab = new AddressBook();
        Person p1 = ab.addPerson().setName("name 1").setEmail("name1@example.com").setId(5);
        p1.addPhone().setNumber("xxx-zzz-1111").setType(Person.PhoneType.HOME);
        p1.addPhone().setNumber("xxx-zzz-2222").setType(Person.PhoneType.MOBILE);

        Person p2 = ab.addPerson().setName("name 2").setEmail("name2@example.com").setId(6);
        p2.addPhone().setNumber("xxx-zzz-2222").setType(Person.PhoneType.HOME);

        assertThat(ab.getPersonsCount()).isEqualTo(2);
        assertThat(ab.getPersonAt(0).getName()).isEqualTo("name 1");
        assertThat(ab.getPersonAt(0).getEmail()).isEqualTo("name1@example.com");
        assertThat(ab.getPersonAt(0).getId()).isEqualTo(5);
        assertThat(ab.getPersonAt(0).getPhoneAt(0).getNumber()).isEqualTo("xxx-zzz-1111");
        assertThat(ab.getPersonAt(0).getPhoneAt(0).getType()).isEqualTo(Person.PhoneType.HOME);
        assertThat(ab.getPersonAt(1).getName()).isEqualTo("name 2");
        assertThat(ab.getPersonAt(1).getEmail()).isEqualTo("name2@example.com");
        assertThat(ab.getPersonAt(1).getId()).isEqualTo(6);
        assertThat(ab.getPersonAt(1).getPhoneAt(0).getNumber()).isEqualTo("xxx-zzz-2222");
        assertThat(ab.getPersonAt(0).getPhoneAt(0).getType()).isEqualTo(Person.PhoneType.HOME);

        AddressBookProtos.AddressBook.Builder pbab = AddressBookProtos.AddressBook.newBuilder();
        AddressBookProtos.Person.Builder pbP1 = AddressBookProtos.Person.newBuilder();
        pbP1.setName("name 1");
        pbP1.setEmail("name1@example.com");
        pbP1.setId(5);
        AddressBookProtos.Person.PhoneNumber.Builder pb1Pn1 =
                AddressBookProtos.Person.PhoneNumber.newBuilder();
        pb1Pn1.setNumber("xxx-zzz-1111");
        pb1Pn1.setType(AddressBookProtos.Person.PhoneType.HOME);

        AddressBookProtos.Person.PhoneNumber.Builder pb1Pn2 =
                AddressBookProtos.Person.PhoneNumber.newBuilder();
        pb1Pn2.setNumber("xxx-zzz-2222");
        pb1Pn2.setType(AddressBookProtos.Person.PhoneType.MOBILE);

        pbP1.addPhone(pb1Pn1);
        pbP1.addPhone(pb1Pn2);

        AddressBookProtos.Person.Builder pbP2 = AddressBookProtos.Person.newBuilder();
        pbP2.setName("name 2");
        pbP2.setEmail("name2@example.com");
        pbP2.setId(6);

        AddressBookProtos.Person.PhoneNumber.Builder pb2Pn1 =
                AddressBookProtos.Person.PhoneNumber.newBuilder();
        pb2Pn1.setNumber("xxx-zzz-2222");
        pb2Pn1.setType(AddressBookProtos.Person.PhoneType.HOME);

        pbP2.addPhone(pb2Pn1);

        pbab.addPerson(pbP1);
        pbab.addPerson(pbP2);

        assertThat(ab.totalSize()).isEqualTo(pbab.build().getSerializedSize());

        ab.writeTo(bb1);
        assertThat(bb1.readableBytes()).isEqualTo(ab.totalSize());

        pbab.build().writeTo(CodedOutputStream.newInstance(b2));

        assertThat(b2).isEqualTo(b1);

        AddressBook parsed = new AddressBook();
        parsed.parseFrom(bb1, bb1.readableBytes());

        assertThat(parsed.getPersonsCount()).isEqualTo(2);
        assertThat(parsed.getPersonAt(0).getName()).isEqualTo("name 1");
        assertThat(parsed.getPersonAt(0).getEmail()).isEqualTo("name1@example.com");
        assertThat(parsed.getPersonAt(0).getId()).isEqualTo(5);
        assertThat(parsed.getPersonAt(0).getPhoneAt(0).getNumber()).isEqualTo("xxx-zzz-1111");
        assertThat(parsed.getPersonAt(0).getPhoneAt(0).getType()).isEqualTo(Person.PhoneType.HOME);
        assertThat(parsed.getPersonAt(1).getName()).isEqualTo("name 2");
        assertThat(parsed.getPersonAt(1).getEmail()).isEqualTo("name2@example.com");
        assertThat(parsed.getPersonAt(1).getId()).isEqualTo(6);
        assertThat(parsed.getPersonAt(1).getPhoneAt(0).getNumber()).isEqualTo("xxx-zzz-2222");
        assertThat(parsed.getPersonAt(1).getPhoneAt(0).getType()).isEqualTo(Person.PhoneType.HOME);
    }
}
