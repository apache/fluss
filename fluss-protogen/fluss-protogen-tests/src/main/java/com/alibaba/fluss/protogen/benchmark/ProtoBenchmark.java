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

package com.alibaba.fluss.protogen.benchmark;

import com.alibaba.fluss.protogen.tests.AddressBook;
import com.alibaba.fluss.protogen.tests.AddressBookProtos;
import com.alibaba.fluss.protogen.tests.Person;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import com.google.protobuf.CodedOutputStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;

/** Benchmark for comparing protogen with protobuf. */
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 3)
@Fork(value = 1)
public class ProtoBenchmark {

    static final byte[] SERIALIZED;

    static {
        AddressBook ab = new AddressBook();
        Person p1 = ab.addPerson();
        p1.setName("name");
        p1.setEmail("name@example.com");
        p1.setId(5);
        Person.PhoneNumber p1Pn1 = p1.addPhone();
        p1Pn1.setNumber("xxx-zzz-yyyyy");
        p1Pn1.setType(Person.PhoneType.HOME);

        Person.PhoneNumber p1Pn2 = p1.addPhone();
        p1Pn2.setNumber("xxx-zzz-yyyyy");
        p1Pn2.setType(Person.PhoneType.MOBILE);

        Person p2 = ab.addPerson();
        p2.setName("name 2");
        p2.setEmail("name2@example.com");
        p2.setId(6);

        Person.PhoneNumber p2Pn1 = p2.addPhone();
        p2Pn1.setNumber("xxx-zzz-yyyyy");
        p2Pn1.setType(Person.PhoneType.HOME);

        SERIALIZED = new byte[ab.totalSize()];
        ab.writeTo(Unpooled.wrappedBuffer(SERIALIZED).resetWriterIndex());
    }

    private final AddressBook frame = new AddressBook();

    private final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer(1024);
    byte[] data = new byte[1024];
    private final ByteBuf serializeByteBuf = Unpooled.wrappedBuffer(SERIALIZED);

    @Benchmark
    public void protobufSerialize(Blackhole bh) throws Exception {
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

        CodedOutputStream s = CodedOutputStream.newInstance(data);
        pbab.build().writeTo(s);

        bh.consume(pbab);
        bh.consume(s);
    }

    @Benchmark
    public void protogenSerialize(Blackhole bh) {
        frame.clear();

        Person p1 = frame.addPerson();
        p1.setName("name");
        p1.setEmail("name@example.com");
        p1.setId(5);
        Person.PhoneNumber p1Pn1 = p1.addPhone();
        p1Pn1.setNumber("xxx-zzz-yyyyy");
        p1Pn1.setType(Person.PhoneType.HOME);

        Person.PhoneNumber p1Pn2 = p1.addPhone();
        p1Pn2.setNumber("xxx-zzz-yyyyy");
        p1Pn2.setType(Person.PhoneType.MOBILE);

        Person p2 = frame.addPerson();
        p2.setName("name 2");
        p2.setEmail("name2@example.com");
        p2.setId(6);

        Person.PhoneNumber p2Pn1 = p1.addPhone();
        p2Pn1.setNumber("xxx-zzz-yyyyy");
        p2Pn1.setType(Person.PhoneType.HOME);

        frame.writeTo(buffer);
        buffer.clear();

        bh.consume(frame);
    }

    @Benchmark
    public void protobufDeserialize(Blackhole bh) throws Exception {
        AddressBookProtos.AddressBook ab =
                AddressBookProtos.AddressBook.newBuilder().mergeFrom(SERIALIZED).build();
        bh.consume(ab);
    }

    @Benchmark
    public void protogenDeserialize(Blackhole bh) {
        frame.parseFrom(serializeByteBuf, serializeByteBuf.readableBytes());
        serializeByteBuf.resetReaderIndex();
        bh.consume(frame);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + ProtoBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(opt).run();
    }
}
