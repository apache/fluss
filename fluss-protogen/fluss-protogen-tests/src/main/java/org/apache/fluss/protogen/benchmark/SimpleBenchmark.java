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

package org.apache.fluss.protogen.benchmark;

import org.apache.fluss.protogen.tests.Frame;
import org.apache.fluss.protogen.tests.Point;
import org.apache.fluss.protogen.tests.PointFrame;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;

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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** Benchmark for comparing protogen with protobuf. */
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 3)
@Fork(value = 1)
public class SimpleBenchmark {

    static final byte[] SERIALIZED;

    static {
        PointFrame.Point.Builder b = PointFrame.Point.newBuilder();
        b.setX(1);
        b.setY(2);
        b.setZ(3);

        PointFrame.Frame.Builder frameBuilder = PointFrame.Frame.newBuilder();
        frameBuilder.setName("xyz");
        frameBuilder.setPoint(b.build());

        PointFrame.Frame frame = frameBuilder.build();
        int size = frame.getSerializedSize();

        SERIALIZED = new byte[size];
        CodedOutputStream s = CodedOutputStream.newInstance(SERIALIZED);
        try {
            frame.writeTo(s);
        } catch (IOException ignored) {
        }
    }

    byte[] data = new byte[1024];
    Frame frame = new Frame();
    ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer(1024);
    private final ByteBuf serializeByteBuf = Unpooled.wrappedBuffer(SERIALIZED);

    @Benchmark
    public void protobufSerialize(Blackhole bh) throws Exception {
        PointFrame.Point.Builder b = PointFrame.Point.newBuilder();
        b.setX(1);
        b.setY(2);
        b.setZ(3);

        PointFrame.Frame.Builder frameBuilder = PointFrame.Frame.newBuilder();
        frameBuilder.setName("xyz");
        frameBuilder.setPoint(b.build());

        PointFrame.Frame frame = frameBuilder.build();

        CodedOutputStream s = CodedOutputStream.newInstance(data);
        frame.writeTo(s);
        bh.consume(b);
        bh.consume(s);
        bh.consume(frame);
    }

    @Benchmark
    public void protogenSerialize(Blackhole bh) {
        frame.clear();
        Point p = frame.setPoint();
        p.setX(1);
        p.setY(2);
        p.setZ(3);
        frame.setName("xyz");

        p.writeTo(buffer);
        buffer.clear();

        bh.consume(p);
    }

    @Benchmark
    public void protobufDeserialize(Blackhole bh) throws Exception {
        PointFrame.Frame.Builder b = PointFrame.Frame.newBuilder().mergeFrom(SERIALIZED);
        PointFrame.Frame f = b.build();
        f.getName();
        bh.consume(f);
    }

    @Benchmark
    public void protogenDeserialize(Blackhole bh) {
        frame.parseFrom(serializeByteBuf, serializeByteBuf.readableBytes());
        serializeByteBuf.resetReaderIndex();
        bh.consume(frame);
    }

    @Benchmark
    public void protogenDeserializeReadString(Blackhole bh) {
        frame.parseFrom(serializeByteBuf, serializeByteBuf.readableBytes());
        bh.consume(frame.getName());
        serializeByteBuf.resetReaderIndex();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + SimpleBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(opt).run();
    }
}
