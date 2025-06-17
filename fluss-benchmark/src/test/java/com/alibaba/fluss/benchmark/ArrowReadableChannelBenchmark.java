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

package com.alibaba.fluss.benchmark;

import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.IntVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.LargeVarBinaryVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.VectorLoader;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.VectorUnloader;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.ReadChannel;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.WriteChannel;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.IpcOption;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageSerializer;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.FieldType;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema;
import com.alibaba.fluss.shaded.zookeeper3.org.apache.zookeeper.server.ByteBufferInputStream;
import com.alibaba.fluss.utils.ByteBufferReadableChannel;

import org.apache.commons.lang3.RandomUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.alibaba.fluss.utils.Preconditions.checkState;
import static java.util.Arrays.asList;

/**
 * Benchmark for different Arrow input {@link ReadableByteChannel}s.
 *
 * <pre>
 * Benchmark                                              Mode  Cnt  Score    Error   Units
 * ArrowReadableChannelBenchmark.testByteBufferChannel   thrpt    3  0.003 ±  0.001  ops/us
 * ArrowReadableChannelBenchmark.testInputStreamChannel  thrpt    3  0.001 ±  0.001  ops/us
 * </pre>
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 3)
@Fork(value = 0)
public class ArrowReadableChannelBenchmark {

    private static final List<ByteBuffer> buffers = new ArrayList<>();
    private static final int ROW_COUNT = 10_000;
    private static BufferAllocator rootAllocator;
    private static VectorSchemaRoot root;
    private static VectorLoader vectorLoader;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        rootAllocator = new RootAllocator();
        Field name = new Field("name", FieldType.nullable(new ArrowType.LargeBinary()), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schemaPerson = new Schema(asList(name, age));
        root = VectorSchemaRoot.create(schemaPerson, rootAllocator);
        vectorLoader = new VectorLoader(root);
        VectorUnloader vectorUnloader = new VectorUnloader(root);

        // allocate 10 * 10MB buffers
        for (int i = 0; i < 10; i++) {
            LargeVarBinaryVector nameVector = (LargeVarBinaryVector) root.getVector("name");
            nameVector.allocateNew(ROW_COUNT * 100);
            IntVector ageVector = (IntVector) root.getVector("age");
            ageVector.allocateNew(ROW_COUNT * 4);
            for (int j = 0; j < ROW_COUNT; j++) {
                nameVector.setSafe(j, RandomUtils.nextBytes(100));
                ageVector.setSafe(j, j);
            }
            root.setRowCount(ROW_COUNT);

            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                ArrowRecordBatch arrowRecordBatch = vectorUnloader.getRecordBatch();
                MessageSerializer.serialize(
                        new WriteChannel(Channels.newChannel(out)),
                        arrowRecordBatch,
                        new IpcOption());
                byte[] byteArray = out.toByteArray();
                ByteBuffer buf = ByteBuffer.allocateDirect(byteArray.length);
                buf.put(byteArray);
                buffers.add(buf);
                arrowRecordBatch.close();
                root.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @TearDown
    public void teardown() throws Exception {
        buffers.clear();
        root.close();
        rootAllocator.close();
    }

    @Benchmark
    public void testByteBufferChannel() throws IOException {
        for (ByteBuffer buffer : buffers) {
            buffer.rewind();
            ByteBufferReadableChannel channel = new ByteBufferReadableChannel(buffer);
            try (ArrowRecordBatch batch =
                    MessageSerializer.deserializeRecordBatch(
                            new ReadChannel(channel), rootAllocator)) {
                vectorLoader.load(batch);
                checkState(root.getRowCount() == ROW_COUNT);
            }
        }
    }

    @Benchmark
    public void testInputStreamChannel() throws IOException {
        for (ByteBuffer buffer : buffers) {
            buffer.rewind();
            ByteBufferInputStream inputStream = new ByteBufferInputStream(buffer);
            ReadableByteChannel channel = Channels.newChannel(inputStream);
            try (ArrowRecordBatch batch =
                    MessageSerializer.deserializeRecordBatch(
                            new ReadChannel(channel), rootAllocator)) {
                vectorLoader.load(batch);
                checkState(root.getRowCount() == ROW_COUNT);
            }
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(
                                ".*"
                                        + ArrowReadableChannelBenchmark.class.getCanonicalName()
                                        + ".*")
                        .build();

        new Runner(opt).run();
    }
}
