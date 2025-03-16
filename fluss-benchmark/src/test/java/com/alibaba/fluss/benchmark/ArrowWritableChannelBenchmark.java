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

package com.alibaba.fluss.benchmark;

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.IntVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.LargeVarBinaryVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.VectorUnloader;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.WriteChannel;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.IpcOption;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageSerializer;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.FieldType;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema;
import com.alibaba.fluss.utils.MemorySegmentWritableChannel;

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
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

/**
 * Benchmark for different Arrow input {@link WritableByteChannel}s.
 *
 * <pre>
 * Benchmark                                                      Mode  Cnt   Score    Error   Units
 * ArrowWritableChannelBenchmark.testDirectMemorySegmentChannel  thrpt    3  ≈ 10⁻⁴           ops/us
 * ArrowWritableChannelBenchmark.testMemorySegmentChannel        thrpt    3   0.001 ±  0.001  ops/us
 * ArrowWritableChannelBenchmark.testOutputStreamChannel         thrpt    3   0.001 ±  0.001  ops/us
 * </pre>
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 3)
@Fork(value = 0)
public class ArrowWritableChannelBenchmark {

    private static final List<ArrowRecordBatch> batches = new ArrayList<>();
    private static final int ROW_COUNT = 10_000;
    private static BufferAllocator rootAllocator;
    private static VectorSchemaRoot root;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        rootAllocator = new RootAllocator();
        Field name = new Field("name", FieldType.nullable(new ArrowType.LargeBinary()), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schemaPerson = new Schema(asList(name, age));
        root = VectorSchemaRoot.create(schemaPerson, rootAllocator);
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
            batches.add(vectorUnloader.getRecordBatch());
            root.clear();
        }
    }

    @TearDown
    public void teardown() throws Exception {
        batches.forEach(ArrowRecordBatch::close);
        batches.clear();
        root.close();
        rootAllocator.close();
    }

    @Benchmark
    public void testMemorySegmentChannel() throws IOException {
        MemorySegmentWritableChannel channel =
                new MemorySegmentWritableChannel(
                        MemorySegment.allocateHeapMemory(1024 * 1024 * 10));
        WriteChannel writeChannel = new WriteChannel(channel);
        for (ArrowRecordBatch batch : batches) {
            channel.setPosition(0);
            MessageSerializer.serialize(writeChannel, batch, new IpcOption());
        }
    }

    @Benchmark
    public void testDirectMemorySegmentChannel() throws IOException {
        MemorySegmentWritableChannel channel =
                new MemorySegmentWritableChannel(
                        MemorySegment.allocateOffHeapMemory(1024 * 1024 * 10));
        WriteChannel writeChannel = new WriteChannel(channel);
        for (ArrowRecordBatch batch : batches) {
            channel.setPosition(0);
            MessageSerializer.serialize(writeChannel, batch, new IpcOption());
        }
    }

    @Benchmark
    public void testOutputStreamChannel() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        WriteChannel writeChannel = new WriteChannel(Channels.newChannel(out));
        for (ArrowRecordBatch batch : batches) {
            out.reset();
            MessageSerializer.serialize(writeChannel, batch, new IpcOption());
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(
                                ".*"
                                        + ArrowWritableChannelBenchmark.class.getCanonicalName()
                                        + ".*")
                        .build();

        new Runner(opt).run();
    }
}
