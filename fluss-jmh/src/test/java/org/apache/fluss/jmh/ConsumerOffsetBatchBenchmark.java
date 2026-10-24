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

package org.apache.fluss.jmh;

import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.DefaultKvRecordBatch;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordBatchBuilder;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.server.tablet.consumer.group.ConsumerOffsetAndMetadata;
import org.apache.fluss.server.tablet.consumer.group.ConsumerOffsetSchema;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.server.tablet.consumer.group.ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE;

/**
 * Measures consumer-offset key/row encoding and batch construction with different output page
 * sizes.
 *
 * <p>One operation builds one batch, including a fresh key encoder as in {@code
 * GroupManager.buildUpsertBatch}. Request construction, validation and server writes are excluded.
 * Metadata uses ASCII characters, so its length is also its UTF-8 byte size; 4096 bytes matches the
 * default {@code server.consumer-offsets.metadata.max-size} of 4 KiB.
 *
 * <p>The main entry point enables the GC profiler to report allocated bytes per operation alongside
 * average batch construction time. Setup reports page count and page capacity for one
 * representative batch without adding instrumentation to the measured operation. Page capacity
 * excludes object overhead and encoding allocations; use {@code gc.alloc.rate.norm} for total
 * allocated bytes.
 *
 * <p>Pass JMH arguments to narrow a run, for example {@code -p pageSize=100,4096 -p offsetCount=10
 * -p metadataBytes=128}.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ConsumerOffsetBatchBenchmark {

    private static final String GROUP_ID = "consumer-offset-benchmark";
    private static final int SCHEMA_ID = 1;

    @Param({"100", "512", "1024", "4096", "16384"})
    public int pageSize;

    @Param({"1", "10", "100", "1000"})
    public int offsetCount;

    @Param({"0", "128", "4096"})
    public int metadataBytes;

    private Map<TableBucket, ConsumerOffsetAndMetadata> offsets;

    /** Prepares request data and reports output pages outside the timed operation. */
    @Setup(Level.Trial)
    public void setup() throws IOException {
        char[] characters = new char[metadataBytes];
        Arrays.fill(characters, 'm');
        String metadata = new String(characters);
        offsets = new LinkedHashMap<>();
        for (int i = 0; i < offsetCount; i++) {
            offsets.put(new TableBucket(1L, i), new ConsumerOffsetAndMetadata(i, metadata));
        }

        UnmanagedPagedOutputView output = new UnmanagedPagedOutputView(pageSize);
        buildBatch(output);
        int pages = output.getWrittenSegments().size();
        System.out.printf(
                "Page layout: pageSize=%d, offsetCount=%d, metadataBytes=%d, pages=%d, capacityBytes=%d%n",
                pageSize, offsetCount, metadataBytes, pages, (long) pages * pageSize);
    }

    /** Builds a complete batch and returns it for consumption by the JMH harness. */
    @Benchmark
    public KvRecordBatch buildBatch() throws IOException {
        return buildBatch(new UnmanagedPagedOutputView(pageSize));
    }

    /** Runs this benchmark with GC allocation profiling and optional JMH command-line arguments. */
    public static void main(String[] args) throws RunnerException, CommandLineOptionException {
        Options options =
                new OptionsBuilder()
                        .parent(new CommandLineOptions(args))
                        .include(ConsumerOffsetBatchBenchmark.class.getCanonicalName() + ".*")
                        .addProfiler(GCProfiler.class)
                        .build();
        new Runner(options).run();
    }

    private KvRecordBatch buildBatch(UnmanagedPagedOutputView output) throws IOException {
        KeyEncoder keyEncoder = ConsumerOffsetSchema.createOffsetKeyEncoder();
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        SCHEMA_ID, Integer.MAX_VALUE, output, KvFormat.COMPACTED);
        for (Map.Entry<TableBucket, ConsumerOffsetAndMetadata> entry : offsets.entrySet()) {
            TableBucket tableBucket = entry.getKey();
            CompactedRow row =
                    ConsumerOffsetSchema.encodeOffsetRow(
                            GROUP_ID,
                            OFFSET_COMMIT_RECORD_TYPE,
                            tableBucket.getTableId(),
                            tableBucket.getPartitionId(),
                            tableBucket.getBucket(),
                            entry.getValue());
            byte[] key = keyEncoder.encodeKey(row);
            builder.append(key, row);
        }
        return DefaultKvRecordBatch.pointToBytesView(builder.build());
    }
}
