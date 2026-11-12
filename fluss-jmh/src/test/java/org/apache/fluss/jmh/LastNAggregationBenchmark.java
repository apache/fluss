/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.jmh;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.BinaryRow.BinaryRowFormat;
import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.array.CompactedArray;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.serializer.ArraySerializer;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldLastNAgg;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.ByteArrayWrapper;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Compares array aggregation and subsequent row encoding with object collection. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(2)
public class LastNAggregationBenchmark {

    @Param({"10", "100", "1000"})
    public int size;

    @Param({"BIGINT", "BIGINT_REPEATED_HALVES", "SHORT_STRING", "LONG_STRING", "BYTES", "MAP"})
    public String elementType;

    @Param({"NEW", "TAIL", "MIDDLE", "BATCH_UNIQUE", "BATCH_REPEATED"})
    public String pattern;

    private FieldLastNAgg plain;
    private FieldLastNAgg distinct;
    private InternalArray.ElementGetter getter;
    private RowEncoder encoder;
    private DataType type;
    private BinaryArray older;
    private BinaryArray newer;

    /** Builds independent input arrays outside the measured operations. */
    @Setup
    public void setup() {
        switch (elementType) {
            case "BIGINT":
            case "BIGINT_REPEATED_HALVES":
                type = DataTypes.BIGINT();
                break;
            case "BYTES":
                type = DataTypes.BYTES();
                break;
            case "MAP":
                type = DataTypes.MAP(DataTypes.INT(), DataTypes.INT());
                break;
            default:
                type = DataTypes.STRING();
        }
        plain = new FieldLastNAgg(DataTypes.ARRAY(type), size);
        distinct = new FieldLastNAgg(DataTypes.ARRAY(type), size, true);
        getter = InternalArray.createElementGetter(type);
        ArraySerializer serializer = new ArraySerializer(type, BinaryRowFormat.COMPACTED);
        encoder = RowEncoder.create(KvFormat.COMPACTED, new DataType[] {DataTypes.ARRAY(type)});
        Object[] previous = new Object[size];
        for (int i = 0; i < previous.length; i++) {
            previous[i] = value(i);
        }
        older = snapshot(serializer.toBinaryArray(new GenericArray(previous)));
        Object[] input = new Object[pattern.startsWith("BATCH") ? size * 4 : 1];
        for (int i = 0; i < input.length; i++) {
            int id =
                    pattern.equals("TAIL")
                            ? size - 1
                            : pattern.equals("MIDDLE")
                                    ? size / 2
                                    : pattern.equals("BATCH_REPEATED") ? i % 3 : size + i;
            input[i] = value(id);
        }
        newer = snapshot(serializer.toBinaryArray(new GenericArray(input)));
    }

    private Object value(int id) {
        if (elementType.equals("BIGINT_REPEATED_HALVES")) {
            return ((long) id << 32) | (id & 0xffffffffL);
        }
        if (elementType.equals("MAP")) {
            return GenericMap.of(id, id);
        }
        if (elementType.equals("BIGINT")) {
            return (long) id;
        }
        String text =
                elementType.equals("SHORT_STRING")
                        ? "v" + id
                        : "a value stored outside the fixed length slot " + id;
        BinaryString string = BinaryString.fromString(text);
        return elementType.equals("BYTES") ? text.getBytes(StandardCharsets.UTF_8) : string;
    }

    /** Measures the existing aggregation and row encoding without deduplication. */
    @Benchmark
    public Object plain() {
        return encode(plain.agg(older, newer));
    }

    /** Measures deduplication without materializing binary elements. */
    @Benchmark
    public Object distinct() {
        return encode(distinct.agg(older, newer));
    }

    /** Measures object collection followed by the same row encoder as the binary paths. */
    @Benchmark
    public Object objectCollection() {
        Set<Object> seen = new HashSet<>(size * 2);
        Object[] selected = new Object[size];
        int count = 0;
        for (InternalArray source : new InternalArray[] {newer, older}) {
            for (int i = source.size() - 1; i >= 0 && count < size; i--) {
                Object value = getter.getElementOrNull(source, i);
                Object key = value instanceof byte[] ? new ByteArrayWrapper((byte[]) value) : value;
                if (seen.add(key)) {
                    selected[count++] = value;
                }
            }
        }
        Object[] result = new Object[count];
        for (int i = 0; i < count; i++) {
            result[i] = selected[count - i - 1];
        }
        return encode(new GenericArray(result));
    }

    private Object encode(Object array) {
        encoder.startNewRow();
        encoder.encodeField(0, array);
        return encoder.finishRow();
    }

    /** Releases the row encoder. */
    @TearDown
    public void tearDown() throws Exception {
        encoder.close();
    }

    private BinaryArray snapshot(BinaryArray array) {
        byte[] bytes =
                BinarySegmentUtils.copyToBytes(
                        array.getSegments(), array.getOffset(), array.getSizeInBytes());
        BinaryArray copy = new CompactedArray(type);
        copy.pointTo(MemorySegment.wrap(bytes), 0, bytes.length);
        return copy;
    }
}
