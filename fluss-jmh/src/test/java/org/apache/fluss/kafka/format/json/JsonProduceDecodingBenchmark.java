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

package org.apache.fluss.kafka.format.json;

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/** Compares complete-tree and streaming JSON conversion throughput and allocation. */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
public class JsonProduceDecodingBenchmark {
    private static final ObjectMapper MAPPER =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    @Param({"flat", "nested", "rescue"})
    private String shape;

    private byte[] payload;
    private JsonToFlussConverter treeConverter;
    private JsonParserToFlussConverters.Converter streamingConverter;
    private boolean rescue;

    @Setup
    public void setup() {
        rescue = false;
        RowType rowType;
        if ("flat".equals(shape)) {
            rowType =
                    DataTypes.ROW(
                            DataTypes.FIELD("id", DataTypes.BIGINT()),
                            DataTypes.FIELD("name", DataTypes.STRING()),
                            DataTypes.FIELD("enabled", DataTypes.BOOLEAN()),
                            DataTypes.FIELD("score", DataTypes.DOUBLE()),
                            DataTypes.FIELD("amount", DataTypes.DECIMAL(20, 4)));
            payload =
                    bytes(
                            "{\"id\":7842,\"name\":\"fluss\",\"enabled\":true,"
                                    + "\"score\":98.125,\"amount\":1234567890.1234}");
        } else {
            RowType itemType =
                    DataTypes.ROW(
                            DataTypes.FIELD("sku", DataTypes.STRING()),
                            DataTypes.FIELD("quantity", DataTypes.INT()),
                            DataTypes.FIELD("price", DataTypes.DECIMAL(12, 2)));
            rowType =
                    DataTypes.ROW(
                            DataTypes.FIELD("id", DataTypes.BIGINT()),
                            DataTypes.FIELD("items", DataTypes.ARRAY(itemType)),
                            DataTypes.FIELD(
                                    "attributes",
                                    DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())));
            payload =
                    bytes(
                            "{\"id\":7842,\"items\":[{\"sku\":\"A-1\",\"quantity\":2,"
                                    + "\"price\":12.50},{\"sku\":\"B-9\",\"quantity\":1,"
                                    + "\"price\":199.99}],\"attributes\":{\"region\":\"eu\","
                                    + "\"channel\":\"mobile\"}}");
            rescue = "rescue".equals(shape);
            if (rescue) {
                payload =
                        bytes(
                                "{\"id\":7842,\"items\":[{\"sku\":\"A-1\","
                                        + "\"quantity\":\"bad\",\"price\":12.50,"
                                        + "\"source\":\"legacy\"}],\"attributes\":{"
                                        + "\"region\":{\"unexpected\":true},"
                                        + "\"channel\":\"mobile\"}}");
            }
        }
        treeConverter = JsonToFlussConverters.create(rowType);
        streamingConverter = JsonParserToFlussConverters.create(rowType);
    }

    @Benchmark
    public void tree(Blackhole blackhole) throws Exception {
        JsonNode node = MAPPER.readTree(payload);
        JsonNode[] rescued = new JsonNode[1];
        blackhole.consume(
                treeConverter.convert(node, "$", rescue ? value -> rescued[0] = value : null));
        if (rescued[0] != null) {
            blackhole.consume(rescued[0].toString());
        }
    }

    @Benchmark
    public void streaming(Blackhole blackhole) throws Exception {
        JsonParserToFlussConverters.RescueValue[] rescued =
                new JsonParserToFlussConverters.RescueValue[1];
        try (JsonParser parser = MAPPER.getFactory().createParser(payload)) {
            parser.nextToken();
            blackhole.consume(
                    streamingConverter.convert(
                            parser, "$", rescue ? value -> rescued[0] = value : null));
        }
        if (rescued[0] != null) {
            blackhole.consume(rescued[0].toJson(MAPPER));
        }
    }

    private static byte[] bytes(String json) {
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .include(JsonProduceDecodingBenchmark.class.getSimpleName())
                        .addProfiler(GCProfiler.class)
                        .build();
        new Runner(options).run();
    }
}
