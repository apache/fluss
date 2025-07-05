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

package com.alibaba.fluss.examples;

import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Example of using FlussSource to read data from a Fluss database in a Flink application.
 * https://alibaba.github.io/fluss-docs/docs/engine-flink/getting-started/
 */
public class DataStreamExample {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Create a FlussSource using the builder pattern
        FlussSource<Order> flussSource =
                FlussSource.<Order>builder()
                        .setBootstrapServers("localhost:9123")
                        .setDatabase("my_db")
                        .setTable("orders")
                        .setProjectedFields("orderId", "itemId", "amount", "address")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new OrderDeserializationSchema())
                        .build();

        DataStreamSource<Order> stream =
                env.fromSource(
                        flussSource, WatermarkStrategy.noWatermarks(), "Fluss Orders Source");

        stream.print();
    }
}
