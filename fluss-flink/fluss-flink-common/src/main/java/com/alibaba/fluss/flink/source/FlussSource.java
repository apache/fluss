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

package com.alibaba.fluss.flink.source;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.source.deserializer.FlussDeserializationSchema;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.RowType;

import javax.annotation.Nullable;

/**
 * A Flink DataStream source implementation for reading data from Fluss tables.
 *
 * <p>This class extends the {@code FlinkSource} base class and implements {@code
 * ResultTypeQueryable} to provide type information for Flink's type system.
 *
 * <p>Sample usage:
 *
 * <pre>{@code
 * FlussSource<Order> flussSource = FlussSource.<Order>builder()
 *     .setBootstrapServers("localhost:9092")
 *     .setDatabase("mydb")
 *     .setTable("orders")
 *     .setProjectedFields("orderId", "amount")
 *     .setStartingOffsets(OffsetsInitializer.earliest())
 *     .setScanPartitionDiscoveryIntervalMs(1000L)
 *     .setDeserializationSchema(new OrderDeserializationSchema())
 *     .build();
 *
 * DataStreamSource<Order> stream = env.fromSource(
 *     flussSource,
 *     WatermarkStrategy.noWatermarks(),
 *     "Fluss Source"
 * );
 * }</pre>
 *
 * @param <OUT> The type of records produced by this source
 */
public class FlussSource<OUT> extends FlinkSource<OUT> {
    private static final long serialVersionUID = 1L;

    FlussSource(
            Configuration flussConf,
            TablePath tablePath,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            RowType sourceOutputType,
            @Nullable int[] projectedFields,
            OffsetsInitializer offsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            FlussDeserializationSchema<OUT> deserializationSchema,
            boolean streaming) {
        super(
                flussConf,
                tablePath,
                hasPrimaryKey,
                isPartitioned,
                sourceOutputType,
                projectedFields,
                offsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                deserializationSchema,
                streaming);
    }

    /**
     * Get a FlussSourceBuilder to build a {@link FlussSource}.
     *
     * @return a Fluss source builder.
     */
    public static <T> FlussSourceBuilder<T> builder() {
        return new FlussSourceBuilder<>();
    }

    @VisibleForTesting
    OffsetsInitializer getOffsetsInitializer() {
        return offsetsInitializer;
    }
}
