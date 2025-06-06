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

package com.alibaba.fluss.flink.sink;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.flink.sink.writer.FlinkSinkWriter;

/**
 * FlussSink is a specialized Flink sink for writing data to Fluss.
 *
 * <p>This class extends {@link FlinkSink} and provides a builder for constructing Fluss sink
 * instances with custom configurations. It is intended to be used as the main entry point for
 * integrating Fluss as a sink in Flink data pipelines.
 *
 * @param <InputT> the type of input elements accepted by the sink
 * @since 0.7
 */
@PublicEvolving
public class FlussSink<InputT> extends FlinkSink<InputT> {
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a FlussSink with the given SinkWriterBuilder.
     *
     * @param builder the builder used to create the sink writer
     */
    FlussSink(SinkWriterBuilder<? extends FlinkSinkWriter<InputT>, InputT> builder) {
        super(builder);
    }

    /**
     * Creates a new {@link FlussSinkBuilder} instance for building a FlussSink.
     *
     * @param <T> the type of input elements
     * @return a new FlussSinkBuilder instance
     */
    public static <T> FlussSinkBuilder<T> builder() {
        return new FlussSinkBuilder<>();
    }
}
