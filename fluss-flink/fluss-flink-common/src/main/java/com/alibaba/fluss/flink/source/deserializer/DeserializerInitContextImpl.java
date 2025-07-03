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

package com.alibaba.fluss.flink.source.deserializer;

import com.alibaba.fluss.types.RowType;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

/**
 * Contextual information that can be used during initialization of {@link
 * FlussDeserializationSchema}.
 */
public class DeserializerInitContextImpl
        implements FlussDeserializationSchema.InitializationContext {

    private final MetricGroup metricGroup;
    private final UserCodeClassLoader userCodeClassLoader;
    private final RowType rowSchema;

    public DeserializerInitContextImpl(
            MetricGroup metricGroup, UserCodeClassLoader userCodeClassLoader, RowType rowSchema) {
        this.metricGroup = metricGroup;
        this.userCodeClassLoader = userCodeClassLoader;
        this.rowSchema = rowSchema;
    }

    @Override
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return userCodeClassLoader;
    }

    @Override
    public RowType getRowSchema() {
        return rowSchema;
    }
}
