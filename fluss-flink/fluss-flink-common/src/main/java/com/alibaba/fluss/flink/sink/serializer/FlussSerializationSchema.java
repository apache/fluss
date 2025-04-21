/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.sink.serializer;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.UserCodeClassLoader;

import java.io.Serializable;

/**
 * A serialization schema for Fluss.
 *
 * @param <T> The type to be serialized.
 */
@PublicEvolving
public interface FlussSerializationSchema<T> extends Serializable {

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #serialize} and thus suitable for one time setup work.
     *
     * <p>The provided {@link InitializationContext} can be used to access additional features such
     * as e.g. registering user metrics, accessing row schema.
     *
     * @param context Contextual information that can be used during initialization.
     */
    void open(InitializationContext context) throws Exception;

    /**
     * Serializes an object of type T into a {@link RowData}.
     *
     * @param value The object to serialize.
     * @return The serialized RowData.
     * @throws Exception If the serialization fails.
     */
    RowData serialize(T value) throws Exception;

    /**
     * A contextual information provided for {@link #open(InitializationContext)} method. It can be
     * used to:
     *
     * <ul>
     *   <li>Register user metrics via {@link InitializationContext#getMetricGroup()}
     *   <li>Access the user code class loader.
     *   <li>Access the target row schema.
     * </ul>
     */
    @PublicEvolving
    interface InitializationContext {
        /**
         * Returns the metric group for the parallel subtask of the sink that runs this {@link
         * FlussSerializationSchema}.
         *
         * <p>Instances of this class can be used to register new metrics with Flink and to create a
         * nested hierarchy based on the group names. See {@link MetricGroup} for more information
         * for the metrics system.
         *
         * @see MetricGroup
         */
        MetricGroup getMetricGroup();

        /**
         * Gets the {@link UserCodeClassLoader} to load classes that are not in system's classpath,
         * but are part of the jar file of a user job.
         *
         * @see UserCodeClassLoader
         */
        UserCodeClassLoader getUserCodeClassLoader();

        /**
         * Returns the target row schema.
         *
         * @return The schema of the target row.
         */
        RowType getRowSchema();
    }
}
