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

package org.apache.fluss.flink.sink.serializer;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.flink.row.RowWithOp;
import org.apache.fluss.types.RowType;

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
     * Serializes an object of type T into a {@link RowWithOp}.
     *
     * @param value The object to serialize.
     * @return The serialized RowWithOp.
     * @throws Exception If the serialization fails.
     */
    RowWithOp serialize(T value) throws Exception;

    /**
     * A contextual information provided for {@link #open(InitializationContext)} method. It can be
     * used to:
     *
     * <ul>
     *   <li>Access the target row schema.
     * </ul>
     */
    @PublicEvolving
    interface InitializationContext {
        /**
         * Returns the Fluss physical row schema.
         *
         * @return The schema of the target row.
         */
        RowType getRowSchema();

        /**
         * Returns the Flink table row schema.
         *
         * @return The schema of the Flink table row.
         */
        org.apache.flink.table.types.logical.RowType getTableRowType();
    }
}
