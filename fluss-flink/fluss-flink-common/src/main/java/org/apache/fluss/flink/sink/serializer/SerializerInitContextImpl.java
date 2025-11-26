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

import org.apache.fluss.types.RowType;

import static org.apache.fluss.flink.utils.FlinkConversions.toFlinkRowType;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Contextual information that can be used during initialization of {@link
 * FlussSerializationSchema}.
 */
public class SerializerInitContextImpl implements FlussSerializationSchema.InitializationContext {

    private final RowType flussRowSchema;
    private final org.apache.flink.table.types.logical.RowType flinkRowType;

    public SerializerInitContextImpl(RowType rowSchema) {
        this(rowSchema, toFlinkRowType(rowSchema));
    }

    public SerializerInitContextImpl(
            RowType rowSchema, org.apache.flink.table.types.logical.RowType flinkRowType) {
        this.flussRowSchema = checkNotNull(rowSchema, "flussRowSchema");
        this.flinkRowType = checkNotNull(flinkRowType, "flinkRowType");
    }

    @Override
    public RowType getRowSchema() {
        return flussRowSchema;
    }

    @Override
    public org.apache.flink.table.types.logical.RowType getTableRowType() {
        return flinkRowType;
    }
}
